"""Image understanding via xAI's OpenAI-compatible chat completions API.

Security posture: image bytes are downloaded to memory, sent to xAI over TLS,
and discarded — only sha256 + the model's description are persisted. The API
key comes from the environment and is never logged.
"""

import asyncio
import base64
import hashlib
import logging
from typing import Any, Optional, Tuple

import httpx

from .db import Store

logger = logging.getLogger(__name__)

PROMPT = (
    "Describe this image concisely for a searchable message archive. "
    "Transcribe any visible text verbatim. Plain text only."
)

MAX_ATTEMPTS = 3


def image_meta(message: Any) -> Optional[Tuple[str, Optional[int]]]:
    """Return (mime_type, size_bytes) if the message carries a supported image.

    Telegram photos are always jpeg; documents are accepted only with an
    explicit jpeg/png mime type (xAI supports jpg/png).
    """
    if message.photo:
        size = message.file.size if message.file else None
        return ("image/jpeg", size)
    if message.document and message.document.mime_type in ("image/jpeg", "image/png"):
        return (message.document.mime_type, message.document.size)
    return None


class XaiVisionClient:
    def __init__(self, api_key: str, model: str, base_url: str = "https://api.x.ai/v1"):
        self.model = model
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(90.0, connect=15.0),
        )

    async def describe(self, image_bytes: bytes, mime_type: str) -> str:
        b64 = base64.b64encode(image_bytes).decode()
        resp = await self._http.post(
            "/chat/completions",
            json={
                "model": self.model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:{mime_type};base64,{b64}"},
                            },
                            {"type": "text", "text": PROMPT},
                        ],
                    }
                ],
            },
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

    async def close(self) -> None:
        await self._http.aclose()


class VisionWorker:
    """Sequential worker that turns queued Telegram images into descriptions.

    Items are durably queued in the media table; the in-memory queue is just a
    wake-up signal carrying the message object when we already have it (live
    sync). On restart, pending rows are re-enqueued and the message re-fetched.
    """

    def __init__(
        self,
        client: Any,  # TelegramClient
        store: Store,
        xai: XaiVisionClient,
        max_image_bytes: int,
        min_interval: float = 1.0,
    ):
        self.client = client
        self.store = store
        self.xai = xai
        self.max_image_bytes = max_image_bytes
        self.min_interval = min_interval
        self.queue: asyncio.Queue = asyncio.Queue()

    def enqueue(self, chat_id: int, message_id: int, message: Any = None) -> None:
        self.queue.put_nowait((chat_id, message_id, message))

    async def requeue_pending(self) -> int:
        rows = await self.store.get_pending_media(max_attempts=MAX_ATTEMPTS)
        for row in rows:
            self.enqueue(row["chat_id"], row["message_id"])
        if rows:
            logger.info("Re-queued %d unprocessed images", len(rows))
        return len(rows)

    async def _fetch_message(self, chat_id: int, message_id: int) -> Optional[Any]:
        entity = await self.client.get_entity(chat_id)
        msgs = await self.client.get_messages(entity, ids=[message_id])
        return msgs[0] if msgs else None

    async def _process(self, chat_id: int, message_id: int, message: Any) -> None:
        if message is None:
            message = await self._fetch_message(chat_id, message_id)
        if message is None:
            await self.store.finish_media(chat_id, message_id, "skipped", error="message gone")
            return

        meta = image_meta(message)
        if not meta:
            await self.store.finish_media(chat_id, message_id, "skipped", error="unsupported type")
            return
        mime_type, size = meta
        if size and size > self.max_image_bytes:
            await self.store.finish_media(
                chat_id, message_id, "skipped", error=f"too large ({size} bytes)"
            )
            return

        data = await message.download_media(file=bytes)
        if not data or len(data) > self.max_image_bytes:
            await self.store.finish_media(
                chat_id, message_id, "skipped", error="empty or oversized download"
            )
            return

        sha256 = hashlib.sha256(data).hexdigest()
        description = await self.xai.describe(data, mime_type)
        del data  # image bytes are never persisted

        await self.store.finish_media(
            chat_id,
            message_id,
            "done",
            description=description,
            sha256=sha256,
            model=self.xai.model,
            mime_type=mime_type,
            size_bytes=size,
        )
        logger.info("Described image %d in chat %d: %.60s", message_id, chat_id, description)

    async def run(self) -> None:
        logger.info("Vision worker started (model=%s)", self.xai.model)
        while True:
            chat_id, message_id, message = await self.queue.get()
            try:
                attempts = await self.store.bump_media_attempts(chat_id, message_id)
                if attempts is None or attempts > MAX_ATTEMPTS:
                    continue
                await self._process(chat_id, message_id, message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Log the error class/message only — never request payloads
                logger.warning(
                    "Vision failed for msg %d in chat %d: %s: %s",
                    message_id, chat_id, type(e).__name__, e,
                )
                await self.store.finish_media(
                    chat_id, message_id, "failed", error=f"{type(e).__name__}: {e}"[:500]
                )
            await asyncio.sleep(self.min_interval)
