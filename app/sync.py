"""Core sync engine: incremental history sync + live updates + periodic reconcile.

Extracted from the telegram-mcp bridge's TelegramService and improved:
- incremental sync via a per-chat last_message_id watermark instead of
  re-fetching a fixed window every pass
- batched upserts instead of one commit per message
- FloodWaitError handling
- edited-message capture (events.MessageEdited)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, User
from telethon.utils import get_display_name

from .db import Store
from .vision import image_meta

logger = logging.getLogger(__name__)


def _chat_info(entity: Any) -> Optional[Dict[str, Any]]:
    """Normalize a Telegram entity (User/Chat/Channel) into a chat row."""
    if isinstance(entity, User):
        return {
            "id": entity.id,
            "title": get_display_name(entity),
            "username": entity.username,
            "type": "user",
        }
    if isinstance(entity, Chat):
        return {"id": entity.id, "title": entity.title, "username": None, "type": "group"}
    if isinstance(entity, Channel):
        return {
            "id": entity.id,
            "title": entity.title,
            "username": entity.username,
            "type": "channel" if entity.broadcast else "supergroup",
        }
    return None


class SyncEngine:
    """Syncs Telegram chats and messages into Postgres."""

    def __init__(
        self,
        client: TelegramClient,
        store: Store,
        initial_sync_limit: int = 500,
        incremental_fetch_cap: int = 2000,
    ):
        self.client = client
        self.store = store
        self.initial_sync_limit = initial_sync_limit
        self.incremental_fetch_cap = incremental_fetch_cap
        self._me_id: Optional[int] = None
        self.last_reconcile_at: Optional[datetime] = None
        # Optional VisionWorker; when set, image messages are queued for description
        self.vision = None

    async def start(self) -> None:
        self._me_id = (await self.client.get_me()).id
        self.client.add_event_handler(self._on_new_message, events.NewMessage)
        self.client.add_event_handler(self._on_edited_message, events.MessageEdited)

    def _msg_row(self, message: Any, chat_id: int, edited: bool = False) -> Optional[Dict[str, Any]]:
        has_image = self.vision is not None and image_meta(message) is not None
        if not message.text and not has_image:
            return None  # text and (when vision is on) image messages only
        sender_id = message.sender_id or 0
        return {
            "id": message.id,
            "chat_id": chat_id,
            "sender_id": sender_id,
            "sender_name": None,  # filled by caller when the sender is resolved
            "content": message.text or "",  # caption, or empty for bare images
            "timestamp": message.date,
            "is_from_me": sender_id == self._me_id,
            "edited_at": message.edit_date if edited else None,
        }

    async def _queue_images(self, messages: List[Any], chat_id: int) -> None:
        """Durably queue supported images for the vision worker (if enabled)."""
        if self.vision is None:
            return
        for message in messages:
            if image_meta(message) is None:
                continue
            if await self.store.enqueue_media(chat_id, message.id):
                self.vision.enqueue(chat_id, message.id, message)

    async def _resolve_sender_names(self, messages: List[Any], rows: List[Dict[str, Any]]) -> None:
        """Fill sender_name on rows, caching per unique sender within the batch."""
        cache: Dict[int, str] = {}
        by_id = {r["id"]: r for r in rows}
        for message in messages:
            row = by_id.get(message.id)
            if row is None:
                continue
            sid = row["sender_id"]
            if sid not in cache:
                try:
                    sender = await message.get_sender()
                    cache[sid] = get_display_name(sender) if sender else "Unknown"
                except Exception:
                    cache[sid] = "Unknown"
            row["sender_name"] = cache[sid]

    async def sync_dialog(self, dialog: Any) -> int:
        """Incrementally sync one dialog. Returns the number of messages stored."""
        info = _chat_info(dialog.entity)
        if not info:
            return 0
        info["last_message_time"] = dialog.date
        await self.store.upsert_chat(info)

        state = await self.store.get_sync_state(info["id"])
        if state and state["initial_sync_done"]:
            # Only fetch what's newer than the watermark
            messages = await self.client.get_messages(
                dialog.entity,
                limit=self.incremental_fetch_cap,
                min_id=state["last_message_id"],
            )
        else:
            messages = await self.client.get_messages(
                dialog.entity, limit=self.initial_sync_limit
            )

        rows = [r for m in messages if (r := self._msg_row(m, info["id"]))]
        await self._resolve_sender_names(messages, rows)
        await self.store.upsert_messages(rows)
        stored_ids = {r["id"] for r in rows}
        await self._queue_images([m for m in messages if m.id in stored_ids], info["id"])

        max_id = max((m.id for m in messages), default=state["last_message_id"] if state else 0)
        await self.store.set_sync_state(info["id"], max_id)

        if rows:
            logger.info("Synced %d messages from %s", len(rows), info["title"])
        return len(rows)

    async def sync_all(self, dialog_limit: int = 200) -> None:
        """Sync every dialog, honoring Telegram flood-wait backoff."""
        logger.info("Starting sync of up to %d dialogs", dialog_limit)
        dialogs = await self.client.get_dialogs(limit=dialog_limit)
        total = 0
        for dialog in dialogs:
            try:
                total += await self.sync_dialog(dialog)
            except FloodWaitError as e:
                wait = min(e.seconds, 300)
                logger.warning("Flood wait: sleeping %ds", wait)
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error("Error syncing dialog %s: %s", getattr(dialog, "name", "?"), e)
        logger.info("Sync complete: %d dialogs, %d new/updated messages", len(dialogs), total)

    async def run_periodic_reconcile(self, interval: int, dialog_limit: int) -> None:
        """Self-heal loop: Telethon's live event stream can silently drop
        messages after long uptime, so periodically re-check recent dialogs.
        Incremental watermarks make each pass cheap."""
        logger.info("Periodic reconcile every %ds over %d dialogs", interval, dialog_limit)
        while True:
            await asyncio.sleep(interval)
            try:
                dialogs = await self.client.get_dialogs(limit=dialog_limit)
                for dialog in dialogs:
                    try:
                        await self.sync_dialog(dialog)
                    except FloodWaitError as e:
                        await asyncio.sleep(min(e.seconds, 300))
                    except Exception as e:
                        logger.warning("Reconcile failed for %s: %s", getattr(dialog, "name", "?"), e)
                self.last_reconcile_at = datetime.now(timezone.utc)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Reconcile pass error: %s", e)

    async def _store_event_message(self, event: Any, edited: bool = False) -> None:
        message = event.message
        chat = await event.get_chat()
        info = _chat_info(chat)
        if not info:
            return
        info["last_message_time"] = message.date
        await self.store.upsert_chat(info)

        row = self._msg_row(message, info["id"], edited=edited)
        if not row:
            return
        try:
            sender = await message.get_sender()
            row["sender_name"] = get_display_name(sender) if sender else "Unknown"
        except Exception:
            row["sender_name"] = "Unknown"
        await self.store.upsert_messages([row])
        if not edited:
            await self._queue_images([message], info["id"])
        # Watermarks are managed only by sync passes: bumping here would mark
        # initial_sync_done on chats whose history was never backfilled.
        logger.info(
            "%s message in %s from %s: %.40s",
            "Edited" if edited else "New",
            info["title"],
            row["sender_name"],
            row["content"],
        )

    async def _on_new_message(self, event: Any) -> None:
        try:
            await self._store_event_message(event)
        except Exception as e:
            logger.error("Failed to store new message: %s", e)

    async def _on_edited_message(self, event: Any) -> None:
        try:
            await self._store_event_message(event, edited=True)
        except Exception as e:
            logger.error("Failed to store edited message: %s", e)
