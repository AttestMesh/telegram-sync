"""Telegram → Postgres sync daemon.

Runs headless: expects an already-authorized Telethon session file (create one
with `python -m app.login`, or copy an existing session into the volume).
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

from telethon import TelegramClient

from . import config
from .db import Store
from .health import start_health_server
from .sync import SyncEngine
from .vision import VisionWorker, XaiVisionClient

logger = logging.getLogger(__name__)


async def wait_for_db(url: str, attempts: int = 30, delay: float = 2.0) -> Store:
    """Retry DB connection so the container survives Postgres starting up."""
    for i in range(attempts):
        try:
            return await Store.create(url)
        except Exception as e:
            if i == attempts - 1:
                raise
            logger.info("Waiting for Postgres (%s)...", e)
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")


async def run() -> None:
    store = await wait_for_db(config.DATABASE_URL)

    client = TelegramClient(config.SESSION_FILE, config.API_ID, config.API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        logger.error(
            "Telegram session is not authorized. Run the interactive login first:\n"
            "  docker compose run --rm sync python -m app.login\n"
            "or copy an existing telegram_session file into the session volume."
        )
        await store.close()
        sys.exit(2)

    engine = SyncEngine(
        client,
        store,
        initial_sync_limit=config.INITIAL_SYNC_LIMIT,
        incremental_fetch_cap=config.INCREMENTAL_FETCH_CAP,
    )
    await engine.start()

    xai = None
    vision_task = None
    if config.XAI_API_KEY:
        xai = XaiVisionClient(config.XAI_API_KEY, config.XAI_MODEL, config.XAI_BASE_URL)
        worker = VisionWorker(
            client,
            store,
            xai,
            max_image_bytes=config.MAX_IMAGE_BYTES,
            min_interval=config.VISION_MIN_INTERVAL,
        )
        engine.vision = worker
        await worker.requeue_pending()
        vision_task = asyncio.create_task(worker.run())
    else:
        logger.info("XAI_API_KEY not set — image understanding disabled")

    async def status():
        stats = await store.stats()
        return {
            "status": "ok",
            "connected": client.is_connected(),
            "last_reconcile_at": (
                engine.last_reconcile_at.isoformat() if engine.last_reconcile_at else None
            ),
            "now": datetime.now(timezone.utc).isoformat(),
            **stats,
        }

    health_server = await start_health_server(config.HEALTH_PORT, status)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await engine.sync_all(dialog_limit=config.DIALOG_LIMIT)

    reconcile = asyncio.create_task(
        engine.run_periodic_reconcile(
            interval=config.RECONCILE_INTERVAL,
            dialog_limit=config.RECONCILE_DIALOG_LIMIT,
        )
    )

    logger.info("Sync daemon running; live updates + reconcile active")
    await stop.wait()

    logger.info("Shutting down")
    reconcile.cancel()
    if vision_task:
        vision_task.cancel()
    if xai:
        await xai.close()
    health_server.close()
    await client.disconnect()
    await store.close()


if __name__ == "__main__":
    asyncio.run(run())
