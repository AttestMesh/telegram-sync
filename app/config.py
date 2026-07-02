"""Configuration for the Telegram sync service, loaded from environment variables."""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

# Telegram API credentials (https://my.telegram.org/auth)
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

# Telethon session file (mounted volume in Docker so auth survives restarts)
SESSION_DIR = os.getenv("SESSION_DIR", "/data")
SESSION_FILE = os.path.join(SESSION_DIR, "telegram_session")

# Postgres connection
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://telegram:telegram@localhost:5432/telegram"
)

# Sync tuning
DIALOG_LIMIT = int(os.getenv("DIALOG_LIMIT", "200"))
# Messages fetched for a chat never seen before
INITIAL_SYNC_LIMIT = int(os.getenv("INITIAL_SYNC_LIMIT", "500"))
# Cap on messages fetched per chat in one incremental pass
INCREMENTAL_FETCH_CAP = int(os.getenv("INCREMENTAL_FETCH_CAP", "2000"))
# Seconds between reconcile passes (self-heal for dropped live updates)
RECONCILE_INTERVAL = int(os.getenv("RECONCILE_INTERVAL", "60"))
# Dialogs covered by each reconcile pass (most recently active first)
RECONCILE_DIALOG_LIMIT = int(os.getenv("RECONCILE_DIALOG_LIMIT", "30"))

# Health endpoint
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8082"))

# Image understanding via xAI (optional — disabled when no API key is set).
# Images are processed in memory and discarded; only hash + description stored.
XAI_API_KEY = os.getenv("XAI_API_KEY")
XAI_MODEL = os.getenv("XAI_MODEL", "grok-4.3")
XAI_BASE_URL = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1")
# Reject images larger than this before download (xAI caps at 20 MiB)
MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(10 * 1024 * 1024)))
# Minimum seconds between xAI calls (simple rate limit)
VISION_MIN_INTERVAL = float(os.getenv("VISION_MIN_INTERVAL", "1.0"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

if not API_ID or not API_HASH:
    raise ValueError(
        "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set "
        "(get them from https://my.telegram.org/auth)"
    )
