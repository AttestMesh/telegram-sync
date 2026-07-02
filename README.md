# telegram-sync

A headless daemon that syncs your Telegram account into Postgres — every chat
and message, continuously, with optional AI-generated descriptions of images.
Point it at your account once and you get a queryable, full-text-searchable
archive that stays current.

## What it does

- **Full history sync** on startup: walks your dialogs and backfills message
  history into Postgres (depth configurable per never-seen chat).
- **Live updates**: new and edited messages are captured in real time via
  Telethon's event stream.
- **Self-healing**: a periodic reconcile pass re-checks recently active chats,
  so anything the live stream drops is picked up within a minute.
- **Incremental**: each chat has a `last_message_id` watermark in `sync_state`,
  so sync passes fetch only what's new — cheap enough to run every 60 seconds.
- **Fast writes**: fully async (`asyncpg`), batched `ON CONFLICT` upserts in a
  single transaction per chat, flood-wait aware.
- **Searchable**: GIN full-text indexes on message content and image
  descriptions.
- **Observable**: `GET :8082/health` reports connection state, row counts,
  media queue status, and last reconcile time; the container handles SIGTERM
  cleanly and runs as a non-root user.

## Image understanding (optional)

Set `XAI_API_KEY` and the daemon also "reads" images sent over Telegram using
xAI's vision API (default model `grok-4.3`). Photos and jpeg/png documents are
described and any visible text transcribed; results land in the `media` table
with a full-text index, so screenshots become searchable alongside regular
messages.

Security posture:

- Image bytes are downloaded to memory, sent to xAI over TLS, and discarded —
  only the SHA-256 hash, mime type, size, and generated description persist.
- The API key is read from the environment and never logged; error logging
  captures the exception only, not request payloads.
- Only Telegram photos and explicit `image/jpeg` / `image/png` documents are
  processed, capped at `MAX_IMAGE_BYTES` (default 10 MiB) *before* download.
- Work is durably queued in Postgres (`media.status`): unprocessed images
  survive restarts, retries are capped at 3 attempts, and calls are rate-limited
  (`VISION_MIN_INTERVAL`, default 1/s).

Privacy note: enabling this sends every incoming image (from every synced chat)
to xAI. Leave `XAI_API_KEY` unset to keep the service text-only.

## Quick start

```bash
cp .env.example .env       # fill in TELEGRAM_API_ID / TELEGRAM_API_HASH
docker compose build
docker compose up -d postgres

# One-time interactive login (creates the session file in the `session` volume)
docker compose run --rm sync python -m app.login

docker compose up -d sync
curl -s localhost:8082/health | jq
```

API credentials come from https://my.telegram.org/auth. If you already have an
authorized Telethon session file, you can copy it into the `session` volume as
`/data/telegram_session.session` instead of logging in — but make sure no other
client is actively using that session.

## Querying

Postgres is exposed on `127.0.0.1:5434`:

```bash
psql postgresql://telegram:$POSTGRES_PASSWORD@localhost:5434/telegram

-- full-text search
SELECT timestamp, sender_name, content FROM messages
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'deploy')
ORDER BY timestamp DESC LIMIT 20;

-- search inside images (screenshots, photos of documents, ...)
SELECT m.timestamp, m.sender_name, md.description
FROM media md JOIN messages m ON (m.chat_id, m.id) = (md.chat_id, md.message_id)
WHERE md.status = 'done'
  AND to_tsvector('simple', md.description) @@ plainto_tsquery('simple', 'invoice')
ORDER BY m.timestamp DESC LIMIT 20;
```

## Schema

- `chats(id, title, username, type, last_message_time)`
- `messages(chat_id, id, sender_id, sender_name, content, timestamp, is_from_me, edited_at)` — PK `(chat_id, id)`
- `sync_state(chat_id, last_message_id, last_synced_at, initial_sync_done)` — incremental watermarks
- `media(chat_id, message_id, mime_type, size_bytes, sha256, status, description, model, attempts, error, ...)` — image descriptions + durable processing queue

IDs are `BIGINT` (Telegram IDs exceed 32 bits) and timestamps are `TIMESTAMPTZ`.
Only text-bearing messages are stored; image messages are stored with their
caption (or empty content) when image understanding is enabled.

## Configuration

All via environment (see `.env.example`): `DATABASE_URL`, `DIALOG_LIMIT`,
`INITIAL_SYNC_LIMIT`, `INCREMENTAL_FETCH_CAP`, `RECONCILE_INTERVAL`,
`RECONCILE_DIALOG_LIMIT`, `HEALTH_PORT`, `LOG_LEVEL`, `XAI_API_KEY`,
`XAI_MODEL`, `XAI_BASE_URL`, `MAX_IMAGE_BYTES`, `VISION_MIN_INTERVAL`.
