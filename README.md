# telegram-sync

Standalone Telegram → Postgres sync daemon, extracted from the `telegram-mcp`
bridge (`~/telegram-mcp/telegram-bridge`). It keeps only the syncing
functionality — no REST API, no MCP server — and stores everything in Postgres.

## Improvements over the original bridge sync

| Original bridge | This service |
|---|---|
| SQLite, 32-bit `Integer` IDs (overflow on Telegram IDs in Postgres) | Postgres, `BIGINT` IDs, `TIMESTAMPTZ` timestamps |
| Re-fetches a fixed 100 messages per dialog on every pass | Incremental: per-chat `last_message_id` watermark (`sync_state` table), only new messages fetched |
| Blocking SQLAlchemy calls inside the event loop, one commit per message | Fully async `asyncpg`, batched `ON CONFLICT` upserts in one transaction per dialog |
| No flood-wait handling | Catches `FloodWaitError` and backs off |
| New messages only | Also captures message edits (`edited_at` column) |
| No search index that Postgres can use | GIN full-text index on message content |
| No health reporting | `GET :8082/health` returns connection state, row counts, last reconcile time |
| No graceful shutdown | Handles SIGTERM cleanly (Docker-friendly) |

Kept from the original: the periodic reconcile loop that self-heals messages
dropped by Telethon's live event stream (now cheap, since passes are
incremental).

## Image understanding (optional)

Set `XAI_API_KEY` and the service also "reads" images sent over Telegram using
xAI's vision API (default model `grok-4.3`, OpenAI-compatible chat completions).
Photos and jpeg/png documents are described and any visible text transcribed;
results land in the `media` table with a GIN full-text index, so screenshots
become searchable alongside regular messages.

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
- The container runs as a non-root user.

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

### Reusing the existing bridge session

Instead of logging in again you can copy the already-authorized session from
the running bridge — but **stop the old bridge first** if you want only one
client using that session:

```bash
docker compose up -d postgres   # creates volumes
docker compose cp ~/telegram-mcp/telegram-bridge/store/telegram_session.session \
    sync:/data/telegram_session.session   # or: docker run --rm -v telegram-sync_session:/data ...
```

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

## Configuration

All via environment (see `.env.example`): `DATABASE_URL`, `DIALOG_LIMIT`,
`INITIAL_SYNC_LIMIT`, `INCREMENTAL_FETCH_CAP`, `RECONCILE_INTERVAL`,
`RECONCILE_DIALOG_LIMIT`, `HEALTH_PORT`, `LOG_LEVEL`.
