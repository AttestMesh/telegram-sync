"""Postgres storage layer.

Uses asyncpg directly with batched ON CONFLICT upserts. IDs are BIGINT
(Telegram IDs exceed 32 bits) and timestamps are timezone-aware.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    id                BIGINT PRIMARY KEY,
    title             TEXT NOT NULL,
    username          TEXT,
    type              TEXT NOT NULL,
    last_message_time TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS messages (
    id          BIGINT NOT NULL,
    chat_id     BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    sender_id   BIGINT,
    sender_name TEXT,
    content     TEXT NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    is_from_me  BOOLEAN NOT NULL DEFAULT FALSE,
    edited_at   TIMESTAMPTZ,
    PRIMARY KEY (chat_id, id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    chat_id              BIGINT PRIMARY KEY REFERENCES chats(id) ON DELETE CASCADE,
    last_message_id      BIGINT NOT NULL DEFAULT 0,
    last_synced_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    initial_sync_done    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS media (
    chat_id      BIGINT NOT NULL,
    message_id   BIGINT NOT NULL,
    mime_type    TEXT,
    size_bytes   BIGINT,
    sha256       TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending|done|failed|skipped
    description  TEXT,
    model        TEXT,
    attempts     INT NOT NULL DEFAULT 0,
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (chat_id, message_id),
    FOREIGN KEY (chat_id, message_id)
        REFERENCES messages(chat_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_status ON media (status);
CREATE INDEX IF NOT EXISTS idx_media_description_fts
    ON media USING gin (to_tsvector('simple', coalesce(description, '')));

CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender_id ON messages (sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_content_fts
    ON messages USING gin (to_tsvector('simple', content));
"""

UPSERT_CHAT = """
INSERT INTO chats (id, title, username, type, last_message_time)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET
    title = EXCLUDED.title,
    username = EXCLUDED.username,
    type = EXCLUDED.type,
    last_message_time = GREATEST(chats.last_message_time, EXCLUDED.last_message_time)
"""

UPSERT_MESSAGE = """
INSERT INTO messages (id, chat_id, sender_id, sender_name, content, timestamp, is_from_me, edited_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (chat_id, id) DO UPDATE SET
    content = EXCLUDED.content,
    sender_name = EXCLUDED.sender_name,
    edited_at = COALESCE(EXCLUDED.edited_at, messages.edited_at)
"""

UPSERT_SYNC_STATE = """
INSERT INTO sync_state (chat_id, last_message_id, last_synced_at, initial_sync_done)
VALUES ($1, $2, now(), TRUE)
ON CONFLICT (chat_id) DO UPDATE SET
    last_message_id = GREATEST(sync_state.last_message_id, EXCLUDED.last_message_id),
    last_synced_at = now(),
    initial_sync_done = TRUE
"""


class Store:
    """Async Postgres store for chats, messages, and sync watermarks."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def create(cls, database_url: str) -> "Store":
        pool = await asyncpg.create_pool(database_url, min_size=1, max_size=5)
        async with pool.acquire() as conn:
            await conn.execute(SCHEMA)
        logger.info("Database schema ready")
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def upsert_chat(self, chat: Dict[str, Any]) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                UPSERT_CHAT,
                chat["id"],
                chat["title"],
                chat.get("username"),
                chat["type"],
                chat.get("last_message_time"),
            )

    async def upsert_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Batch-upsert messages in a single transaction."""
        if not messages:
            return
        rows = [
            (
                m["id"],
                m["chat_id"],
                m["sender_id"],
                m["sender_name"],
                m["content"],
                m["timestamp"],
                m["is_from_me"],
                m.get("edited_at"),
            )
            for m in messages
        ]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(UPSERT_MESSAGE, rows)

    async def get_sync_state(self, chat_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT last_message_id, initial_sync_done FROM sync_state WHERE chat_id = $1",
                chat_id,
            )

    async def set_sync_state(self, chat_id: int, last_message_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(UPSERT_SYNC_STATE, chat_id, last_message_id)

    async def enqueue_media(self, chat_id: int, message_id: int) -> bool:
        """Insert a pending media row; returns True if it was newly queued."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "INSERT INTO media (chat_id, message_id) VALUES ($1, $2) "
                "ON CONFLICT (chat_id, message_id) DO NOTHING",
                chat_id,
                message_id,
            )
        return result.endswith("1")

    async def get_pending_media(self, max_attempts: int) -> List[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT chat_id, message_id FROM media "
                "WHERE status IN ('pending', 'failed') AND attempts < $1 "
                "ORDER BY created_at",
                max_attempts,
            )

    async def bump_media_attempts(self, chat_id: int, message_id: int) -> Optional[int]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "UPDATE media SET attempts = attempts + 1 "
                "WHERE chat_id = $1 AND message_id = $2 RETURNING attempts",
                chat_id,
                message_id,
            )

    async def finish_media(
        self,
        chat_id: int,
        message_id: int,
        status: str,
        description: Optional[str] = None,
        sha256: Optional[str] = None,
        model: Optional[str] = None,
        mime_type: Optional[str] = None,
        size_bytes: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE media SET status = $3, description = $4, sha256 = $5, "
                "model = $6, mime_type = COALESCE($7, mime_type), "
                "size_bytes = COALESCE($8, size_bytes), error = $9, processed_at = now() "
                "WHERE chat_id = $1 AND message_id = $2",
                chat_id,
                message_id,
                status,
                description,
                sha256,
                model,
                mime_type,
                size_bytes,
                error,
            )

    async def stats(self) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            chats = await conn.fetchval("SELECT count(*) FROM chats")
            messages = await conn.fetchval("SELECT count(*) FROM messages")
            latest = await conn.fetchval("SELECT max(timestamp) FROM messages")
            media = {
                r["status"]: r["n"]
                for r in await conn.fetch("SELECT status, count(*) n FROM media GROUP BY status")
            }
        return {
            "chats": chats,
            "messages": messages,
            "latest_message": latest.isoformat() if latest else None,
            "media": media,
        }
