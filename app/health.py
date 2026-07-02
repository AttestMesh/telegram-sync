"""Minimal HTTP health endpoint (no framework — the service is a daemon, not an API)."""

import asyncio
import json
import logging
from typing import Awaitable, Callable, Dict

logger = logging.getLogger(__name__)


async def start_health_server(
    port: int, status_fn: Callable[[], Awaitable[Dict]]
) -> asyncio.AbstractServer:
    """Serve GET /health returning JSON from status_fn. Any other path gets 404."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5)
            path = request_line.split()[1].decode() if len(request_line.split()) > 1 else "/"
            # Drain headers
            while await asyncio.wait_for(reader.readline(), timeout=5) not in (b"\r\n", b""):
                pass
            if path == "/health":
                try:
                    body = json.dumps(await status_fn()).encode()
                    status = "200 OK"
                except Exception as e:
                    body = json.dumps({"status": "error", "error": str(e)}).encode()
                    status = "503 Service Unavailable"
            else:
                body = b'{"error": "not found"}'
                status = "404 Not Found"
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
            )
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    logger.info("Health endpoint on :%d/health", port)
    return server
