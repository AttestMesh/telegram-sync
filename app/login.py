"""One-time interactive Telegram login to create the session file.

Run inside the container with a TTY:
    docker compose run --rm sync python -m app.login
"""

import asyncio

from telethon import TelegramClient

from . import config


async def main() -> None:
    client = TelegramClient(config.SESSION_FILE, config.API_ID, config.API_HASH)
    # Telethon's start() handles phone -> code -> 2FA password interactively
    await client.start()
    me = await client.get_me()
    print(f"Logged in as {me.first_name} (id={me.id}). Session saved to {config.SESSION_FILE}")
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
