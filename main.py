"""Entry point for pump-dump-bot."""
import asyncio
import logging
import os

# Load .env only when running locally (not via systemd EnvironmentFile)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from scanner import PumpScanner


async def main():
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["CHAT_ID"]
    min_pump = float(os.getenv("MIN_PUMP_PCT", "11.0"))
    interval = int(os.getenv("SCAN_INTERVAL", "60"))

    scanner = PumpScanner(
        telegram_token=token,
        chat_id=chat_id,
        min_pump_pct=min_pump,
        scan_interval=interval,
    )
    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
