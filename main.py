"""Entry point for pump-dump-bot."""
import asyncio
import json
import logging
import os

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

import aiohttp
from aiohttp import web
from scanner import PumpScanner

_scanner: PumpScanner | None = None


async def handle_stats(request: web.Request) -> web.Response:
    if _scanner is None:
        return web.Response(status=503, text="not ready")
    stats = _scanner.tracker.get_verdict_stats()
    return web.Response(
        content_type="application/json",
        text=json.dumps(stats),
    )


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def run_http_server():
    port = int(os.getenv("PORT", "8080"))
    app = web.Application()
    app.router.add_get("/stats", handle_stats)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.getLogger(__name__).info(f"Stats server listening on port {port}")


async def main():
    global _scanner
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["CHAT_ID"]
    min_pump = float(os.getenv("MIN_PUMP_PCT", "11.0"))
    interval = int(os.getenv("SCAN_INTERVAL", "30"))
    min_volume = float(os.getenv("MIN_VOLUME_USDT", "1000000"))

    _scanner = PumpScanner(
        telegram_token=token,
        chat_id=chat_id,
        min_pump_pct=min_pump,
        scan_interval=interval,
        min_volume_usdt=min_volume,
    )
    await asyncio.gather(run_http_server(), _scanner.run())


if __name__ == "__main__":
    asyncio.run(main())
