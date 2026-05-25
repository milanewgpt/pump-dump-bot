"""Core scanner: polls BingX, detects pumps, fires Telegram signals."""
import asyncio
import logging
from typing import Optional

import aiohttp

from bingx_api import BingXAPI
from indicators import calculate_rsi
from formatter import format_pump_signal
from tracker import SignalTracker

logger = logging.getLogger(__name__)

CONCURRENCY = 20   # parallel BingX requests during full scan


class PumpScanner:
    def __init__(
        self,
        telegram_token: str,
        chat_id: str,
        min_pump_pct: float = 11.0,
        scan_interval: int = 60,
    ):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        self.min_pump_pct = min_pump_pct
        self.scan_interval = scan_interval
        self.tracker = SignalTracker()

    # ------------------------------------------------------------------ #
    #  Main loop
    # ------------------------------------------------------------------ #

    async def run(self):
        logger.info(
            f"PumpScanner started | threshold={self.min_pump_pct}% | "
            f"interval={self.scan_interval}s"
        )
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.api = BingXAPI(session)
            while True:
                try:
                    await self._scan(session)
                except Exception as e:
                    logger.error(f"Scan cycle error: {e}", exc_info=True)
                await asyncio.sleep(self.scan_interval)

    # ------------------------------------------------------------------ #
    #  Scan cycle
    # ------------------------------------------------------------------ #

    async def _scan(self, session: aiohttp.ClientSession):
        symbols = await self.api.get_all_symbols()
        if not symbols:
            logger.warning("Got 0 symbols from BingX — skipping cycle")
            return

        logger.info(f"Scanning {len(symbols)} symbols…")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def check(sym):
            async with sem:
                return await self._check_symbol(sym)

        results = await asyncio.gather(*[check(s) for s in symbols], return_exceptions=True)
        sent = sum(1 for r in results if r is True)
        if sent:
            logger.info(f"✅ Sent {sent} signal(s) this cycle")

    # ------------------------------------------------------------------ #
    #  Per-symbol check
    # ------------------------------------------------------------------ #

    async def _check_symbol(self, symbol: str) -> bool:
        # --- 1. Get last two 30m candles (index -1 = current/live) ---
        klines = await self.api.get_klines(symbol, "30m", limit=2)
        if not klines:
            return False

        candle = klines[-1]
        try:
            open_p = float(candle["open"])
            close_p = float(candle["close"])
            candle_time = int(candle["time"])
        except (KeyError, ValueError, TypeError):
            return False

        if open_p == 0:
            return False

        pct = (close_p - open_p) / open_p * 100

        if pct < self.min_pump_pct:
            return False

        # --- 2. Dedup: one signal per (symbol, candle_open_time) ---
        if self.tracker.is_duplicate(symbol, candle_time):
            return False

        # --- 3. Enrich & send ---
        logger.info(f"🔥 Pump detected: {symbol} +{pct:.2f}%")
        await self._send_signal(symbol, pct, open_p, close_p, candle_time)
        return True

    # ------------------------------------------------------------------ #
    #  Enrich & format
    # ------------------------------------------------------------------ #

    async def _send_signal(
        self,
        symbol: str,
        pct: float,
        open_p: float,
        close_p: float,
        candle_time: int,
    ):
        # Fetch enrichment data concurrently
        rsi_1h, rsi_4h, rsi_1d, funding, ath_x = await asyncio.gather(
            self._get_rsi(symbol, "1h"),
            self._get_rsi(symbol, "4h"),
            self._get_rsi(symbol, "1d"),
            self.api.get_funding_rate(symbol),
            self._get_ath_x(symbol, close_p),
            return_exceptions=True,
        )

        # Replace exceptions with None / default
        rsi_1h = rsi_1h if isinstance(rsi_1h, float) else None
        rsi_4h = rsi_4h if isinstance(rsi_4h, float) else None
        rsi_1d = rsi_1d if isinstance(rsi_1d, float) else None
        funding = funding if isinstance(funding, float) else None
        ath_x = ath_x if isinstance(ath_x, float) else 0.0

        daily_count = self.tracker.mark_sent(symbol, candle_time)

        msg = format_pump_signal(
            symbol=symbol,
            pct=pct,
            open_price=open_p,
            close_price=close_p,
            rsi_1h=rsi_1h,
            rsi_4h=rsi_4h,
            rsi_1d=rsi_1d,
            funding=funding,
            signal_per_day=daily_count,
            ath_x=ath_x,
        )

        await self._send_telegram(msg)

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    async def _get_rsi(self, symbol: str, interval: str) -> Optional[float]:
        klines = await self.api.get_klines(symbol, interval, limit=30)
        if not klines:
            return None
        try:
            closes = [float(k["close"]) for k in klines]
            return calculate_rsi(closes)
        except Exception:
            return None

    async def _get_ath_x(self, symbol: str, current_price: float) -> float:
        """Return ath / current_price (approximate ATH from last 200 daily candles)."""
        klines = await self.api.get_klines(symbol, "1d", limit=200)
        if not klines or current_price == 0:
            return 0.0
        try:
            ath = max(float(k["high"]) for k in klines)
            return ath / current_price
        except Exception:
            return 0.0

    async def _send_telegram(self, text: str):
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"Telegram error {resp.status}: {body}")
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
