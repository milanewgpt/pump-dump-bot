"""Core scanner: polls BingX, detects pumps, fires Telegram signals.

Architecture (rate-limit-safe):
  - Every SCAN_INTERVAL: fetch all tickers in ONE request (694 pairs).
  - Compare lastPrice vs cached 30m candle open.
  - Cache miss (new candle or first run): fetch klines lazily, in small batches.
  - Enrichment (RSI/funding/ATH): fetched only when a signal fires.
"""
import asyncio
import logging
import time
from typing import Optional

import aiohttp

from bingx_api import BingXAPI
from indicators import calculate_rsi
from formatter import format_pump_signal
from tracker import SignalTracker

logger = logging.getLogger(__name__)

CANDLE_PERIOD_MS = 30 * 60 * 1000      # 30 minutes in milliseconds
REFRESH_BATCH_SIZE = 15                  # symbols per batch when refreshing cache
REFRESH_BATCH_DELAY = 1.5               # seconds between refresh batches


def current_candle_ts() -> int:
    """Return the open timestamp (ms) of the current 30m candle."""
    now_ms = int(time.time() * 1000)
    return (now_ms // CANDLE_PERIOD_MS) * CANDLE_PERIOD_MS


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

        # symbol -> (candle_open_time_ms, open_price)
        self._candle_cache: dict[str, tuple[int, float]] = {}

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
                    await self._scan()
                except Exception as e:
                    logger.error(f"Scan cycle error: {e}", exc_info=True)
                await asyncio.sleep(self.scan_interval)

    # ------------------------------------------------------------------ #
    #  Scan cycle  (1 ticker request + lazy klines refreshes)
    # ------------------------------------------------------------------ #

    async def _scan(self):
        tickers = await self.api.get_all_tickers()
        if not tickers:
            logger.warning("Ticker returned 0 items — skipping cycle")
            return

        now_candle = current_candle_ts()
        logger.info(f"Scanning {len(tickers)} symbols via ticker…")

        stale: list[str] = []   # symbols needing a cache refresh
        candidates: list[tuple[str, float]] = []  # (symbol, last_price) to check

        for t in tickers:
            sym = t.get("symbol", "")
            try:
                last_price = float(t["lastPrice"])
            except (KeyError, ValueError, TypeError):
                continue

            cached = self._candle_cache.get(sym)
            if cached is None or cached[0] < now_candle:
                stale.append(sym)
            else:
                candidates.append((sym, last_price))

        # Refresh stale cache entries (new candle or first run) in batches
        if stale:
            logger.info(f"Refreshing cache for {len(stale)} symbol(s)…")
            refreshed = await self._refresh_cache(stale)
            logger.info(f"Cache refreshed: {refreshed}/{len(stale)}")
            # After refresh, add newly cached symbols to candidates
            for t in tickers:
                sym = t.get("symbol", "")
                if sym in stale and sym in self._candle_cache:
                    try:
                        candidates.append((sym, float(t["lastPrice"])))
                    except (ValueError, TypeError):
                        pass

        # Check candidates against cached opens
        sent = 0
        for sym, last_price in candidates:
            cached = self._candle_cache.get(sym)
            if not cached:
                continue
            candle_time, open_price = cached
            if open_price == 0:
                continue

            pct = (last_price - open_price) / open_price * 100
            if pct < self.min_pump_pct:
                continue
            if self.tracker.is_duplicate(sym, candle_time):
                continue

            logger.info(f"🔥 Pump detected: {sym} +{pct:.2f}%")
            await self._send_signal(sym, pct, open_price, last_price, candle_time)
            sent += 1

        if sent:
            logger.info(f"✅ Sent {sent} signal(s) this cycle")

    # ------------------------------------------------------------------ #
    #  Cache refresh
    # ------------------------------------------------------------------ #

    async def _refresh_cache(self, symbols: list[str]) -> int:
        """Fetch 30m klines for each symbol and update cache. Returns count refreshed."""
        count = 0
        for i in range(0, len(symbols), REFRESH_BATCH_SIZE):
            batch = symbols[i : i + REFRESH_BATCH_SIZE]
            results = await asyncio.gather(
                *[self._fetch_candle_open(sym) for sym in batch],
                return_exceptions=True,
            )
            for sym, res in zip(batch, results):
                if isinstance(res, tuple):
                    self._candle_cache[sym] = res
                    count += 1
            if i + REFRESH_BATCH_SIZE < len(symbols):
                await asyncio.sleep(REFRESH_BATCH_DELAY)
        return count

    async def _fetch_candle_open(self, symbol: str) -> Optional[tuple[int, float]]:
        klines = await self.api.get_klines(symbol, "30m", limit=1)
        if not klines:
            return None
        try:
            k = klines[-1]
            return (int(k["time"]), float(k["open"]))
        except (KeyError, ValueError, TypeError):
            return None

    # ------------------------------------------------------------------ #
    #  Enrich & send signal
    # ------------------------------------------------------------------ #

    async def _send_signal(
        self,
        symbol: str,
        pct: float,
        open_p: float,
        close_p: float,
        candle_time: int,
    ):
        rsi_1h, rsi_4h, rsi_1d, funding, ath_x = await asyncio.gather(
            self._get_rsi(symbol, "1h"),
            self._get_rsi(symbol, "4h"),
            self._get_rsi(symbol, "1d"),
            self.api.get_funding_rate(symbol),
            self._get_ath_x(symbol, close_p),
            return_exceptions=True,
        )

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
                async with s.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"Telegram error {resp.status}: {body}")
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
