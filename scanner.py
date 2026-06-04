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
from short_analyzer import format_short_analysis
from tracker import SignalTracker

logger = logging.getLogger(__name__)

CANDLE_PERIOD_MS = 30 * 60 * 1000      # 30 minutes in milliseconds
REFRESH_BATCH_SIZE = 15                  # symbols per batch when refreshing cache
REFRESH_BATCH_DELAY = 1.0               # seconds between refresh batches
MAX_STALE_PER_CYCLE = 50                # cap stale refresh per cycle (reduces blind window from 66s→~8s)


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
        scan_interval: int = 10,
        min_volume_usdt: float = 1_000_000,
    ):
        self.telegram_token = telegram_token
        self.chat_id = chat_id
        self.min_pump_pct = min_pump_pct
        self.scan_interval = scan_interval
        self.min_volume_usdt = min_volume_usdt
        self.tracker = SignalTracker()

        # symbol -> (candle_open_time_ms, open_price)
        self._candle_cache: dict[str, tuple[int, float]] = {}
        # symbols present in the previous bulk ticker response (for dropout detection)
        self._last_ticker_syms: set[str] = set()
        # symbols currently absent from bulk ticker (fetched individually every cycle)
        self._absent_syms: set[str] = set()

    # ------------------------------------------------------------------ #
    #  Main loop
    # ------------------------------------------------------------------ #

    async def run(self):
        logger.info(
            f"PumpScanner started | threshold={self.min_pump_pct}% | "
            f"interval={self.scan_interval}s | min_volume={self.min_volume_usdt:,.0f} USDT"
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

        # Build volume lookup for sorting stale symbols (high-volume first)
        vol_by_sym: dict[str, float] = {}
        for t in tickers:
            sym = t.get("symbol", "")
            try:
                vol_by_sym[sym] = float(t.get("quoteVolume", 0))
            except (ValueError, TypeError):
                vol_by_sym[sym] = 0.0

        stale: list[str] = []
        for t in tickers:
            sym = t.get("symbol", "")
            cached = self._candle_cache.get(sym)
            if cached is None or cached[0] < now_candle:
                stale.append(sym)

        # Sort stale by volume descending — high-volume coins refreshed first
        stale.sort(key=lambda s: vol_by_sym.get(s, 0), reverse=True)

        # Cap per-cycle refresh to avoid long blind windows at candle boundaries
        # (top MAX_STALE_PER_CYCLE by volume; remainder picked up in subsequent cycles)
        if len(stale) > MAX_STALE_PER_CYCLE:
            logger.info(f"Refreshing top {MAX_STALE_PER_CYCLE}/{len(stale)} stale symbols by volume")
            stale = stale[:MAX_STALE_PER_CYCLE]

        # Refresh stale cache entries, then re-fetch fresh ticker prices
        if stale:
            logger.info(f"Refreshing cache for {len(stale)} symbol(s)…")
            refreshed = await self._refresh_cache(stale)
            logger.info(f"Cache refreshed: {refreshed}/{len(stale)}")
            # Re-fetch ticker so we check FRESH prices after cache is updated
            tickers = await self.api.get_all_tickers() or tickers

        # Persistent absent-symbol tracking: keep fetching symbols missing from bulk
        # ticker every cycle until they return (one-shot was insufficient for slow pumps)
        ticker_syms = {t.get("symbol", "") for t in tickers}
        newly_gone = self._last_ticker_syms - ticker_syms
        self._absent_syms = (self._absent_syms | newly_gone) - ticker_syms
        self._absent_syms &= set(self._candle_cache.keys())  # only if cache exists
        self._last_ticker_syms = ticker_syms
        if self._absent_syms:
            to_fetch = list(self._absent_syms)[:5]
            logger.info(f"Fetching {len(to_fetch)} absent symbol(s) individually: {to_fetch}")
            extras = await asyncio.gather(
                *[self.api.get_ticker(s) for s in to_fetch],
                return_exceptions=True,
            )
            for item in extras:
                if isinstance(item, dict) and item.get("symbol"):
                    tickers.append(item)

        # Build candidates: cache must exist, must be current candle, volume + min price filters
        candidates: list[tuple[str, float, int, float, float]] = []  # sym, last_price, candle_time, open_price, vol_24h
        skipped_vol = 0
        for t in tickers:
            sym = t.get("symbol", "")
            cached = self._candle_cache.get(sym)
            if not cached:
                continue  # cache still missing (rate-limited) — skip this cycle
            candle_time, open_price = cached
            if candle_time < now_candle:
                continue  # stale open from previous candle — skip to avoid wrong-candle signals
            if open_price == 0:
                continue
            try:
                vol = float(t.get("quoteVolume", 0))
                if vol < self.min_volume_usdt:
                    skipped_vol += 1
                    logger.debug(f"Vol skip: {sym} = {vol:,.0f} USDT")
                    continue
                last_price = float(t["lastPrice"])
                if last_price < 0.001:
                    continue
                candidates.append((sym, last_price, candle_time, open_price, vol))
            except (ValueError, TypeError):
                pass
        if skipped_vol:
            logger.debug(f"Skipped {skipped_vol} low-volume symbols (<{self.min_volume_usdt:,.0f} USDT)")

        # Check candidates against cached opens
        sent = 0
        for sym, last_price, candle_time, open_price, vol_24h in candidates:
            pct = (last_price - open_price) / open_price * 100
            if pct < self.min_pump_pct:
                continue
            if self.tracker.is_duplicate(sym, candle_time):
                continue

            logger.info(f"🔥 Pump detected: {sym} +{pct:.2f}%")
            await self._send_signal(sym, pct, open_price, last_price, candle_time, vol_24h)
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
        # startTime=current candle open ensures we get THIS candle, not the previous closed one
        klines = await self.api.get_klines(symbol, "30m", limit=1, start_time=current_candle_ts())
        if not klines:
            return None
        try:
            k = klines[-1]
            ts = int(k["time"])
            # Reject if BingX returned a candle from a previous period
            if ts < current_candle_ts():
                return None
            return (ts, float(k["open"]))
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
        vol_24h: float = 0.0,
    ):
        rsi_1h, rsi_4h, rsi_1d, funding, ath_x, vol_mult, btc_6h = await asyncio.gather(
            self._get_rsi(symbol, "1h"),
            self._get_rsi(symbol, "4h"),
            self._get_rsi(symbol, "1d"),
            self.api.get_funding_rate(symbol),
            self._get_ath_x(symbol, close_p),
            self._get_vol_multiplier(symbol),
            self._get_btc_6h_change(),
            return_exceptions=True,
        )

        rsi_1h = rsi_1h if isinstance(rsi_1h, float) else None
        rsi_4h = rsi_4h if isinstance(rsi_4h, float) else None
        rsi_1d = rsi_1d if isinstance(rsi_1d, float) else None
        funding = funding if isinstance(funding, float) else None
        ath_x = ath_x if isinstance(ath_x, float) else 0.0
        vol_mult = vol_mult if isinstance(vol_mult, float) else None
        btc_6h = btc_6h if isinstance(btc_6h, float) else None

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

        short_msg = format_short_analysis(
            symbol=symbol,
            pct=pct,
            current_price=close_p,
            rsi_1h=rsi_1h,
            vol_multiplier=vol_mult,
            vol_24h=vol_24h,
            btc_6h_pct=btc_6h,
            ath_x=ath_x,
            funding=funding,
        )
        await self._send_telegram(short_msg)

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    async def _get_vol_multiplier(self, symbol: str) -> Optional[float]:
        """Current 30m candle USDT volume vs average of 50 completed candles.

        Uses startTime to fetch the current OPEN candle (BingX without startTime
        returns only completed candles, so klines[-1] would be the previous candle).
        USDT volume = volume * close for price-adjusted comparison.
        """
        curr_klines, hist_klines = await asyncio.gather(
            self.api.get_klines(symbol, "30m", limit=1, start_time=current_candle_ts()),
            self.api.get_klines(symbol, "30m", limit=50),
            return_exceptions=True,
        )
        if not isinstance(curr_klines, list) or not curr_klines:
            return None
        if not isinstance(hist_klines, list) or not hist_klines:
            return None
        try:
            def usdt_vol(k: dict) -> float:
                return float(k.get("volume", 0)) * float(k.get("close", 1))
            current_vol = usdt_vol(curr_klines[0])
            avg_vol = sum(usdt_vol(k) for k in hist_klines) / len(hist_klines)
            return current_vol / avg_vol if avg_vol > 0 else None
        except Exception:
            return None

    async def _get_btc_6h_change(self) -> Optional[float]:
        """BTC-USDT price change over last ~6 hours.

        Uses 7 completed 1h candles: klines[-6].open ≈ 6h ago,
        klines[-1].close ≈ current. Previous impl used klines[-7] (limit=8) = ~7h back.
        """
        klines = await self.api.get_klines("BTC-USDT", "1h", limit=7)
        if len(klines) < 6:
            return None
        try:
            ref = float(klines[-6]["open"])
            cur = float(klines[-1]["close"])
            return (cur - ref) / ref * 100 if ref > 0 else None
        except Exception:
            return None

    async def _get_rsi(self, symbol: str, interval: str) -> Optional[float]:
        # 100 candles → RSI fully warmed up (Wilder's needs ~50+ to stabilise)
        klines = await self.api.get_klines(symbol, interval, limit=100)
        if not klines:
            return None
        try:
            closes = [float(k["close"]) for k in klines]
            return calculate_rsi(closes)
        except Exception:
            return None

    async def _get_ath_x(self, symbol: str, current_price: float) -> float:
        klines = await self.api.get_klines(symbol, "1d", limit=1440)
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
