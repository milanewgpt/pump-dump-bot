"""Core scanner: polls BingX, detects pumps, fires Telegram signals.

Architecture:
  - Every SCAN_INTERVAL: fetch all tickers in ONE request (~660 pairs).
  - Rolling 30-minute window: compare lastPrice vs price 30 min ago per symbol.
  - No candle-cache needed — reference price is recorded from live ticker history.
  - Enrichment (RSI/funding/ATH): fetched only when a signal fires.
"""
import asyncio
import logging
import os
import time
from collections import deque
from typing import Optional

import aiohttp

_TRADE_WEBHOOK_URL = os.environ.get("TRADE_WEBHOOK_URL", "")
_TRADE_WEBHOOK_SECRET = os.environ.get("TRADE_WEBHOOK_SECRET", "")

from bingx_api import BingXAPI
from indicators import calculate_rsi
from formatter import format_pump_signal
from short_analyzer import format_short_analysis
from tracker import SignalTracker

logger = logging.getLogger(__name__)

ROLL_WINDOW_MS    = 30 * 60 * 1000   # lookback: compare current price to price 30 min ago
ROLL_MAX_AGE_MS   = 65 * 60 * 1000   # keep price history up to 65 min (need 60-min lookback)
ROLL_LOOKBACK_60_MS = 60 * 60 * 1000  # for "return to level" check: price 60 min ago
SIGNAL_COOLDOWN_MS = 25 * 60 * 1000  # suppress re-signal for same symbol for 25 min
CANDLE_PERIOD_MS  = 30 * 60 * 1000   # still used for vol-multiplier and tracker

# Resistance-approach scanner
RESISTANCE_SCAN_INTERVAL  = 180          # run every 3 minutes
RESISTANCE_COOLDOWN_MS    = 4 * 60 * 60 * 1000  # 4h cooldown per symbol
RESISTANCE_MOVE_MIN_PCT   = 4.0          # minimum 30-min upward move to qualify (was 2.0 — too much noise)
RESISTANCE_MAX_ABOVE_PCT  = 12.0         # resistance must be within 12% above price
RESISTANCE_RSI_MIN        = 62           # RSI floor — only perky coins (was 45)
RESISTANCE_RSI_MAX        = 78           # RSI ceiling (above = pump scanner range)
RESISTANCE_SCAN_BATCH     = 15           # max candidates per cycle


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

        # Rolling price history: symbol -> deque of (timestamp_ms, price), oldest first
        self._price_history: dict[str, deque] = {}
        # Timestamp (ms) of last signal sent per symbol (for dedup)
        self._last_signal_ms: dict[str, int] = {}
        # symbols present in the previous bulk ticker response (for dropout detection)
        self._last_ticker_syms: set[str] = set()
        # symbols currently absent from bulk ticker (fetched individually every cycle)
        self._absent_syms: set[str] = set()
        # Latest 24h quote volume per symbol (updated every scan cycle)
        self._last_vol: dict[str, float] = {}
        # Timestamp (ms) of last resistance-approach signal per symbol
        self._last_resistance_signal_ms: dict[str, int] = {}

    # ------------------------------------------------------------------ #
    #  Main loop
    # ------------------------------------------------------------------ #

    async def _pump_scan_loop(self):
        while True:
            try:
                await self._scan()
            except Exception as e:
                logger.error(f"Pump scan error: {e}", exc_info=True)
            await asyncio.sleep(self.scan_interval)

    async def run(self):
        logger.info(
            f"PumpScanner started | threshold={self.min_pump_pct}% | "
            f"interval={self.scan_interval}s | min_volume={self.min_volume_usdt:,.0f} USDT"
        )
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            self.api = BingXAPI(session)
            self.tracker.load_state()
            await self._warmup_history()
            await asyncio.gather(self._pump_scan_loop(), self._resistance_scan_loop())

    async def _warmup_history(self):
        """Pre-populate price history from 1m klines so cold start window = 0."""
        logger.info("Warming up price history from 1m klines…")
        tickers = await self.api.get_all_tickers()
        if not tickers:
            logger.warning("Warmup: ticker empty, skipping")
            return

        # Only warm up symbols with enough volume (same filter as scan)
        symbols = [
            t["symbol"] for t in tickers
            if float(t.get("quoteVolume", 0)) >= self.min_volume_usdt
            and float(t.get("lastPrice", 0)) >= 0.001
        ]
        logger.info(f"Warmup: fetching 1m klines for {len(symbols)} symbols…")

        # Batch to avoid rate limits (15 at a time, 1s delay)
        BATCH = 15
        loaded = 0
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            results = await asyncio.gather(
                *[self.api.get_klines(sym, "1m", limit=70) for sym in batch],
                return_exceptions=True,
            )
            for sym, klines in zip(batch, results):
                if isinstance(klines, Exception) or not klines:
                    continue
                if sym not in self._price_history:
                    self._price_history[sym] = deque()
                hist = self._price_history[sym]
                for k in klines:
                    try:
                        ts = int(k["time"])
                        px = float(k["close"])
                        hist.append((ts, px))
                    except (KeyError, TypeError, ValueError):
                        continue
                loaded += 1
            if i + BATCH < len(symbols):
                await asyncio.sleep(1.0)

        logger.info(f"Warmup complete: {loaded}/{len(symbols)} symbols pre-loaded")

    # ------------------------------------------------------------------ #
    #  Scan cycle  (1 ticker request + lazy klines refreshes)
    # ------------------------------------------------------------------ #

    async def _scan(self):
        tickers = await self.api.get_all_tickers()
        if not tickers:
            logger.warning("Ticker returned 0 items — skipping cycle")
            return

        now_ms = int(time.time() * 1000)
        ref_ts = now_ms - ROLL_WINDOW_MS
        logger.info(f"Scanning {len(tickers)} symbols via ticker…")

        # Persistent absent-symbol tracking
        ticker_syms = {t.get("symbol", "") for t in tickers}
        newly_gone = self._last_ticker_syms - ticker_syms
        self._absent_syms = (self._absent_syms | newly_gone) - ticker_syms
        self._absent_syms &= set(self._price_history.keys())
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

        # Build price lookup and collect candidates
        prices: dict[str, float] = {}
        candidates: list[tuple[str, float, float, float, float, Optional[float]]] = []  # sym, pct, ref_price, last_price, vol_24h, price_60min_ago
        skipped_vol = 0

        for t in tickers:
            sym = t.get("symbol", "")
            try:
                vol = float(t.get("quoteVolume", 0))
                last_price = float(t["lastPrice"])
            except (KeyError, ValueError, TypeError):
                continue

            prices[sym] = last_price
            self._last_vol[sym] = vol  # keep for resistance-approach scan

            if last_price < 0.00001:
                continue

            # Update rolling price history for ALL symbols — even low-volume ones.
            # This ensures we catch volume spikes on previously quiet coins (e.g. IDOL, TAIKO).
            if sym not in self._price_history:
                self._price_history[sym] = deque()
            hist = self._price_history[sym]
            hist.append((now_ms, last_price))
            while hist and hist[0][0] < now_ms - ROLL_MAX_AGE_MS:
                hist.popleft()

            if vol < self.min_volume_usdt:
                skipped_vol += 1
                continue

            # Find reference price from ~30 min ago (most recent entry at or before ref_ts)
            # Also find price from ~60 min ago for "return to level" check
            ref_ts_60 = now_ms - ROLL_LOOKBACK_60_MS
            ref_price = None
            price_60min_ago: Optional[float] = None
            for ts, px in hist:
                if ts <= ref_ts_60:
                    price_60min_ago = px
                if ts <= ref_ts:
                    ref_price = px
                else:
                    break
            if ref_price is None:
                continue  # less than 30 min of history — skip

            pct = (last_price - ref_price) / ref_price * 100
            if pct < self.min_pump_pct:
                if pct >= 7.0:
                    logger.info(f"📊 Near-miss {sym}: +{pct:.1f}% (ref={ref_price:.6g}, last={last_price:.6g}, need {self.min_pump_pct}%)")
                continue

            # Dedup: suppress same symbol for SIGNAL_COOLDOWN_MS after last signal
            last_sig = self._last_signal_ms.get(sym, 0)
            if now_ms - last_sig < SIGNAL_COOLDOWN_MS:
                remaining = int((SIGNAL_COOLDOWN_MS - (now_ms - last_sig)) / 1000)
                logger.info(f"⏭️ {sym} +{pct:.1f}% — cooldown {remaining}s remaining")
                continue

            candidates.append((sym, pct, ref_price, last_price, vol, price_60min_ago))

        if skipped_vol:
            logger.debug(f"Skipped {skipped_vol} low-volume symbols (<{self.min_volume_usdt:,.0f} USDT)")

        # Check active positions against current prices — stats tracked internally, no Telegram message
        self.tracker.check_positions(prices)

        # Fire signals
        sent = 0
        candle_time = current_candle_ts()
        for sym, pct, ref_price, last_price, vol_24h, price_60min_ago in candidates:
            self._last_signal_ms[sym] = now_ms
            logger.info(f"🔥 Pump detected: {sym} +{pct:.2f}% (rolling 30m)")
            await self._send_signal(sym, pct, ref_price, last_price, candle_time, vol_24h, price_60min_ago)
            sent += 1

        if sent:
            logger.info(f"✅ Sent {sent} signal(s) this cycle")

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
        price_60min_ago: Optional[float] = None,
    ):
        rsi_1h, rsi_4h, rsi_1d, funding, ath_x, vol_mult, btc_6h, resistance_info, resistance_1h_info, ema_info, oi_usd, binance_price = (
            await asyncio.gather(
                self._get_rsi(symbol, "1h", current_price=close_p),
                self._get_rsi(symbol, "4h", current_price=close_p),
                self._get_rsi(symbol, "1d"),
                self.api.get_funding_rate(symbol),
                self._get_ath_x(symbol, close_p),
                self._get_vol_multiplier(symbol),
                self._get_btc_6h_change(),
                self._find_resistance(symbol, close_p, "4h"),
                self._find_resistance(symbol, close_p, "1h"),
                self._get_ema_resistance(symbol, close_p),
                self._get_oi_usd(symbol, close_p),
                self._get_binance_price(symbol),
                return_exceptions=True,
            )
        )

        rsi_1h = rsi_1h if isinstance(rsi_1h, float) else None
        rsi_4h = rsi_4h if isinstance(rsi_4h, float) else None
        rsi_1d = rsi_1d if isinstance(rsi_1d, float) else None
        funding = funding if isinstance(funding, float) else None
        ath_x = ath_x if isinstance(ath_x, float) else 0.0
        vol_mult = vol_mult if isinstance(vol_mult, float) else None
        btc_6h = btc_6h if isinstance(btc_6h, float) else None
        resistance_info = resistance_info if isinstance(resistance_info, tuple) else None
        resistance_1h_info = resistance_1h_info if isinstance(resistance_1h_info, tuple) else None
        ema_info = ema_info if isinstance(ema_info, tuple) else None
        oi_usd = oi_usd if isinstance(oi_usd, float) else None
        binance_price = binance_price if isinstance(binance_price, float) else None

        arb_spread_pct: Optional[float] = None
        if binance_price and close_p > 0:
            arb_spread_pct = (binance_price - close_p) / close_p * 100

        daily_count = self.tracker.mark_sent(symbol, candle_time)
        stops_today, stop_cooldown_mins = await self._get_executor_cooldown(symbol)

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
            vol_24h=vol_24h,
            oi_usd=oi_usd,
        )

        short_msg, total, wait_mode, has_real_entry, verdict = format_short_analysis(
            symbol=symbol,
            pct=pct,
            current_price=close_p,
            rsi_1h=rsi_1h,
            rsi_4h=rsi_4h,
            vol_multiplier=vol_mult,
            vol_24h=vol_24h,
            btc_6h_pct=btc_6h,
            ath_x=ath_x,
            funding=funding,
            signal_per_day=daily_count,
            price_60min_ago=price_60min_ago,
            resistance_info=resistance_info,
            resistance_1h_info=resistance_1h_info,
            ema_info=ema_info,
            stops_today=stops_today,
            arb_spread_pct=arb_spread_pct,
            stop_cooldown_mins=stop_cooldown_mins,
            oi_usd=oi_usd or 0,
        )
        if verdict == "skip" and not wait_mode:
            logger.info(f"📋 {symbol} → ПРОПУСК (score={total:.1f}), не отправляем")
            self.tracker.register_position(symbol, close_p, candle_time, verdict=verdict)
            return
        await self._send_telegram(msg + "\n➖➖➖➖➖\n" + short_msg)

        if not wait_mode:
            self.tracker.register_position(symbol, close_p, candle_time, verdict=verdict)

        if has_real_entry and _TRADE_WEBHOOK_URL:
            asyncio.create_task(self._fire_trade_webhook(symbol, close_p))

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    async def _find_resistance(
        self, symbol: str, current_price: float, interval: str = "4h"
    ) -> Optional[tuple[float, float, float]]:
        """Find nearest strong resistance level above current price within 10%.

        Returns (level, pct_above, drop_pct) or None.
        Only returns levels with historical drop ≥ 20% (strong resistance).
        """
        if interval == "1h":
            limit, window, look_ahead_max = 300, 5, 48
        else:  # 4h
            limit, window, look_ahead_max = 300, 3, 20

        klines = await self.api.get_klines(symbol, interval, limit=limit)
        if not klines or current_price <= 0:
            return None
        try:
            highs = [float(k["high"]) for k in klines]
            closes = [float(k["close"]) for k in klines]
        except (KeyError, ValueError, TypeError):
            return None

        n = len(highs)
        if n < 20:
            return None

        candidates: list[tuple[float, float, float]] = []

        for i in range(window, n - window - 1):
            local_high = highs[i]
            if not all(local_high >= highs[j] for j in range(i - window, i + window + 1) if j != i):
                continue
            if local_high <= current_price:
                continue
            pct_above = (local_high - current_price) / current_price * 100
            if pct_above > 14.0:
                continue
            look_ahead = min(look_ahead_max, n - i - 1)
            # Close levels (≤5%) may be recent highs with short look-ahead — allow 1 candle
            min_look_ahead = 1 if pct_above <= 5.0 else 3
            if look_ahead < min_look_ahead:
                continue
            min_close_after = min(closes[i + 1 : i + 1 + look_ahead])
            drop_pct = (local_high - min_close_after) / local_high * 100
            candidates.append((local_high, pct_above, drop_pct))

        if not candidates:
            return None

        # Close levels (≤5%): accept 10% historical drop — recent formation, less time to drop fully
        # Far levels (>5%): require 20% drop for proven strong resistance
        strong = [(lv, pa, dp) for lv, pa, dp in candidates if dp >= (10.0 if pa <= 5.0 else 20.0)]
        if strong:
            return min(strong, key=lambda x: x[1])
        return None

    async def _get_oi_usd(self, symbol: str, current_price: float) -> Optional[float]:
        """Return open interest in USD (OI in coins × current price)."""
        oi = await self.api.get_open_interest(symbol)
        if oi is None or oi <= 0:
            return None
        return oi * current_price

    async def _get_vol_multiplier(self, symbol: str) -> Optional[float]:
        """Current 30m candle USDT volume rate vs average of 50 completed candles.

        Normalizes partial candle volume to a 30m rate so that a pump at minute 3
        of the candle compares fairly to completed 30m candles.
        Formula: (current_vol * 30 / elapsed_minutes) / avg_historical_vol
        """
        now_ms = int(time.time() * 1000)
        candle_start = current_candle_ts()
        elapsed_min = max((now_ms - candle_start) / 60_000, 2.0)  # min 2 min

        curr_klines, hist_klines = await asyncio.gather(
            self.api.get_klines(symbol, "30m", limit=1, start_time=candle_start),
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
            if avg_vol <= 0:
                return None
            return current_vol * 30.0 / elapsed_min / avg_vol
        except Exception:
            return None

    async def _get_btc_6h_change(self) -> Optional[float]:
        """BTC-USDT price change over last ~6 hours.

        Fetches live BTC ticker alongside 7 completed 1h candles.
        klines[-6].open ≈ 6h ago; live lastPrice = current (avoids up to 59min
        stale close when BTC reverses near a candle boundary).
        """
        klines, ticker = await asyncio.gather(
            self.api.get_klines("BTC-USDT", "1h", limit=7),
            self.api.get_ticker("BTC-USDT"),
            return_exceptions=True,
        )
        if not isinstance(klines, list) or len(klines) < 6:
            return None
        try:
            ref = float(klines[-6]["open"])
            if isinstance(ticker, dict) and ticker.get("lastPrice"):
                cur = float(ticker["lastPrice"])
            else:
                cur = float(klines[-1]["close"])
            return (cur - ref) / ref * 100 if ref > 0 else None
        except Exception:
            return None

    async def _get_rsi(self, symbol: str, interval: str, current_price: Optional[float] = None) -> Optional[float]:
        """RSI. For 1h: pass current_price to include the live pump in the calculation."""
        klines = await self.api.get_klines(symbol, interval, limit=100)
        if not klines:
            return None
        try:
            closes = [float(k["close"]) for k in klines]
            if current_price is not None:
                closes.append(current_price)
            return calculate_rsi(closes)
        except Exception:
            return None

    async def _get_binance_price(self, symbol: str) -> Optional[float]:
        """Fetch price from Binance futures; falls back to Gate.io spot for tokens not on Binance."""
        coin = symbol.replace("-USDT", "").replace("-USDC", "")

        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={coin}USDT"
        try:
            async with self.api.session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                if isinstance(data, dict) and "price" in data:
                    return float(data["price"])
        except Exception:
            pass

        # Gate.io spot fallback (catches tokens absent from Binance futures, e.g. H-USDT)
        gate_url = f"https://api.gateio.ws/api/v4/spot/tickers?currency_pair={coin}_USDT"
        try:
            async with self.api.session.get(gate_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                if isinstance(data, list) and data:
                    return float(data[0]["last"])
        except Exception:
            pass

        return None

    async def _get_ema_resistance(self, symbol: str, current_price: float) -> Optional[tuple[float, float]]:
        """Check if 50 EMA on 4H is within 15% above current price (dynamic resistance).

        Returns (ema_value, pct_above) or None.
        """
        klines = await self.api.get_klines(symbol, "4h", limit=60)
        if not klines or current_price <= 0:
            return None
        try:
            closes = [float(k["close"]) for k in klines]
            if len(closes) < 50:
                return None
            k_factor = 2 / (50 + 1)
            ema = closes[0]
            for c in closes[1:]:
                ema = c * k_factor + ema * (1 - k_factor)
            if ema <= current_price:
                return None
            pct_above = (ema - current_price) / current_price * 100
            if pct_above > 15.0:
                return None
            return ema, pct_above
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

    def _format_result(self, result: dict) -> str:
        """Format stop/take result message for a tracked position."""
        sym = result["symbol"].replace("-USDT", "").replace("-USDC", "")
        elapsed = result["elapsed_h"]
        is_real = result.get("is_real", True)
        tag = "" if is_real else " (статистика)"
        if result["outcome"] == "stop":
            return (
                f"⏱ {sym}/USDT итог{tag} (~{elapsed:.0f}ч): "
                f"⛔ СТОП (+3%) — памп продолжился, шорт не сработал"
            )
        if result["outcome"] == "take":
            return (
                f"⏱ {sym}/USDT итог{tag} (~{elapsed:.0f}ч): "
                f"✅ ТЕЙК (−5%) — откат отработал"
            )
        return ""  # timeout — no message

    async def _get_executor_cooldown(self, symbol: str) -> tuple[int, int]:
        """Query bingx-executor for real SL count and cooldown. Falls back to local tracker."""
        if _TRADE_WEBHOOK_URL:
            base = _TRADE_WEBHOOK_URL.rsplit("/", 1)[0]
            url = f"{base}/cooldown/{symbol}"
            headers = {"X-Secret": _TRADE_WEBHOOK_SECRET} if _TRADE_WEBHOOK_SECRET else {}
            try:
                async with self.api.session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=3)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("stops_24h", 0), data.get("cooldown_mins", 0)
            except Exception as e:
                logger.debug(f"Executor cooldown query failed for {symbol}: {e}")
        return self.tracker.get_stops_today(symbol), self.tracker.get_stop_cooldown_mins(symbol)

    async def _fire_trade_webhook(self, symbol: str, price: float):
        payload = {"symbol": symbol, "price": price}
        headers = {"X-Secret": _TRADE_WEBHOOK_SECRET, "Content-Type": "application/json"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    _TRADE_WEBHOOK_URL, json=payload, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Trade webhook {symbol}: status {resp.status}")
        except Exception as e:
            logger.warning(f"Trade webhook {symbol}: {e}")

    # ------------------------------------------------------------------ #
    #  Resistance-approach scanner
    # ------------------------------------------------------------------ #

    async def _resistance_scan_loop(self):
        await asyncio.sleep(90)  # let price history warm up before first check
        while True:
            try:
                await self._resistance_approach_scan()
            except Exception as e:
                logger.error(f"Resistance scan error: {e}", exc_info=True)
            await asyncio.sleep(RESISTANCE_SCAN_INTERVAL)

    async def _resistance_approach_scan(self):
        """Find coins trending up 2–9% in 30 min and within 7% below strong resistance."""
        now_ms = int(time.time() * 1000)
        ref_ts_30 = now_ms - 30 * 60 * 1000

        candidates: list[tuple[str, float, float]] = []
        for sym, hist in self._price_history.items():
            if len(hist) < 5:
                continue
            if self._last_vol.get(sym, 0) < self.min_volume_usdt:
                continue
            if now_ms - self._last_resistance_signal_ms.get(sym, 0) < RESISTANCE_COOLDOWN_MS:
                continue
            if now_ms - self._last_signal_ms.get(sym, 0) < SIGNAL_COOLDOWN_MS:
                continue

            current_price = hist[-1][1]
            ref_price: Optional[float] = None
            for ts, px in hist:
                if ts <= ref_ts_30:
                    ref_price = px

            if ref_price is None or ref_price <= 0:
                continue
            move_30m = (current_price - ref_price) / ref_price * 100
            if not (RESISTANCE_MOVE_MIN_PCT <= move_30m < self.min_pump_pct):
                continue

            candidates.append((sym, current_price, move_30m))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[2], reverse=True)
        candidates = candidates[:RESISTANCE_SCAN_BATCH]
        logger.info(f"Resistance scan: {len(candidates)} candidates")

        results = await asyncio.gather(
            *[self._evaluate_resistance_candidate(sym, price, move)
              for sym, price, move in candidates],
            return_exceptions=True,
        )

        candle_time = current_candle_ts()
        for res in results:
            if isinstance(res, Exception):
                logger.debug(f"Resistance candidate error: {res}")
                continue
            if res is None:
                continue
            sym, price, move, resistance, rsi = res
            self._last_resistance_signal_ms[sym] = now_ms
            self._last_signal_ms[sym] = now_ms
            await self._send_resistance_signal(sym, price, move, resistance, rsi, candle_time)

    async def _evaluate_resistance_candidate(
        self, sym: str, current_price: float, move_30m: float
    ) -> Optional[tuple]:
        """Returns (sym, price, move, resistance, rsi) or None if not a valid candidate."""
        resistance, rsi = await asyncio.gather(
            self._find_resistance(sym, current_price, "4h"),
            self._get_rsi(sym, "1h", current_price=current_price),
            return_exceptions=True,
        )
        if isinstance(resistance, Exception) or resistance is None:
            return None
        if isinstance(rsi, Exception) or rsi is None:
            return None
        _, pct_above, _ = resistance
        if pct_above > RESISTANCE_MAX_ABOVE_PCT:
            return None
        if not (RESISTANCE_RSI_MIN <= rsi <= RESISTANCE_RSI_MAX):
            return None
        return sym, current_price, move_30m, resistance, rsi

    async def _send_resistance_signal(
        self,
        sym: str,
        current_price: float,
        move_30m: float,
        resistance: tuple,
        rsi_1h: float,
        candle_time: int,
    ):
        vol_24h = self._last_vol.get(sym, 0)
        coin = sym.replace("-USDT", "").replace("-USDC", "")

        funding, ath_x, btc_6h, resistance_1h, ema_info, oi_usd, binance_price, rsi_4h = await asyncio.gather(
            self.api.get_funding_rate(sym),
            self._get_ath_x(sym, current_price),
            self._get_btc_6h_change(),
            self._find_resistance(sym, current_price, "1h"),
            self._get_ema_resistance(sym, current_price),
            self._get_oi_usd(sym, current_price),
            self._get_binance_price(sym),
            self._get_rsi(sym, "4h", current_price=current_price),
            return_exceptions=True,
        )
        funding = funding if isinstance(funding, float) else None
        ath_x = ath_x if isinstance(ath_x, float) else 0.0
        btc_6h = btc_6h if isinstance(btc_6h, float) else None
        resistance_1h = resistance_1h if isinstance(resistance_1h, tuple) else None
        ema_info = ema_info if isinstance(ema_info, tuple) else None
        oi_usd = oi_usd if isinstance(oi_usd, float) else None
        binance_price = binance_price if isinstance(binance_price, float) else None
        rsi_4h = rsi_4h if isinstance(rsi_4h, float) else None

        arb_spread_pct: Optional[float] = None
        if binance_price and current_price > 0:
            arb_spread_pct = (binance_price - current_price) / current_price * 100

        daily_count = self.tracker.mark_sent(sym, candle_time)
        stops_today, stop_cooldown_mins = await self._get_executor_cooldown(sym)

        level, pct_above, _ = resistance
        from short_analyzer import format_short_analysis as _fsa
        short_msg, total, wait_mode, has_real_entry, verdict = _fsa(
            symbol=sym,
            pct=move_30m,
            current_price=current_price,
            rsi_1h=rsi_1h,
            rsi_4h=rsi_4h,
            vol_multiplier=None,
            vol_24h=vol_24h,
            btc_6h_pct=btc_6h,
            ath_x=ath_x,
            funding=funding,
            signal_per_day=daily_count,
            price_60min_ago=None,
            resistance_info=resistance,
            resistance_1h_info=resistance_1h,
            ema_info=ema_info,
            stops_today=stops_today,
            arb_spread_pct=arb_spread_pct,
            stop_cooldown_mins=stop_cooldown_mins,
            oi_usd=oi_usd or 0,
            title_override=f"📍 {coin}/USDT · подход к сопр. +{move_30m:.1f}%/30м",
        )

        if verdict == "skip":
            logger.info(f"📋 {sym} (resistance) → ПРОПУСК (score={total:.1f}), не отправляем")
            return

        await self._send_telegram(short_msg)

        if not wait_mode:
            self.tracker.register_position(sym, current_price, candle_time, verdict=verdict)
        if has_real_entry and _TRADE_WEBHOOK_URL:
            asyncio.create_task(self._fire_trade_webhook(sym, current_price))

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
