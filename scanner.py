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

            if vol < self.min_volume_usdt:
                skipped_vol += 1
                continue
            if last_price < 0.001:
                continue

            # Update rolling price history
            if sym not in self._price_history:
                self._price_history[sym] = deque()
            hist = self._price_history[sym]
            hist.append((now_ms, last_price))
            while hist and hist[0][0] < now_ms - ROLL_MAX_AGE_MS:
                hist.popleft()

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

        # Check active positions against current prices and send results
        position_results = self.tracker.check_positions(prices)
        for result in position_results:
            msg = self._format_result(result)
            if msg:
                await self._send_telegram(msg)

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
        rsi_1h, rsi_4h, rsi_1d, funding, ath_x, vol_mult, btc_6h, resistance_info, resistance_1h_info, oi_usd, binance_price = (
            await asyncio.gather(
                self._get_rsi(symbol, "1h"),
                self._get_rsi(symbol, "4h"),
                self._get_rsi(symbol, "1d"),
                self.api.get_funding_rate(symbol),
                self._get_ath_x(symbol, close_p),
                self._get_vol_multiplier(symbol),
                self._get_btc_6h_change(),
                self._find_resistance(symbol, close_p, "4h"),
                self._find_resistance(symbol, close_p, "1h"),
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
        oi_usd = oi_usd if isinstance(oi_usd, float) else None
        binance_price = binance_price if isinstance(binance_price, float) else None

        arb_spread_pct: Optional[float] = None
        if binance_price and close_p > 0:
            arb_spread_pct = (binance_price - close_p) / close_p * 100

        daily_count = self.tracker.mark_sent(symbol, candle_time)
        stops_today = self.tracker.get_stops_today(symbol)
        stop_cooldown_mins = self.tracker.get_stop_cooldown_mins(symbol)

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

        short_msg, total, wait_mode, has_real_entry = format_short_analysis(
            symbol=symbol,
            pct=pct,
            current_price=close_p,
            rsi_1h=rsi_1h,
            vol_multiplier=vol_mult,
            vol_24h=vol_24h,
            btc_6h_pct=btc_6h,
            ath_x=ath_x,
            funding=funding,
            signal_per_day=daily_count,
            price_60min_ago=price_60min_ago,
            resistance_info=resistance_info,
            resistance_1h_info=resistance_1h_info,
            stops_today=stops_today,
            arb_spread_pct=arb_spread_pct,
            stop_cooldown_mins=stop_cooldown_mins,
            oi_usd=oi_usd or 0,
        )
        await self._send_telegram(msg + "\n➖➖➖➖➖\n" + short_msg)

        if not wait_mode and total >= 1.0:
            self.tracker.register_position(symbol, close_p, candle_time, is_real=has_real_entry)

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
            if pct_above > 10.0:
                continue
            look_ahead = min(look_ahead_max, n - i - 1)
            if look_ahead < 3:
                continue
            min_close_after = min(closes[i + 1 : i + 1 + look_ahead])
            drop_pct = (local_high - min_close_after) / local_high * 100
            candidates.append((local_high, pct_above, drop_pct))

        if not candidates:
            return None

        strong = [(lv, pa, dp) for lv, pa, dp in candidates if dp >= 20.0]
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

    async def _get_rsi(self, symbol: str, interval: str) -> Optional[float]:
        """RSI on completed candles only (no current open candle)."""
        klines = await self.api.get_klines(symbol, interval, limit=100)
        if not klines:
            return None
        try:
            closes = [float(k["close"]) for k in klines]
            return calculate_rsi(closes)
        except Exception:
            return None

    async def _get_binance_price(self, symbol: str) -> Optional[float]:
        """Fetch last price from Binance futures for arb-pump detection."""
        coin = symbol.replace("-USDT", "").replace("-USDC", "")
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={coin}USDT"
        try:
            async with self.api.session.get(
                url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                data = await resp.json()
                return float(data["price"])
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
