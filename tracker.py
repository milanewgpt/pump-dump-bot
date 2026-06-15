"""Signal deduplication, daily counter, and position result tracking."""
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

_STOP_WINDOW_S = 24 * 3600  # rolling window for stop counter

# Persist state here if the directory exists (Railway Volume or local /data)
_STATE_PATH = os.environ.get("STATE_PATH", "/data/tracker_state.json")

logger = logging.getLogger(__name__)


@dataclass
class ActivePosition:
    symbol: str
    entry_price: float
    sl_price: float
    tp_price: float
    candle_time: int
    signal_time: float = field(default_factory=time.time)
    is_real: bool = True  # False = статистический мониторинг без реального входа


class SignalTracker:
    def __init__(self):
        self._sent: set[tuple] = set()
        self._daily: dict[str, int] = defaultdict(int)
        self._date: date = date.today()
        # Position tracking
        self._active: dict[str, ActivePosition] = {}
        self._stop_times: dict[str, list[float]] = defaultdict(list)  # rolling 24h timestamps
        self._stop_cooldown_end: dict[str, float] = {}  # symbol → timestamp when 1h cooldown expires

    def _maybe_reset(self):
        today = date.today()
        if today != self._date:
            self._daily.clear()
            self._date = today

    # ---- dedup + counter ----

    def is_duplicate(self, symbol: str, candle_time: int) -> bool:
        return (symbol, candle_time) in self._sent

    def mark_sent(self, symbol: str, candle_time: int) -> int:
        """Mark signal as sent, increment daily counter. Returns new daily count."""
        self._maybe_reset()
        self._sent.add((symbol, candle_time))
        self._daily[symbol] += 1
        if len(self._sent) > 20_000:
            items = list(self._sent)
            self._sent = set(items[10_000:])
        return self._daily[symbol]

    def daily_count(self, symbol: str) -> int:
        self._maybe_reset()
        return self._daily[symbol]

    # ---- position tracking ----

    def register_position(self, symbol: str, entry_price: float, candle_time: int, is_real: bool = True):
        """Register a short position for result tracking (SL +3%, TP -5%).

        is_real=False: monitor price for stats only, no actual trade registered.
        """
        self._active[symbol] = ActivePosition(
            symbol=symbol,
            entry_price=entry_price,
            sl_price=entry_price * 1.03,
            tp_price=entry_price * 0.95,
            candle_time=candle_time,
            is_real=is_real,
        )
        self.save_state()

    def check_positions(self, prices: dict[str, float]) -> list[dict]:
        """Check all active positions against current prices.

        Returns list of closed position results.
        Removes closed positions from active tracking.
        """
        now = time.time()
        results = []
        to_close = []

        for sym, pos in self._active.items():
            price = prices.get(sym)
            if price is None:
                continue

            elapsed_h = (now - pos.signal_time) / 3600
            outcome = None

            if price >= pos.sl_price:
                outcome = "stop"
            elif price <= pos.tp_price:
                outcome = "take"
            elif elapsed_h >= 4.0:
                outcome = "timeout"

            if outcome:
                results.append({
                    "symbol": sym,
                    "entry_price": pos.entry_price,
                    "current_price": price,
                    "sl_price": pos.sl_price,
                    "tp_price": pos.tp_price,
                    "outcome": outcome,
                    "elapsed_h": elapsed_h,
                    "is_real": pos.is_real,
                })
                to_close.append(sym)
                if outcome == "stop" and pos.is_real:
                    self._record_stop(sym)

        for sym in to_close:
            del self._active[sym]

        if to_close:
            self.save_state()

        return results

    def _record_stop(self, symbol: str):
        now = time.time()
        self._stop_times[symbol].append(now)
        cutoff = now - _STOP_WINDOW_S
        self._stop_times[symbol] = [t for t in self._stop_times[symbol] if t > cutoff]
        self._stop_cooldown_end[symbol] = now + 3600  # 1h cooldown after SL
        self.save_state()

    def get_stops_today(self, symbol: str) -> int:
        """Return number of stops on this coin in the last 24h (rolling window)."""
        cutoff = time.time() - _STOP_WINDOW_S
        return sum(1 for t in self._stop_times[symbol] if t > cutoff)

    def get_stop_cooldown_mins(self, symbol: str) -> int:
        """Returns minutes remaining in cooldown after SL. 0 = no cooldown."""
        remaining = self._stop_cooldown_end.get(symbol, 0) - time.time()
        return max(0, int(remaining / 60))

    # ---- persistence ----

    def save_state(self):
        """Persist stop times and cooldowns to disk (requires Railway Volume at /data)."""
        try:
            os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
            now = time.time()
            cutoff = now - _STOP_WINDOW_S
            state = {
                "stop_times": {
                    sym: [t for t in ts if t > cutoff]
                    for sym, ts in self._stop_times.items()
                    if any(t > cutoff for t in ts)
                },
                "cooldown_end": {
                    sym: end for sym, end in self._stop_cooldown_end.items()
                    if end > now
                },
                "active_positions": {
                    sym: {
                        "entry_price": pos.entry_price,
                        "sl_price": pos.sl_price,
                        "tp_price": pos.tp_price,
                        "candle_time": pos.candle_time,
                        "signal_time": pos.signal_time,
                        "is_real": pos.is_real,
                    }
                    for sym, pos in self._active.items()
                },
                "saved_at": now,
            }
            with open(_STATE_PATH, "w") as f:
                json.dump(state, f)
        except Exception as e:
            logger.warning(f"State save failed: {e}")

    def load_state(self):
        """Restore stop times and cooldowns from disk on startup."""
        try:
            with open(_STATE_PATH) as f:
                state = json.load(f)
            now = time.time()
            cutoff = now - _STOP_WINDOW_S
            for sym, ts in state.get("stop_times", {}).items():
                valid = [t for t in ts if t > cutoff]
                if valid:
                    self._stop_times[sym] = valid
            for sym, end in state.get("cooldown_end", {}).items():
                if end > now:
                    self._stop_cooldown_end[sym] = end
            for sym, p in state.get("active_positions", {}).items():
                self._active[sym] = ActivePosition(
                    symbol=sym,
                    entry_price=p["entry_price"],
                    sl_price=p["sl_price"],
                    tp_price=p["tp_price"],
                    candle_time=p["candle_time"],
                    signal_time=p["signal_time"],
                    is_real=p.get("is_real", True),
                )
            age = now - state.get("saved_at", now)
            logger.info(f"State loaded: {len(self._stop_times)} symbols with stops, "
                        f"{len(self._active)} active positions (state age {age/60:.0f} min)")
        except FileNotFoundError:
            logger.info("No saved state found — starting fresh")
        except Exception as e:
            logger.warning(f"State load failed: {e}")
