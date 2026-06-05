"""Signal deduplication, daily counter, and position result tracking."""
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class ActivePosition:
    symbol: str
    entry_price: float
    sl_price: float
    tp_price: float
    candle_time: int
    signal_time: float = field(default_factory=time.time)


class SignalTracker:
    def __init__(self):
        self._sent: set[tuple] = set()
        self._daily: dict[str, int] = defaultdict(int)
        self._date: date = date.today()
        # Position tracking
        self._active: dict[str, ActivePosition] = {}
        self._stops_today: dict[str, int] = defaultdict(int)
        self._stops_date: date = date.today()

    def _maybe_reset(self):
        today = date.today()
        if today != self._date:
            self._daily.clear()
            self._date = today

    def _maybe_reset_stops(self):
        today = date.today()
        if today != self._stops_date:
            self._stops_today.clear()
            self._stops_date = today

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

    def register_position(self, symbol: str, entry_price: float, candle_time: int):
        """Register a short position for result tracking (SL +3%, TP -5%)."""
        self._active[symbol] = ActivePosition(
            symbol=symbol,
            entry_price=entry_price,
            sl_price=entry_price * 1.03,
            tp_price=entry_price * 0.95,
            candle_time=candle_time,
        )

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
                })
                to_close.append(sym)
                if outcome == "stop":
                    self._record_stop(sym)

        for sym in to_close:
            del self._active[sym]

        return results

    def _record_stop(self, symbol: str):
        self._maybe_reset_stops()
        self._stops_today[symbol] += 1

    def get_stops_today(self, symbol: str) -> int:
        self._maybe_reset_stops()
        return self._stops_today[symbol]
