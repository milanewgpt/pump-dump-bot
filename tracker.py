"""Signal deduplication + daily counter."""
from collections import defaultdict
from datetime import date


class SignalTracker:
    def __init__(self):
        self._sent: set[tuple] = set()       # (symbol, candle_open_time)
        self._daily: dict[str, int] = defaultdict(int)
        self._date: date = date.today()

    # ---- internal ----

    def _maybe_reset(self):
        today = date.today()
        if today != self._date:
            self._daily.clear()
            self._date = today
            # keep sent cache for dedup (candle times are unique enough)

    # ---- public ----

    def is_duplicate(self, symbol: str, candle_time: int) -> bool:
        return (symbol, candle_time) in self._sent

    def mark_sent(self, symbol: str, candle_time: int) -> int:
        """Mark signal as sent, increment daily counter. Returns new daily count."""
        self._maybe_reset()
        self._sent.add((symbol, candle_time))
        self._daily[symbol] += 1
        # Prevent unbounded growth
        if len(self._sent) > 20_000:
            # Drop oldest half (sets are unordered; acceptable approximation)
            items = list(self._sent)
            self._sent = set(items[10_000:])
        return self._daily[symbol]

    def daily_count(self, symbol: str) -> int:
        self._maybe_reset()
        return self._daily[symbol]
