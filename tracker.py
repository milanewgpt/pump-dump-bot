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
    verdict: str = "entry"  # "entry" | "weak" | "skip"

    @property
    def is_real(self) -> bool:
        return self.verdict == "entry"


class SignalTracker:
    def __init__(self):
        self._sent: set[tuple] = set()
        self._daily: dict[str, int] = defaultdict(int)
        self._date: date = date.today()
        # Position tracking
        self._active: dict[str, ActivePosition] = {}
        # Cooldown: real trades only (entry verdict)
        self._stop_times: dict[str, list[float]] = defaultdict(list)
        self._stop_cooldown_end: dict[str, float] = {}
        # Rolling 24h stats: all signals
        self._all_stop_times: dict[str, list[float]] = defaultdict(list)
        self._all_tp_times: dict[str, list[float]] = defaultdict(list)
        # All-time accumulated counters by verdict (persisted)
        self._acc: dict[str, int] = {"entry_tp": 0, "entry_sl": 0, "weak_tp": 0, "weak_sl": 0, "skip_tp": 0, "skip_sl": 0}

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

    def register_position(self, symbol: str, entry_price: float, candle_time: int,
                          verdict: str = "entry", is_real: bool = True):
        """Register a short position for result tracking (SL +3%, TP -5%).

        verdict: "entry" (real trade) | "weak" (monitored, no trade) | "skip" (blocked signal)
        is_real kept for backward compat; verdict takes priority if provided.
        """
        if verdict not in ("entry", "weak", "skip"):
            verdict = "entry" if is_real else "weak"
        self._active[symbol] = ActivePosition(
            symbol=symbol,
            entry_price=entry_price,
            sl_price=entry_price * 1.03,
            tp_price=entry_price * 0.95,
            candle_time=candle_time,
            verdict=verdict,
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
                if outcome == "stop":
                    self._record_stop(sym, verdict=pos.verdict)
                elif outcome == "take":
                    self._record_tp(sym, verdict=pos.verdict)

        for sym in to_close:
            del self._active[sym]

        if to_close:
            self.save_state()

        return results

    def _record_stop(self, symbol: str, verdict: str = "entry"):
        now = time.time()
        cutoff = now - _STOP_WINDOW_S
        self._all_stop_times[symbol].append(now)
        self._all_stop_times[symbol] = [t for t in self._all_stop_times[symbol] if t > cutoff]
        if verdict == "entry":
            self._stop_times[symbol].append(now)
            self._stop_times[symbol] = [t for t in self._stop_times[symbol] if t > cutoff]
            self._stop_cooldown_end[symbol] = now + 3600
        key = f"{verdict}_sl"
        if key in self._acc:
            self._acc[key] += 1
        self.save_state()

    def _record_tp(self, symbol: str, verdict: str = "entry"):
        now = time.time()
        cutoff = now - _STOP_WINDOW_S
        self._all_tp_times[symbol].append(now)
        self._all_tp_times[symbol] = [t for t in self._all_tp_times[symbol] if t > cutoff]
        key = f"{verdict}_tp"
        if key in self._acc:
            self._acc[key] += 1
        self.save_state()

    def get_stops_today(self, symbol: str) -> int:
        """Real stops only — used for cooldown check."""
        cutoff = time.time() - _STOP_WINDOW_S
        return sum(1 for t in self._stop_times[symbol] if t > cutoff)

    def get_all_stops_today(self, symbol: str) -> int:
        """All SL outcomes (real + stats) in last 24h."""
        cutoff = time.time() - _STOP_WINDOW_S
        return sum(1 for t in self._all_stop_times[symbol] if t > cutoff)

    def get_all_tps_today(self, symbol: str) -> int:
        """All TP outcomes (real + stats) in last 24h."""
        cutoff = time.time() - _STOP_WINDOW_S
        return sum(1 for t in self._all_tp_times[symbol] if t > cutoff)

    def get_stop_cooldown_mins(self, symbol: str) -> int:
        """Returns minutes remaining in cooldown after SL. 0 = no cooldown."""
        remaining = self._stop_cooldown_end.get(symbol, 0) - time.time()
        return max(0, int(remaining / 60))

    def get_verdict_stats(self) -> dict:
        """All-time accumulated SL/TP counts by verdict category."""
        def wr(tp: int, sl: int) -> float:
            total = tp + sl
            return round(tp / total * 100, 1) if total else 0.0

        result = {}
        for v in ("entry", "weak", "skip"):
            tp = self._acc.get(f"{v}_tp", 0)
            sl = self._acc.get(f"{v}_sl", 0)
            result[v] = {"tp": tp, "sl": sl, "total": tp + sl, "winrate": wr(tp, sl)}
        return result

    def get_total_stats(self) -> dict:
        """Total SL/TP counts across all symbols in last 24h (all signals)."""
        cutoff = time.time() - _STOP_WINDOW_S
        total_sl = sum(
            sum(1 for t in ts if t > cutoff) for ts in self._all_stop_times.values()
        )
        total_tp = sum(
            sum(1 for t in ts if t > cutoff) for ts in self._all_tp_times.values()
        )
        return {"sl_24h": total_sl, "tp_24h": total_tp}

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
                "all_stop_times": {
                    sym: [t for t in ts if t > cutoff]
                    for sym, ts in self._all_stop_times.items()
                    if any(t > cutoff for t in ts)
                },
                "all_tp_times": {
                    sym: [t for t in ts if t > cutoff]
                    for sym, ts in self._all_tp_times.items()
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
                        "verdict": pos.verdict,
                    }
                    for sym, pos in self._active.items()
                },
                "acc": self._acc,
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
            for sym, ts in state.get("all_stop_times", {}).items():
                valid = [t for t in ts if t > cutoff]
                if valid:
                    self._all_stop_times[sym] = valid
            for sym, ts in state.get("all_tp_times", {}).items():
                valid = [t for t in ts if t > cutoff]
                if valid:
                    self._all_tp_times[sym] = valid
            for sym, end in state.get("cooldown_end", {}).items():
                if end > now:
                    self._stop_cooldown_end[sym] = end
            for sym, p in state.get("active_positions", {}).items():
                # backward compat: old state has is_real, new has verdict
                verdict = p.get("verdict") or ("entry" if p.get("is_real", True) else "weak")
                self._active[sym] = ActivePosition(
                    symbol=sym,
                    entry_price=p["entry_price"],
                    sl_price=p["sl_price"],
                    tp_price=p["tp_price"],
                    candle_time=p["candle_time"],
                    signal_time=p["signal_time"],
                    verdict=verdict,
                )
            saved_acc = state.get("acc", {})
            for k in self._acc:
                self._acc[k] = saved_acc.get(k, 0)
            age = now - state.get("saved_at", now)
            logger.info(f"State loaded: {len(self._stop_times)} symbols with stops, "
                        f"{len(self._active)} active positions (state age {age/60:.0f} min)")
        except FileNotFoundError:
            seed = os.environ.get("SEED_STATE")
            if seed:
                try:
                    os.environ.pop("SEED_STATE", None)
                    state = json.loads(seed)
                    # re-enter with the seeded state dict
                    now = time.time()
                    cutoff = now - _STOP_WINDOW_S
                    for sym, ts in state.get("stop_times", {}).items():
                        valid = [t for t in ts if t > cutoff]
                        if valid:
                            self._stop_times[sym] = valid
                    for sym, end in state.get("cooldown_end", {}).items():
                        if end > now:
                            self._stop_cooldown_end[sym] = end
                    self.save_state()
                    logger.info(f"State seeded from SEED_STATE env var: {len(self._stop_times)} symbols")
                except Exception as e:
                    logger.warning(f"SEED_STATE parse failed: {e}")
            else:
                logger.info("No saved state found — starting fresh")
        except Exception as e:
            logger.warning(f"State load failed: {e}")
