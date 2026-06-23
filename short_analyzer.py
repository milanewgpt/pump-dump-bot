"""Post-pump short analysis: scoring criteria and Telegram message formatter."""
from __future__ import annotations
import time
from typing import Optional

# BingX charges funding every 8h at 00:00, 08:00, 16:00 UTC
_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
_FUNDING_WARN_MINUTES = 30   # warn if next funding < 30 min away
_FUNDING_WARN_PCT = -0.5     # trigger if funding ≤ -0.5% (fraction: -0.005)


def _minutes_to_next_funding() -> float:
    now_ms = int(time.time() * 1000)
    next_ms = ((now_ms // _FUNDING_INTERVAL_MS) + 1) * _FUNDING_INTERVAL_MS
    return (next_ms - now_ms) / 60_000


def _fmt_price(p: float) -> str:
    if p == 0:
        return "0"
    if p >= 1000:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.001:
        return f"{p:.6f}"
    return f"{p:.8f}"


def _score_rsi(rsi: Optional[float]) -> tuple[str, str, float]:
    if rsi is None:
        return "RSI 1H н/д", "▫️", 0.0
    r = round(rsi)
    if r >= 70:
        return f"RSI 1H {r} — перекуплен, откат вероятен", "✅", 1.0
    if r < 20:
        return f"RSI 1H {r} — экстремально низкий, жёсткий блок", "🚫", -1.0
    if r < 40:
        return f"RSI 1H {r} — низкий, памп без перегрева", "❌", -1.0
    return f"RSI 1H {r} — умеренный, не влияет", "▫️", 0.0


def _score_rsi_4h(rsi_4h: Optional[float]) -> tuple[str, str, float]:
    if rsi_4h is None:
        return "", "▫️", 0.0
    r = round(rsi_4h)
    if r >= 60:
        return f"RSI 4H {r} — подтверждает перекупленность", "✅", 0.5
    if r < 35:
        return f"RSI 4H {r} — слабость на 4H, риск продолжения", "⚠️", -0.5
    return "", "▫️", 0.0


def _score_vol(mult: Optional[float]) -> tuple[str, str, float]:
    if mult is None:
        return "Объём н/д", "▫️", 0.0
    return f"Объём {mult:.2f}x — справочно", "▫️", 0.0


def _score_btc(pct: Optional[float]) -> tuple[str, str, float]:
    if pct is None:
        return "BTC н/д", "▫️", 0.0
    if pct <= -1.5:
        return f"BTC {pct:+.1f}%/6ч — снижается, фон в плюс", "✅", 1.0
    if pct >= 1.5:
        return f"BTC {pct:+.1f}%/6ч — растёт, фон против шорта", "❌", -1.0
    return f"BTC {pct:+.1f}%/6ч — боковик, не влияет", "▫️", 0.0


def _score_ath(ath_x: float) -> tuple[str, str, float]:
    """ath_x = ath_price / current_price. 0 means unknown."""
    if ath_x <= 0:
        return "", "", 0.0
    if ath_x < 1.05:
        return "Новый ATH — выше ценовой истории, осторожно", "⚠️", -0.5
    if ath_x < 2.0:
        return "У самого ATH — сопротивления сверху почти нет", "⚠️", -0.5
    return f"До ATH {int(ath_x)}x — далеко, не влияет", "▫️", 0.0


def _score_funding(funding: Optional[float]) -> tuple[Optional[str], Optional[str], float]:
    """Positive funding = longs paying shorts = good for short entry.
    Only strongly positive (≥0.05%) or strongly negative (<-1%) affect score.
    Normal range (0–0.05%) is neutral — doesn't indicate meaningful long positioning.
    """
    if funding is None:
        return None, None, 0.0
    pct = funding * 100
    if pct >= 0.05:
        return f"Фандинг {pct:.4f}% — высокий, явный перевес лонгов (хорошо для шорта)", "✅", 1.0
    if pct < -1.0:
        return f"Фандинг {pct:.4f}% — сильно отрицательный, перевес шортов (риск сквиза вверх)", "❌", -1.0
    return f"Фандинг {pct:.4f}% — нейтральный, не влияет", "▫️", 0.0


def _score_arb_pump(arb_spread_pct: Optional[float]) -> tuple[Optional[str], Optional[str], float]:
    """Arbitrage pump: Binance price significantly above BingX = spread closing, not real momentum."""
    if arb_spread_pct is None or arb_spread_pct <= 0:
        return None, None, 0.0
    if arb_spread_pct >= 5.0:
        return (
            f"Арбитражный памп — Binance выше BingX на {arb_spread_pct:.1f}%, закрытие спреда",
            "❌", -2.0,
        )
    if arb_spread_pct >= 2.0:
        return (
            f"Спред с Binance {arb_spread_pct:.1f}% — возможный арбитраж, осторожно",
            "⚠️", -0.5,
        )
    return None, None, 0.0


def _score_liquidity(vol_24h: float) -> tuple[Optional[str], Optional[str], float]:
    if vol_24h <= 0 or vol_24h >= 5_000_000:
        return None, None, 0.0
    m = vol_24h / 1_000_000
    return f"Оборот ${m:.1f}M — низкая ликвидность", "⚠️", -0.5


def _score_repeat(signal_per_day: int) -> tuple[Optional[str], Optional[str], float]:
    if signal_per_day >= 3:
        return f"Памп №{signal_per_day} сегодня — монета-ракета, повторные пампы", "❌", -1.0
    if signal_per_day >= 2:
        return f"Памп №{signal_per_day} сегодня — повторный, осторожно", "⚠️", -0.5
    return None, None, 0.0


def _score_level(
    current_price: float, price_60min_ago: Optional[float]
) -> tuple[Optional[str], Optional[str], float]:
    """Compare current price to actual price 60 min ago (from rolling history).

    diff_pct = (current - price_60min_ago) / price_60min_ago × 100:
    - > 10%: fresh pump above 60-min level → reversal likely ✅
    - 5–10%: moderate move, not decisive ▫️
    - 0–5%: barely moved in 1h → not a fresh pump ❌ (hard block)
    - < 0%: price lower than 60 min ago → bounce/recovery, not a fresh pump ❌ (hard block)
    """
    if not price_60min_ago or price_60min_ago <= 0:
        return None, None, 0.0
    diff_pct = (current_price - price_60min_ago) / price_60min_ago * 100
    if diff_pct > 10.0:
        return (
            f"Свежий памп (+{diff_pct:.1f}% за 1ч) — разворот вероятен",
            "✅", 1.0,
        )
    if diff_pct >= 5.0:
        return (
            f"60 мин назад цена была на {diff_pct:.1f}% ниже — умеренно, не влияет",
            "▫️", 0.0,
        )
    if diff_pct >= 0.0:
        return (
            f"Памп не свежий — за 1ч рост всего +{diff_pct:.1f}%, цена не ушла от уровня",
            "❌", -1.0,
        )
    return (
        f"Возврат к уровню — 60 мин назад цена была выше (на {abs(diff_pct):.1f}%)",
        "❌", -1.0,
    )


def _score_resistance(
    resistance_info: Optional[tuple], label: str = "4h"
) -> tuple[Optional[str], Optional[str], float]:
    """Strong historical resistance within 10% above entry with ≥20% historical drop."""
    if resistance_info is None:
        return None, None, 0.0
    level, pct_above, drop_pct = resistance_info
    if drop_pct >= 20.0:
        return (
            f"Сопр. {label}: {_fmt_price(level)} (+{pct_above:.1f}%, обвал −{drop_pct:.0f}%) — мощный уровень",
            "✅", 1.0,
        )
    return (
        f"Сопр. {label}: {_fmt_price(level)} (+{pct_above:.1f}%, обвал −{drop_pct:.0f}%) — слабый уровень",
        "▫️", 0.0,
    )


def _score_stops(stops_today: int, coin: str) -> tuple[Optional[str], Optional[str], float]:
    if stops_today >= 2:
        return (
            f"{stops_today} стопа по {coin} за 24ч — монета-ракета",
            "❌", -1.0,
        )
    return None, None, 0.0


def _verdict(score: float, has_resistance: bool = True) -> tuple[str, str]:
    threshold = 2.0 if has_resistance else 3.0
    if score >= threshold:
        return "🟢", "ВХОД — сильный сигнал"
    if score >= 1.0:
        if not has_resistance and score >= 2.0:
            return "🟡", "СЛАБЫЙ — нет ближнего сопротивления для входа"
        return "🟡", "СЛАБЫЙ СИГНАЛ — лучше пропустить"
    return "🔴", "ПРОПУСК — не заходим"


def format_short_analysis(
    *,
    symbol: str,
    pct: float,
    current_price: float,
    rsi_1h: Optional[float],
    rsi_4h: Optional[float] = None,
    vol_multiplier: Optional[float],
    vol_24h: float,
    btc_6h_pct: Optional[float],
    ath_x: float,
    funding: Optional[float],
    signal_per_day: int = 1,
    price_60min_ago: Optional[float] = None,
    resistance_info: Optional[tuple] = None,
    resistance_1h_info: Optional[tuple] = None,
    stops_today: int = 0,
    arb_spread_pct: Optional[float] = None,
    stop_cooldown_mins: int = 0,
    oi_usd: float = 0,
    title_override: Optional[str] = None,
) -> tuple[str, float, bool, bool]:
    """Returns (message, total_score, wait_mode, has_real_entry)."""
    criteria: list[tuple[str, str]] = []
    total = 0.0
    coin = symbol.replace("-USDT", "").replace("-USDC", "")

    # Stops blocker — shown first
    stops_label, stops_icon, stops_score = _score_stops(stops_today, coin)
    if stops_icon:
        criteria.append((stops_icon, stops_label))
        total += stops_score

    label, icon, score = _score_rsi(rsi_1h)
    criteria.append((icon, label))
    total += score

    rsi4h_label, rsi4h_icon, rsi4h_score = _score_rsi_4h(rsi_4h)
    if rsi4h_label:
        criteria.append((rsi4h_icon, rsi4h_label))
    total += rsi4h_score

    label, icon, score = _score_vol(vol_multiplier)
    criteria.append((icon, label))
    total += score

    label, icon, score = _score_btc(btc_6h_pct)
    criteria.append((icon, label))
    total += score

    if pct >= 25:
        criteria.append(("▫️", f"Памп +{pct:.2f}% — крупный, высокая волатильность"))

    ath_label, ath_icon, ath_score = _score_ath(ath_x)
    if ath_icon:
        criteria.append((ath_icon, ath_label))
        total += ath_score

    fund_label, fund_icon, fund_score = _score_funding(funding)
    if fund_icon:
        criteria.append((fund_icon, fund_label))
        total += fund_score

    liq_label, liq_icon, liq_score = _score_liquidity(vol_24h)
    if liq_icon:
        criteria.append((liq_icon, liq_label))
        total += liq_score

    # Level analysis (60-min rolling reference)
    lvl_label, lvl_icon, lvl_score = _score_level(current_price, price_60min_ago)
    if lvl_icon:
        criteria.append((lvl_icon, lvl_label))
        total += lvl_score

    # Historical resistance (4h scores, 1h informational — skip if same level as 4h)
    res_label, res_icon, res_score = _score_resistance(resistance_info, "4h")
    # Very close resistance (<2%) is a stronger signal — price hits the wall immediately
    if resistance_info is not None and res_score > 0:
        _, pct_above_4h, _ = resistance_info
        if pct_above_4h < 2.0:
            res_score = 1.5
    if res_icon:
        criteria.append((res_icon, res_label))
        total += res_score

    res_1h_label, res_1h_icon, _ = _score_resistance(resistance_1h_info, "1h")
    if res_1h_icon:
        same_level = (
            resistance_info is not None
            and resistance_1h_info is not None
            and abs(resistance_info[0] - resistance_1h_info[0]) / resistance_info[0] < 0.005
        )
        if not same_level:
            criteria.append((res_1h_icon, res_1h_label))

    # Repeat pump counter
    rep_label, rep_icon, rep_score = _score_repeat(signal_per_day)
    if rep_icon:
        criteria.append((rep_icon, rep_label))
        total += rep_score

    # Arbitrage pump check
    arb_label, arb_icon, arb_score = _score_arb_pump(arb_spread_pct)
    if arb_icon:
        criteria.append((arb_icon, arb_label))
        total += arb_score

    # Resistance check for ВХОД threshold
    has_resistance = resistance_info is not None or resistance_1h_info is not None

    # Wait mode: large negative funding about to be charged
    fund_pct = funding * 100 if funding is not None else 0.0
    mins = _minutes_to_next_funding()
    wait_mode = fund_pct <= _FUNDING_WARN_PCT and mins < _FUNDING_WARN_MINUTES

    # Hard block: RSI 1H < 20 = cold pump, continuation likely
    rsi_block = rsi_1h is not None and round(rsi_1h) < 20

    # Hard block: funding < -1% = squeeze risk
    funding_block = funding is not None and funding * 100 < -1.0

    # Hard block: arbitrage pump = spread ≥5%, not real momentum
    arb_block = arb_spread_pct is not None and arb_spread_pct >= 5.0

    # Hard block: 60-min delta < 5% = price barely moved = not a fresh pump
    level_block = (
        price_60min_ago is not None
        and price_60min_ago > 0
        and (current_price - price_60min_ago) / price_60min_ago * 100 < 5.0
    )

    # Hard block: 2+ stops in 24h on this coin = persistent uptrend, not reversing
    hard_stop_block = stops_today >= 2

    # Hard block: ≥6 signals today on this coin = overheated, like competitor
    signal_block = signal_per_day >= 6

    # Hard block: 1h cooldown after SL on this coin
    cooldown_block = stop_cooldown_mins > 0

    # OI is informational only — low OI doesn't mean contract is untradeable on BingX
    oi_block = False

    if wait_mode:
        v_emoji, v_label = "🕒", "ПОДОЖДАТЬ — скоро начисление фандинга"
        wait_line = (
            "❌",
            f"Крупный отрицательный фандинг {fund_pct:.2f}% начисляется через "
            f"~{int(mins)} мин — шорт сразу заплатит, лучше подождать",
        )
        criteria.insert(0, wait_line)
    elif arb_block:
        v_emoji, v_label = "🔴", "ПРОПУСК — арбитражный памп, закрытие спреда с Binance"
    elif funding_block:
        v_emoji, v_label = "🔴", f"ПРОПУСК — отрицательный фандинг {fund_pct:.4f}%, риск сквиза"
    elif rsi_block:
        v_emoji, v_label = "🔴", "ПРОПУСК — RSI экстремально низкий, памп без перегрева"
    elif level_block:
        diff_pct_val = (current_price - price_60min_ago) / price_60min_ago * 100
        if diff_pct_val < 0:
            lvl_diff = abs(diff_pct_val)
            v_emoji, v_label = "🔴", f"ПРОПУСК — возврат к уровню, 60 мин назад цена была выше на {lvl_diff:.1f}%"
        else:
            v_emoji, v_label = "🔴", f"ПРОПУСК — памп не свежий, за 1ч рост всего +{diff_pct_val:.1f}%"
    elif hard_stop_block:
        v_emoji, v_label = "🔴", f"ПРОПУСК — {stops_today} стопа за 24ч, монета в устойчивом тренде"
    elif signal_block:
        v_emoji, v_label = "🔴", f"ПРОПУСК — {signal_per_day}-й сигнал по монете за день, перегрета"
    elif cooldown_block:
        v_emoji, v_label = "🔴", f"ПРОПУСК — кулдаун 1ч после стопа, осталось ~{stop_cooldown_mins} мин"
    elif oi_block:
        oi_m = oi_usd / 1_000_000
        v_emoji, v_label = "🔴", f"ПРОПУСК — OI ${oi_m:.1f}M, нет ликвидности для фьючерсов"
    else:
        v_emoji, v_label = _verdict(total, has_resistance)

    msg_lines = [
        title_override or f"{coin}/USDT · шорт после пампа +{pct:.2f}%",
        f"Цена {_fmt_price(current_price)}",
        "",
        f"{v_emoji} {v_label}",
        "",
    ]
    for icon, label in criteria:
        msg_lines.append(f"{icon} {label}")

    hard_block = wait_mode or arb_block or funding_block or rsi_block or level_block or hard_stop_block or signal_block or cooldown_block or oi_block
    vход_threshold = 2.0 if has_resistance else 3.0
    has_real_entry = not hard_block and total >= vход_threshold
    if has_real_entry:
        sl = current_price * 1.03
        tp = current_price * 0.95
        msg_lines.extend([
            "",
            f"🎯 Вход (шорт) около {_fmt_price(current_price)}",
            f"⛔ Стоп-лосс {_fmt_price(sl)} (+3%)",
            f"✅ Цель: 50% на {_fmt_price(tp)} (−5%), стоп в безубыток, остаток трейлингом",
        ])

    if wait_mode:
        msg_lines.extend([
            "",
            "🕒 Дождитесь начисления фандинга и перепроверьте сигнал",
        ])

    if has_real_entry:
        verdict = "entry"
    elif hard_block or total < 1.0:
        verdict = "skip"
    else:
        verdict = "weak"  # score 1.0 to threshold = СЛАБЫЙ

    effective_total = min(total, 0.9) if hard_block else total
    return "\n".join(msg_lines), effective_total, wait_mode, has_real_entry, verdict
