"""Post-pump short analysis: scoring criteria and Telegram message formatter."""
from __future__ import annotations
from typing import Optional


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
    if r >= 60:
        return f"RSI 1H {r} — сильная перекупленность", "✅", 1.0
    if r >= 45:
        return f"RSI 1H {r} — перекуплен, откат вероятен", "✅", 1.0
    if r >= 38:
        return f"RSI 1H {r} — перегрев умеренный", "▫️", 0.0
    if r >= 32:
        return f"RSI 1H {r} — перепродан, риск продолжения роста", "❌", -1.0
    return f"RSI 1H {r} — перепродан, риск продолжения роста", "❌", -2.0


def _score_vol(mult: Optional[float]) -> tuple[str, str, float]:
    if mult is None:
        return "Объём н/д", "▫️", 0.0
    if mult < 1.0:
        return f"Объём {mult:.2f}x — низкий, импульс без поддержки", "✅", 1.0
    if mult < 3.0:
        return f"Объём {mult:.2f}x — умеренный", "✅", 1.0
    if mult < 5.0:
        return f"Объём {mult:.2f}x — повышенный, осторожно", "⚠️", -0.5
    return f"Объём {mult:.2f}x — экстремальный, сильный моментум", "❌", -1.0


def _score_btc(pct: Optional[float]) -> tuple[str, str, float]:
    if pct is None:
        return "BTC н/д", "▫️", 0.0
    if pct <= -1.5:
        return f"BTC {pct:+.1f}%/6ч — снижается, фон в плюс", "✅", 1.0
    if pct >= 1.5:
        return f"BTC {pct:+.1f}%/6ч — растёт, фон против шорта", "❌", -1.0
    return f"BTC {pct:+.1f}%/6ч — боковик", "▫️", 0.0


def _score_ath(ath_x: float) -> tuple[str, str, float]:
    """ath_x = ath_price / current_price. 0 means unknown."""
    if ath_x <= 0:
        return "", "", 0.0
    if ath_x < 1.05:
        return "Новый ATH — сверху нет сопротивления, риск сильного пролёта вверх", "❌", -1.0
    if ath_x < 2.0:
        return "У самого ATH — сопротивления сверху почти нет", "⚠️", -0.5
    return f"До ATH {int(ath_x)}x — сверху есть сопротивление", "✅", 1.0


def _score_funding(funding: Optional[float]) -> tuple[Optional[str], Optional[str], float]:
    """funding as fraction (0.0001 = 0.01%). Returns (label, icon, score) or (None, None, 0)."""
    if funding is None:
        return None, None, 0.0
    pct = funding * 100
    if pct < 0:
        return f"Фандинг {pct:.2f}% — отрицательный, перевес шортов", "✅", 1.0
    if pct > 0.05:
        return f"Фандинг {pct:.2f}% — повышенный", "⚠️", -0.5
    return None, None, 0.0  # neutral (0–0.05%) — not shown


def _score_liquidity(vol_24h: float) -> tuple[Optional[str], Optional[str], float]:
    if vol_24h <= 0 or vol_24h >= 5_000_000:
        return None, None, 0.0
    m = vol_24h / 1_000_000
    return f"Оборот ${m:.1f}M — низкая ликвидность", "⚠️", -0.5


def _verdict(score: float) -> tuple[str, str]:
    if score >= 1.0:
        return "🟢", "ВХОД — сильный сигнал"
    if score >= 0.0:
        return "🟡", "СЛАБЫЙ СИГНАЛ — лучше пропустить"
    return "🔴", "ПРОПУСК — не заходим"


def format_short_analysis(
    *,
    symbol: str,
    pct: float,
    current_price: float,
    rsi_1h: Optional[float],
    vol_multiplier: Optional[float],
    vol_24h: float,
    btc_6h_pct: Optional[float],
    ath_x: float,
    funding: Optional[float],
) -> str:
    criteria: list[tuple[str, str]] = []
    total = 0.0

    label, icon, score = _score_rsi(rsi_1h)
    criteria.append((icon, label))
    total += score

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

    v_emoji, v_label = _verdict(total)
    coin = symbol.replace("-USDT", "").replace("-USDC", "")

    msg_lines = [
        f"📌 {coin}/USDT · шорт после пампа +{pct:.2f}%",
        f"💰 Цена {_fmt_price(current_price)}",
        "",
        f"{v_emoji} {v_label}",
        "",
    ]
    for icon, label in criteria:
        msg_lines.append(f"{icon} {label}")

    if total >= 0.0:
        sl = current_price * 1.03
        tp = current_price * 0.95
        msg_lines.extend([
            "",
            f"🎯 Вход (шорт) около {_fmt_price(current_price)}",
            f"⛔ Стоп-лосс {_fmt_price(sl)} (+3%)",
            f"✅ Цель: 50% на {_fmt_price(tp)} (−5%), стоп в безубыток, остаток трейлингом",
        ])

    return "\n".join(msg_lines)
