"""Format pump signal message (HTML for Telegram)."""
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


def _fmt_rsi(v: Optional[float]) -> str:
    return str(round(v)) if v is not None else "—"


def _fmt_funding(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v * 100:.2f}%"


def _ath_line(ath_x: float, current_price: float) -> str:
    """
    ath_x = ath / current_price
    Examples from competitor:
        ath_x=1.0  → New ATH!
        ath_x=1.3  → More than 0x to ATH
        ath_x=2.5  → More than 1x to ATH
        ath_x=27   → More than 26x to ATH
    """
    if ath_x <= 1.01:
        return "New ATH!"
    n = int(ath_x) - 1
    return f"More than {n}x to ATH"


def format_pump_signal(
    *,
    symbol: str,
    pct: float,
    open_price: float,
    close_price: float,
    rsi_1h: Optional[float],
    rsi_4h: Optional[float],
    rsi_1d: Optional[float],
    funding: Optional[float],
    signal_per_day: int,
    ath_x: float,
    vol_24h: float = 0.0,
    oi_usd: Optional[float] = None,
) -> str:
    bingx_url = f"https://swap.bingx.com/en/{symbol}"
    cg_url = f"https://www.coinglass.com/tv/BingX_{symbol}"
    tv_url = f"https://www.tradingview.com/chart/?symbol=BINGX:{symbol}.P"

    ath_line = _ath_line(ath_x, close_price)

    oi_line = ""
    if oi_usd is not None and oi_usd > 0:
        oi_m = oi_usd / 1_000_000
        if vol_24h > 0:
            oi_line = f"OI: ${oi_m:.1f}M (×{oi_usd / vol_24h:.1f} к объёму)\n"
        else:
            oi_line = f"OI: ${oi_m:.1f}M\n"

    return (
        f"<a href='{bingx_url}'>BingX</a> -30- "
        f"<a href='{cg_url}'>{symbol}</a> - "
        f"<a href='{tv_url}'>TV</a>\n"
        f"Pump: {pct:.2f}% ({_fmt_price(open_price)} - {_fmt_price(close_price)})\n"
        f"RSI: 1H {_fmt_rsi(rsi_1h)} / 4H {_fmt_rsi(rsi_4h)} / 1D {_fmt_rsi(rsi_1d)}\n"
        f"Funding: {_fmt_funding(funding)}   💧 Объём 24ч: ${vol_24h / 1_000_000:.1f}M\n"
        f"{oi_line}"
        f"Signal per day: {signal_per_day}\n"
        f"{ath_line}"
    )
