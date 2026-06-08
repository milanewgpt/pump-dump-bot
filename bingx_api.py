"""BingX Perpetual Futures public API client."""
import asyncio
import logging
from typing import Optional

import aiohttp

BASE_URL = "https://open-api.bingx.com"
logger = logging.getLogger(__name__)


class BingXAPI:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def _get(self, path: str, params: dict = None) -> Optional[dict]:
        url = f"{BASE_URL}{path}"
        try:
            async with self.session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                code = data.get("code")
                if code == 0:
                    return data.get("data")
                if code == 100410:
                    logger.warning(f"Rate limit (100410): {path} params={params}")
                else:
                    logger.debug(f"API error {path}: code={code} msg={data.get('msg')}")
                return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout: {path} {params}")
            return None
        except Exception as e:
            logger.debug(f"Request error {path}: {e}")
            return None

    async def get_all_symbols(self) -> list[str]:
        """Return all active USDT perpetual symbols."""
        data = await self._get("/openApi/swap/v2/quote/contracts")
        if not data:
            return []
        return [
            c["symbol"]
            for c in data
            if isinstance(c, dict) and c.get("symbol", "").endswith("-USDT")
        ]

    async def get_all_tickers(self) -> list[dict]:
        """Return 24h ticker for ALL symbols in one request.
        Fields: symbol, lastPrice, openPrice, priceChangePercent, highPrice, lowPrice, …
        """
        data = await self._get("/openApi/swap/v2/quote/ticker")
        if not data or not isinstance(data, list):
            return []
        return [t for t in data if t.get("symbol", "").endswith("-USDT")]

    async def get_klines(
        self, symbol: str, interval: str, limit: int = 30, start_time: int = None
    ) -> list[dict]:
        """Return list of kline dicts: {open, high, low, close, volume, time}."""
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        data = await self._get("/openApi/swap/v3/quote/klines", params=params)
        if not data or not isinstance(data, list):
            return []
        return data

    async def get_ticker(self, symbol: str) -> Optional[dict]:
        """Fetch ticker for a single symbol (fallback when missing from bulk response)."""
        data = await self._get("/openApi/swap/v2/quote/ticker", params={"symbol": symbol})
        if isinstance(data, list):
            return data[0] if data else None
        if isinstance(data, dict) and data.get("symbol"):
            return data
        return None

    async def get_open_interest(self, symbol: str) -> Optional[float]:
        """Return open interest in base currency (coins). Multiply by price for USD."""
        data = await self._get(
            "/openApi/swap/v2/quote/openInterest",
            params={"symbol": symbol},
        )
        if not data:
            return None
        try:
            return float(data.get("openInterest", 0))
        except (TypeError, ValueError):
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[float]:
        """Return current funding rate as float (e.g. 0.0001 = 0.01%)."""
        data = await self._get(
            "/openApi/swap/v2/quote/premiumIndex",
            params={"symbol": symbol},
        )
        if not data:
            return None
        try:
            return float(data.get("lastFundingRate", 0))
        except (TypeError, ValueError):
            return None
