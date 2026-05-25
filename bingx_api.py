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
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if data.get("code") == 0:
                    return data.get("data")
                logger.debug(f"API error {path}: code={data.get('code')} msg={data.get('msg')}")
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

    async def get_klines(self, symbol: str, interval: str, limit: int = 30) -> list[dict]:
        """Return list of kline dicts: {open, high, low, close, volume, time}."""
        data = await self._get(
            "/openApi/swap/v3/quote/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not data or not isinstance(data, list):
            return []
        return data

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
