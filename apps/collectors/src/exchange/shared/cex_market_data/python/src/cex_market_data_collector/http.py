from __future__ import annotations

from typing import Any, Mapping

import aiohttp


class HttpJsonClient:
    def __init__(self, timeout_s: float = 10.0):
        self.timeout_s = timeout_s
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "HttpJsonClient":
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": "takora-cex-market-data-collector/0.1"},
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_json(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("HttpJsonClient must be used as an async context manager")
        async with self._session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None)
