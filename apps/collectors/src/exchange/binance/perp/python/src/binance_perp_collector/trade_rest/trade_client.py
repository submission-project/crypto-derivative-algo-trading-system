import aiohttp
from typing import Any, Dict, List, Optional

from common.logging import setup_logger

logger = setup_logger(__name__)


class RestApiError(Exception):
    """
    Binance REST API가 비정상 응답(4xx/5xx)을 반환했을 때 발생하는 예외.

    이전 구현은 비정상 응답을 빈 리스트로 swallow 했기 때문에,
    인증 실패(401) 같은 critical 이슈가 "데이터 없음"과 구분되지 않았습니다.
    이제는 호출자가 반드시 처리하도록 강제합니다.
    """

    def __init__(self, status: int, body: str, *, url: str, code: Optional[int] = None):
        self.status = status
        self.body = body
        self.url = url
        self.code = code  # Binance 자체 에러 코드 (예: -2014)
        super().__init__(f"REST API error {status} ({url}) code={code}: {body}")


class RestAuthError(RestApiError):
    """401/403 — API key 누락 또는 무효."""


class RestRateLimitError(RestApiError):
    """429/418 — rate limit / IP ban."""


class RestTradeClient:
    """
    REST API를 사용하여 체결 내역(Trade)을 가져옵니다.

    인증이 필요한 엔드포인트(예: /fapi/v1/historicalTrades)는 ``X-MBX-APIKEY``
    헤더가 필요합니다. ``api_key``를 생성자에서 주입하지 않으면 ``signed=True``
    호출 시 명시적으로 :class:`RestAuthError` 가 발생합니다.

    Binance Futures 엔드포인트별 limit 제약:
      - /fapi/v1/trades              : default 500, max 1000
      - /fapi/v1/historicalTrades    : default 100, max  500
    클라이언트는 max 초과 입력을 네트워크 호출 전에 ``ValueError`` 로 거부합니다.
    """

    MAX_RECENT_LIMIT = 1000  # /fapi/v1/trades
    MAX_HISTORICAL_LIMIT = 500  # /fapi/v1/historicalTrades

    def __init__(
        self,
        base_url: str = "https://fapi.binance.com",
        api_key: Optional[str] = None,
    ):
        self.base_url = base_url
        self._api_key = api_key
        self._session: aiohttp.ClientSession | None = None

    async def connect(self):
        """HTTP 세션을 생성합니다. collector 시작 시 1회 호출."""
        self._session = aiohttp.ClientSession()
        logger.info(
            "RestTradeClient connected (session created, api_key=%s).",
            "set" if self._api_key else "absent",
        )

    async def close(self):
        """HTTP 세션을 닫습니다. collector 종료 시 호출."""
        if self._session:
            await self._session.close()
            self._session = None
            logger.info("RestTradeClient closed.")

    async def fetch_recent_trades(
        self, symbol: str, limit: int = 500
    ) -> List[Dict[str, Any]]:
        """최근 체결 내역을 조회합니다. (PUBLIC, API key 불필요. limit max=1000)"""
        if not 0 < limit <= self.MAX_RECENT_LIMIT:
            raise ValueError(
                f"fetch_recent_trades: limit must be 1..{self.MAX_RECENT_LIMIT}, got {limit}"
            )
        url = f"{self.base_url}/fapi/v1/trades"
        params = {"symbol": symbol, "limit": limit}
        return await self._get(url, params)

    async def fetch_trades_from_id(
        self,
        symbol: str,
        from_id: int,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        특정 trade_id부터 체결 내역을 조회합니다 (MARKET_DATA, API key 필요).
        aggTrade의 f~l 범위를 REST로 복원할 때 사용합니다.

        제약 (Binance):
          - limit max = 500. 초과 시 ValueError (네트워크 호출 전 fail-fast).
          - 한 달 이전의 trade는 반환되지 않음.
          - **마켓 체결만 반환** — insurance fund / ADL(Auto-Deleveraging) 체결은
            제외됩니다. 이로 인해 trade_id 시퀀스에 gap 이 생길 수 있습니다.
            (예: fromId=100 으로 요청해도 100~115 가 모두 ADL이면 응답은 116부터 시작)

        TODO: 한 RepairJob의 범위가 500보다 클 경우 페이지네이션 필요.
              현재는 첫 500건만 복원되고 나머지는 partial repair 경고로 끝남.
        """

        url = f"{self.base_url}/fapi/v1/historicalTrades"
        params = {
            "symbol": symbol,
            "fromId": from_id,
            "limit": limit,
        }
        return await self._get(url, params, signed=True)

    async def _get(
        self,
        url: str,
        params: dict,
        *,
        signed: bool = False,
    ) -> List[Dict[str, Any]]:
        """공통 HTTP GET 요청 처리. 4xx/5xx는 RestApiError 로 raise."""
        if signed and not self._api_key:
            raise RestAuthError(
                status=0,
                body="X-MBX-APIKEY missing — set BINANCE_API_KEY",
                url=url,
            )

        headers = {"X-MBX-APIKEY": self._api_key} if signed and self._api_key else {}

        session = self._session or aiohttp.ClientSession()
        close_after = self._session is None

        try:
            async with session.get(url, params=params, headers=headers) as response:
                text = await response.text()
                if response.status == 200:
                    return await response.json()

                code = _parse_binance_code(text)
                if response.status in (401, 403):
                    raise RestAuthError(response.status, text, url=url, code=code)
                if response.status in (418, 429):
                    raise RestRateLimitError(response.status, text, url=url, code=code)
                raise RestApiError(response.status, text, url=url, code=code)
        finally:
            if close_after:
                await session.close()


def _parse_binance_code(body: str) -> Optional[int]:
    """Binance 에러 응답 본문에서 'code' 필드만 best-effort로 추출."""
    try:
        import json

        return int(json.loads(body).get("code"))
    except Exception:
        return None
