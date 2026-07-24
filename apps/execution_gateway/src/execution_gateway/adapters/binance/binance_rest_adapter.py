"""
Binance USDⓈ-M Futures REST API Adapter.

지원 인증:
  - HMAC system-generated API key
  - Ed25519 self-generated API key


설계 원칙:
  - key_type을 명시적으로 관리한다.
  - newClientOrderId를 강제 주입한다.
  - 503 Unknown은 실패로 단정하지 않고 UNKNOWN 예외로 올린다.
  - 503 Service Unavailable / Internal Error / -1008은 UNKNOWN과 구분한다.
  - 429 / 418 / 403 / 408 / 5XX를 분리 처리한다.
  - batchOrders JSON은 compact 직렬화한다.
  - rate limit header를 수집한다.

주의:
  - RSA self-generated key는 이 어댑터에서 미지원.
  - RSA가 필요하면 sign_rest_rsa()와 BinanceKeyType.RSA를 별도 추가.

지원 엔드포인트:
  - POST   /fapi/v1/order          단건 주문
  - POST   /fapi/v1/batchOrders    일괄 주문 (최대 5건)
  - PUT    /fapi/v1/order          주문 수정 (LIMIT 전용)
  - DELETE /fapi/v1/order          단건 취소
  - DELETE /fapi/v1/batchOrders    일괄 취소 (최대 10건)
  - DELETE /fapi/v1/allOpenOrders  심볼 전체 미체결 취소
  - GET    /fapi/v1/openOrders     미체결 주문 조회
  - GET    /fapi/v1/order          단건 주문 조회
  - GET    /fapi/v1/time           서버 시간 조회
  - POST   /fapi/v1/listenKey      listenKey 발급
  - PUT    /fapi/v1/listenKey      listenKey keepalive
  - DELETE /fapi/v1/listenKey      listenKey 해제
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional, cast

import httpx

from common.logging import setup_logger
from common.ids import generate_order_id
from .auth.binance_rest_auth import (
    sign_rest_hmac,
    sign_rest_ed25519,
    create_rest_headers,
)
from enum import Enum

from common.config import settings

from execution_gateway.exchange import AsyncClosable

from execution_gateway.adapters.binance.dto.resp.OrderResponseDto import (
    CancelAlgoOrderRespDto,
    CancelAllOpenOrdersRespDto,
    CancelOrderRespDto,
    OrderRespDto,
    ModifyOrderRespDto
)
from execution_gateway.adapters.binance.dto.resp.AlgoOrderResponseDto import (
    AlgoOrderRespDto,
    CancelAllAlgoOpenOrdersRespDto
)
from execution_gateway.adapters.binance.dto.resp.AccountResponseDto import (
    AccountInfoRespDto
)
from execution_gateway.adapters.binance.dto.resp.PositionResponseDto import (
    PositionRiskRespDto,
    SymbolConfigRespDto
)
from execution_gateway.adapters.binance.dto.resp.MarketDataResponseDto import (
    SymbolPriceTickerRespDto
)
from execution_gateway.adapters.binance.dto.resp.TradingConfigResponseDto import (
    ChangeLeverageRespDto,
    ListenKeyRespDto
)



logger = setup_logger(__name__)


class BinanceKeyType(str, Enum):
    HMAC = "HMAC"
    ED25519 = "ED25519"


# ── Binance API 제한 ──
MAX_BATCH_ORDERS = 5
MAX_BATCH_CANCEL = 10


# ── 에러 클래스 계층 ──


class BinanceApiError(Exception):
    """Binance REST API 에러."""

    def __init__(self, code: int, msg: str, status_code: int = 0):
        self.code = code
        self.msg = msg
        self.status_code = status_code
        super().__init__(
            f"BinanceApiError(code={code}, msg={msg}, status_code={status_code})"
        )


class BinanceRateLimitError(BinanceApiError):
    """HTTP 429: Rate limit 초과. 즉시 backoff 필요."""

    pass


class BinanceIpBanError(BinanceApiError):
    """HTTP 418: IP 차단. 즉시 중단 필요."""

    pass


class BinanceWafError(BinanceApiError):
    """HTTP 403: WAF(웹 방화벽) 제한."""

    pass


class BinanceUnknownExecutionError(BinanceApiError):
    """
    실행 상태 불명확.

    예:
      - 503 Unknown error
      - 주문/취소 계열 요청 중 client timeout
      - 주문/취소 계열 요청 중 408 backend timeout

    처리:
      - 즉시 재주문 금지
      - newClientOrderId / origClientOrderId로 get_order 확인
      - User Data Stream 이벤트 확인
    """

    pass


class BinanceServiceUnavailableError(BinanceApiError):
    """503 Service Unavailable. 실패 확정에 가깝고 backoff retry 가능."""

    pass


class BinanceInternalRetryableError(BinanceApiError):
    """503 Internal error. 실패 확정에 가깝고 backoff retry 가능."""

    pass


class BinanceSystemThrottleError(BinanceApiError):
    """
    -1008 system-level protection.

    의미:
      - 시스템 과부하
      - 실패 확정
      - concurrency 감소 필요
      - reduce-only / close-position 계열은 예외 또는 우선 처리될 수 있음
    """

    pass


class BinanceNetworkError(BinanceApiError):
    """비주문성 요청에서 발생한 네트워크/타임아웃 오류."""

    pass


class BinanceLeveragePolicyError(Exception):
    """
    앱 설정 상한(binance_max_leverage)을 넘기는 레버리지 요청.

    거래소 호출 전에 차단한다. API 레이어에서 HTTP 400으로 매핑한다.
    """

    def __init__(self, *, requested: int, max_allowed: int) -> None:
        self.requested = requested
        self.max_allowed = max_allowed
        super().__init__(
            f"Leverage {requested}x exceeds configured maximum {max_allowed}x"
        )


# ── Rate Limit 상태 ──


@dataclass
class RateLimitState:
    """응답 헤더에서 추출한 rate limit 현황."""

    used_weight_1m: int | None = None
    order_count_10s: int | None = None
    order_count_1m: int | None = None

    def update_from_headers(self, headers: httpx.Headers) -> None:
        self.used_weight_1m = _safe_int_header(
            headers,
            "X-MBX-USED-WEIGHT-1M",
            self.used_weight_1m,
        )
        self.order_count_10s = _safe_int_header(
            headers,
            "X-MBX-ORDER-COUNT-10S",
            self.order_count_10s,
        )
        self.order_count_1m = _safe_int_header(
            headers,
            "X-MBX-ORDER-COUNT-1M",
            self.order_count_1m,
        )

    def snapshot(self) -> dict[str, int | None]:
        return {
            "used_weight_1m": self.used_weight_1m,
            "order_count_10s": self.order_count_10s,
            "order_count_1m": self.order_count_1m,
        }

    def log(self) -> None:
        logger.debug(
            "RateLimit | "
            f"weight_1m={self.used_weight_1m} "
            f"order_10s={self.order_count_10s} "
            f"order_1m={self.order_count_1m}"
        )


def _safe_int_header(
    headers: httpx.Headers,
    key: str,
    default: int | None,
) -> int | None:
    value = headers.get(key)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid Binance rate header: {key}={value}")
        return default


# ── Batch 응답 분리 유틸 ──


# def split_batch_result(
#     items: list[dict[str, Any]],
# ) -> tuple[list[dict[str, Any]], list[BinanceApiError]]:
#     """
#     batch 응답 리스트에서 성공 응답과 에러를 분리.

#     batch API는 리스트 내부에 성공/실패가 섞일 수 있음.
#     """
#     ok: list[dict[str, Any]] = []
#     errors: list[BinanceApiError] = []

#     for item in items:
#         if (
#             isinstance(item, dict)
#             and isinstance(item.get("code"), int)
#             and item["code"] < 0
#         ):
#             errors.append(
#                 BinanceApiError(
#                     item["code"],
#                     item.get("msg", "Unknown batch item error"),
#                 )
#             )
#         else:
#             ok.append(item)

#     return ok, errors


def _compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {
            "code": -1,
            "msg": resp.text,
        }


def _extract_error(data: Any) -> tuple[int, str]:
    if isinstance(data, dict):
        code = data.get("code", -1)
        msg = data.get("msg", "")

        if not isinstance(code, int):
            code = -1

        return code, str(msg)

    return -1, str(data)


def _is_mutating_request(method: str) -> bool:
    return method.upper() in {"POST", "PUT", "DELETE"}


def _is_order_or_cancel_path(path: str) -> bool:
    """
    실행 상태 UNKNOWN이 치명적인 주문/취소/수정 계열 endpoint인지 판단.

    POST /fapi/v1/order
    POST /fapi/v1/batchOrders
    PUT /fapi/v1/order
    PUT /fapi/v1/batchOrders
    DELETE /fapi/v1/order
    DELETE /fapi/v1/batchOrders
    DELETE /fapi/v1/allOpenOrders
    """
    return path in {
        "/fapi/v1/order",
        "/fapi/v1/batchOrders",
        "/fapi/v1/allOpenOrders",
    }


# ── Main Adapter ──


class BinanceRestAdapter(AsyncClosable):
    """
    Binance USDⓈ-M Futures REST API Adapter.

    설계 원칙:
    - HMAC system-generated key 지원
    - Ed25519 self-generated key 지원
    - newClientOrderId 강제 주입
    - HTTP 상태코드별 에러 분기
    - batch 응답 내 개별 에러 감지
    - Rate limit 헤더 수집
    - batchOrders JSON compact 직렬화

    주의:
    - RSA self-generated key는 현재 미지원
    - WS Trade API도 Ed25519 private key를 사용 가능
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        key_type: BinanceKeyType,
        api_secret: str | None = None,
        private_key_pem: str | None = None,
        timeout: float = 10.0,
    ):
        if not api_secret and not private_key_pem:
            raise ValueError("Either api_secret or private_key_pem must be provided")

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.key_type = key_type
        self.api_secret = api_secret
        self.private_key_pem = private_key_pem

        if self.key_type == BinanceKeyType.HMAC and not self.api_secret:
            raise ValueError("HMAC key requires api_secret")

        if self.key_type == BinanceKeyType.ED25519 and not self.private_key_pem:
            raise ValueError("Ed25519 key requires private_key_pem")

        self.rate_limit = RateLimitState()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=create_rest_headers(api_key),
            timeout=timeout,
        )

    async def __aenter__(self) -> BinanceRestAdapter:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    # # ──────────────────────────── 서버 시간 ────────────────────────────

    # async def get_server_time(self) -> int:
    #     """서버 시간 조회 (ms). 시계 동기화에 활용."""
    #     data = await self._request("GET", "/fapi/v1/time", sign=False)
    #     return cast(int, data["serverTime"])

    # ──────────────────────────── Private helpers ────────────────────────────

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
        if self.key_type == BinanceKeyType.ED25519:
            return sign_rest_ed25519(params, self.private_key_pem)

        if self.key_type == BinanceKeyType.HMAC:
            return sign_rest_hmac(params, self.api_secret)

        raise ValueError(f"Unsupported Binance key type: {self.key_type}")

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        sign: bool = True,
    ) -> Any:
        """
        요청 전송 및 Binance 응답 처리.

        핵심:
          - signed endpoint면 signature 추가
          - 모든 요청은 query string params= 로 전송
          - HTTP 상태 코드와 Binance code/msg를 함께 분석
          - 503 variant를 분리 처리
        """
        params = dict(params or {})


        if sign:
            params = self._sign_params(params)

        try:
            logger.info(f"{method} {path} {params}")
            resp = await self._client.request(method, path, params=params)
        except httpx.TimeoutException as e:
            if sign and _is_mutating_request(method) and _is_order_or_cancel_path(path):
                raise BinanceUnknownExecutionError(
                    -1007,
                    f"HTTP client timeout during mutating signed request: {e}",
                    0,
                ) from e

            raise BinanceNetworkError(
                -1007,
                f"HTTP client timeout: {e}",
                0,
            ) from e
        except httpx.HTTPError as e:
            if sign and _is_mutating_request(method) and _is_order_or_cancel_path(path):
                raise BinanceUnknownExecutionError(
                    -1006,
                    f"HTTP network error during mutating signed request: {e}",
                    0,
                ) from e

            raise BinanceNetworkError(
                -1006,
                f"HTTP network error: {e}",
                0,
            ) from e

        # Rate limit 헤더 수집
        self.rate_limit.update_from_headers(resp.headers)
        self.rate_limit.log()

        data = _safe_json(resp)
        code, msg = _extract_error(data)
        # msg_lower = msg.lower()

        # ── HTTP status 기반 분기 ──

        if resp.status_code == 403:
            raise BinanceWafError(
                code if code < 0 else -403,
                msg or "WAF limit violated",
                403,
            )

        if resp.status_code == 418:
            raise BinanceIpBanError(
                code if code < 0 else -418,
                msg or "IP auto-banned due to repeated rate limit violations",
                418,
            )

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "unknown")
            raise BinanceRateLimitError(
                code if code < 0 else -429,
                msg or f"Rate limit exceeded. Retry-After={retry_after}",
                429,
            )

        if resp.status_code == 408:
            if sign and _is_mutating_request(method) and _is_order_or_cancel_path(path):
                raise BinanceUnknownExecutionError(
                    code if code < 0 else -408,
                    msg or "Backend timeout during mutating signed request",
                    408,
                )

            raise BinanceNetworkError(
                code if code < 0 else -408,
                msg or "Backend timeout",
                408,
            )

        if resp.status_code == 503:
            self._raise_503_variant(code, msg)

        if 500 <= resp.status_code < 600:
            raise BinanceServiceUnavailableError(
                code if code < 0 else -500,
                msg or f"HTTP {resp.status_code} server error",
                resp.status_code,
            )

        # ── Binance JSON error payload 처리 ──
        if isinstance(data, dict) and isinstance(data.get("code"), int):
            payload_code = data["code"]
            if payload_code < 0:
                # -1008이 HTTP 200/4XX/5XX 등으로 들어와도 분리
                if payload_code == -1008:
                    raise BinanceSystemThrottleError(
                        payload_code,
                        data.get("msg", "System-level protection throttled request"),
                        resp.status_code,
                    )

                raise BinanceApiError(
                    payload_code,
                    data.get("msg", "Unknown Binance API error"),
                    resp.status_code,
                )

        if resp.status_code >= 400:
            raise BinanceApiError(
                code if code < 0 else -1,
                msg or f"HTTP {resp.status_code} error",
                resp.status_code,
            )

        return data

    def _raise_503_variant(self, code: int, msg: str) -> None:
        msg_lower = msg.lower()

        if code == -1008 or "request throttled by system-level protection" in msg_lower:
            raise BinanceSystemThrottleError(
                code if code < 0 else -1008,
                msg or "Request throttled by system-level protection",
                503,
            )

        if "unknown error" in msg_lower and "check your request" in msg_lower:
            raise BinanceUnknownExecutionError(
                code if code < 0 else -503,
                msg or "Unknown execution status",
                503,
            )

        if "service unavailable" in msg_lower:
            raise BinanceServiceUnavailableError(
                code if code < 0 else -503,
                msg or "Service Unavailable",
                503,
            )

        if "internal error" in msg_lower and "unable to process" in msg_lower:
            raise BinanceInternalRetryableError(
                code if code < 0 else -503,
                msg or "Internal error; unable to process request",
                503,
            )

        # 알 수 없는 503은 실패성 service unavailable 쪽으로 둔다.
        # "Unknown error..." 문구가 없으면 UNKNOWN으로 두지 않는 것이 안전.
        raise BinanceServiceUnavailableError(
            code if code < 0 else -503,
            msg or "HTTP 503 Service Unavailable",
            503,
        )

    # ──────────────────────────── Order Placement ────────────────────────────

    async def place_regular_order(self, order_params: dict[str, Any]) -> OrderRespDto:
        """
        단건 주문: POST /fapi/v1/order

        - newClientOrderId가 없으면 자동 주입 (503 unknown 복구 대비)
        """
        params = dict(order_params)
        if "newClientOrderId" not in params:
            params["newClientOrderId"] = generate_order_id()
        return OrderRespDto.from_response(await self._request("POST", "/fapi/v1/order", params))

    async def place_batch_orders(self, orders_params: list[dict[str, Any]]) -> tuple[list[OrderRespDto], list[dict[str, Any]]]:
        """
        일괄 주문 (최대 5건): POST /fapi/v1/batchOrders

        주의:
          - 응답 리스트 순서 = 입력 순서와 동일
          - 체결/매칭 순서는 보장 안 됨 (병렬 처리)
          - 응답 내 개별 에러는 split_batch_result()로 감지

        Returns:
            각 주문의 응답 DTO 또는 에러 dict 리스트 (성공/실패 혼재 가능)
        """
        if not orders_params:
            raise ValueError("batchOrders는 최소 1건 이상이어야 합니다.")

        if len(orders_params) > MAX_BATCH_ORDERS:
            raise ValueError(
                f"batchOrders는 최대 {MAX_BATCH_ORDERS}건입니다. "
                f"받은 건수: {len(orders_params)}"
            )

        # newClientOrderId 자동 주입
        enriched: list[dict[str, Any]] = []

        for item in orders_params:
            order = dict(item)
            if "newClientOrderId" not in order:
                order["newClientOrderId"] = generate_order_id()
            enriched.append(order)

        # compact JSON (서명 payload와 전송 payload 일치 보장)
        params = {
            "batchOrders": _compact_json(enriched),
        }
        resp = await self._request("POST", "/fapi/v1/batchOrders", params)
        
        ok: list[OrderRespDto] = []
        errors: list[dict[str, Any]] = []

        for item in resp:
            if isinstance(item, dict) and isinstance(item.get("code"), int) and item["code"] < 0:
                errors.append(item)
            else:
                ok.append(OrderRespDto.from_response(item))

        return ok, errors

    async def modify_order(self, modify_params: dict[str, Any]) -> ModifyOrderRespDto:
        """
        주문 수정 (LIMIT 전용): PUT /fapi/v1/order

        주의:
          - LIMIT 주문만 지원
          - GTX 주문 수정 시 즉시 체결 가능 가격이면 자동 취소될 수 있음
          - 수정된 주문은 매칭 큐에서 재정렬됨

        but 수정 보다는 기존 주문을 취소하고 새롭게 주문 생성
        """
        resp = await self._request("PUT", "/fapi/v1/order", dict(modify_params))
        return ModifyOrderRespDto.from_response(resp)

    async def modify_batch_orders(self, modify_params_list: list[dict[str, Any]]) -> list[ModifyOrderRespDto | dict[str, Any]]:
        """
        일괄 주문 수정 (최대 5건): PUT /fapi/v1/batchOrders
        """
        if not modify_params_list:
            raise ValueError("batchModify는 최소 1건 이상이어야 합니다.")

        if len(modify_params_list) > MAX_BATCH_ORDERS:
            raise ValueError(
                f"batchModify는 최대 {MAX_BATCH_ORDERS}건입니다. "
                f"받은 건수: {len(modify_params_list)}"
            )

        params = {
            "batchOrders": _compact_json(modify_params_list),
        }
        resp = await self._request("PUT", "/fapi/v1/batchOrders", params)
        results: list[ModifyOrderRespDto | dict[str, Any]] = []
        for item in resp:
            if isinstance(item, dict) and isinstance(item.get("code"), int) and item["code"] < 0:
                results.append(item)
            else:
                results.append(ModifyOrderRespDto.from_response(item))
        return results

    async def place_algo_order(
        self,
        params: dict[str, Any],
    ) -> AlgoOrderRespDto:
        """
        Binance USD-M Futures New Algo Order.

        사용 대상:
        - STOP_MARKET
        - STOP_LIMIT 내부 매핑 STOP

        Takora 내부:
          OrderRoute.CONDITIONAL

        Binance:
          POST /fapi/v1/algoOrder
        """
        resp = await self._request(
            method="POST",
            path="/fapi/v1/algoOrder",
            params=params,
        )

        if not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected algo order response: {resp}")

        return AlgoOrderRespDto.from_response(resp)

    async def get_open_algo_orders(
        self,
        *,
        symbol: str | None = None,
        algo_id: str | int | None = None,
        algo_type: str | None = "CONDITIONAL",
    ) -> list[AlgoOrderRespDto]:
        """
        실제 거래소에서 열려있는 조건부 주문(new) 조회.

        Binance:
          GET /fapi/v1/openAlgoOrders

        조건부 주문 
        """
        params: dict[str, Any] = {}

        if symbol:
            params["symbol"] = symbol

        if algo_type is not None:
            params["algoType"] = algo_type

        if algo_id is not None:
            params["algoId"] = str(algo_id)


        resp = await self._request(
            method="GET",
            path="/fapi/v1/openAlgoOrders",
            params=params,
        )

        if not isinstance(resp, list):
            raise RuntimeError(f"Unexpected open algo orders response: {resp}")

        return [AlgoOrderRespDto.from_response(row) for row in resp]

    async def get_all_algo_orders(
        self,
        *,
        symbol: str,
        algo_id: str | int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[AlgoOrderRespDto]:
        """
        모든 상태의 조건부 주문(NEW, CANCELED, TRIGGERED, FINISHED 등) algo/conditional order 조회.

        Binance:
        GET /fapi/v1/allAlgoOrders

        사용처:
        - Redis conditional open인데 Binance openAlgoOrders에 없는 경우
        - UNKNOWN conditional order 복구
        - 최종 상태 CANCELED / EXPIRED / TRIGGERED / FINISHED 확인
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "limit": limit,
        }

        if algo_id is not None:
            params["algoId"] = str(algo_id)

        if start_time is not None:
            params["startTime"] = int(start_time)

        if end_time is not None:
            params["endTime"] = int(end_time)

        resp = await self._request(
            method="GET",
            path="/fapi/v1/allAlgoOrders",
            params=params,
        )

        if not isinstance(resp, list):
            raise RuntimeError(f"Unexpected all algo orders response: {resp}")

        return [AlgoOrderRespDto.from_response(row) for row in resp]

    # ──────────────────────────── Order Cancellation ─────────────────────────

    async def cancel_order(
        self,
        symbol: str,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> CancelOrderRespDto:
        """
        단건 취소: DELETE /fapi/v1/order

        order_id 또는 client_order_id 중 정확히 하나 필수.
        """
        if (order_id is None) == (client_order_id is None):
            raise ValueError(
                "order_id 또는 client_order_id 중 정확히 하나만 제공해야 합니다."
            )

        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        return CancelOrderRespDto.from_response(await self._request("DELETE", "/fapi/v1/order", params))

    async def cancel_batch_orders(
        self,
        symbol: str,
        order_ids: list[int] | None = None,
        client_order_ids: list[str] | None = None,
    ) -> list[CancelOrderRespDto]:
        """
        일괄 취소 (최대 10건): DELETE /fapi/v1/batchOrders

        - order_ids 또는 client_order_ids 중 정확히 하나만 제공
        - 빈 리스트 불허
        """
        if (order_ids is None) == (client_order_ids is None):
            raise ValueError(
                "order_ids 또는 client_order_ids 중 정확히 하나만 제공해야 합니다."
            )

        ids = order_ids if order_ids is not None else client_order_ids
        if not ids:
            raise ValueError("취소할 주문 ID 목록이 비어 있습니다.")

        if len(ids) > MAX_BATCH_CANCEL:
            raise ValueError(
                f"batchCancel은 최대 {MAX_BATCH_CANCEL}건입니다. "
                f"받은 건수: {len(ids)}"
            )

        params: dict[str, Any] = {"symbol": symbol}
        if order_ids is not None:
            params["orderIdList"] = _compact_json(order_ids)
        else:
            assert client_order_ids is not None
            params["origClientOrderIdList"] = _compact_json(client_order_ids)

        return [CancelOrderRespDto.from_response(row) for row in await self._request("DELETE", "/fapi/v1/batchOrders", params)]

    async def cancel_all_open_orders(self, symbol: str) -> CancelAllOpenOrdersRespDto:
        """심볼 전체 미체결 취소: DELETE /fapi/v1/allOpenOrders"""
        resp = await self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            {"symbol": symbol},
        )
        if not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected cancel all open orders response: {resp}")
        return CancelAllOpenOrdersRespDto.from_response(resp)

    async def cancel_all_algo_open_orders(
        self,
        symbol: str,
    ) -> CancelAllAlgoOpenOrdersRespDto:
        """심볼 전체 algo/conditional 미체결 취소: DELETE /fapi/v1/algoOpenOrders"""
        resp = await self._request(
            "DELETE",
            "/fapi/v1/algoOpenOrders",
            {"symbol": symbol},
        )
        if not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected cancel all algo open orders response: {resp}")
        return CancelAllAlgoOpenOrdersRespDto.from_response(resp)

    async def cancel_algo_order(
        self,
        *,
        symbol: str,
        client_algo_id: str | None = None,
        algo_id: str | int | None = None,
    ) -> CancelAlgoOrderRespDto:
        """
        DOCS: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Algo-Order

        Request Parameters
        - Name		Type		Mandatory	
        - algoId	LONG	NO	
        - clientAlgoId	STRING	NO	
        - recvWindow	LONG	NO	
        - timestamp	LONG	YES	

        Response
        ```
        {
            "algoId": 2146760,
            "clientAlgoId": "6B2I9XVcJpCjqPAJ4YoFX7",
            "code": "200",
            "msg": "success"
        }
        ```
        """
        params: dict[str, Any] = {
            "symbol": symbol,
        }

        if client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id

        if algo_id is not None:
            params["algoId"] = str(algo_id)

        if client_algo_id is None and algo_id is None:
            raise ValueError("client_algo_id or algo_id is required")

        resp = await self._request(
            method="DELETE",
            path="/fapi/v1/algoOrder",
            params=params,
        )

        if not isinstance(resp, dict):
            raise RuntimeError(f"Unexpected cancel algo order response: {resp}")

        return CancelAlgoOrderRespDto.from_response(resp)


    # ──────────────────────────── Query ──────────────────────────────────────

    async def get_open_orders(self, symbol: str | None = None) -> list[OrderRespDto]:
        """미체결 주문 조회: GET /fapi/v1/openOrders"""
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        resp = await self._request("GET", "/fapi/v1/openOrders", params)
        return [OrderRespDto.from_response(row) for row in resp]

    async def get_order(
        self,
        symbol: str,
        order_id: int | None = None,
        client_order_id: str | None = None,
    ) -> OrderRespDto:
        """
        단건 주문 조회: GET /fapi/v1/order

        503 Unknown / timeout 이후 복구에는 client_order_id 조회 권장.
        """
        if (order_id is None) == (client_order_id is None):
            raise ValueError(
                "order_id 또는 client_order_id 중 정확히 하나만 제공해야 합니다."
            )

        params: dict[str, Any] = {"symbol": symbol}

        if order_id is not None:
            params["orderId"] = order_id

        if client_order_id is not None:
            params["origClientOrderId"] = client_order_id

        resp = await self._request("GET", "/fapi/v1/order", params)
        return OrderRespDto.from_response(resp)

    async def get_all_orders(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000,
    ) -> list[OrderRespDto]:
        """
        DOCS: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/All-Orders

        HTTP Request
        - GET /fapi/v1/allOrders

        Request Parameters
        Name	Type	Mandatory	Description
        symbol	STRING	YES	
        orderId	LONG	NO	
        startTime	LONG	NO	
        endTime	LONG	NO	
        limit	INT	NO	Default 500; max 1000.
        recvWindow	LONG	NO	
        timestamp	LONG	YES

        Notice
        - 한 번에 조회할 수 있는 시간 범위(Query time period)는 최대 7일 -> 만약 시작 시간(startTime)과 종료 시간(endTime)을 직접 입력하지 않으면, 기본값으로 최근 7일간의 데이터만 조회
        - orderId(주문 ID)를 지정해서 요청하면, 그 ID와 같거나 그 이후에 생성된 주문들(>= orderId)만 가져옴 -> 지정을 안 하면 가장 최근 주문들 위주로 먼저 리턴
        - 주문 상태가 취소(CANCELED) 또는 만료(EXPIRED) 되었으면서 + 단 1건도 체결되지 않았고(NO filled trade) + 주문 생성 후 3일이 지난 경우 (AND 주문 상태와 관계없이 주문이 생성된 지 90일이 지난 경우 조회 안됨
        """
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "limit": limit,
        }

        if order_id is not None:
            params["orderId"] = int(order_id)

        if start_time is not None:
            params["startTime"] = int(start_time)

        if end_time is not None:
            params["endTime"] = int(end_time)

        return [OrderRespDto.from_response(row) for row in await self._request("GET", "/fapi/v1/allOrders", params)]

    # ──────────────────────────── User Data Stream ───────────────────────────

    async def create_listen_key(self) -> ListenKeyRespDto:
        """listenKey 발급: POST /fapi/v1/listenKey"""
        data = await self._request("POST", "/fapi/v1/listenKey", sign=False)
        return ListenKeyRespDto.from_response(data)

    async def keepalive_listen_key(self, listen_key: str) -> None:
        """listenKey keepalive: PUT /fapi/v1/listenKey"""
        await self._request(
            "PUT",
            "/fapi/v1/listenKey",
            {"listenKey": listen_key},
            sign=False,
        )

    async def close_listen_key(self, listen_key: str) -> None:
        """listenKey 해제: DELETE /fapi/v1/listenKey"""
        await self._request(
            "DELETE",
            "/fapi/v1/listenKey",
            {},
            sign=False,
        )

    # ──────────────────────────── Account ────────────────────────────────────

    async def get_account_info(self) -> AccountInfoRespDto:
        """
        계정 정보 조회: GET /fapi/v3/account

        DOCS: https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V3
        """
        resp = await self._request("GET", "/fapi/v3/account")
        return AccountInfoRespDto.from_response(resp)

    async def get_symbol_price_ticker(self, symbol: str) -> SymbolPriceTickerRespDto:
        """
        현재가 조회: GET /fapi/v1/ticker/price

        DOCS: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Symbol-Price-Ticker

        Response Example

        {
            "symbol": "BTCUSDT",
            "price": "6000.01",
            "time": 1589437530011   // Transaction time
        }

        or

        [
            {
                "symbol": "BTCUSDT",
                "price": "6000.01",
                "time": 1589437530011   // Transaction time
            }
        ]

        """
        resp = await self._request(
            method="GET",
            path="/fapi/v1/ticker/price",
            params={"symbol": symbol},
            sign=False,
        )

        if isinstance(resp, dict):
            return SymbolPriceTickerRespDto.from_response(resp)

        if isinstance(resp, list):
            return SymbolPriceTickerRespDto.from_response(resp[0])

        raise RuntimeError(f"Unexpected ticker response: {resp}")


    async def get_position_risk_v3(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> list[PositionRiskRespDto]:
        """
        Binance USD-M Futures Position Information V3.

        GET /fapi/v3/positionRisk
        Weight: 5
        """
        params: dict[str, Any] = {}

        if symbol:
            params["symbol"] = symbol.upper()

        resp = await self._request(
            method="GET",
            path="/fapi/v3/positionRisk",
            params=params,
        )

        if not isinstance(resp, list):
            raise RuntimeError(f"Unexpected positionRisk response: {resp}")

        return [PositionRiskRespDto.from_response(row) for row in resp]

    async def get_symbol_config(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> list[SymbolConfigRespDto]:
        """
        Binance USD-M Futures Symbol Configuration.

        GET /fapi/v1/symbolConfig
        Weight: 5

        v3 positionRisk에서 제거된 leverage, marginType 등 설정값을 조회.
        """
        params: dict[str, Any] = {}

        if symbol:
            params["symbol"] = symbol.upper()

        resp = await self._request(
            method="GET",
            path="/fapi/v1/symbolConfig",
            params=params,
        )

        if not isinstance(resp, list):
            raise RuntimeError(f"Unexpected symbolConfig response: {resp}")

        return [SymbolConfigRespDto.from_response(row) for row in resp]

    # ──────────────────────────── Leverage ────────────────────────────────────
    async def change_leverage(
        self,
        *,
        symbol: str,
        leverage: int,
    ) -> ChangeLeverageRespDto:
        """
        Binance USD-M Futures symbol initial leverage 변경.

        Endpoint:
        POST /fapi/v1/leverage

        주의:
        - leverage는 주문별 파라미터가 아니라 symbol/account 설정이다.
        - 주문 전에 원하는 leverage로 먼저 설정해야 한다.
        """
        
        if leverage < 1 or leverage > 125:
            raise ValueError(f"leverage must be between 1 and 125: {leverage}")

        if leverage > settings.binance_max_leverage:
            raise BinanceLeveragePolicyError(
                requested=leverage,
                max_allowed=int(settings.binance_max_leverage),
            )

        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "leverage": int(leverage),
        }

        return ChangeLeverageRespDto.from_response(await self._request(
            method="POST",
            path="/fapi/v1/leverage",
            params=params,
        ))
