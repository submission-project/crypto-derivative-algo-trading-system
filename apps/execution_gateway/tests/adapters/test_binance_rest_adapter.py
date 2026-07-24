import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx
import pytest_asyncio

from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceRestAdapter,
    BinanceKeyType,
    BinanceRateLimitError,
    BinanceIpBanError,
    BinanceUnknownExecutionError,
    BinanceServiceUnavailableError,
    BinanceInternalRetryableError,
    BinanceSystemThrottleError,
    BinanceNetworkError,
)
from common.logging import setup_logger

logger = setup_logger(__name__)

# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def adapter() -> BinanceRestAdapter:
    adapter = BinanceRestAdapter(
        base_url="https://demo-fapi.binance.com",
        api_key="test_key",
        key_type=BinanceKeyType.HMAC,
        api_secret="test_secret",
    )
    try:
        yield adapter
    finally:
        await adapter.close()


def make_mock_response(json_data, status_code=200, headers=None):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.text = json.dumps(json_data) if not isinstance(json_data, str) else json_data
    resp.status_code = status_code
    resp.headers = httpx.Headers(headers or {})
    return resp

@pytest.mark.stable
@pytest.mark.asyncio
async def test_place_order_injects_client_order_id(adapter:BinanceRestAdapter):
    """newClientOrderId가 없으면 자동 생성되는지 확인."""
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {"orderId": 123, "clientOrderId": "auto"}
        )

        await adapter.place_regular_order({"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT"})

        call_kwargs = mock_req.call_args
        sent_params = call_kwargs.kwargs.get("params", {})
        assert "newClientOrderId" in sent_params


@pytest.mark.asyncio
async def test_place_order_keeps_existing_client_order_id(adapter:BinanceRestAdapter):
    """newClientOrderId가 이미 있으면 유지되는지 확인."""
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response({"orderId": 456})

        await adapter.place_regular_order(
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "newClientOrderId": "my_custom_id",
            }
        )

        call_kwargs = mock_req.call_args
        sent_params = call_kwargs.kwargs.get("params", {})
        assert sent_params.get("newClientOrderId") == "my_custom_id"


# ── batch 주문 ──


@pytest.mark.asyncio
async def test_place_batch_orders_compact_json(adapter):
    """batchOrders JSON이 compact (공백 없음)인지 확인."""
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response([{"orderId": 1}])

        await adapter.place_batch_orders(
            [
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "price": "60000",
                    "quantity": "0.1",
                }
            ]
        )

        call_kwargs = mock_req.call_args
        sent_params = call_kwargs.kwargs.get("params", {})
        batch_json = sent_params.get("batchOrders", "")
        # compact: ","와 ":" 사이에 공백이 없어야 함
        assert " " not in batch_json

# ── HTTP 상태코드 에러 분기 ──


@pytest.mark.asyncio
async def test_http_429_raises_rate_limit_error(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {}, status_code=429, headers={"Retry-After": "10"}
        )
        with pytest.raises(BinanceRateLimitError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_http_418_raises_ip_ban_error(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response({}, status_code=418)
        with pytest.raises(BinanceIpBanError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_http_503_service_unavailable(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {"code": -1, "msg": "Service Unavailable."},
            status_code=503,
        )

        with pytest.raises(BinanceServiceUnavailableError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_http_503_internal_retryable(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {
                "code": -1,
                "msg": "Internal error; unable to process your request. Please try again.",
            },
            status_code=503,
        )

        with pytest.raises(BinanceInternalRetryableError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_http_503_system_throttle(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {
                "code": -1008,
                "msg": "Request throttled by system-level protection. Reduce-only/close-position orders are exempt. Please try again.",
            },
            status_code=503,
        )

        with pytest.raises(BinanceSystemThrottleError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


# ── cancel_batch_orders 파라미터 검증 ──
@pytest.mark.asyncio
async def test_cancel_batch_orders_raises_when_both_ids_provided(adapter:BinanceRestAdapter):
    with pytest.raises(ValueError, match="정확히 하나"):
        await adapter.cancel_batch_orders(
            "BTCUSDT", order_ids=[1, 2], client_order_ids=["a", "b"]
        )


@pytest.mark.asyncio
async def test_cancel_batch_orders_raises_when_no_ids_provided(adapter:BinanceRestAdapter):
    with pytest.raises(ValueError, match="정확히 하나"):
        await adapter.cancel_batch_orders("BTCUSDT")


@pytest.mark.asyncio
async def test_cancel_batch_orders_raises_when_empty(adapter:BinanceRestAdapter):
    with pytest.raises(ValueError, match="비어 있습니다"):
        await adapter.cancel_batch_orders("BTCUSDT", order_ids=[])



@pytest.mark.asyncio
async def test_http_408_mutating_order_raises_unknown(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {"code": -1, "msg": "Backend timeout"},
            status_code=408,
        )

        with pytest.raises(BinanceUnknownExecutionError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_http_408_non_mutating_raises_network_error(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = make_mock_response(
            {"code": -1, "msg": "Backend timeout"},
            status_code=408,
        )

        with pytest.raises(BinanceNetworkError):
            await adapter.get_account_info()


@pytest.mark.asyncio
async def test_timeout_mutating_order_raises_unknown(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = httpx.TimeoutException("timeout")

        with pytest.raises(BinanceUnknownExecutionError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})


@pytest.mark.asyncio
async def test_timeout_non_mutating_raises_network_error(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = httpx.TimeoutException("timeout")

        with pytest.raises(BinanceNetworkError):
            await adapter.get_account_info()


@pytest.mark.asyncio
async def test_network_error_mutating_order_raises_unknown(adapter:BinanceRestAdapter):
    with patch.object(adapter._client, "request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = httpx.ConnectError("connection reset")

        with pytest.raises(BinanceUnknownExecutionError):
            await adapter.place_regular_order({"symbol": "BTCUSDT"})
