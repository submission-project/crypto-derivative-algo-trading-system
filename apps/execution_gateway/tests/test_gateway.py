"""
ExecutionGateway 단위 테스트: Binance·Redis를 MagicMock으로 대체해
상태 전이·에러 매핑·배치 분기만 빠르게 검증한다.

실제 Testnet·Redis와의 연동은 `test_gateway_integration.py`(pytest -m integration)를 본다.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.gateway import ExecutionGateway
from execution_gateway.gateway.cancellation_service import (
    CancelOrderSkipped,
    CancelSkipReason,
)
from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeErrorCategory,
    ExchangeCapabilities,
    ExchangeLeverageResult,
    ExchangeOrderAck,
    ExchangeOrderReject,
)
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from schemas.order import Order, OrderRequest, OrderSource, OrderStatus, RejectReason, PositionAction
from schemas.market import Exchange, MarketType
from common.logging import setup_logger


logger = setup_logger(__name__)


def _order_ack(order: Order, *, exchange_order_id: str = "9999") -> ExchangeOrderAck:

    client_order_id = order.client_order_id or order.order_id
    assert client_order_id is not None
    
    return ExchangeOrderAck(
        exchange=order.exchange,
        market_type=order.market_type,
        symbol=order.symbol,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        raw={"orderId": int(exchange_order_id)},
    )


def _order_reject(
    order: Order,
    *,
    code: int = -2010,
    message: str = "Order would immediately match and take.",
) -> ExchangeOrderReject:
    client_order_id = order.client_order_id or order.order_id
    assert client_order_id is not None
    return ExchangeOrderReject(
        exchange=order.exchange,
        market_type=order.market_type,
        symbol=order.symbol,
        client_order_id=client_order_id,
        reject_reason=RejectReason.EXCHANGE_REJECTED,
        message=message,
        code=code,
        raw={"code": code, "msg": message},
    )


def make_limit_req(
    *,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    price: str = "60000",
    quantity: str = "0.1",
) -> OrderRequest:
    return OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type="LIMIT",
        time_in_force="GTC",
        price=price,
        quantity=quantity,
        position_action=PositionAction.OPEN,
    )


@pytest.fixture
def mock_adapter():
    adapter = MagicMock(spec=BinanceRestAdapter)
    adapter.change_leverage = AsyncMock(
        return_value={"leverage": 20, "symbol": "BTCUSDT", "maxNotionalValue": "0"}
    )
    return adapter


@pytest.fixture
def mock_repo():
    repo = MagicMock(spec=OrderStateRedisRepository)

    async def mock_update_status(order_id, status, updated_ts, **kwargs):
        return {
            "order_id": order_id,
            "source": OrderSource.MANUAL.value,
            "exchange": Exchange.BINANCE.value,
            "market_type": MarketType.PERP.value,
            "symbol": "BTCUSDT",
            "side": "BUY",
            "order_type": "LIMIT",
            "time_in_force": "GTC",
            "price": "60000",
            "quantity": "0.1",
            "status": status.value if hasattr(status, "value") else status,
            "position_action": PositionAction.OPEN.value,
            "created_ts": 1234567890,
            "updated_ts": updated_ts,
            **kwargs,
        }

    repo.save = AsyncMock()
    repo.update_status = AsyncMock(side_effect=mock_update_status)
    repo.get = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_state_service():
    service = MagicMock()

    async def transition_order(*, current_order, updated_order):
        updated = updated_order.model_copy(deep=True)
        updated.version = current_order.version + 1
        return updated

    service.create_order = AsyncMock(side_effect=lambda order: order)
    service.transition_order = AsyncMock(side_effect=transition_order)
    service.load_order = AsyncMock(return_value=None)
    return service


@pytest.fixture
def mock_execution_client():
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities(
        supports_batch_order=True,
        max_batch_order_size=5,
    )
    client.place_order = AsyncMock(side_effect=lambda order: _order_ack(order))
    client.place_batch_orders = AsyncMock(
        side_effect=lambda orders: [_order_ack(order, exchange_order_id=str(1001 + i)) for i, order in enumerate(orders)]
    )
    client.change_leverage = AsyncMock(
        return_value=ExchangeLeverageResult(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            leverage=20,
            raw={"leverage": 20, "symbol": "BTCUSDT", "maxNotionalValue": "0"},
        )
    )
    return client


@pytest.fixture
def gateway(mock_adapter, mock_repo, mock_state_service, mock_execution_client):
    registry = ExchangeExecutionClientRegistry()
    registry.register(mock_execution_client)
    return ExecutionGateway(
        # adapter=mock_adapter,
        state_repo=mock_repo,
        state_service=mock_state_service,
        exchange_clients=registry,
    )


@pytest.mark.asyncio
async def test_submit_order_success(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_order = AsyncMock(
        side_effect=lambda order: _order_ack(order, exchange_order_id="9999")
    )

    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        price="60000",
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )

    order = await gateway.submit_order(req)

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.exchange_order_id == "9999"

    assert mock_state_service.create_order.call_count == 1
    assert mock_repo.save.call_count == 0

    assert mock_state_service.transition_order.call_count == 2
    assert mock_execution_client.place_order.call_count == 1


@pytest.mark.asyncio
async def test_submit_batch_orders_success(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_batch_orders = AsyncMock(
        side_effect=lambda orders: [
            _order_ack(orders[0], exchange_order_id="1001"),
            _order_ack(orders[1], exchange_order_id="1002"),
        ]
    )

    req1 = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        price="60001",
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )
    req2 = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        price="60002",
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[req1, req2],
    )

    assert len(orders) == 2
    assert orders[0].status == OrderStatus.ACKNOWLEDGED
    assert orders[1].status == OrderStatus.ACKNOWLEDGED
    assert mock_state_service.create_order.call_count == 2
    assert mock_repo.save.call_count == 0
    assert mock_execution_client.place_batch_orders.call_count == 1


@pytest.mark.asyncio
async def test_submit_order_rejected_by_exchange(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_order = AsyncMock(
        side_effect=ExchangeApiError(
            exchange=Exchange.BINANCE,
            category=ExchangeErrorCategory.EXCHANGE_REJECTED,
            message="Order would immediately match and take.",
            code=-2010,
        )
    )

    req = make_limit_req(price="60000")

    order = await gateway.submit_order(req)

    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason == RejectReason.EXCHANGE_REJECTED

    # 최초 생성은 state_service가 담당
    assert mock_state_service.create_order.call_count == 1

    # Gateway가 Redis에 직접 저장하지 않아야 함
    assert mock_repo.save.call_count == 0
    assert mock_repo.update_status.call_count == 0

    # 상태 전이는 state_service가 담당
    assert mock_state_service.transition_order.call_count == 2
    assert mock_execution_client.place_order.call_count == 1

    transition_calls = mock_state_service.transition_order.call_args_list

    # 첫 번째 전이: PENDING_NEW -> SUBMITTED
    first_call = transition_calls[0]
    first_current_order = first_call.kwargs["current_order"]
    first_updated_order = first_call.kwargs["updated_order"]

    assert first_current_order.status == OrderStatus.PENDING_NEW
    assert first_updated_order.status == OrderStatus.SUBMITTED

    # 두 번째 전이: SUBMITTED -> REJECTED
    second_call = transition_calls[1]
    second_current_order = second_call.kwargs["current_order"]
    second_updated_order = second_call.kwargs["updated_order"]

    assert second_current_order.status == OrderStatus.SUBMITTED
    assert second_updated_order.status == OrderStatus.REJECTED
    assert second_updated_order.reject_reason == RejectReason.EXCHANGE_REJECTED
    assert second_updated_order.exchange_error_code == -2010
    assert (
        second_updated_order.detail_msg
        and "immediately match" in second_updated_order.detail_msg.lower()
    )


@pytest.mark.asyncio
async def test_submit_order_insufficient_balance(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_order = AsyncMock(
        side_effect=ExchangeApiError(
            exchange=Exchange.BINANCE,
            category=ExchangeErrorCategory.INSUFFICIENT_BALANCE,
            message="Margin is insufficient.",
            code=-2019,
        )
    )

    req = make_limit_req(price="60000")

    order = await gateway.submit_order(req)

    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason == RejectReason.INSUFFICIENT_BALANCE
    assert order.exchange_error_code == -2019
    assert order.detail_msg and "margin" in order.detail_msg.lower()

    assert mock_state_service.create_order.call_count == 1
    assert mock_repo.save.call_count == 0

    assert mock_state_service.transition_order.call_count == 2
    assert mock_execution_client.place_order.call_count == 1


@pytest.mark.asyncio
async def test_submit_order_unknown_execution(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_order = AsyncMock(
        side_effect=ExchangeApiError(
            exchange=Exchange.BINANCE,
            category=ExchangeErrorCategory.UNKNOWN_EXECUTION,
            message="503 Unknown error",
            code=503,
            status_code=503,
        )
    )

    req = make_limit_req(price="60000")

    order = await gateway.submit_order(req)

    assert order.status == OrderStatus.UNKNOWN
    assert order.reject_reason == RejectReason.UNKNOWN_EXECUTION

    assert mock_state_service.create_order.call_count == 1
    assert mock_repo.save.call_count == 0

    assert mock_state_service.transition_order.call_count == 2
    assert mock_execution_client.place_order.call_count == 1

    first_call = mock_state_service.transition_order.call_args_list[0]
    second_call = mock_state_service.transition_order.call_args_list[1]

    assert first_call.kwargs["current_order"].status == OrderStatus.PENDING_NEW
    assert first_call.kwargs["updated_order"].status == OrderStatus.SUBMITTED

    assert second_call.kwargs["current_order"].status == OrderStatus.SUBMITTED
    assert second_call.kwargs["updated_order"].status == OrderStatus.UNKNOWN


@pytest.mark.asyncio
async def test_submit_order_internal_error(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_order = AsyncMock(
        side_effect=RuntimeError("unexpected error")
    )

    req = make_limit_req(price="60000")

    order = await gateway.submit_order(req)

    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason == RejectReason.INTERNAL_ERROR
    assert order.exchange_error_code is None
    assert order.detail_msg == "unexpected error"

    assert mock_state_service.create_order.call_count == 1
    assert mock_repo.save.call_count == 0

    assert mock_state_service.transition_order.call_count == 2
    assert mock_execution_client.place_order.call_count == 1


@pytest.mark.asyncio
async def test_submit_batch_orders_empty(gateway, mock_adapter, mock_repo):
    mock_adapter.place_batch_orders = AsyncMock()

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[],
    )

    assert orders == []
    assert mock_adapter.place_batch_orders.call_count == 0
    assert mock_repo.save.call_count == 0
    assert mock_repo.update_status.call_count == 0


@pytest.mark.asyncio
async def test_submit_batch_orders_partial_failure(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_batch_orders = AsyncMock(
        side_effect=lambda orders: [
            _order_ack(orders[0], exchange_order_id="1001"),
            _order_reject(orders[1]),
        ]
    )

    req1 = make_limit_req(price="60001")
    req2 = make_limit_req(price="60002")

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[req1, req2],
    )

    assert len(orders) == 2

    assert orders[0].status == OrderStatus.ACKNOWLEDGED
    assert orders[0].exchange_order_id == "1001"

    assert orders[1].status == OrderStatus.REJECTED
    assert orders[1].reject_reason == RejectReason.EXCHANGE_REJECTED
    assert orders[1].exchange_error_code == -2010
    assert (
        orders[1].detail_msg
        and "immediately match" in orders[1].detail_msg.lower()
    )

    assert mock_state_service.create_order.call_count == 2
    assert mock_repo.save.call_count == 0
    assert mock_execution_client.place_batch_orders.call_count == 1


@pytest.mark.asyncio
async def test_submit_batch_orders_unknown_execution(
    gateway, mock_execution_client, mock_repo, mock_state_service
):
    mock_execution_client.place_batch_orders = AsyncMock(
        side_effect=ExchangeApiError(
            exchange=Exchange.BINANCE,
            category=ExchangeErrorCategory.UNKNOWN_EXECUTION,
            message="503 Unknown error",
            code=503,
            status_code=503,
        )
    )

    req1 = make_limit_req(price="60001")
    req2 = make_limit_req(price="60002")

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[req1, req2],
    )

    assert len(orders) == 2
    assert orders[0].status == OrderStatus.UNKNOWN
    assert orders[1].status == OrderStatus.UNKNOWN

    assert orders[0].reject_reason == RejectReason.UNKNOWN_EXECUTION
    assert orders[1].reject_reason == RejectReason.UNKNOWN_EXECUTION

    assert mock_state_service.create_order.call_count == 2
    assert mock_repo.save.call_count == 0
    assert mock_execution_client.place_batch_orders.call_count == 1


@pytest.mark.asyncio
async def test_submit_batch_orders_response_length_short_marks_missing_unknown(
    gateway,
    mock_execution_client,
    mock_repo,
):
    mock_execution_client.place_batch_orders = AsyncMock(
        side_effect=lambda orders: [
            _order_ack(orders[0], exchange_order_id="1001"),
        ]
    )

    req1 = make_limit_req(price="60001")
    req2 = make_limit_req(price="60002")

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[req1, req2],
    )

    assert len(orders) == 2

    assert orders[0].status == OrderStatus.ACKNOWLEDGED
    assert orders[0].exchange_order_id == "1001"

    assert orders[1].status == OrderStatus.UNKNOWN
    assert orders[1].reject_reason == RejectReason.UNKNOWN_EXECUTION

    assert mock_execution_client.place_batch_orders.call_count == 1


@pytest.mark.asyncio
async def test_cancel_order_success_without_local_order(
    gateway, mock_adapter, mock_repo
):
    mock_repo.get = AsyncMock(return_value=None)
    mock_adapter.cancel_order = AsyncMock(
        return_value={
            "symbol": "BTCUSDT",
            "clientOrderId": "ORD-TEST",
            "status": "CANCELED",
        }
    )

    with pytest.raises(CancelOrderSkipped) as exc:
        await gateway.cancel_order("ORD-TEST")

    assert exc.value.reason == CancelSkipReason.LOCAL_ORDER_NOT_FOUND
    assert exc.value.order_id == "ORD-TEST"
    assert mock_repo.update_status.call_count == 0
    mock_adapter.cancel_order.assert_not_awaited()
