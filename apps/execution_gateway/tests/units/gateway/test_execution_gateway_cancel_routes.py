from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeCancelResult,
    ExchangeCapabilities,
    ExchangeErrorCategory,
)
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.gateway.cancellation_service import (
    CancelOrderSkipped,
    CancelSkipReason,
)
from execution_gateway.gateway.dto.cancel_service_resp import (
    BatchCancelResultStatus,
    CancelBatchOrderResp,
)
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    RejectReason,
)
from schemas.position import PositionSide

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState, BinanceOrderState


_NOW_MS = lambda: time.time_ns() // 1_000_000


class DummyRateLimiter:
    async def acquire_costs(self, **kwargs):
        return None

    async def acquire_request_weight(self, weight: int = 1):
        return None

    async def acquire_order_slot(self, count: int = 1):
        return None

    async def acquire_single_order(self):
        return None

    async def acquire_batch_orders(self):
        return None


class DummyStateRepo:
    async def get(self, order_id: str):
        return None


class DummyStateService:
    def __init__(self, order: Order | None) -> None:
        self.order = order
        self.transition_calls: list[tuple[Order, Order]] = []

    async def load_order(self, *, order_id: str) -> Order | None:
        if self.order and self.order.order_id == order_id:
            return self.order
        return None

    async def load_order_from_repo(self, order_id: str) -> Order | None:
        return await self.load_order(order_id=order_id)

    async def load_order_from_postgres(self, order_id: str) -> Order | None:
        return await self.load_order(order_id=order_id)

    async def list_open_orders_by_symbol(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        refresh_projection: bool = True,
    ) -> list[Order]:
        if (
            self.order
            and self.order.exchange == exchange
            and self.order.market_type == market_type
            and self.order.symbol == symbol
            and self.order.status not in {
                OrderStatus.CANCELLED,
                OrderStatus.FILLED,
                OrderStatus.REJECTED,
                OrderStatus.EXPIRED,
            }
        ):
            return [self.order]
        return []

    async def transition_order(
        self,
        *,
        current_order: Order,
        updated_order: Order,
    ) -> Order:
        self.transition_calls.append((current_order, updated_order))

        persisted = updated_order.model_copy(deep=True)
        persisted.version = current_order.version + 1

        self.order = persisted
        return persisted

    async def create_order(self, order: Order) -> Order:
        self.order = order
        return order


def make_regular_order(
    *,
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 3,
) -> Order:
    now = _NOW_MS()

    return Order(
        order_id="ORD-REG-CANCEL",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        order_route=OrderRoute.REGULAR,
        quantity="0.01",
        price="50000",
        time_in_force="GTC",
        reduce_only=False,
        close_position=False,
        client_order_id="ORD-REG-CANCEL",
        exchange_order_id="12345",
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=status,
        created_ts=now,
        updated_ts=now,
        version=version,
    )


def make_conditional_order(
    *,
    conditional_status: ConditionalStatus | None = ConditionalStatus.NEW,
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 3,
) -> Order:
    now = _NOW_MS()

    return Order(
        order_id="ORD-COND-CANCEL",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        price=None,
        trigger_price="59000",
        reduce_only=True,
        close_position=False,
        client_conditional_id="ORD-COND-CANCEL",
        exchange_conditional_id="98765",
        conditional_status=conditional_status,
        exchange_conditional_status=(
            conditional_status.value if conditional_status else None
        ),
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=status,
        created_ts=now,
        updated_ts=now,
        version=version,
    )


def _make_mock_client(
    *,
    cancel_return: ExchangeCancelResult | None = None,
    cancel_side_effect: Exception | None = None,
) -> MagicMock:
    """Exchange-neutral mock client registered for BINANCE/PERP."""
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities(
        supports_batch_cancel=True,
        max_batch_cancel_size=10,
        supports_cancel_all=True,
    )
    if cancel_side_effect is not None:
        client.cancel_order = AsyncMock(side_effect=cancel_side_effect)
    else:
        client.cancel_order = AsyncMock(return_value=cancel_return)
    client.cancel_batch_orders = AsyncMock(return_value=[cancel_return])
    client.cancel_all_regular_open_orders = AsyncMock(return_value=cancel_return)
    client.cancel_all_conditional_open_orders = AsyncMock(return_value=cancel_return)
    client.close = AsyncMock()
    return client


def make_gateway(
    *,
    order: Order | None,
    adapter: MagicMock = None,
    mock_client: MagicMock | None = None,
) -> tuple[ExecutionGateway, DummyStateService]:
    state_service = DummyStateService(order)

    registry = ExchangeExecutionClientRegistry()
    if mock_client is not None:
        registry.register(mock_client)

    gateway = ExecutionGateway(
        # pyrefly: ignore [bad-argument-type]
        state_repo=DummyStateRepo(),
        # pyrefly: ignore [bad-argument-type]
        state_service=state_service,
        exchange_clients=registry,
    )

    return gateway, state_service


@pytest.mark.asyncio
async def test_cancel_regular_order_uses_regular_cancel_endpoint() -> None:
    raw_resp = {
        "orderId": 12345,
        "clientOrderId": "ORD-REG-CANCEL",
        "status": BinanceOrderState.canceled,
    }
    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id="ORD-REG-CANCEL",
        exchange_order_id="12345",
        status=OrderStatus.CANCELLED,
        raw_status=BinanceOrderState.canceled,
        raw=raw_resp,
    )

    mock_client = _make_mock_client(cancel_return=cancel_result)
    adapter = MagicMock()

    order = make_regular_order()
    gateway, state_service = make_gateway(
        order=order, adapter=adapter, mock_client=mock_client,
    )

    assert order.order_id

    resp = await gateway.cancel_order(order_id=order.order_id)

    assert resp.raw["status"] == BinanceOrderState.canceled
    assert resp.status == OrderStatus.CANCELLED

    mock_client.cancel_order.assert_awaited_once()
    called_order = mock_client.cancel_order.call_args[0][0]
    assert called_order.order_id == order.order_id
    assert called_order.order_route == OrderRoute.REGULAR

    assert state_service.order

    assert state_service.order.status == OrderStatus.CANCELLED
    assert state_service.order.cancelled_ts is not None
    assert state_service.order.raw_exchange_response == raw_resp


@pytest.mark.asyncio
async def test_cancel_conditional_order_uses_algo_cancel_endpoint() -> None:
    raw_resp = {
        "algoId": 98765,
        "clientAlgoId": "ORD-COND-CANCEL",
        "algoStatus": BinanceConditionalOrderState.canceled,
    }
    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_conditional_id="ORD-COND-CANCEL",
        exchange_conditional_id="98765",
        conditional_status=ConditionalStatus.CANCELLED,
        raw_status=BinanceConditionalOrderState.canceled,
        raw=raw_resp,
    )

    mock_client = _make_mock_client(cancel_return=cancel_result)
    adapter = MagicMock()

    order = make_conditional_order()
    gateway, state_service = make_gateway(
        order=order, adapter=adapter, mock_client=mock_client,
    )

    assert order.order_id

    resp = await gateway.cancel_order(order_id=order.order_id)

    assert resp.raw["algoStatus"] == BinanceConditionalOrderState.canceled
    assert resp.conditional_status == ConditionalStatus.CANCELLED

    mock_client.cancel_order.assert_awaited_once()
    called_order = mock_client.cancel_order.call_args[0][0]
    assert called_order.order_id == order.order_id
    assert called_order.order_route == OrderRoute.CONDITIONAL

    assert state_service.order

    assert state_service.order.status == OrderStatus.CANCELLED
    assert state_service.order.conditional_status == ConditionalStatus.CANCELLED
    assert state_service.order.exchange_conditional_status == BinanceConditionalOrderState.canceled
    assert state_service.order.cancelled_ts is not None
    assert state_service.order.raw_exchange_response == raw_resp


@pytest.mark.asyncio
async def test_cancel_conditional_order_unknown_execution_marks_unknown() -> None:
    cancel_error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.UNKNOWN_EXECUTION,
        message="503 Unknown error",
        code=503,
        status_code=503,
    )
    mock_client = _make_mock_client(cancel_side_effect=cancel_error)
    adapter = MagicMock()

    order = make_conditional_order()
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    with pytest.raises(ExchangeApiError) as exc_info:
        assert order.order_id
        await gateway.cancel_order(order_id=order.order_id)

    assert state_service.order

    assert exc_info.value.category == ExchangeErrorCategory.UNKNOWN_EXECUTION
    assert state_service.order.status == OrderStatus.UNKNOWN
    assert state_service.order.reject_reason == RejectReason.UNKNOWN_EXECUTION

    statuses = [updated.status for _, updated in state_service.transition_calls]
    assert OrderStatus.PENDING_CANCEL in statuses
    assert OrderStatus.UNKNOWN in statuses


@pytest.mark.asyncio
async def test_cancel_conditional_order_exchange_error_rolls_back_to_previous_status() -> None:
    cancel_error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.EXCHANGE_REJECTED,
        message="Unknown order sent.",
        code=-2011,
    )
    mock_client = _make_mock_client(cancel_side_effect=cancel_error)
    adapter = MagicMock()

    order = make_conditional_order(status=OrderStatus.ACKNOWLEDGED)
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    assert state_service.order
    assert order.order_id

    with pytest.raises(ExchangeApiError) as exc_info:
        await gateway.cancel_order(order_id=order.order_id)

    assert exc_info.value.category == ExchangeErrorCategory.EXCHANGE_REJECTED
    
    assert state_service.order.status == OrderStatus.ACKNOWLEDGED

    statuses = [updated.status for _, updated in state_service.transition_calls]
    assert OrderStatus.PENDING_CANCEL in statuses
    assert statuses[-1] == OrderStatus.ACKNOWLEDGED


@pytest.mark.asyncio
async def test_cancel_regular_order_unknown_execution_marks_unknown() -> None:
    cancel_error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.UNKNOWN_EXECUTION,
        message="503 Unknown error",
        code=503,
        status_code=503,
    )
    mock_client = _make_mock_client(cancel_side_effect=cancel_error)
    adapter = MagicMock()

    order = make_regular_order()
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    assert state_service.order
    assert order.order_id

    with pytest.raises(ExchangeApiError):
        await gateway.cancel_order(order_id=order.order_id)

    assert state_service.order.status == OrderStatus.UNKNOWN
    assert state_service.order.reject_reason == RejectReason.UNKNOWN_EXECUTION


@pytest.mark.asyncio
async def test_cancel_regular_order_exchange_error_rolls_back_to_previous_status() -> None:
    cancel_error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.EXCHANGE_REJECTED,
        message="Unknown order sent.",
        code=-2011,
    )
    mock_client = _make_mock_client(cancel_side_effect=cancel_error)
    adapter = MagicMock()

    order = make_regular_order(status=OrderStatus.ACKNOWLEDGED)
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    assert state_service.order
    assert order.order_id

    with pytest.raises(ExchangeApiError):
        await gateway.cancel_order(order_id=order.order_id)

    assert state_service.order.status == OrderStatus.ACKNOWLEDGED


@pytest.mark.asyncio
async def test_cancel_terminal_order_is_skipped() -> None:
    adapter = MagicMock()

    order = make_regular_order(status=OrderStatus.FILLED)
    gateway, _ = make_gateway(order=order, adapter=adapter)

    assert order.order_id

    with pytest.raises(CancelOrderSkipped) as exc:
        await gateway.cancel_order(order_id=order.order_id)

    assert exc.value.reason == CancelSkipReason.ALREADY_FILLED
    assert exc.value.status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_cancel_missing_order_is_skipped() -> None:
    adapter = MagicMock()

    gateway, _ = make_gateway(order=None, adapter=adapter)

    with pytest.raises(CancelOrderSkipped) as exc:
        await gateway.cancel_order(order_id="MISSING")

    assert exc.value.reason == CancelSkipReason.LOCAL_ORDER_NOT_FOUND
    assert exc.value.order_id == "MISSING"


@pytest.mark.asyncio
async def test_cancel_triggered_conditional_order_is_skipped() -> None:
    adapter = MagicMock()

    order = make_conditional_order(
        conditional_status=ConditionalStatus.TRIGGERED,
    )
    order.triggered_order_id = "ACTUAL-ORDER-123"

    gateway, state_service = make_gateway(order=order, adapter=adapter)

    assert order.order_id

    with pytest.raises(CancelOrderSkipped) as exc:
        await gateway.cancel_order(order_id=order.order_id)

    assert exc.value.reason == CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE
    assert exc.value.status == order.status
    assert exc.value.conditional_status == ConditionalStatus.TRIGGERED
    assert exc.value.triggered_order_id == "ACTUAL-ORDER-123"

    assert state_service.order
    assert state_service.order.conditional_status == ConditionalStatus.TRIGGERED


@pytest.mark.asyncio
async def test_cancel_batch_orders_rejects_conditional_order() -> None:
    adapter = MagicMock()
    adapter.cancel_batch_orders = AsyncMock()
    adapter.cancel_algo_order = AsyncMock()

    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        raw={},
    )
    mock_client = _make_mock_client(cancel_return=cancel_result)

    order = make_conditional_order()
    gateway, _ = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    assert order.order_id
    resp = await gateway.cancel_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        order_ids=[order.order_id],
    )

    assert len(resp) == 1
    item = resp[0]

    assert isinstance(item, CancelBatchOrderResp)
    assert item.order_id == order.order_id
    assert item.result == BatchCancelResultStatus.SKIPPED
    assert item.reason == CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE
    assert item.status == order.status
    assert item.conditional_status == order.conditional_status
    assert item.message == "Batch cancel supports only regular orders."

    adapter.cancel_batch_orders.assert_not_awaited()
    adapter.cancel_algo_order.assert_not_awaited()
    mock_client.cancel_batch_orders.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_batch_orders_uses_exchange_client() -> None:
    raw_resp = {
        "orderId": 12345,
        "clientOrderId": "ORD-REG-CANCEL",
        "status": BinanceOrderState.canceled,
    }
    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id="ORD-REG-CANCEL",
        exchange_order_id="12345",
        status=OrderStatus.CANCELLED,
        raw_status=BinanceOrderState.canceled,
        raw=raw_resp,
    )

    mock_client = _make_mock_client(cancel_return=cancel_result)
    adapter = MagicMock()

    order = make_regular_order()
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    assert order.order_id
    resp = await gateway.cancel_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        order_ids=[order.order_id],
    )

    assert len(resp) == 1
    item = resp[0]

    assert isinstance(item, CancelBatchOrderResp)
    assert item.order_id == order.order_id
    assert item.result == BatchCancelResultStatus.CANCELLED
    assert item.reason is None
    assert item.status == OrderStatus.CANCELLED
    assert item.conditional_status is None
    assert item.client_order_id == "ORD-REG-CANCEL"
    assert item.exchange_order_id == "12345"
    assert item.raw == raw_resp

    mock_client.cancel_batch_orders.assert_awaited_once()
    called_orders = mock_client.cancel_batch_orders.await_args.args[0]
    assert [called.order_id for called in called_orders] == [order.order_id]

    assert state_service.order
    assert state_service.order.status == OrderStatus.CANCELLED
    assert state_service.order.raw_exchange_response == raw_resp


@pytest.mark.asyncio
async def test_cancel_all_conditional_open_orders_uses_client() -> None:
    raw_resp = {
        "code": 200,
        "msg": "The operation of cancel all open algo orders is done.",
    }
    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        raw=raw_resp,
    )

    mock_client = _make_mock_client(cancel_return=cancel_result)
    adapter = MagicMock()

    order = make_conditional_order()
    gateway, state_service = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )

    resp = await gateway.cancel_all_conditional_open_orders(
        Exchange.BINANCE,
        MarketType.PERP,
        "BTCUSDT",
    )

    assert resp == raw_resp

    mock_client.cancel_all_conditional_open_orders.assert_awaited_once_with(
        symbol="BTCUSDT",
    )

    assert state_service.order
    assert state_service.order.status == OrderStatus.PENDING_CANCEL
    assert state_service.order.conditional_status == ConditionalStatus.NEW


@pytest.mark.asyncio
async def test_cancel_all_open_orders_cancels_regular_and_conditional() -> None:
    raw_resp = {
        "code": 200,
        "msg": "done",
    }
    cancel_result = ExchangeCancelResult(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        raw=raw_resp,
    )

    mock_client = _make_mock_client(cancel_return=cancel_result)
    adapter = MagicMock()

    order = make_conditional_order()
    gateway, _ = make_gateway(
        order=order,
        adapter=adapter,
        mock_client=mock_client,
    )
    resp = await gateway.cancel_all_open_orders(
        Exchange.BINANCE,
        MarketType.PERP,
        "BTCUSDT",
    )

    assert resp["ok"] is True
    assert resp["regular"] == {
        "ok": True,
        "response": raw_resp,
    }
    assert resp["conditional"] == {
        "ok": True,
        "response": raw_resp,
    }

    mock_client.cancel_all_regular_open_orders.assert_awaited_once_with(
        symbol="BTCUSDT",
    )
    mock_client.cancel_all_conditional_open_orders.assert_awaited_once_with(
        symbol="BTCUSDT",
    )
