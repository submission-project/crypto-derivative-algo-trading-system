from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from execution_gateway.gateway import ExecutionGateway
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
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
)
from schemas.position import PositionSide

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState


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
    def __init__(self, order: Order | None = None) -> None:
        self.order = order
        self.transition_calls: list[tuple[Order, Order]] = []

    async def create_order(self, order: Order) -> Order:
        self.order = order
        return order

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

    async def load_order_by_client_conditional_id(
        self,
        *,
        exchange,
        market_type,
        client_conditional_id: str,
        refresh_projection: bool = True,
    ) -> Order | None:
        if not self.order:
            return None

        if self.order.client_conditional_id == client_conditional_id:
            return self.order

        return None

    async def load_order_by_exchange_conditional_id(
        self,
        *,
        exchange,
        market_type,
        exchange_conditional_id: str,
        refresh_projection: bool = True,
    ) -> Order | None:
        if not self.order:
            return None

        if self.order.exchange_conditional_id == exchange_conditional_id:
            return self.order

        return None


def make_conditional_order(
    *,
    conditional_status: ConditionalStatus | None = ConditionalStatus.NEW,
    exchange_conditional_id: str | None = "123456",
    version: int = 2,
) -> Order:
    now = _NOW_MS()

    return Order(
        order_id="ORD-COND-001",
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
        client_conditional_id="TKSTOP001",
        exchange_conditional_id=exchange_conditional_id,
        conditional_status=conditional_status,
        exchange_conditional_status=(
            conditional_status.value if conditional_status else None
        ),
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.ACKNOWLEDGED,
        created_ts=now,
        updated_ts=now,
        acknowledged_ts=now,
        version=version,
    )


def make_gateway(
    *,
    order: Order | None,
) -> tuple[ExecutionGateway, DummyStateService]:
    state_service = DummyStateService(order)

    gateway = ExecutionGateway(
        # pyrefly: ignore [bad-argument-type]
        state_repo=DummyStateRepo(),
        # pyrefly: ignore [bad-argument-type]
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    return gateway, state_service


def make_conditional_event(
    *,
    client_conditional_id: str | None = "TKSTOP001",
    exchange_conditional_id: str | None = "123456",
    target_status: ConditionalStatus = ConditionalStatus.TRIGGERED,
    triggered_order_id: str | None = "987654",
    raw_status: str = BinanceConditionalOrderState.triggered,
) -> NormalizedConditionalOrderEvent:
    return NormalizedConditionalOrderEvent(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_conditional_id=client_conditional_id,
        exchange_conditional_id=exchange_conditional_id,
        target_status=target_status,
        exchange_conditional_status=raw_status,
        triggered_order_id=triggered_order_id,
        triggered_client_order_id=None,
        filled_quantity=None,
        avg_fill_price=None,
        reject_reason_text=None,
        event_time=1_700_000_000_100,
        transaction_time=1_700_000_000_050,
        raw={
            "e": "ALGO_UPDATE",
            "E": 1_700_000_000_100,
            "T": 1_700_000_000_050,
            "o": {
                "s": "BTCUSDT",
                "caid": client_conditional_id,
                "aid": exchange_conditional_id,
                "X": raw_status,
                "ai": triggered_order_id,
            },
        },
    )


@pytest.mark.asyncio
async def test_apply_conditional_event_triggered_by_client_conditional_id() -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.NEW,
        exchange_conditional_id="123456",
        version=2,
    )

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id="TKSTOP001",
        exchange_conditional_id="123456",
        target_status=ConditionalStatus.TRIGGERED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.triggered,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.order_id == order.order_id
    assert updated.conditional_status == ConditionalStatus.TRIGGERED
    assert updated.exchange_conditional_status == BinanceConditionalOrderState.triggered
    assert updated.exchange_conditional_id == "123456"
    assert updated.triggered_order_id == "987654"
    assert updated.triggered_ts is not None
    assert updated.version == 3

    assert len(state_service.transition_calls) == 1


@pytest.mark.asyncio
async def test_apply_conditional_event_lookup_by_exchange_conditional_id() -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.ACTIVE,
        exchange_conditional_id="EX-ALGO-001",
        version=4,
    )

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id=None,
        exchange_conditional_id="EX-ALGO-001",
        target_status=ConditionalStatus.TRIGGERED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.triggering,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.conditional_status == ConditionalStatus.TRIGGERED
    assert updated.exchange_conditional_status == BinanceConditionalOrderState.triggering
    assert updated.triggered_order_id == "987654"
    assert updated.version == 5

    assert len(state_service.transition_calls) == 1


@pytest.mark.asyncio
async def test_apply_conditional_event_finished_after_triggered() -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.TRIGGERED,
        exchange_conditional_id="123456",
        version=5,
    )
    order.triggered_order_id = "987654"
    order.triggered_ts = _NOW_MS()

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id="TKSTOP001",
        exchange_conditional_id="123456",
        target_status=ConditionalStatus.FINISHED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.finished,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.conditional_status == ConditionalStatus.FINISHED
    assert updated.exchange_conditional_status == BinanceConditionalOrderState.finished
    assert updated.triggered_order_id == "987654"
    assert updated.version == 6

    assert len(state_service.transition_calls) == 1


@pytest.mark.asyncio
async def test_apply_conditional_event_terminal_protection_blocks_regression() -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.FINISHED,
        exchange_conditional_id="123456",
        version=10,
    )

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id="TKSTOP001",
        exchange_conditional_id="123456",
        target_status=ConditionalStatus.TRIGGERED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.triggered,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.conditional_status == ConditionalStatus.FINISHED
    assert updated.version == 10

    # terminal protection에서 그대로 반환하므로 transition 없음
    assert len(state_service.transition_calls) == 0


@pytest.mark.asyncio
async def test_apply_conditional_event_invalid_transition_returns_existing_order() -> None:
    """
    NEW -> FINISHED는 직접 전이로 보지 않는다.
    TRIGGERED를 거쳐야 한다.
    """
    order = make_conditional_order(
        conditional_status=ConditionalStatus.NEW,
        exchange_conditional_id="123456",
        version=2,
    )

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id="TKSTOP001",
        exchange_conditional_id="123456",
        target_status=ConditionalStatus.FINISHED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.finished,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.conditional_status == ConditionalStatus.NEW
    assert updated.version == 2

    assert len(state_service.transition_calls) == 0


@pytest.mark.asyncio
async def test_apply_conditional_event_returns_none_when_order_not_found() -> None:
    gateway, state_service = make_gateway(order=None)

    event = make_conditional_event(
        client_conditional_id="MISSING",
        exchange_conditional_id="MISSING-EX",
        target_status=ConditionalStatus.TRIGGERED,
        triggered_order_id="987654",
        raw_status=BinanceConditionalOrderState.triggered,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is None
    assert len(state_service.transition_calls) == 0


@pytest.mark.asyncio
async def test_apply_conditional_event_same_terminal_status_is_idempotent() -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.FINISHED,
        exchange_conditional_id="123456",
        version=10,
    )

    gateway, state_service = make_gateway(order=order)

    event = make_conditional_event(
        client_conditional_id="TKSTOP001",
        exchange_conditional_id="123456",
        target_status=ConditionalStatus.FINISHED,
        triggered_order_id=None,
        raw_status=BinanceConditionalOrderState.finished,
    )

    updated = await gateway.apply_conditional_order_event(event)

    assert updated is not None
    assert updated.conditional_status == ConditionalStatus.FINISHED
    assert updated.version == 11

    assert len(state_service.transition_calls) == 1