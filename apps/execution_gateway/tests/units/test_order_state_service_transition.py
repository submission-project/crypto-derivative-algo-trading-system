from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any
from unittest.mock import AsyncMock

import pytest

from execution_gateway.services.order_state_service import OrderStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
    PositionAction,
)


def make_order(
    *,
    status: OrderStatus = OrderStatus.PENDING_NEW,
    version: int = 1,
) -> Order:
    return Order(
        order_id="ORD-TEST-TRANSITION-001",
        source=OrderSource.MANUAL,
        signal_id=None,
        strategy_name=None,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        stop_price=None,
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=1_700_000_000_000,
        submitted_ts=None,
        filled_ts=None,
        updated_ts=1_700_000_000_000,
        status=status,
        version=version,
    )


class FakeTransaction(AbstractAsyncContextManager):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def __aenter__(self) -> "FakeTransaction":
        self.events.append("begin")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        if exc_type is None:
            self.events.append("commit")
        else:
            self.events.append("rollback")


class FakeConnection:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self.events)


class FakeAcquire(AbstractAsyncContextManager):
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn

    async def __aenter__(self) -> FakeConnection:
        return self.conn

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        return None


class FakePool:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.conn)


class FakePostgresClient:
    def __init__(self, pool: FakePool) -> None:
        self.pool = pool

    def require_pool(self) -> FakePool:
        return self.pool


@pytest.mark.asyncio
async def test_transition_order_success_writes_pg_outbox_then_redis() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    current_order = make_order(
        status=OrderStatus.PENDING_NEW,
        version=1,
    )

    updated_order = current_order.model_copy(deep=True)
    updated_order.status = OrderStatus.SUBMITTED
    updated_order.updated_ts = 1_700_000_000_100
    updated_order.submitted_ts = 1_700_000_000_100

    async def transition(*args, **kwargs):
        events.append("pg_transition")
        return {
            "order_id": updated_order.order_id,
            "status": updated_order.status.value,
            "version": 2,
        }

    async def insert_outbox(*args, **kwargs):
        events.append("insert_outbox")

    async def upsert_projection(*args, **kwargs):
        events.append("upsert_projection")

    order_repo.transition.side_effect = transition
    outbox_repo.insert.side_effect = insert_outbox
    redis_order_repo.save.side_effect = upsert_projection

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    result = await service.transition_order(
        current_order=current_order,
        updated_order=updated_order,
    )

    assert result.status == OrderStatus.SUBMITTED
    assert result.version == 2

    order_repo.transition.assert_awaited_once_with(
        conn=conn,
        order=updated_order,
        expected_version=1,
    )
    outbox_repo.insert.assert_awaited_once()
    redis_order_repo.save.assert_awaited_once()

    assert events == [
        "begin",
        "pg_transition",
        "insert_outbox",
        "commit",
        "upsert_projection",
    ]


@pytest.mark.asyncio
async def test_transition_order_pg_failure_does_not_write_redis() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    current_order = make_order()
    updated_order = current_order.model_copy(deep=True)
    updated_order.status = OrderStatus.SUBMITTED

    async def fail_transition(*args, **kwargs):
        events.append("pg_transition")
        raise RuntimeError("pg transition failed")

    order_repo.transition.side_effect = fail_transition

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    with pytest.raises(RuntimeError, match="pg transition failed"):
        await service.transition_order(
            current_order=current_order,
            updated_order=updated_order,
        )

    outbox_repo.insert.assert_not_awaited()
    redis_order_repo.save.assert_not_awaited()

    assert events == [
        "begin",
        "pg_transition",
        "rollback",
    ]


@pytest.mark.asyncio
async def test_transition_order_redis_failure_does_not_rollback_pg() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    current_order = make_order()
    updated_order = current_order.model_copy(deep=True)
    updated_order.status = OrderStatus.SUBMITTED
    updated_order.updated_ts = 1_700_000_000_100

    async def transition(*args, **kwargs):
        events.append("pg_transition")
        return {
            "order_id": updated_order.order_id,
            "status": updated_order.status.value,
            "version": 2,
        }

    async def insert_outbox(*args, **kwargs):
        events.append("insert_outbox")

    async def fail_projection(*args, **kwargs):
        events.append("upsert_projection")
        raise RuntimeError("redis projection failed")

    order_repo.transition.side_effect = transition
    outbox_repo.insert.side_effect = insert_outbox
    redis_order_repo.save.side_effect = fail_projection

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    result = await service.transition_order(
        current_order=current_order,
        updated_order=updated_order,
    )

    assert result.status == OrderStatus.SUBMITTED
    assert result.version == 2

    assert events == [
        "begin",
        "pg_transition",
        "insert_outbox",
        "commit",
        "upsert_projection",
    ]