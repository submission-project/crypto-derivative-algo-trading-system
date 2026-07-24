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
    order_id: str = "ORD-TEST-001",
) -> Order:
    return Order(
        order_id=order_id,
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
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
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
async def test_create_order_success_writes_pg_then_redis() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    async def insert_intent(*args, **kwargs):
        events.append("insert_intent")

    async def insert_order(*args, **kwargs):
        events.append("insert_order")

    async def insert_outbox(*args, **kwargs):
        events.append("insert_outbox")

    async def save_redis(*args, **kwargs):
        events.append("save_redis")

    intent_repo.insert.side_effect = insert_intent
    order_repo.insert_initial.side_effect = insert_order
    outbox_repo.insert.side_effect = insert_outbox
    redis_order_repo.save.side_effect = save_redis

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    order = make_order()

    result = await service.create_order(order)

    assert result == order

    intent_repo.insert.assert_awaited_once_with(conn=conn, order=order)
    order_repo.insert_initial.assert_awaited_once_with(conn=conn, order=order)
    outbox_repo.insert.assert_awaited_once()
    redis_order_repo.save.assert_awaited_once()

    assert events == [
        "begin",
        "insert_intent",
        "insert_order",
        "insert_outbox",
        "commit",     # PG Transaction
        "save_redis", # Redis SetAsync
    ]


@pytest.mark.asyncio
async def test_create_order_pg_failure_does_not_write_redis() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    async def fail_insert_intent(*args, **kwargs):
        events.append("insert_intent")
        raise RuntimeError("pg insert failed")

    intent_repo.insert.side_effect = fail_insert_intent

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    with pytest.raises(RuntimeError, match="pg insert failed"):
        await service.create_order(make_order())

    # 한번만 호출되었는지
    intent_repo.insert.assert_awaited_once()  
    order_repo.insert_initial.assert_not_awaited()
    outbox_repo.insert.assert_not_awaited()
    redis_order_repo.save.assert_not_awaited()

    # PG rollback이 호출되었는지 확인
    assert events == [
        "begin",
        "insert_intent",
        "rollback",
    ]


@pytest.mark.asyncio
async def test_create_order_redis_failure_happens_after_pg_commit() -> None:
    events: list[str] = []

    conn = FakeConnection(events)
    postgres = FakePostgresClient(FakePool(conn))

    intent_repo = AsyncMock()
    order_repo = AsyncMock()
    outbox_repo = AsyncMock()
    redis_order_repo = AsyncMock()

    async def insert_intent(*args, **kwargs):
        events.append("insert_intent")

    async def insert_order(*args, **kwargs):
        events.append("insert_order")

    async def insert_outbox(*args, **kwargs):
        events.append("insert_outbox")

    async def fail_redis_save(*args, **kwargs):
        events.append("save_redis")
        raise RuntimeError("redis save failed")

    intent_repo.insert.side_effect = insert_intent
    order_repo.insert_initial.side_effect = insert_order
    outbox_repo.insert.side_effect = insert_outbox
    redis_order_repo.save.side_effect = fail_redis_save

    service = OrderStateService(
        postgres=postgres,
        intent_repo=intent_repo,
        postgres_order_repo=order_repo,
        outbox_repo=outbox_repo,
        redis_order_repo=redis_order_repo,
    )

    with pytest.raises(RuntimeError, match="redis save failed"):
        await service.create_order(make_order())

    assert events == [
        "begin",
        "insert_intent",
        "insert_order",
        "insert_outbox",
        "commit",
        "save_redis",
    ]