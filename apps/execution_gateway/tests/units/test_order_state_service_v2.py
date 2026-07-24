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
    order_id: str = "ORD-LOAD-001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 3,
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
        submitted_ts=1_700_000_000_100,
        filled_ts=None,
        updated_ts=1_700_000_000_200,
        status=status,
        version=version,
    )


class FakeConnection:
    pass


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


def make_service(
    *,
    redis_order_repo,
    order_repo,
) -> OrderStateService:
    postgres = FakePostgresClient(FakePool(FakeConnection()))

    return OrderStateService(
        postgres=postgres,
        intent_repo=AsyncMock(),
        postgres_order_repo=order_repo,
        outbox_repo=AsyncMock(),
        redis_order_repo=redis_order_repo,
    )


@pytest.mark.asyncio
async def test_load_order_redis_hit_does_not_query_postgres() -> None:
    order = make_order()

    redis_order_repo = AsyncMock()
    redis_order_repo.get = AsyncMock(
        return_value=order.model_dump(mode="json", exclude_none=True)
    )
    redis_order_repo.save = AsyncMock()

    order_repo = AsyncMock()
    order_repo.get_joined_order = AsyncMock()

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.load_order(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id
    assert result.version == 3

    redis_order_repo.get.assert_awaited_once_with(order.order_id)
    order_repo.get_joined_order.assert_not_awaited()
    redis_order_repo.save.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_order_redis_miss_falls_back_to_postgres_and_refreshes_projection() -> (
    None
):
    order = make_order()

    redis_order_repo = AsyncMock()
    redis_order_repo.get = AsyncMock(return_value=None)
    redis_order_repo.save = AsyncMock(return_value=True)

    order_repo = AsyncMock()
    order_repo.get_joined_order = AsyncMock(
        return_value=order.model_dump(mode="json", exclude_none=True)
    )

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.load_order(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id
    assert result.version == 3

    redis_order_repo.get.assert_awaited_once_with(order.order_id)
    order_repo.get_joined_order.assert_awaited_once()
    redis_order_repo.save.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_order_invalid_redis_projection_falls_back_to_postgres() -> None:
    order = make_order()

    redis_order_repo = AsyncMock()
    redis_order_repo.get = AsyncMock(
        return_value={
            "order_id": order.order_id,
            "status": "INVALID_STATUS",
        }
    )
    redis_order_repo.save = AsyncMock(return_value=True)

    order_repo = AsyncMock()
    order_repo.get_joined_order = AsyncMock(
        return_value=order.model_dump(mode="json", exclude_none=True)
    )

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.load_order(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id
    assert result.status == OrderStatus.ACKNOWLEDGED

    order_repo.get_joined_order.assert_awaited_once()
    redis_order_repo.save.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_order_returns_none_when_not_found_anywhere() -> None:
    redis_order_repo = AsyncMock()
    redis_order_repo.get = AsyncMock(return_value=None)
    redis_order_repo.save = AsyncMock()

    order_repo = AsyncMock()
    order_repo.get_joined_order = AsyncMock(return_value=None)

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.load_order(order_id="ORD-NOT-FOUND")

    assert result is None

    redis_order_repo.get.assert_awaited_once_with("ORD-NOT-FOUND")
    order_repo.get_joined_order.assert_awaited_once()
    redis_order_repo.save.assert_not_awaited()
