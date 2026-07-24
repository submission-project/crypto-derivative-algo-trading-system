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
    order_id: str,
    symbol: str = "BTCUSDT",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 2,
) -> Order:
    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        signal_id=None,
        strategy_name=None,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
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
        # pyrefly: ignore [bad-argument-type]
        postgres=postgres,
        intent_repo=AsyncMock(),
        postgres_order_repo=order_repo,
        outbox_repo=AsyncMock(),
        redis_order_repo=redis_order_repo,
    )


@pytest.mark.asyncio
async def test_list_open_projection_by_symbol_uses_redis_projection() -> None:
    order = make_order(order_id="ORD-OPEN-PROJ-001")

    redis_order_repo = AsyncMock()
    redis_order_repo.list_open_by_symbol = AsyncMock(
        return_value=[
            order.model_dump(mode="json", exclude_none=True),
        ]
    )

    order_repo = AsyncMock()

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.list_open_orders_by_symbol_from_redis(
        exchange="BINANCE",
        market_type="PERP",
        symbol="BTCUSDT"
    )

    assert len(result) == 1
    assert result[0].order_id == "ORD-OPEN-PROJ-001"

    redis_order_repo.list_open_by_symbol.assert_awaited_once_with(
        exchange="BINANCE",
        market_type="PERP",
        symbol="BTCUSDT"
    )


@pytest.mark.asyncio
async def test_list_open_orders_by_symbol_uses_postgres_and_refreshes_projection() -> (
    None
):
    order1 = make_order(order_id="ORD-OPEN-PG-001", version=2)
    order2 = make_order(order_id="ORD-OPEN-PG-002", version=3)

    redis_order_repo = AsyncMock()
    redis_order_repo.save = AsyncMock(return_value=True)

    order_repo = AsyncMock()
    order_repo.list_open_joined_by_symbol = AsyncMock(
        return_value=[
            order1,
            order2,
        ]
    )

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.list_open_orders_by_symbol(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        refresh_projection=True,
    )

    assert len(result) == 2
    assert {o.order_id for o in result} == {
        "ORD-OPEN-PG-001",
        "ORD-OPEN-PG-002",
    }

    order_repo.list_open_joined_by_symbol.assert_awaited_once()
    assert redis_order_repo.save.await_count == 2


@pytest.mark.asyncio
async def test_list_open_orders_by_symbol_can_skip_projection_refresh() -> None:
    order = make_order(order_id="ORD-OPEN-NO-REFRESH-001")

    redis_order_repo = AsyncMock()
    redis_order_repo.save = AsyncMock()

    order_repo = AsyncMock()
    order_repo.list_open_joined_by_symbol = AsyncMock(
        return_value=[
            order,
        ]
    )

    service = make_service(
        redis_order_repo=redis_order_repo,
        order_repo=order_repo,
    )

    result = await service.list_open_orders_by_symbol(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        refresh_projection=False,
    )

    assert len(result) == 1
    assert result[0].order_id == "ORD-OPEN-NO-REFRESH-001"

    redis_order_repo.save.assert_not_awaited()
