from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.gateway import ExecutionGateway
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)

from execution_gateway.services.order_state_service import OrderStateService


def make_order(
    *,
    order_id: str = "ORD-FALLBACK-001",
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


def make_service(redis_order_repo: MagicMock) -> OrderStateService:
    return OrderStateService(
        postgres=MagicMock(),
        intent_repo=MagicMock(),
        postgres_order_repo=MagicMock(),
        outbox_repo=MagicMock(),
        redis_order_repo=redis_order_repo,
    )


@pytest.mark.asyncio
async def test_load_order_uses_redis_projection_first() -> None:
    redis_repo = MagicMock()
    order = make_order()

    redis_repo.get = AsyncMock(
        return_value=order.model_dump(mode="json", exclude_none=True)
    )

    service = make_service(redis_repo)
    service.load_order_from_postgres = AsyncMock()

    assert order.order_id

    result = await service.load_order(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id
    assert result.version == 3

    redis_repo.get.assert_awaited_once_with(order.order_id)
    service.load_order_from_postgres.assert_not_awaited()


@pytest.mark.asyncio
async def test_load_order_falls_back_to_postgres_when_redis_miss() -> None:
    order = make_order()

    adapter = MagicMock()
    state_repo = MagicMock()
    state_service = MagicMock()

    state_service.load_order = AsyncMock(return_value=order)

    gateway = ExecutionGateway(
        state_repo=state_repo,
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    assert order.order_id

    result = await gateway.transitions._load_order_from_repo(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id
    assert result.version == 3

    state_service.load_order.assert_awaited_once_with(order_id=order.order_id)
    state_repo.get.assert_not_called()


@pytest.mark.asyncio
async def test_load_order_falls_back_to_postgres_when_redis_data_invalid() -> None:
    adapter = MagicMock()
    state_repo = MagicMock()
    state_service = MagicMock()

    order = make_order()

    state_repo.get = AsyncMock(
        return_value={
            "order_id": order.order_id,
            "status": "INVALID_STATUS",
        }
    )
    state_service.load_order = AsyncMock(return_value=order)

    gateway = ExecutionGateway(
        state_repo=state_repo,
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    assert order.order_id

    result = await gateway.transitions._load_order_from_repo(order_id=order.order_id)

    assert result is not None
    assert result.order_id == order.order_id

    state_service.load_order.assert_awaited_once_with(order_id=order.order_id)


@pytest.mark.asyncio
async def test_load_order_returns_none_when_not_in_redis_or_postgres() -> None:
    adapter = MagicMock()
    state_repo = MagicMock()
    state_service = MagicMock()

    state_repo.get = AsyncMock(return_value=None)
    state_service.load_order = AsyncMock(return_value=None)

    gateway = ExecutionGateway(
        state_repo=state_repo,
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    result = await gateway.transitions._load_order_from_repo(order_id="ORD-NOT-FOUND")

    assert result is None

    state_service.load_order.assert_awaited_once_with(order_id="ORD-NOT-FOUND")
