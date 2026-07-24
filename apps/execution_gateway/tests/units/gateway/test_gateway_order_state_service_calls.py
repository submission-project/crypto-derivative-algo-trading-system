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


def make_order(
    *,
    order_id: str = "ORD-GW-001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
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
        version=2,
    )


@pytest.mark.asyncio
async def test_gateway_load_order_delegates_to_state_service() -> None:
    state_service = MagicMock()
    state_service.load_order = AsyncMock(return_value=make_order())

    gateway = ExecutionGateway(
        state_repo=MagicMock(),
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    result = await gateway.transitions._load_order_from_repo("ORD-GW-001")

    assert result is not None
    assert result.order_id == "ORD-GW-001"

    state_service.load_order.assert_awaited_once_with(order_id="ORD-GW-001")


@pytest.mark.asyncio
async def test_gateway_safe_get_open_orders_delegates_to_state_service() -> None:
    orders = [
        make_order(order_id="ORD-GW-OPEN-001"),
        make_order(order_id="ORD-GW-OPEN-002"),
    ]

    state_service = MagicMock()
    state_service.list_open_orders_by_symbol = AsyncMock(return_value=orders)

    gateway = ExecutionGateway(
        state_repo=MagicMock(),
        state_service=state_service,
        exchange_clients=MagicMock(),
    )

    result = await gateway.transitions._safe_get_local_open_orders_by_symbol(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT"
    )

    assert len(result) == 2
    assert {o.order_id for o in result} == {
        "ORD-GW-OPEN-001",
        "ORD-GW-OPEN-002",
    }

    state_service.list_open_orders_by_symbol.assert_awaited_once_with(
        exchange="BINANCE",
        market_type="PERP",
        symbol="BTCUSDT",
        refresh_projection=True,
    )
