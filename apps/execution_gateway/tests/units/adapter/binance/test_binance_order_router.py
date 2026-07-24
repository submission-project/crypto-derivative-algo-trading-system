from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)
from schemas.position import PositionSide


def make_regular_market_order() -> Order:
    return Order(
        order_id="ORD-REG-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        order_route=OrderRoute.REGULAR,
        quantity="0.01",
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=2,
    )


def make_regular_limit_order() -> Order:
    return Order(
        order_id="ORD-LIMIT-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        order_route=OrderRoute.REGULAR,
        quantity="0.01",
        price="60000",
        time_in_force=TimeInForce.GTC,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=2,
    )


def make_stop_market_order() -> Order:
    return Order(
        order_id="ORD-STOP-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        trigger_price="59000",
        reduce_only=True,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.SUBMITTED,
        conditional_status=None,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=2,
    )


def make_stop_market_close_position_order() -> Order:
    return Order(
        order_id="ORD-STOP-CLOSEPOS-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0",
        trigger_price="59000",
        reduce_only=False,
        close_position=True,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.SUBMITTED,
        conditional_status=None,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=2,
    )


def make_stop_limit_order() -> Order:
    return Order(
        order_id="ORD-STOP-LIMIT-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_LIMIT,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        trigger_price="59000",
        price="58950",
        time_in_force=TimeInForce.GTC,
        reduce_only=True,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.SUBMITTED,
        conditional_status=None,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=2,
    )


def make_router() -> tuple[BinanceOrderRouter, MagicMock]:
    adapter = MagicMock()
    adapter.place_regular_order = AsyncMock(return_value={"orderId": 1001})
    adapter.place_algo_order = AsyncMock(return_value={"algoId": 2001})
    return BinanceOrderRouter(adapter), adapter

@pytest.mark.stable
@pytest.mark.asyncio
async def test_regular_market_uses_place_order() -> None:
    router, adapter = make_router()
    order = make_regular_market_order()

    resp = await router.place_regular_order(order)

    assert resp == {"orderId": 1001}

    adapter.place_regular_order.assert_awaited_once()
    adapter.place_algo_order.assert_not_awaited()

    params = adapter.place_regular_order.await_args.args[0]

    assert params["symbol"] == "BTCUSDT"
    assert params["side"] == "BUY"
    assert params["type"] == "MARKET"
    assert params["quantity"] == "0.01"
    assert params["newClientOrderId"] == order.order_id
    assert params["positionSide"] == "BOTH"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_regular_limit_maps_price_and_time_in_force() -> None:
    router, adapter = make_router()
    order = make_regular_limit_order()

    await router.place_regular_order(order)

    params = adapter.place_regular_order.await_args.args[0]

    assert params["type"] == "LIMIT"
    assert params["price"] == "60000"
    assert params["timeInForce"] == "GTC"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_stop_market_uses_place_algo_order() -> None:
    router, adapter = make_router()
    order = make_stop_market_order()

    resp = await router.place_conditional_order(order)

    assert resp == {"algoId": 2001}

    adapter.place_regular_order.assert_not_awaited()
    adapter.place_algo_order.assert_awaited_once()

    params = adapter.place_algo_order.await_args.args[0]

    assert params["algoType"] == "CONDITIONAL"
    assert params["symbol"] == "BTCUSDT"
    assert params["side"] == "SELL"
    assert params["type"] == "STOP_MARKET"
    assert params["quantity"] == "0.01"
    assert params["triggerPrice"] == "59000"
    assert params["clientAlgoId"] == order.order_id
    assert params["reduceOnly"] == "true"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_stop_market_close_position_omits_quantity() -> None:
    router, adapter = make_router()
    order = make_stop_market_close_position_order()

    await router.place_conditional_order(order)

    params = adapter.place_algo_order.await_args.args[0]

    assert params["type"] == "STOP_MARKET"
    assert params["closePosition"] == "true"
    assert "quantity" not in params
    assert "reduceOnly" not in params

@pytest.mark.stable
@pytest.mark.asyncio
async def test_stop_limit_maps_to_binance_stop() -> None:
    router, adapter = make_router()
    order = make_stop_limit_order()

    await router.place_conditional_order(order)

    params = adapter.place_algo_order.await_args.args[0]

    assert params["algoType"] == "CONDITIONAL"
    assert params["type"] == "STOP"
    assert params["triggerPrice"] == "59000"
    assert params["price"] == "58950"
    assert params["timeInForce"] == "GTC"
    assert params["quantity"] == "0.01"
    assert params["reduceOnly"] == "true"