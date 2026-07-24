from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.services.position_order_service import (
    PositionCloseError,
    PositionOrderService,
)

from schemas.market import Exchange, MarketType
from schemas.order import OrderSide, OrderType, PositionAction, OrderSource
from schemas.position import Position, PositionSide


def make_position(
    *,
    amt: str,
    side: PositionSide = PositionSide.BOTH,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=side,
        position_amt=amt,
        entry_price="60000",
        updated_ts=1_700_000_000_000,
    )


def make_service(position: Position | None) -> tuple[PositionOrderService, MagicMock]:
    position_state_service = MagicMock()
    position_state_service.load_position = AsyncMock(return_value=position)

    gateway = MagicMock()
    gateway.submit_order = AsyncMock(return_value=MagicMock())

    service = PositionOrderService(
        position_state_service=position_state_service,
        gateway=gateway,
    )

    return service, gateway


@pytest.mark.asyncio
async def test_close_long_one_way_position_creates_sell_reduce_only_market_order():
    position = make_position(amt="0.01", side=PositionSide.BOTH)
    service, gateway = make_service(position)

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        source=OrderSource.MANUAL,
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
    )

    req = gateway.submit_order.await_args.kwargs["req"]

    assert req.symbol == "BTCUSDT"
    assert req.side == OrderSide.SELL
    assert req.order_type == OrderType.MARKET
    assert req.quantity == "0.01"
    assert req.reduce_only is True
    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_close_short_one_way_position_creates_buy_reduce_only_market_order():
    position = make_position(amt="-0.02", side=PositionSide.BOTH)
    service, gateway = make_service(position)

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        source=OrderSource.MANUAL,
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
    )

    req = gateway.submit_order.await_args.kwargs["req"]

    assert req.side == OrderSide.BUY
    assert req.quantity == "0.02"
    assert req.reduce_only is True
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_close_hedge_long_position_uses_sell_without_reduce_only():
    position = make_position(amt="0.01", side=PositionSide.LONG)
    service, gateway = make_service(position)

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        source=OrderSource.MANUAL,
        symbol="BTCUSDT",
        position_side=PositionSide.LONG,
    )

    req = gateway.submit_order.await_args.kwargs["req"]

    assert req.side == OrderSide.SELL
    assert req.position_side == PositionSide.LONG
    assert req.reduce_only is False


@pytest.mark.asyncio
async def test_close_hedge_short_position_uses_buy_without_reduce_only():
    position = make_position(amt="-0.01", side=PositionSide.SHORT)
    service, gateway = make_service(position)

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        source=OrderSource.MANUAL,
        symbol="BTCUSDT",
        position_side=PositionSide.SHORT,
    )

    req = gateway.submit_order.await_args.kwargs["req"]

    assert req.side == OrderSide.BUY
    assert req.position_side == PositionSide.SHORT
    assert req.reduce_only is False


@pytest.mark.asyncio
async def test_reduce_position_rejects_quantity_greater_than_position_size():
    position = make_position(amt="0.01", side=PositionSide.BOTH)
    service, _ = make_service(position)

    with pytest.raises(PositionCloseError):
        await service.reduce_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            source=OrderSource.MANUAL,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            quantity="0.02",
        )


@pytest.mark.asyncio
async def test_close_flat_position_raises():
    position = make_position(amt="0", side=PositionSide.BOTH)
    service, _ = make_service(position)

    with pytest.raises(PositionCloseError):
        await service.close_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            source=OrderSource.MANUAL,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
        )


@pytest.mark.asyncio
async def test_close_missing_position_raises():
    service, _ = make_service(None)

    with pytest.raises(PositionCloseError):
        await service.close_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            source=OrderSource.MANUAL,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
        )

@pytest.mark.asyncio
async def test_increase_position_market_uses_increase_action() -> None:
    position_state_service = MagicMock()
    position_state_service.load_position = AsyncMock(return_value=make_position(amt="0.01"))
    gateway = MagicMock()
    gateway.submit_order = AsyncMock(return_value=MagicMock())

    service = PositionOrderService(
        position_state_service=position_state_service,
        gateway=gateway,
    )

    await service.increase_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        source=OrderSource.MANUAL,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity="0.01",
        position_side=PositionSide.BOTH,
    )

    # pyrefly: ignore [missing-attribute]
    req = gateway.submit_order.await_args.kwargs["req"]

    assert req.position_action == PositionAction.INCREASE