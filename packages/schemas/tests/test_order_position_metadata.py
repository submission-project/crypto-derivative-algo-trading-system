from __future__ import annotations

import pytest
from pydantic import ValidationError

from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRequest,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
)
from schemas.position import PositionSide


def test_position_action_enum_has_expected_values() -> None:
    assert PositionAction.OPEN.value == "OPEN"
    assert PositionAction.INCREASE.value == "INCREASE"
    assert PositionAction.REDUCE.value == "REDUCE"
    assert PositionAction.CLOSE.value == "CLOSE"
    assert PositionAction.FLIP.value == "FLIP"
    assert PositionAction.UNKNOWN.value == "UNKNOWN"
    assert PositionAction.NOT_APPLICABLE.value == "NOT_APPLICABLE"


def test_perp_order_request_defaults_to_unknown_position_action() -> None:
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        position_action=PositionAction.UNKNOWN,
    )

    assert req.symbol == "BTCUSDT"
    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.UNKNOWN


def test_spot_order_request_defaults_to_not_applicable_position_action() -> None:
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.SPOT,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        position_action=PositionAction.UNKNOWN,
    )

    assert req.symbol == "BTCUSDT"
    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.NOT_APPLICABLE


def test_spot_order_request_rejects_position_action_open() -> None:
    with pytest.raises(ValidationError):
        OrderRequest(
            exchange=Exchange.BINANCE,
            market_type=MarketType.SPOT,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity="0.01",
            position_action=PositionAction.OPEN,
        )


def test_spot_order_request_rejects_position_side_long() -> None:
    with pytest.raises(ValidationError):
        OrderRequest(
            exchange=Exchange.BINANCE,
            market_type=MarketType.SPOT,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity="0.01",
            position_side=PositionSide.LONG,
        )


def test_perp_order_request_rejects_not_applicable_position_action() -> None:
    with pytest.raises(ValidationError):
        OrderRequest(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity="0.01",
            position_action=PositionAction.NOT_APPLICABLE,
        )


def test_perp_order_request_accepts_close_action() -> None:
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity="0.01",
        reduce_only=True,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
    )

    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.CLOSE
    assert req.reduce_only is True


def test_perp_order_request_accepts_increase_action() -> None:
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        position_side=PositionSide.BOTH,
        position_action=PositionAction.INCREASE,
    )

    assert req.position_action == PositionAction.INCREASE


def test_order_generates_id_and_defaults_position_action_unknown() -> None:
    order = Order(
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        position_action=PositionAction.UNKNOWN,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
    )

    assert order.order_id is not None
    assert order.symbol == "BTCUSDT"
    assert order.position_side == PositionSide.BOTH
    assert order.position_action == PositionAction.UNKNOWN


def test_spot_order_normalizes_position_action_to_not_applicable() -> None:
    order = Order(
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.SPOT,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        position_action=PositionAction.NOT_APPLICABLE,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
    )

    assert order.order_id is not None
    assert order.symbol == "BTCUSDT"
    assert order.position_side == PositionSide.BOTH
    assert order.position_action == PositionAction.NOT_APPLICABLE


def test_order_rejects_spot_with_close_action() -> None:
    with pytest.raises(ValidationError):
        Order(
            source=OrderSource.MANUAL,
            exchange=Exchange.BINANCE,
            market_type=MarketType.SPOT,
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity="0.01",
            position_action=PositionAction.CLOSE,
            created_ts=1_700_000_000_000,
            updated_ts=1_700_000_000_000,
            status=OrderStatus.PENDING_NEW,
        )


def test_order_rejects_perp_with_not_applicable_action() -> None:
    with pytest.raises(ValidationError):
        Order(
            source=OrderSource.MANUAL,
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity="0.01",
            position_action=PositionAction.NOT_APPLICABLE,
            created_ts=1_700_000_000_000,
            updated_ts=1_700_000_000_000,
            status=OrderStatus.PENDING_NEW,
        )
