import pytest
from decimal import Decimal
from schemas.order import (
    Order,
    OrderSource,
    OrderSide,
    OrderType,
    TimeInForce,
    OrderStatus,
    RejectReason,
    OrderRequest,
    CancelRequest,
    TERMINAL_STATUSES,
    PositionAction,
)
from schemas.market import Exchange, MarketType

def test_order_creation():
    order = Order(
        order_id="O123",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.001",
        price="70000.5",
        position_action=PositionAction.OPEN,
        created_ts=1700000000000,
        updated_ts=1700000000000,
    )
    assert order.status == OrderStatus.PENDING_NEW
    assert order.position_action == PositionAction.OPEN
    assert order.reject_reason is None
    assert order.filled_quantity == "0"
    
    # Test decimal helpers
    assert order.quantity_decimal() == Decimal("0.001")
    assert order.filled_quantity_decimal() == Decimal("0")
    assert order.remaining_quantity() == Decimal("0.001")
    
    # Test terminal status check
    assert not order.is_terminal
    order.status = OrderStatus.FILLED
    assert order.is_terminal
    order.status = OrderStatus.CANCELLED
    assert order.is_terminal
    order.status = OrderStatus.REJECTED
    assert order.is_terminal
    order.status = OrderStatus.EXPIRED
    assert order.is_terminal
    order.status = OrderStatus.RECONCILE_UNRESOLVED
    assert order.is_terminal

def test_order_id_generation():
    # order_id를 넘기지 않으면 자동 생성 (접두사 포함)
    order = Order(
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.001",
        price="70000.5",
        position_action=PositionAction.OPEN,
        created_ts=1700000000000,
        updated_ts=1700000000000,
    )
    assert order.order_id is not None
    assert order.order_id.startswith("O-BINANCE-PERP-")

def test_order_request_creation():
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="ETHUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )
    assert req.time_in_force is None
    assert req.position_action == PositionAction.OPEN
    assert req.price is None
    assert not req.reduce_only

def test_cancel_request_creation():
    req = CancelRequest(order_id="O123", reason="timeout")
    assert req.order_id == "O123"
    assert req.reason == "timeout"
