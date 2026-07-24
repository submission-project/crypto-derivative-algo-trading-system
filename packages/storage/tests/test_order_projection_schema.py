from __future__ import annotations

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
)
from schemas.position import PositionSide
from storage.repositories.redis.domain.order_projection_schema import (
    OrderRedisProjection,
)


def make_regular_order() -> Order:
    return Order(
        order_id="ORD-REDIS-REGULAR-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        order_route=OrderRoute.REGULAR,
        quantity="0.01",
        price=None,
        trigger_price=None,
        reduce_only=False,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.PENDING_NEW,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=1,
    )


def make_conditional_order() -> Order:
    return Order(
        order_id="ORD-REDIS-CONDITIONAL-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        price=None,
        trigger_price="59000",
        reduce_only=True,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.ACKNOWLEDGED,
        conditional_status=ConditionalStatus.NEW,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_001_000,
        version=2,
    )


def test_regular_order_projection_always_stores_empty_conditional_status() -> None:
    projection = OrderRedisProjection.from_order(make_regular_order())
    fields = projection.to_hash()

    assert fields["order_id"] == "ORD-REDIS-REGULAR-001"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["order_route"] == OrderRoute.REGULAR.value
    assert fields["conditional_status"] == ""
    assert projection.conditional_status == ""


def test_conditional_order_projection_stores_conditional_status() -> None:
    projection = OrderRedisProjection.from_order(make_conditional_order())
    fields = projection.to_hash()

    assert fields["order_id"] == "ORD-REDIS-CONDITIONAL-001"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["order_route"] == OrderRoute.CONDITIONAL.value
    assert fields["conditional_status"] == ConditionalStatus.NEW.value
    assert projection.conditional_status == ConditionalStatus.NEW.value


def test_projection_schema_describe_uses_attr_metadata() -> None:
    described = OrderRedisProjection.describe()
    source_field = next(field for field in described if field["field"] == "source")
    conditional_status_field = next(
        field for field in described if field["field"] == "conditional_status"
    )

    assert source_field["attr"] == "source"
    assert "source" not in source_field
    assert conditional_status_field["always_store"] is True
