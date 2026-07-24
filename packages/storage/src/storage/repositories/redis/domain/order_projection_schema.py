from __future__ import annotations

from typing import Any, ClassVar, cast

from schemas.order import Order, OrderRoute, OrderStatus
from storage.repositories.redis.domain.base_projection_schema import (
    BaseRedisProjection,
    RedisProjectionField,
    normalize_int_string,
    normalize_upper_string,
)


def normalize_order_status(value: Any) -> str:
    if value is None:
        raise ValueError("order.status is required for Redis projection")

    if isinstance(value, OrderStatus):
        return value.value

    try:
        return OrderStatus(str(value)).value
    except ValueError:
        raise ValueError(f"Invalid order.status: {value!r}") from None


def normalize_order_route(value: Any) -> str:
    if value is None:
        return OrderRoute.REGULAR.value

    if isinstance(value, OrderRoute):
        return value.value

    try:
        return OrderRoute(str(value)).value
    except ValueError:
        raise ValueError(f"Invalid order.order_route: {value!r}") from None


class OrderRedisProjection(BaseRedisProjection):
    """
    Redis order projection 저장 포맷을 명시한다.

    live hash:
        order:live:{order_id} -> Hash

    index 판단 필수 필드:
        order_id, version, status, updated_ts,
        exchange, market_type, symbol,
        order_route
    """

    MODEL: ClassVar[type[Any]] = Order
    PROJECTION_NAME: ClassVar[str] = "Redis order projection"

    FIELD_DEFINITIONS: ClassVar[tuple[RedisProjectionField, ...]] = (
        RedisProjectionField("order_id", "order_id", True, "primary id"),
        RedisProjectionField(
            "version",
            "version",
            True,
            "stale write guard",
            normalizer=normalize_int_string,
        ),
        RedisProjectionField(
            "status",
            "status",
            True,
            "order state",
            normalizer=normalize_order_status,
        ),
        RedisProjectionField(
            "updated_ts",
            "updated_ts",
            True,
            "zset score",
            normalizer=normalize_int_string,
        ),
        RedisProjectionField(
            "exchange",
            "exchange",
            True,
            "index namespace",
            normalizer=normalize_upper_string,
        ),
        RedisProjectionField(
            "market_type",
            "market_type",
            True,
            "index namespace",
            normalizer=normalize_upper_string,
        ),
        RedisProjectionField(
            "symbol",
            "symbol",
            True,
            "symbol index",
            normalizer=normalize_upper_string,
        ),
        RedisProjectionField(
            "order_route",
            "order_route",
            True,
            "regular/conditional split",
            normalizer=normalize_order_route,
        ),
        RedisProjectionField(
            "conditional_status",
            "conditional_status",
            purpose="conditional index",
            always_store=True,
        ),

        RedisProjectionField("source", "source"),
        RedisProjectionField("signal_id", "signal_id"),
        RedisProjectionField("strategy_name", "strategy_name"),
        RedisProjectionField("side", "side"),
        RedisProjectionField("order_type", "order_type"),
        RedisProjectionField("time_in_force", "time_in_force"),
        RedisProjectionField("quantity", "quantity"),
        RedisProjectionField("price", "price"),
        RedisProjectionField("trigger_price", "trigger_price"),
        RedisProjectionField("reduce_only", "reduce_only"),
        RedisProjectionField("close_position", "close_position"),
        RedisProjectionField("position_side", "position_side"),
        RedisProjectionField("position_action", "position_action"),

        RedisProjectionField("client_order_id", "client_order_id"),
        RedisProjectionField("exchange_order_id", "exchange_order_id"),
        RedisProjectionField("client_conditional_id", "client_conditional_id"),
        RedisProjectionField("exchange_conditional_id", "exchange_conditional_id"),
        RedisProjectionField("exchange_conditional_status", "exchange_conditional_status"),
        RedisProjectionField("triggered_order_id", "triggered_order_id"),
        RedisProjectionField("triggered_client_order_id", "triggered_client_order_id"),

        RedisProjectionField("reject_reason", "reject_reason"),
        RedisProjectionField("exchange_error_code", "exchange_error_code"),
        RedisProjectionField("detail_msg", "detail_msg"),
        RedisProjectionField("filled_quantity", "filled_quantity"),
        RedisProjectionField("avg_fill_price", "avg_fill_price"),

        RedisProjectionField("created_ts", "created_ts"),
        RedisProjectionField("submitted_ts", "submitted_ts"),
        RedisProjectionField("acknowledged_ts", "acknowledged_ts"),
        RedisProjectionField("triggered_ts", "triggered_ts"),
        RedisProjectionField("filled_ts", "filled_ts"),
        RedisProjectionField("cancelled_ts", "cancelled_ts"),
        RedisProjectionField("expired_ts", "expired_ts"),

        RedisProjectionField("raw_exchange_response", "raw_exchange_response"),
    )

    @classmethod
    def from_order(cls, order: Order) -> "OrderRedisProjection":
        return cast("OrderRedisProjection", cls._from_source(order))

    @property
    def order_id(self) -> str:
        return self._fields["order_id"]

    @property
    def version(self) -> int:
        return int(self._fields["version"])

    @property
    def status(self) -> str:
        return self._fields["status"]

    @property
    def updated_ts(self) -> int:
        return int(self._fields["updated_ts"])

    @property
    def exchange(self) -> str:
        return self._fields["exchange"]

    @property
    def market_type(self) -> str:
        return self._fields["market_type"]

    @property
    def symbol(self) -> str:
        return self._fields["symbol"]

    @property
    def order_route(self) -> str:
        return self._fields["order_route"]

    @property
    def conditional_status(self) -> str:
        return self._fields["conditional_status"]


OrderRedisProjection.validate_schema_once()
