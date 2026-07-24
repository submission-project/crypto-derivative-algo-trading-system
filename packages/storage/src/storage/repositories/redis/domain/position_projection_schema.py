from __future__ import annotations

from typing import Any, ClassVar, cast

from schemas.position import Position, PositionStatus
from storage.repositories.redis.domain.base_projection_schema import (
    BaseRedisProjection,
    RedisProjectionField,
    normalize_int_string,
    normalize_upper_string,
)


def normalize_position_status(value: Any) -> str:
    if value is None:
        raise ValueError("position.status is required for Redis projection")

    if isinstance(value, PositionStatus):
        return value.value

    try:
        return PositionStatus(str(value)).value
    except ValueError:
        raise ValueError(f"Invalid position.status: {value!r}") from None


class PositionRedisProjection(BaseRedisProjection):
    """
    Redis position projection 저장 포맷을 명시한다.

    live hash:
        position:live:{position_id} -> Hash

    index 판단 필수 필드:
        position_id, status, position_amt,
        exchange, market_type, symbol, position_side
    """

    MODEL: ClassVar[type[Any]] = Position
    PROJECTION_NAME: ClassVar[str] = "Redis position projection"

    FIELD_DEFINITIONS: ClassVar[tuple[RedisProjectionField, ...]] = (
        RedisProjectionField("position_id", "position_id", True, "primary id"),
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
            "position state",
            normalizer=normalize_position_status,
        ),
        RedisProjectionField(
            "updated_ts",
            "updated_ts",
            True,
            "last update time",
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
            "position_side",
            "position_side",
            True,
            "position side index",
            normalizer=normalize_upper_string,
        ),
        RedisProjectionField(
            "position_amt",
            "position_amt",
            True,
            "open/flat decision",
        ),
        RedisProjectionField("entry_price", "entry_price"),
        RedisProjectionField("break_even_price", "break_even_price"),
        RedisProjectionField("mark_price", "mark_price"),
        RedisProjectionField("unrealized_pnl", "unrealized_pnl"),
        RedisProjectionField("isolated_margin", "isolated_margin"),
        RedisProjectionField("isolated_wallet", "isolated_wallet"),
        RedisProjectionField("margin_type", "margin_type"),
        RedisProjectionField("leverage", "leverage", normalizer=normalize_int_string),
        RedisProjectionField("liquidation_price", "liquidation_price"),
        RedisProjectionField("notional", "notional"),
        RedisProjectionField("update_reason", "update_reason"),
        RedisProjectionField(
            "last_event_time",
            "last_event_time",
            normalizer=normalize_int_string,
        ),
        RedisProjectionField(
            "last_transaction_time",
            "last_transaction_time",
            normalizer=normalize_int_string,
        ),
        RedisProjectionField("opened_ts", "opened_ts", normalizer=normalize_int_string),
        RedisProjectionField("closed_ts", "closed_ts", normalizer=normalize_int_string),
    )

    @classmethod
    def from_position(
        cls,
        position: Position | dict[str, Any],
    ) -> "PositionRedisProjection":
        return cast("PositionRedisProjection", cls._from_source(position))

    @property
    def position_id(self) -> str:
        return self._fields["position_id"]

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
    def position_side(self) -> str:
        return self._fields["position_side"]

    @property
    def position_amt(self) -> str:
        return self._fields["position_amt"]


PositionRedisProjection.validate_schema_once()
