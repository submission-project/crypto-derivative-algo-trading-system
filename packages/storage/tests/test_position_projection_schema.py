from __future__ import annotations

import pytest

from schemas.market import Exchange, MarketType
from schemas.position import Position, PositionSide, PositionStatus
from storage.repositories.redis.domain.position_projection_schema import (
    PositionRedisProjection,
)


def make_position(
    *,
    amt: str,
    updated_ts: int = 1_700_000_000_000,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        position_side=PositionSide.BOTH,
        position_amt=amt,
        entry_price="60000",
        mark_price="61000",
        unrealized_pnl="10",
        leverage=10,
        last_event_time=1_700_000_000_100,
        opened_ts=1_700_000_000_000,
        updated_ts=updated_ts,
        version=2,
    )


def test_open_position_projection_normalizes_index_fields() -> None:
    projection = PositionRedisProjection.from_position(make_position(amt="0.01"))
    fields = projection.to_hash()

    assert fields["position_id"] == "BINANCE:PERP:BTCUSDT:BOTH"
    assert fields["exchange"] == Exchange.BINANCE.value
    assert fields["market_type"] == MarketType.PERP.value
    assert fields["symbol"] == "BTCUSDT"
    assert fields["position_side"] == PositionSide.BOTH.value
    assert fields["status"] == PositionStatus.OPEN.value
    assert fields["position_amt"] == "0.01"
    assert fields["leverage"] == "10"
    assert fields["version"] == "2"

    assert projection.position_id == "BINANCE:PERP:BTCUSDT:BOTH"
    assert projection.status == PositionStatus.OPEN.value
    assert projection.position_amt == "0.01"


def test_flat_position_projection_stores_zero_amount() -> None:
    projection = PositionRedisProjection.from_position(make_position(amt="0"))
    fields = projection.to_hash()

    assert fields["status"] == PositionStatus.FLAT.value
    assert fields["position_amt"] == "0"
    assert fields["symbol"] == "BTCUSDT"


def test_position_projection_accepts_dict_and_normalizes_fields() -> None:
    projection = PositionRedisProjection.from_position(
        {
            "position_id": "BINANCE:PERP:BTCUSDT:BOTH",
            "exchange": "binance",
            "market_type": "perp",
            "symbol": "btcusdt",
            "position_side": "both",
            "status": PositionStatus.OPEN.value,
            "position_amt": "0.01",
            "updated_ts": 1_700_000_000_000,
            "version": 3,
        }
    )
    fields = projection.to_hash()

    assert fields["exchange"] == "BINANCE"
    assert fields["market_type"] == "PERP"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["position_side"] == "BOTH"
    assert fields["version"] == "3"


def test_position_projection_rejects_missing_required_field() -> None:
    with pytest.raises(ValueError, match="position_id"):
        PositionRedisProjection.from_position(
            {
                "exchange": "BINANCE",
                "market_type": "PERP",
                "symbol": "BTCUSDT",
                "position_side": "BOTH",
                "status": PositionStatus.OPEN.value,
                "position_amt": "0.01",
                "updated_ts": 1_700_000_000_000,
                "version": 1,
            }
        )


def test_position_projection_schema_describe_uses_attr_metadata() -> None:
    described = PositionRedisProjection.describe()
    position_amt_field = next(
        field for field in described if field["field"] == "position_amt"
    )
    position_id_field = next(
        field for field in described if field["field"] == "position_id"
    )

    assert position_amt_field["attr"] == "position_amt"
    assert position_amt_field["required"] is True
    assert position_id_field["purpose"] == "primary id"
