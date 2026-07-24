from __future__ import annotations

import pytest

from schemas.market import Exchange, MarketType
from schemas.position import (
    Position,
    PositionSide,
    PositionStatus,
    infer_position_status,
    make_position_id,
)

@pytest.mark.stable
def test_make_position_id() -> None:
    position_id = make_position_id(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        position_side=PositionSide.BOTH,
    )

    assert position_id == "BINANCE:PERP:BTCUSDT:BOTH"

@pytest.mark.stable
def test_infer_position_status_flat() -> None:
    assert infer_position_status("0") == PositionStatus.FLAT
    assert infer_position_status("0.0") == PositionStatus.FLAT
    assert infer_position_status("0.00000000") == PositionStatus.FLAT

@pytest.mark.stable
def test_infer_position_status_open_long() -> None:
    assert infer_position_status("0.01") == PositionStatus.OPEN

@pytest.mark.stable
def test_infer_position_status_open_short() -> None:
    assert infer_position_status("-0.01") == PositionStatus.OPEN


@pytest.mark.stable
def test_position_generates_position_id_and_status() -> None:
    position = Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        position_side=PositionSide.BOTH,
        position_amt="0.01",
        entry_price="60000",
        updated_ts=1_700_000_000_000,
    )

    assert position.symbol == "BTCUSDT"
    assert position.position_id == "BINANCE:PERP:BTCUSDT:BOTH"
    assert position.status == PositionStatus.OPEN

@pytest.mark.stable
def test_position_accepts_matching_position_id() -> None:
    position = Position(
        position_id="BINANCE:PERP:BTCUSDT:BOTH",
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        position_side=PositionSide.BOTH,
        position_amt="0.01",
        entry_price="60000",
        updated_ts=1_700_000_000_000,
    )

    assert position.symbol == "BTCUSDT"
    assert position.position_id == "BINANCE:PERP:BTCUSDT:BOTH"
    assert position.position_id == make_position_id(
        exchange=position.exchange,
        market_type=position.market_type,
        symbol=position.symbol,
        position_side=position.position_side,
    )
    assert position.status == PositionStatus.OPEN

@pytest.mark.stable
def test_position_rejects_mismatched_position_id() -> None:
    with pytest.raises(ValueError, match="position_id mismatch"):
        Position(
            position_id="BINANCE:PERP:ETHUSDT:BOTH",
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            position_amt="0.01",
            entry_price="60000",
            updated_ts=1_700_000_000_000,
        )

@pytest.mark.stable
def test_position_flat_when_amt_zero() -> None:
    position = Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
        position_amt="0",
        entry_price="0",
        updated_ts=1_700_000_000_000,
    )

    assert position.status == PositionStatus.FLAT
