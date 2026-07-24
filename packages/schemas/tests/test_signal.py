import pytest
from schemas.signal import Signal, SignalDirection, SignalStatus
from schemas.market import Exchange, MarketType


def test_signal_creation():
    sig = Signal(
        signal_id="SIG-123",
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="SOLUSDT",
        strategy_name="momentum_v1",
        direction=SignalDirection.LONG,
        confidence=0.85,
        generated_ts=1700000000000
    )
    assert sig.exchange == Exchange.BINANCE
    assert sig.market_type == MarketType.PERP
    assert sig.direction == SignalDirection.LONG
    assert sig.confidence >= 0.85
    assert sig.status == SignalStatus.PENDING

def test_signal_with_semi_auto_fields():
    sig = Signal(
        signal_id="SIG-124",
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="SOLUSDT",
        strategy_name="momentum_v1",
        direction=SignalDirection.LONG,
        confidence=0.85,
        generated_ts=1700000000000,
        status=SignalStatus.APPROVED,
        suggested_side="BUY",
        suggested_quantity="1.0",
        approved_order_id="O123"
    )
    assert sig.status == SignalStatus.APPROVED
    assert sig.suggested_side == "BUY"
    assert sig.approved_order_id == "O123"

def test_signal_id_generation():
    sig = Signal(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="SOLUSDT",
        strategy_name="momentum_v1",
        direction=SignalDirection.LONG,
        confidence=0.85,
        generated_ts=1700000000000,
    )
    assert sig.signal_id is not None
    assert sig.signal_id.startswith("S-BINANCE-PERP-")
