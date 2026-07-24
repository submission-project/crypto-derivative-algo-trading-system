from decimal import Decimal

import pytest
from pydantic import ValidationError
from schemas.market import (
    CanonicalTrade,
    Exchange,
    MarketType,
    Ticker,
    Trade,
    TradeSource,
)


def test_exchange_enum():
    assert Exchange.BINANCE.value == "BINANCE"
    assert Exchange.OKX.value == "OKX"
    assert Exchange.BYBIT.value == "BYBIT"
    assert Exchange.BITGET.value == "BITGET"
    assert Exchange.GATE.value == "GATE"
    assert Exchange.MEXC.value == "MEXC"
    assert Exchange.KUCOIN.value == "KUCOIN"
    assert Exchange.HTX.value == "HTX"
    assert Exchange.KRAKEN.value == "KRAKEN"


def test_market_type_enum():
    assert MarketType.SPOT.value == "SPOT"
    assert MarketType.PERP.value == "PERP"
    assert MarketType.FUTURES.value == "FUTURES"


def test_trade_spot():
    trade = Trade(
        exchange=Exchange.BINANCE,
        market_type=MarketType.SPOT,
        symbol="BTCUSDT",
        price="50000.0",
        size="0.1",
        is_buyer_maker=False,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=12345,
    )
    assert trade.exchange == Exchange.BINANCE
    assert trade.market_type == MarketType.SPOT
    assert trade.symbol == "BTCUSDT"
    assert trade.price == "50000.0"
    assert trade.size == "0.1"
    assert trade.trade_id == 12345


def test_trade_perp():
    trade = Trade(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        price="50000.0",
        size="0.1",
        is_buyer_maker=True,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=67890,
    )
    assert trade.market_type == MarketType.PERP


def test_trade_validation_missing_fields():
    with pytest.raises(ValidationError):
        Trade(symbol="BTCUSDT")  # Missing required fields


def test_price_size_must_be_decimal_string():
    """가격/수량은 정밀도 보존 정책상 십진 문자열이어야 합니다 — float은 거부."""
    base = dict(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        is_buyer_maker=False,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=1,
    )
    # float 입력은 거부되어야 함 (정책 강제)
    with pytest.raises(ValidationError):
        Trade(price=50000.0, size="0.1", **base)
    with pytest.raises(ValidationError):
        Trade(price="50000.0", size=0.1, **base)
    # 잘못된 형식의 문자열도 거부
    with pytest.raises(ValidationError):
        Trade(price="not-a-number", size="0.1", **base)
    # 과학적 표기 (1e5)는 거래소가 보내지 않으므로 거부
    with pytest.raises(ValidationError):
        Trade(price="1e5", size="0.1", **base)


def test_decimal_string_preserves_precision():
    """문자열 운반은 거래소 원본 byte를 그대로 유지."""
    trade = Trade(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        price="70123.123456789012345",  # float이면 손실되는 정밀도
        size="0.000000001",
        is_buyer_maker=False,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=999,
    )
    assert trade.price == "70123.123456789012345"
    assert trade.size == "0.000000001"


def test_price_decimal_helper():
    """비즈니스 로직에서 안전한 산술을 위한 Decimal 헬퍼."""
    trade = Trade(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        price="0.1",
        size="0.2",
        is_buyer_maker=False,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=1,
    )
    assert trade.price_decimal() == Decimal("0.1")
    assert trade.size_decimal() == Decimal("0.2")
    # Decimal 산술은 부동소수점 오차가 없음
    assert trade.price_decimal() + trade.size_decimal() == Decimal("0.3")


def test_canonical_trade_inherits_decimal_string_policy():
    """CanonicalTrade도 동일한 정밀도 정책을 따라야 함."""
    canonical = CanonicalTrade(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        price="70000.5",
        size="0.001",
        is_buyer_maker=False,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        trade_id=12345,
        source=TradeSource.UNDOCUMENTED_TRADE,
    )
    assert canonical.price == "70000.5"
    assert canonical.size == "0.001"
    assert canonical.verified_by_rest is False
    assert canonical.source == TradeSource.UNDOCUMENTED_TRADE


def test_ticker_with_market_type():
    ticker = Ticker(
        exchange=Exchange.OKX,
        market_type=MarketType.PERP,
        symbol="ETHUSDT",
        bid_price=2000.0,
        bid_size=1.5,
        ask_price=2001.0,
        ask_size=1.0,
        exchange_ts=1700000000000,
        local_ts=1700000000010,
    )
    assert ticker.exchange == Exchange.OKX
    assert ticker.market_type == MarketType.PERP
