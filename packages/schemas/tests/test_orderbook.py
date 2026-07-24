import pytest
from schemas.orderbook import OrderbookUpdate, OrderbookSnapshot
from schemas.market import Exchange, MarketType


def test_orderbook_snapshot():
    snap = OrderbookSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.SPOT,
        symbol="ETHUSDT",
        bids=[(2000.0, 1.5), (1999.0, 2.0)],
        asks=[(2001.0, 1.0)],
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        last_update_id=100
    )
    assert snap.exchange == Exchange.BINANCE
    assert snap.market_type == MarketType.SPOT
    assert len(snap.bids) == 2
    assert snap.asks[0][0] == 2001.0


def test_orderbook_update():
    update = OrderbookUpdate(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="ETHUSDT",
        bids=[],
        asks=[(2001.0, 0.0)],  # Delete level
        exchange_ts=1700000000000,
        local_ts=1700000000010,
        update_id=101
    )
    assert update.market_type == MarketType.PERP
    assert update.update_id == 101
