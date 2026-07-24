import pytest
from schemas.execution import ExecutionReport
from schemas.order import OrderSide, OrderSource
from schemas.market import Exchange, MarketType

def test_execution_report_creation():
    report = ExecutionReport(
        execution_id="X456",
        order_id="O123",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        fill_price="70100.5",
        fill_quantity="0.001",
        exchange_ts=1700000000000,
        local_ts=1700000000010,
    )
    assert report.execution_id == "X456"
    assert report.commission == "0"
    assert report.commission_asset == "USDT"
    assert not report.is_maker
    assert report.latency_ms is None

def test_execution_id_generation():
    report = ExecutionReport(
        order_id="O123",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        fill_price="70100.5",
        fill_quantity="0.001",
        exchange_ts=1700000000000,
        local_ts=1700000000010,
    )
    assert report.execution_id is not None
    assert report.execution_id.startswith("X-BINANCE-PERP-")
