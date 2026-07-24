from research.backtests.microstructure.binance_trade_csv import TradeBar
from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.strategy_evaluation import (
    evaluate_market_memory_reversion_bars,
    evaluate_moving_average_crossover_bars,
)
from research.backtests.microstructure.strategies.market_memory_reversion import MarketMemoryReversionConfig


def _bar(
    timestamp: int,
    price: float,
    *,
    buy_quote: float = 50.0,
    sell_quote: float = 50.0,
) -> TradeBar:
    return TradeBar(
        bucket_start_ms=timestamp,
        close_price=price,
        quote_volume=buy_quote + sell_quote,
        taker_buy_quote_volume=buy_quote,
        taker_sell_quote_volume=sell_quote,
        trade_count=10,
    )


def test_evaluate_market_memory_reversion_bars_returns_metrics_and_details() -> None:
    bars = [
        _bar(1, 99.45, buy_quote=50.0, sell_quote=50.0),
        _bar(2, 99.60, buy_quote=70.0, sell_quote=30.0),
        _bar(3, 100.20, buy_quote=50.0, sell_quote=50.0),
        _bar(4, 104.98, buy_quote=50.0, sell_quote=50.0),
        _bar(5, 104.88, buy_quote=30.0, sell_quote=70.0),
        _bar(6, 104.20, buy_quote=50.0, sell_quote=50.0),
    ]
    config = MarketMemoryReversionConfig.from_lines(
        [99.5, 102.0, 105.0],
        line_tolerance_bps=15.0,
        flow_threshold=0.10,
        min_rejection_bps=2.0,
        cooldown_bars=0,
        force_flat_last=False,
    )

    evaluation = evaluate_market_memory_reversion_bars(
        bars,
        config=config,
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0),
        max_holding_periods=2,
    )

    assert evaluation.signal_counts[1] == 1
    assert evaluation.signal_counts[-1] == 1
    assert evaluation.metrics["total_trades"] >= 1
    assert "total_return" in evaluation.metrics
    assert evaluation.details[1].reason == "support_rejection_with_buy_flow"


def test_evaluate_moving_average_crossover_bars_returns_metrics() -> None:
    bars = [_bar(idx, float(price)) for idx, price in enumerate([100, 101, 102, 103, 104, 105], start=1)]

    evaluation = evaluate_moving_average_crossover_bars(
        bars,
        fast_window=2,
        slow_window=3,
        neutral_band_bps=0.0,
        cost_model=CostModel(taker_fee_bps=4.0, slippage_bps=1.0),
        max_holding_periods=2,
    )

    assert evaluation.signal_counts[1] >= 1
    assert "total_return" in evaluation.metrics
    assert "total_cost_paid" in evaluation.metrics
