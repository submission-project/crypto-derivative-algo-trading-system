from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.simulator import BacktestPoint, run_directional_backtest


def test_bid_ask_execution_charges_spread_crossing() -> None:
    points = [
        BacktestPoint(timestamp=1, price=100.0, bid=99.9, ask=100.1, signal=1),
        BacktestPoint(timestamp=2, price=100.0, bid=99.9, ask=100.1, signal=0),
    ]

    result = run_directional_backtest(
        points,
        initial_equity=10_000.0,
        notional_per_trade=1_000.0,
        cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
    )

    assert result.trade_count == 1
    assert result.trade_pnls[0] < 0


def test_latency_executes_on_later_price() -> None:
    points = [
        BacktestPoint(timestamp=1, price=100.0, signal=1),
        BacktestPoint(timestamp=2, price=110.0, signal=0),
        BacktestPoint(timestamp=3, price=110.0, signal=0),
    ]

    no_latency = run_directional_backtest(
        points,
        initial_equity=10_000.0,
        notional_per_trade=1_000.0,
        cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
        latency_periods=0,
        default_spread_bps=0.0,
    )
    one_period_latency = run_directional_backtest(
        points,
        initial_equity=10_000.0,
        notional_per_trade=1_000.0,
        cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
        latency_periods=1,
        default_spread_bps=0.0,
    )

    assert no_latency.trade_pnls[0] > one_period_latency.trade_pnls[0]
    assert one_period_latency.latency_periods == 1


def test_total_cost_paid_is_reported() -> None:
    points = [
        BacktestPoint(timestamp=1, price=100.0, signal=1),
        BacktestPoint(timestamp=2, price=101.0, signal=0),
    ]

    result = run_directional_backtest(
        points,
        initial_equity=10_000.0,
        notional_per_trade=1_000.0,
        cost_model=CostModel(taker_fee_bps=10.0, slippage_bps=0.0),
        default_spread_bps=0.0,
    )

    assert result.total_cost_paid == 2.0
