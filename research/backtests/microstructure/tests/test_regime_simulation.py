import pytest

from research.backtests.microstructure.cost_model import CostModel
from research.backtests.microstructure.markov_simulator import (
    simulate_regime_paths,
    summarize_drawdown_distribution,
)
from research.backtests.microstructure.paper_trading_validation import (
    PaperTradingValidationReport,
    build_paper_trading_checklist,
)
from research.backtests.microstructure.regime_model import (
    MarketRegime,
    classify_regime,
    estimate_transition_matrix,
)
from research.backtests.microstructure.risk_sizing_comparison import (
    SizingMethod,
    run_risk_sizing_comparison,
)
from research.backtests.microstructure.simulator import BacktestPoint
from research.backtests.microstructure.stress_scenarios import (
    StressConfig,
    StressScenario,
    run_stress_backtest,
)


def _sample_points() -> list[BacktestPoint]:
    return [
        BacktestPoint(timestamp=1, price=100.0, signal=1, bar_volume_usd=100_000.0),
        BacktestPoint(timestamp=2, price=101.0, signal=1, bar_volume_usd=100_000.0),
        BacktestPoint(timestamp=3, price=100.5, signal=0, bar_volume_usd=80_000.0),
        BacktestPoint(timestamp=4, price=99.0, signal=-1, bar_volume_usd=70_000.0),
        BacktestPoint(timestamp=5, price=98.5, signal=0, bar_volume_usd=90_000.0),
    ]

@pytest.mark.research
def test_classify_regime_detects_stress() -> None:
    regime = classify_regime(
        price_position=0.5,
        oi_position=0.5,
        taker_buy_density=0.5,
        spread_bps=10.0,
        realized_vol_bps=5.0,
    )
    assert regime == MarketRegime.STRESS


def test_transition_matrix_rows_sum_to_one_with_smoothing() -> None:
    matrix = estimate_transition_matrix(
        [
            MarketRegime.NORMAL_BOX,
            MarketRegime.ACCUMULATION,
            MarketRegime.NORMAL_BOX,
        ],
        smoothing=0.1,
    )
    assert round(sum(matrix[MarketRegime.NORMAL_BOX].values()), 10) == 1.0


def test_markov_simulation_summary_reports_paths() -> None:
    matrix = estimate_transition_matrix(
        [
            MarketRegime.NORMAL_BOX,
            MarketRegime.ACCUMULATION,
            MarketRegime.NORMAL_BOX,
            MarketRegime.STRESS,
        ],
        smoothing=0.1,
    )
    paths = simulate_regime_paths(
        transition_matrix=matrix,
        state_return_samples={
            MarketRegime.NORMAL_BOX: [0.001, -0.0005],
            MarketRegime.ACCUMULATION: [0.002],
            MarketRegime.DISTRIBUTION: [0.001],
            MarketRegime.BREAKOUT: [0.003, -0.003],
            MarketRegime.STRESS: [-0.01],
        },
        initial_regime=MarketRegime.NORMAL_BOX,
        n_steps=10,
        n_paths=20,
    )
    summary = summarize_drawdown_distribution(paths, ruin_drawdown=0.2)
    assert summary.path_count == 20


def test_stress_backtest_runs_flash_crash() -> None:
    result = run_stress_backtest(
        _sample_points(),
        StressConfig(scenario=StressScenario.FLASH_CRASH),
        base_cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
    )
    assert result.scenario == StressScenario.FLASH_CRASH
    assert "max_drawdown" in result.metrics


def test_risk_sizing_comparison_returns_all_methods() -> None:
    rows = run_risk_sizing_comparison(
        _sample_points(),
        cost_model=CostModel(taker_fee_bps=0.0, slippage_bps=0.0),
    )
    assert {row.method for row in rows} == set(SizingMethod)


def test_paper_trading_checklist_summary() -> None:
    checks = build_paper_trading_checklist(
        signal_to_order=True,
        testnet_submit=True,
        evidence_prefix="unit",
    )
    report = PaperTradingValidationReport(venue="binance-testnet", checks=checks)
    assert report.pass_rate == 0.25
    assert not report.is_ready
