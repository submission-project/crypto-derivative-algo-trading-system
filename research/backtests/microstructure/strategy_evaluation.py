from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .binance_trade_csv import CsvEngine, TradeBar, aggregate_trade_csv_to_bars, bars_to_backtest_points
from .cost_model import CostModel
from .metrics import compute_all_metrics
from .simulator import BacktestPoint, BacktestResult, run_directional_backtest
from .strategies.market_memory_reversion import (
    MarketMemoryReversionConfig,
    MarketMemorySignalDetail,
    generate_market_memory_reversion_details,
)
from .strategies.moving_average_crossover import generate_moving_average_crossover_signals


@dataclass(frozen=True, slots=True)
class MarketMemoryEvaluation:
    bars: list[TradeBar]
    points: list[BacktestPoint]
    details: list[MarketMemorySignalDetail]
    backtest_result: BacktestResult
    metrics: dict[str, float]
    signal_counts: dict[int, int]

    @property
    def final_equity(self) -> float:
        return self.backtest_result.equity_curve[-1]

    @property
    def trade_count(self) -> int:
        return len(self.backtest_result.trade_pnls)


@dataclass(frozen=True, slots=True)
class StrategyEvaluation:
    bars: list[TradeBar]
    points: list[BacktestPoint]
    backtest_result: BacktestResult
    metrics: dict[str, float]
    signal_counts: dict[int, int]

    @property
    def final_equity(self) -> float:
        return self.backtest_result.equity_curve[-1]

    @property
    def trade_count(self) -> int:
        return len(self.backtest_result.trade_pnls)


def _evaluate_signals(
    bars: Sequence[TradeBar],
    signals: Sequence[int],
    *,
    initial_equity: float,
    notional_per_trade: float,
    cost_model: CostModel | None,
    latency_periods: int,
    max_holding_periods: int,
    max_drawdown_stop: float,
    default_spread_bps: float,
    periods_per_year: float,
) -> StrategyEvaluation:
    bars_list = list(bars)
    signal_list = list(signals)
    points = bars_to_backtest_points(
        bars_list,
        signals=signal_list,
        default_spread_bps=default_spread_bps,
    )
    backtest_result = run_directional_backtest(
        points,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model or CostModel(),
        latency_periods=latency_periods,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        default_spread_bps=default_spread_bps,
    )
    metrics = compute_all_metrics(
        backtest_result.equity_curve,
        backtest_result.trade_pnls,
        periods_per_year=periods_per_year,
    )
    metrics.update(
        {
            "final_equity": backtest_result.equity_curve[-1],
            "forced_exits": float(backtest_result.forced_exits),
            "total_cost_paid": backtest_result.total_cost_paid,
            "long_signals": float(signal_list.count(1)),
            "short_signals": float(signal_list.count(-1)),
            "flat_signals": float(signal_list.count(0)),
        }
    )
    return StrategyEvaluation(
        bars=bars_list,
        points=points,
        backtest_result=backtest_result,
        metrics=metrics,
        signal_counts=dict(Counter(signal_list)),
    )


def evaluate_market_memory_reversion_bars(
    bars: Sequence[TradeBar],
    *,
    config: MarketMemoryReversionConfig,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    cost_model: CostModel | None = None,
    latency_periods: int = 1,
    max_holding_periods: int = 10,
    max_drawdown_stop: float = 0.10,
    default_spread_bps: float = 1.0,
    periods_per_year: float = 365.0 * 24.0,
) -> MarketMemoryEvaluation:
    bars_list = list(bars)
    details = generate_market_memory_reversion_details(bars_list, config=config)
    signals = [detail.signal for detail in details]
    evaluation = _evaluate_signals(
        bars_list,
        signals,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        latency_periods=latency_periods,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        default_spread_bps=default_spread_bps,
        periods_per_year=periods_per_year,
    )
    return MarketMemoryEvaluation(
        bars=evaluation.bars,
        points=evaluation.points,
        details=details,
        backtest_result=evaluation.backtest_result,
        metrics=evaluation.metrics,
        signal_counts=evaluation.signal_counts,
    )


def evaluate_market_memory_reversion_csv(
    paths: Sequence[str | Path],
    *,
    lines: Sequence[float],
    bucket_ms: int = 60_000,
    max_rows: int | None = None,
    engine: CsvEngine = "auto",
    line_tolerance_bps: float = 8.0,
    flow_threshold: float = 0.05,
    min_rejection_bps: float = 1.0,
    breakout_tolerance_bps: float = 20.0,
    cooldown_bars: int = 3,
    force_flat_last: bool = True,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    cost_model: CostModel | None = None,
    latency_periods: int = 1,
    max_holding_periods: int = 10,
    max_drawdown_stop: float = 0.10,
    default_spread_bps: float = 1.0,
    periods_per_year: float = 365.0 * 24.0,
) -> MarketMemoryEvaluation:
    bars = aggregate_trade_csv_to_bars(
        paths,
        bucket_ms=bucket_ms,
        max_rows=max_rows,
        engine=engine,
    )
    config = MarketMemoryReversionConfig.from_lines(
        lines,
        line_tolerance_bps=line_tolerance_bps,
        flow_threshold=flow_threshold,
        min_rejection_bps=min_rejection_bps,
        breakout_tolerance_bps=breakout_tolerance_bps,
        cooldown_bars=cooldown_bars,
        force_flat_last=force_flat_last,
    )
    return evaluate_market_memory_reversion_bars(
        bars,
        config=config,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        latency_periods=latency_periods,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        default_spread_bps=default_spread_bps,
        periods_per_year=periods_per_year,
    )


def evaluate_moving_average_crossover_bars(
    bars: Sequence[TradeBar],
    *,
    fast_window: int = 20,
    slow_window: int = 80,
    neutral_band_bps: float = 2.0,
    exit_on_neutral: bool = False,
    force_flat_last: bool = True,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    cost_model: CostModel | None = None,
    latency_periods: int = 1,
    max_holding_periods: int = 0,
    max_drawdown_stop: float = 0.10,
    default_spread_bps: float = 1.0,
    periods_per_year: float = 365.0 * 24.0,
) -> StrategyEvaluation:
    bars_list = list(bars)
    signals = generate_moving_average_crossover_signals(
        bars_list,
        fast_window=fast_window,
        slow_window=slow_window,
        neutral_band_bps=neutral_band_bps,
        exit_on_neutral=exit_on_neutral,
        force_flat_last=force_flat_last,
    )
    return _evaluate_signals(
        bars_list,
        signals,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        latency_periods=latency_periods,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        default_spread_bps=default_spread_bps,
        periods_per_year=periods_per_year,
    )


def evaluate_moving_average_crossover_csv(
    paths: Sequence[str | Path],
    *,
    bucket_ms: int = 60_000,
    max_rows: int | None = None,
    engine: CsvEngine = "auto",
    fast_window: int = 20,
    slow_window: int = 80,
    neutral_band_bps: float = 2.0,
    exit_on_neutral: bool = False,
    force_flat_last: bool = True,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    cost_model: CostModel | None = None,
    latency_periods: int = 1,
    max_holding_periods: int = 0,
    max_drawdown_stop: float = 0.10,
    default_spread_bps: float = 1.0,
    periods_per_year: float = 365.0 * 24.0,
) -> StrategyEvaluation:
    bars = aggregate_trade_csv_to_bars(
        paths,
        bucket_ms=bucket_ms,
        max_rows=max_rows,
        engine=engine,
    )
    return evaluate_moving_average_crossover_bars(
        bars,
        fast_window=fast_window,
        slow_window=slow_window,
        neutral_band_bps=neutral_band_bps,
        exit_on_neutral=exit_on_neutral,
        force_flat_last=force_flat_last,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        latency_periods=latency_periods,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        default_spread_bps=default_spread_bps,
        periods_per_year=periods_per_year,
    )
