"""
Walk-Forward Optimization Framework.

Train-window parameter optimization → Test-window out-of-sample validation.
Each fold rolls forward in time to avoid look-ahead bias.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .cost_model import CostModel
from .metrics import compute_all_metrics
from .simulator import BacktestPoint, BacktestResult, run_directional_backtest


@dataclass(frozen=True, slots=True)
class WalkForwardFoldResult:
    fold_index: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    best_threshold: float
    train_metrics: dict[str, float]
    test_metrics: dict[str, float]


def _generate_signals_with_threshold(
    imbalances: Sequence[float],
    threshold: float,
) -> list[int]:
    """Generate directional signals from imbalance values at a given threshold."""
    signals: list[int] = []
    for imb in imbalances:
        if imb >= threshold:
            signals.append(1)
        elif imb <= -threshold:
            signals.append(-1)
        else:
            signals.append(0)
    return signals


def _run_backtest_for_threshold(
    prices: Sequence[float],
    timestamps: Sequence[int],
    imbalances: Sequence[float],
    threshold: float,
    cost_model: CostModel,
    initial_equity: float,
    notional_per_trade: float,
    max_holding_periods: int,
    latency_periods: int,
    default_spread_bps: float,
) -> tuple[BacktestResult, dict[str, float]]:
    """Run a backtest with a specific threshold and return result + metrics."""
    signals = _generate_signals_with_threshold(imbalances, threshold)
    points = [
        BacktestPoint(timestamp=ts, price=p, signal=s)
        for ts, p, s in zip(timestamps, prices, signals)
    ]
    result = run_directional_backtest(
        points,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=cost_model,
        max_holding_periods=max_holding_periods,
        latency_periods=latency_periods,
        default_spread_bps=default_spread_bps,
    )
    metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)
    return result, metrics


def walk_forward_optimize(
    prices: Sequence[float],
    timestamps: Sequence[int],
    imbalances: Sequence[float],
    *,
    n_folds: int = 5,
    train_ratio: float = 0.6,
    threshold_candidates: Sequence[float] = (0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5),
    cost_model: CostModel | None = None,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    max_holding_periods: int = 30,
    latency_periods: int = 1,
    default_spread_bps: float = 1.0,
    optimization_target: str = "sharpe",
) -> list[WalkForwardFoldResult]:
    """
    Walk-Forward Optimization.

    For each fold:
    1. Train: try all threshold candidates, pick the one with best optimization_target
    2. Test: apply best threshold to out-of-sample data and report metrics

    Returns per-fold results with train/test metrics.
    """
    n = len(prices)
    if n != len(timestamps) or n != len(imbalances):
        raise ValueError("prices, timestamps, and imbalances must have the same length")

    cost_model = cost_model or CostModel()
    fold_size = n // n_folds
    if fold_size < 10:
        raise ValueError(f"Not enough data for {n_folds} folds (need at least {n_folds * 10} points)")

    results: list[WalkForwardFoldResult] = []

    for fold_idx in range(n_folds):
        start = fold_idx * fold_size
        end = n if fold_idx == n_folds - 1 else (fold_idx + 1) * fold_size
        split = start + int((end - start) * train_ratio)

        if split - start < 5 or end - split < 5:
            continue

        # Train: find best threshold
        best_threshold = threshold_candidates[0]
        best_score = float("-inf")
        best_train_metrics: dict[str, float] = {}

        for threshold in threshold_candidates:
            _, metrics = _run_backtest_for_threshold(
                prices[start:split],
                timestamps[start:split],
                imbalances[start:split],
                threshold,
                cost_model,
                initial_equity,
                notional_per_trade,
                max_holding_periods,
                latency_periods,
                default_spread_bps,
            )
            score = metrics.get(optimization_target, 0.0)
            if score > best_score:
                best_score = score
                best_threshold = threshold
                best_train_metrics = metrics

        # Test: apply best threshold to OOS data
        _, test_metrics = _run_backtest_for_threshold(
            prices[split:end],
            timestamps[split:end],
            imbalances[split:end],
            best_threshold,
            cost_model,
            initial_equity,
            notional_per_trade,
            max_holding_periods,
            latency_periods,
            default_spread_bps,
        )

        results.append(
            WalkForwardFoldResult(
                fold_index=fold_idx,
                train_start=timestamps[start],
                train_end=timestamps[split - 1],
                test_start=timestamps[split],
                test_end=timestamps[end - 1],
                best_threshold=best_threshold,
                train_metrics=best_train_metrics,
                test_metrics=test_metrics,
            )
        )

    return results


def summarize_walk_forward(folds: Sequence[WalkForwardFoldResult]) -> dict[str, float]:
    """Aggregate walk-forward results across folds."""
    if not folds:
        return {}

    keys = list(folds[0].test_metrics.keys())
    summary: dict[str, float] = {}

    for key in keys:
        values = [f.test_metrics.get(key, 0.0) for f in folds]
        summary[f"avg_oos_{key}"] = sum(values) / len(values)

    # Also report threshold consistency
    thresholds = [f.best_threshold for f in folds]
    summary["avg_best_threshold"] = sum(thresholds) / len(thresholds)
    summary["threshold_std"] = (
        sum((t - summary["avg_best_threshold"]) ** 2 for t in thresholds) / len(thresholds)
    ) ** 0.5

    return summary
