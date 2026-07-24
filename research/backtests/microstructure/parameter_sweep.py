from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .cost_model import CostModel
from .metrics import compute_all_metrics
from .simulator import BacktestPoint, run_directional_backtest


@dataclass(frozen=True, slots=True)
class SweepRow:
    threshold: float
    slippage_bps: float
    latency_periods: int
    total_return: float
    sharpe: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    total_trades: float
    total_cost_paid: float


def _signals_from_threshold(imbalances: Sequence[float], threshold: float) -> list[int]:
    signals: list[int] = []
    for imbalance in imbalances:
        if imbalance >= threshold:
            signals.append(1)
        elif imbalance <= -threshold:
            signals.append(-1)
        else:
            signals.append(0)
    return signals


def run_cost_latency_sweep(
    *,
    prices: Sequence[float],
    timestamps: Sequence[int],
    imbalances: Sequence[float],
    thresholds: Sequence[float] = (0.25, 0.30, 0.35, 0.40, 0.45),
    slippage_values_bps: Sequence[float] = (0.5, 1.0, 2.0),
    latency_values_periods: Sequence[int] = (0, 1, 2),
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    taker_fee_bps: float = 4.0,
    default_spread_bps: float = 1.0,
    max_holding_periods: int = 30,
) -> list[SweepRow]:
    if len(prices) != len(timestamps) or len(prices) != len(imbalances):
        raise ValueError("prices, timestamps, and imbalances must have the same length")

    rows: list[SweepRow] = []
    for threshold in thresholds:
        signals = _signals_from_threshold(imbalances, threshold)
        points = [
            BacktestPoint(timestamp=ts, price=price, signal=signal)
            for ts, price, signal in zip(timestamps, prices, signals)
        ]

        for slippage_bps in slippage_values_bps:
            for latency_periods in latency_values_periods:
                result = run_directional_backtest(
                    points,
                    initial_equity=initial_equity,
                    notional_per_trade=notional_per_trade,
                    cost_model=CostModel(
                        taker_fee_bps=taker_fee_bps,
                        slippage_bps=slippage_bps,
                    ),
                    max_holding_periods=max_holding_periods,
                    latency_periods=latency_periods,
                    default_spread_bps=default_spread_bps,
                )
                metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)
                rows.append(
                    SweepRow(
                        threshold=threshold,
                        slippage_bps=slippage_bps,
                        latency_periods=latency_periods,
                        total_return=metrics["total_return"],
                        sharpe=metrics["sharpe"],
                        max_drawdown=metrics["max_drawdown"],
                        win_rate=metrics["win_rate"],
                        profit_factor=metrics["profit_factor"],
                        total_trades=metrics["total_trades"],
                        total_cost_paid=result.total_cost_paid,
                    )
                )
    return rows
