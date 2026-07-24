from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import log, sqrt
from statistics import pstdev
from typing import Sequence

from risk.sizing import volatility_target_notional

from .cost_model import CostModel
from .metrics import compute_all_metrics
from .simulator import BacktestPoint, run_directional_backtest


class SizingMethod(str, Enum):
    FIXED_NOTIONAL = "fixed_notional"
    FIXED_FRACTIONAL = "fixed_fractional"
    CAPPED_KELLY = "capped_kelly"
    VOLATILITY_TARGET = "volatility_target"


@dataclass(frozen=True, slots=True)
class SizingComparisonRow:
    method: SizingMethod
    notional_used: float
    total_return: float
    sharpe: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    total_trades: float
    total_cost_paid: float


def realized_period_volatility(prices: Sequence[float]) -> float:
    returns = [
        log(prices[idx] / prices[idx - 1])
        for idx in range(1, len(prices))
        if prices[idx - 1] > 0 and prices[idx] > 0
    ]
    return pstdev(returns) if len(returns) > 1 else 0.0


def run_risk_sizing_comparison(
    points: Sequence[BacktestPoint],
    *,
    initial_equity: float = 10_000.0,
    base_notional: float = 1_000.0,
    fixed_fraction: float = 0.10,
    kelly_win_rate: float = 0.55,
    kelly_avg_win: float = 1.0,
    kelly_avg_loss: float = 1.0,
    kelly_cap: float = 0.20,
    target_period_volatility: float = 0.002,
    max_vol_fraction: float = 0.20,
    cost_model: CostModel | None = None,
    max_drawdown_stop: float = 0.10,
    max_position_notional: float = 2_000.0,
) -> list[SizingComparisonRow]:
    cost_model = cost_model or CostModel()
    prices = [point.price for point in points]
    realized_vol = realized_period_volatility(prices)
    vol_target_notional = volatility_target_notional(
        equity=initial_equity,
        target_volatility=target_period_volatility,
        realized_volatility=realized_vol,
        max_fraction=max_vol_fraction,
    )

    runs = [
        (
            SizingMethod.FIXED_NOTIONAL,
            {
                "notional_per_trade": base_notional,
            },
            base_notional,
        ),
        (
            SizingMethod.FIXED_FRACTIONAL,
            {
                "notional_per_trade": base_notional,
                "fixed_fraction": fixed_fraction,
            },
            initial_equity * fixed_fraction,
        ),
        (
            SizingMethod.CAPPED_KELLY,
            {
                "notional_per_trade": base_notional,
                "use_kelly_sizing": True,
                "kelly_win_rate": kelly_win_rate,
                "kelly_avg_win": kelly_avg_win,
                "kelly_avg_loss": kelly_avg_loss,
                "kelly_cap": kelly_cap,
            },
            initial_equity * kelly_cap,
        ),
        (
            SizingMethod.VOLATILITY_TARGET,
            {
                "notional_per_trade": vol_target_notional,
            },
            vol_target_notional,
        ),
    ]

    rows: list[SizingComparisonRow] = []
    for method, kwargs, notional_used in runs:
        result = run_directional_backtest(
            points,
            initial_equity=initial_equity,
            cost_model=cost_model,
            max_drawdown_stop=max_drawdown_stop,
            max_position_notional=max_position_notional,
            **kwargs,
        )
        metrics = compute_all_metrics(result.equity_curve, result.trade_pnls)
        rows.append(
            SizingComparisonRow(
                method=method,
                notional_used=notional_used,
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
