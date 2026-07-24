from __future__ import annotations

from typing import Sequence


def calculate_drawdowns(equity_curve: Sequence[float]) -> list[float]:
    peak = float("-inf")
    drawdowns: list[float] = []
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak <= 0:
            drawdowns.append(0.0)
        else:
            drawdowns.append(equity / peak - 1.0)
    return drawdowns


def max_drawdown(equity_curve: Sequence[float]) -> float:
    drawdowns = calculate_drawdowns(equity_curve)
    return min(drawdowns) if drawdowns else 0.0


def should_stop_trading(equity_curve: Sequence[float], max_allowed_drawdown: float) -> bool:
    if max_allowed_drawdown < 0:
        raise ValueError("max_allowed_drawdown must be non-negative")
    return abs(max_drawdown(equity_curve)) >= max_allowed_drawdown
