from __future__ import annotations

from math import sqrt
from statistics import mean, pstdev
from typing import Sequence


def total_return(equity_curve: Sequence[float]) -> float:
    if len(equity_curve) < 2 or equity_curve[0] == 0:
        return 0.0
    return equity_curve[-1] / equity_curve[0] - 1.0


def returns_from_equity(equity_curve: Sequence[float]) -> list[float]:
    returns: list[float] = []
    for prev, current in zip(equity_curve, equity_curve[1:]):
        returns.append(0.0 if prev == 0 else current / prev - 1.0)
    return returns


def sharpe_ratio(returns: Sequence[float], periods_per_year: float = 365.0 * 24.0) -> float:
    if len(returns) < 2:
        return 0.0
    std = pstdev(returns)
    if std == 0:
        return 0.0
    return mean(returns) / std * sqrt(periods_per_year)


def max_drawdown(equity_curve: Sequence[float]) -> float:
    peak = float("-inf")
    max_dd = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak <= 0:
            continue
        drawdown = equity / peak - 1.0
        max_dd = min(max_dd, drawdown)
    return max_dd


def profit_factor(trade_pnls: Sequence[float]) -> float:
    gross_profit = sum(pnl for pnl in trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in trade_pnls if pnl < 0))
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def win_rate(trade_pnls: Sequence[float]) -> float:
    if not trade_pnls:
        return 0.0
    return sum(1 for pnl in trade_pnls if pnl > 0) / len(trade_pnls)


# ── Advanced Metrics ──


def sortino_ratio(
    returns: Sequence[float],
    periods_per_year: float = 365.0 * 24.0,
    target: float = 0.0,
) -> float:
    """Sortino ratio: penalises only downside volatility."""
    if len(returns) < 2:
        return 0.0
    downside = [min(r - target, 0.0) for r in returns]
    downside_std = pstdev(downside)
    if downside_std == 0:
        return 0.0
    return (mean(returns) - target) / downside_std * sqrt(periods_per_year)


def calmar_ratio(equity_curve: Sequence[float], periods_per_year: float = 365.0 * 24.0) -> float:
    """Calmar ratio: annualised return / max drawdown."""
    if len(equity_curve) < 2:
        return 0.0
    mdd = abs(max_drawdown(equity_curve))
    if mdd == 0:
        return 0.0
    period_returns = returns_from_equity(equity_curve)
    ann_return = mean(period_returns) * periods_per_year
    return ann_return / mdd


def max_consecutive_losses(trade_pnls: Sequence[float]) -> int:
    """Maximum streak of consecutive losing trades."""
    max_streak = 0
    current_streak = 0
    for pnl in trade_pnls:
        if pnl < 0:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    return max_streak


def max_consecutive_wins(trade_pnls: Sequence[float]) -> int:
    """Maximum streak of consecutive winning trades."""
    max_streak = 0
    current_streak = 0
    for pnl in trade_pnls:
        if pnl > 0:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0
    return max_streak


def average_trade_pnl(trade_pnls: Sequence[float]) -> float:
    """Average PnL per trade."""
    if not trade_pnls:
        return 0.0
    return sum(trade_pnls) / len(trade_pnls)


def avg_win_loss_ratio(trade_pnls: Sequence[float]) -> float:
    """Ratio of average winning trade to average losing trade."""
    wins = [pnl for pnl in trade_pnls if pnl > 0]
    losses = [pnl for pnl in trade_pnls if pnl < 0]
    if not wins or not losses:
        return 0.0
    avg_win = sum(wins) / len(wins)
    avg_loss = abs(sum(losses) / len(losses))
    if avg_loss == 0:
        return float("inf")
    return avg_win / avg_loss


def compute_all_metrics(
    equity_curve: Sequence[float],
    trade_pnls: Sequence[float],
    periods_per_year: float = 365.0 * 24.0,
) -> dict[str, float]:
    """Compute a comprehensive metrics dictionary."""
    period_returns = returns_from_equity(equity_curve)
    return {
        "total_return": total_return(equity_curve),
        "sharpe": sharpe_ratio(period_returns, periods_per_year),
        "sortino": sortino_ratio(period_returns, periods_per_year),
        "calmar": calmar_ratio(equity_curve, periods_per_year),
        "max_drawdown": max_drawdown(equity_curve),
        "win_rate": win_rate(trade_pnls),
        "profit_factor": profit_factor(trade_pnls),
        "avg_trade_pnl": average_trade_pnl(trade_pnls),
        "avg_win_loss_ratio": avg_win_loss_ratio(trade_pnls),
        "total_trades": float(len(trade_pnls)),
        "max_consecutive_losses": float(max_consecutive_losses(trade_pnls)),
        "max_consecutive_wins": float(max_consecutive_wins(trade_pnls)),
    }
