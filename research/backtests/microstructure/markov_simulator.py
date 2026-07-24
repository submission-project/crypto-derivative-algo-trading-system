from __future__ import annotations

import random
from dataclasses import dataclass
from statistics import median
from typing import Sequence

from .metrics import max_drawdown, total_return
from .regime_model import MarketRegime


@dataclass(frozen=True, slots=True)
class RegimeSimulationPath:
    regimes: list[MarketRegime]
    returns: list[float]
    equity_curve: list[float]


@dataclass(frozen=True, slots=True)
class DrawdownSimulationSummary:
    path_count: int
    median_total_return: float
    p05_total_return: float
    p95_total_return: float
    median_max_drawdown: float
    p05_max_drawdown: float
    p95_max_drawdown: float
    ruin_probability: float


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    if not 0 <= q <= 1:
        raise ValueError("q must be between 0 and 1")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
    return ordered[idx]


def _weighted_choice(
    rng: random.Random,
    row: dict[MarketRegime, float],
    fallback: MarketRegime,
) -> MarketRegime:
    threshold = rng.random()
    cumulative = 0.0
    for regime, probability in row.items():
        cumulative += max(0.0, probability)
        if threshold <= cumulative:
            return regime
    return fallback


def simulate_regime_paths(
    *,
    transition_matrix: dict[MarketRegime, dict[MarketRegime, float]],
    state_return_samples: dict[MarketRegime, Sequence[float]],
    initial_regime: MarketRegime,
    initial_equity: float = 10_000.0,
    n_steps: int = 250,
    n_paths: int = 1_000,
    seed: int = 7,
) -> list[RegimeSimulationPath]:
    if initial_equity <= 0:
        raise ValueError("initial_equity must be positive")
    if n_steps < 1 or n_paths < 1:
        raise ValueError("n_steps and n_paths must be positive")

    rng = random.Random(seed)
    paths: list[RegimeSimulationPath] = []

    for _ in range(n_paths):
        regime = initial_regime
        regimes = [regime]
        returns: list[float] = []
        equity = initial_equity
        equity_curve = [equity]

        for _step in range(n_steps):
            row = transition_matrix.get(regime, {})
            regime = _weighted_choice(rng, row, fallback=regime)
            samples = list(state_return_samples.get(regime, ()))
            period_return = rng.choice(samples) if samples else 0.0
            equity *= 1.0 + period_return
            regimes.append(regime)
            returns.append(period_return)
            equity_curve.append(equity)

        paths.append(
            RegimeSimulationPath(
                regimes=regimes,
                returns=returns,
                equity_curve=equity_curve,
            )
        )

    return paths


def summarize_drawdown_distribution(
    paths: Sequence[RegimeSimulationPath],
    *,
    ruin_drawdown: float = 0.30,
) -> DrawdownSimulationSummary:
    if ruin_drawdown < 0:
        raise ValueError("ruin_drawdown must be non-negative")

    total_returns = [total_return(path.equity_curve) for path in paths]
    drawdowns = [max_drawdown(path.equity_curve) for path in paths]
    ruin_count = sum(1 for dd in drawdowns if abs(dd) >= ruin_drawdown)

    return DrawdownSimulationSummary(
        path_count=len(paths),
        median_total_return=median(total_returns) if total_returns else 0.0,
        p05_total_return=_quantile(total_returns, 0.05),
        p95_total_return=_quantile(total_returns, 0.95),
        median_max_drawdown=median(drawdowns) if drawdowns else 0.0,
        p05_max_drawdown=_quantile(drawdowns, 0.05),
        p95_max_drawdown=_quantile(drawdowns, 0.95),
        ruin_probability=ruin_count / len(paths) if paths else 0.0,
    )
