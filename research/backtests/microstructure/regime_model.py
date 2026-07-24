from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import log, sqrt
from statistics import mean, pstdev
from typing import Sequence


class MarketRegime(str, Enum):
    NORMAL_BOX = "normal_box"
    ACCUMULATION = "accumulation"
    DISTRIBUTION = "distribution"
    BREAKOUT = "breakout"
    STRESS = "stress"


@dataclass(frozen=True, slots=True)
class RegimePoint:
    timestamp: int
    regime: MarketRegime
    price_position: float
    oi_position: float
    taker_buy_density: float
    spread_bps: float
    realized_vol_bps: float


def rolling_box_position(values: Sequence[float], window: int) -> list[float | None]:
    if window < 2:
        raise ValueError("window must be >= 2")

    positions: list[float | None] = []
    for idx, value in enumerate(values):
        if idx < window - 1:
            positions.append(None)
            continue
        chunk = values[idx - window + 1 : idx + 1]
        low = min(chunk)
        high = max(chunk)
        if high <= low:
            positions.append(0.5)
        else:
            positions.append((value - low) / (high - low))
    return positions


def rolling_return_volatility_bps(prices: Sequence[float], window: int) -> list[float | None]:
    if window < 2:
        raise ValueError("window must be >= 2")

    result: list[float | None] = []
    for idx in range(len(prices)):
        if idx < window:
            result.append(None)
            continue
        returns = [
            log(prices[j] / prices[j - 1])
            for j in range(idx - window + 1, idx + 1)
            if prices[j - 1] > 0 and prices[j] > 0
        ]
        if len(returns) < 2:
            result.append(None)
            continue
        result.append(pstdev(returns) * 10_000.0)
    return result


def classify_regime(
    *,
    price_position: float,
    oi_position: float,
    taker_buy_density: float,
    spread_bps: float,
    realized_vol_bps: float,
    max_spread_bps: float = 5.0,
    stress_vol_bps: float = 30.0,
) -> MarketRegime:
    if spread_bps > max_spread_bps or realized_vol_bps > stress_vol_bps:
        return MarketRegime.STRESS

    if price_position < 0.20 and oi_position < 0.30 and taker_buy_density > 0.55:
        return MarketRegime.ACCUMULATION

    if price_position > 0.80 and oi_position > 0.70 and taker_buy_density < 0.45:
        return MarketRegime.DISTRIBUTION

    if price_position <= 0.05 or price_position >= 0.95:
        return MarketRegime.BREAKOUT

    return MarketRegime.NORMAL_BOX


def estimate_transition_matrix(
    regimes: Sequence[MarketRegime],
    *,
    smoothing: float = 0.0,
) -> dict[MarketRegime, dict[MarketRegime, float]]:
    if smoothing < 0:
        raise ValueError("smoothing must be non-negative")

    counts = {
        src: {dst: smoothing for dst in MarketRegime}
        for src in MarketRegime
    }

    for current, nxt in zip(regimes, regimes[1:]):
        counts[current][nxt] += 1.0

    matrix: dict[MarketRegime, dict[MarketRegime, float]] = {}
    for src, row in counts.items():
        total = sum(row.values())
        if total == 0:
            matrix[src] = {dst: 0.0 for dst in MarketRegime}
        else:
            matrix[src] = {dst: count / total for dst, count in row.items()}
    return matrix


def label_regime_points(
    *,
    timestamps: Sequence[int],
    prices: Sequence[float],
    open_interest: Sequence[float],
    taker_buy_density: Sequence[float],
    spread_bps: Sequence[float] | None = None,
    box_window: int = 50,
    vol_window: int = 20,
    max_spread_bps: float = 5.0,
    stress_vol_bps: float = 30.0,
) -> list[RegimePoint]:
    n = len(prices)
    if not (len(timestamps) == n and len(open_interest) == n and len(taker_buy_density) == n):
        raise ValueError("timestamps, prices, open_interest, and taker_buy_density must match")

    spreads = list(spread_bps) if spread_bps is not None else [1.0] * n
    if len(spreads) != n:
        raise ValueError("spread_bps must match prices length")

    price_positions = rolling_box_position(prices, box_window)
    oi_positions = rolling_box_position(open_interest, box_window)
    vol_bps = rolling_return_volatility_bps(prices, vol_window)

    points: list[RegimePoint] = []
    for idx in range(n):
        if price_positions[idx] is None or oi_positions[idx] is None:
            continue
        realized_vol = vol_bps[idx] if vol_bps[idx] is not None else 0.0
        regime = classify_regime(
            price_position=price_positions[idx] or 0.5,
            oi_position=oi_positions[idx] or 0.5,
            taker_buy_density=taker_buy_density[idx],
            spread_bps=spreads[idx],
            realized_vol_bps=realized_vol,
            max_spread_bps=max_spread_bps,
            stress_vol_bps=stress_vol_bps,
        )
        points.append(
            RegimePoint(
                timestamp=timestamps[idx],
                regime=regime,
                price_position=price_positions[idx] or 0.5,
                oi_position=oi_positions[idx] or 0.5,
                taker_buy_density=taker_buy_density[idx],
                spread_bps=spreads[idx],
                realized_vol_bps=realized_vol,
            )
        )
    return points


def returns_by_regime(
    prices: Sequence[float],
    regimes: Sequence[MarketRegime],
) -> dict[MarketRegime, list[float]]:
    if len(prices) < 2:
        return {regime: [] for regime in MarketRegime}
    if len(regimes) not in {len(prices), len(prices) - 1}:
        raise ValueError("regimes length must match prices or one-step returns")

    buckets = {regime: [] for regime in MarketRegime}
    aligned_regimes = regimes[:-1] if len(regimes) == len(prices) else regimes
    for idx, regime in enumerate(aligned_regimes):
        if prices[idx] <= 0:
            continue
        buckets[regime].append(prices[idx + 1] / prices[idx] - 1.0)
    return buckets


def transition_risk_score(
    matrix: dict[MarketRegime, dict[MarketRegime, float]],
    current: MarketRegime,
) -> float:
    row = matrix.get(current, {})
    return row.get(MarketRegime.BREAKOUT, 0.0) + row.get(MarketRegime.STRESS, 0.0)


def regime_distribution(regimes: Sequence[MarketRegime]) -> dict[MarketRegime, float]:
    if not regimes:
        return {regime: 0.0 for regime in MarketRegime}
    return {
        regime: sum(1 for value in regimes if value == regime) / len(regimes)
        for regime in MarketRegime
    }


def average_regime_return(
    samples: dict[MarketRegime, Sequence[float]],
) -> dict[MarketRegime, float]:
    return {
        regime: mean(values) if values else 0.0
        for regime, values in samples.items()
    }
