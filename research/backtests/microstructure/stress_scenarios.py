from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from .cost_model import CostModel
from .metrics import compute_all_metrics
from .simulator import BacktestPoint, BacktestResult, run_directional_backtest


class StressScenario(str, Enum):
    FLASH_CRASH = "flash_crash"
    SPREAD_WIDENING = "spread_widening"
    LIQUIDITY_DROUGHT = "liquidity_drought"
    LATENCY_SPIKE = "latency_spike"


@dataclass(frozen=True, slots=True)
class StressConfig:
    scenario: StressScenario
    shock_start_fraction: float = 0.5
    flash_crash_return: float = -0.08
    spread_multiplier: float = 5.0
    volume_multiplier: float = 0.2
    slippage_multiplier: float = 3.0
    impact_multiplier: float = 3.0
    latency_periods: int = 4


@dataclass(frozen=True, slots=True)
class StressBacktestResult:
    scenario: StressScenario
    result: BacktestResult
    metrics: dict[str, float]


def _stress_start_index(points: Sequence[BacktestPoint], fraction: float) -> int:
    if not 0 <= fraction <= 1:
        raise ValueError("shock_start_fraction must be between 0 and 1")
    return min(len(points) - 1, max(0, int(len(points) * fraction)))


def apply_stress_scenario(
    points: Sequence[BacktestPoint],
    config: StressConfig,
    *,
    default_spread_bps: float = 1.0,
) -> list[BacktestPoint]:
    if not points:
        return []

    start = _stress_start_index(points, config.shock_start_fraction)
    stressed: list[BacktestPoint] = []

    for idx, point in enumerate(points):
        price = point.price
        spread_bps = default_spread_bps
        volume = point.bar_volume_usd

        if idx >= start:
            if config.scenario == StressScenario.FLASH_CRASH:
                price = point.price * (1.0 + config.flash_crash_return)
            elif config.scenario == StressScenario.SPREAD_WIDENING:
                spread_bps *= config.spread_multiplier
            elif config.scenario == StressScenario.LIQUIDITY_DROUGHT:
                volume = (volume or 0.0) * config.volume_multiplier

        half_spread = price * spread_bps / 20_000.0
        stressed.append(
            BacktestPoint(
                timestamp=point.timestamp,
                price=price,
                signal=point.signal,
                bid=price - half_spread,
                ask=price + half_spread,
                bar_volume_usd=volume,
            )
        )

    return stressed


def run_stress_backtest(
    points: Sequence[BacktestPoint],
    config: StressConfig,
    *,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    base_cost_model: CostModel | None = None,
    max_holding_periods: int = 30,
    max_drawdown_stop: float = 0.10,
    default_spread_bps: float = 1.0,
) -> StressBacktestResult:
    cost_model = base_cost_model or CostModel()
    slippage_multiplier = (
        config.slippage_multiplier
        if config.scenario in {StressScenario.SPREAD_WIDENING, StressScenario.LIQUIDITY_DROUGHT}
        else 1.0
    )
    impact_multiplier = (
        config.impact_multiplier
        if config.scenario == StressScenario.LIQUIDITY_DROUGHT
        else 1.0
    )
    stressed_cost = CostModel(
        taker_fee_bps=cost_model.taker_fee_bps,
        maker_fee_bps=cost_model.maker_fee_bps,
        slippage_bps=cost_model.slippage_bps * slippage_multiplier,
        market_impact_coeff=cost_model.market_impact_coeff * impact_multiplier,
        sqrt_impact_coeff=cost_model.sqrt_impact_coeff * impact_multiplier,
        daily_volume_usd=cost_model.daily_volume_usd,
    )
    latency = config.latency_periods if config.scenario == StressScenario.LATENCY_SPIKE else 1
    stressed_points = apply_stress_scenario(
        points,
        config,
        default_spread_bps=default_spread_bps,
    )
    result = run_directional_backtest(
        stressed_points,
        initial_equity=initial_equity,
        notional_per_trade=notional_per_trade,
        cost_model=stressed_cost,
        max_holding_periods=max_holding_periods,
        max_drawdown_stop=max_drawdown_stop,
        latency_periods=latency,
        default_spread_bps=default_spread_bps,
    )
    return StressBacktestResult(
        scenario=config.scenario,
        result=result,
        metrics=compute_all_metrics(result.equity_curve, result.trade_pnls),
    )
