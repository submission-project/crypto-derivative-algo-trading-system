from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .cost_model import CostModel


@dataclass(frozen=True, slots=True)
class BacktestPoint:
    timestamp: int
    price: float
    signal: int
    bid: float | None = None
    ask: float | None = None
    bar_volume_usd: float | None = None


@dataclass(frozen=True, slots=True)
class BacktestResult:
    equity_curve: list[float]
    trade_pnls: list[float]
    positions: list[int]
    trade_count: int = 0
    forced_exits: int = 0
    total_cost_paid: float = 0.0
    latency_periods: int = 0


def _synthetic_bid_ask(point: BacktestPoint, default_spread_bps: float) -> tuple[float, float]:
    if point.bid is not None and point.ask is not None:
        if point.bid > point.ask:
            raise ValueError("bid must be <= ask")
        return point.bid, point.ask

    half_spread = point.price * default_spread_bps / 20_000.0
    return point.price - half_spread, point.price + half_spread


def _fill_price(
    point: BacktestPoint,
    *,
    side: int,
    is_entry: bool,
    default_spread_bps: float,
) -> float:
    bid, ask = _synthetic_bid_ask(point, default_spread_bps)
    if side not in {-1, 1}:
        raise ValueError("side must be -1 or 1")

    # Long entries lift the ask and long exits hit the bid.
    # Short entries hit the bid and short exits lift the ask.
    if is_entry:
        return ask if side > 0 else bid
    return bid if side > 0 else ask


def _participation_rate(notional: float, point: BacktestPoint) -> float:
    if point.bar_volume_usd is None or point.bar_volume_usd <= 0:
        return 0.0
    return min(1.0, abs(notional) / point.bar_volume_usd)


def run_directional_backtest(
    points: Sequence[BacktestPoint],
    *,
    initial_equity: float = 10_000.0,
    notional_per_trade: float = 1_000.0,
    cost_model: CostModel | None = None,
    max_holding_periods: int = 0,
    latency_periods: int = 0,
    default_spread_bps: float = 1.0,
    # Risk integration parameters
    use_kelly_sizing: bool = False,
    kelly_win_rate: float = 0.55,
    kelly_avg_win: float = 1.0,
    kelly_avg_loss: float = 1.0,
    kelly_cap: float = 0.2,
    fixed_fraction: float = 0.0,
    max_drawdown_stop: float = 0.0,
    max_position_notional: float = 0.0,
) -> BacktestResult:
    """
    Enhanced directional backtest with:
    - Bid/ask execution instead of midpoint fills
    - Signal-to-fill latency in discrete periods
    - Max holding period forced exit
    - Risk package integration (Kelly sizing, fixed fractional, drawdown stop)
    - Position limit enforcement
    """
    if initial_equity <= 0:
        raise ValueError("initial_equity must be positive")
    if latency_periods < 0:
        raise ValueError("latency_periods must be non-negative")
    if default_spread_bps < 0:
        raise ValueError("default_spread_bps must be non-negative")

    cost_model = cost_model or CostModel()
    equity = initial_equity
    position = 0
    entry_price: float | None = None
    entry_notional = 0.0
    holding_counter = 0
    equity_curve = [equity]
    positions: list[int] = []
    trade_pnls: list[float] = []
    trade_count = 0
    forced_exits = 0
    total_cost_paid = 0.0
    peak_equity = initial_equity
    stopped = False

    for idx, point in enumerate(points):
        execution_point = points[min(idx + latency_periods, len(points) - 1)]

        # Drawdown stop check
        if max_drawdown_stop > 0 and not stopped:
            peak_equity = max(peak_equity, equity)
            if peak_equity > 0:
                current_dd = abs(equity / peak_equity - 1.0)
                if current_dd >= max_drawdown_stop:
                    stopped = True

        if stopped:
            # Force flat if drawdown stop triggered
            if position != 0 and entry_price is not None:
                exit_price = _fill_price(
                    execution_point,
                    side=position,
                    is_entry=False,
                    default_spread_bps=default_spread_bps,
                )
                gross = position * entry_notional * (exit_price / entry_price - 1.0)
                cost = cost_model.cost_amount(
                    entry_notional,
                    order_type="market",
                    participation_rate=_participation_rate(entry_notional, execution_point),
                )
                pnl = gross - cost
                equity += pnl
                trade_pnls.append(pnl)
                trade_count += 1
                total_cost_paid += cost
                position = 0
                entry_price = None
                entry_notional = 0.0
                holding_counter = 0
            positions.append(0)
            equity_curve.append(equity)
            continue

        target = 1 if point.signal > 0 else (-1 if point.signal < 0 else 0)

        # Max holding period: force exit if held too long
        if max_holding_periods > 0 and position != 0:
            holding_counter += 1
            if holding_counter >= max_holding_periods:
                target = 0
                forced_exits += 1

        # Determine notional size
        actual_notional = notional_per_trade

        # Kelly Criterion sizing
        if use_kelly_sizing and target != 0:
            from risk.sizing import capped_kelly_fraction
            kelly_f = capped_kelly_fraction(
                win_rate=kelly_win_rate,
                avg_win=kelly_avg_win,
                avg_loss=kelly_avg_loss,
                cap=kelly_cap,
            )
            actual_notional = equity * kelly_f if kelly_f > 0 else 0.0

        # Fixed fractional sizing
        elif fixed_fraction > 0 and target != 0:
            from risk.sizing import fixed_fractional_notional
            actual_notional = fixed_fractional_notional(
                equity=max(0.0, equity),
                fraction=fixed_fraction,
            )

        # Position limit enforcement
        if max_position_notional > 0:
            from risk.constraints import PositionLimit, apply_position_limit
            signed_notional = target * actual_notional
            clamped = apply_position_limit(
                signed_notional,
                PositionLimit(max_abs_notional=max_position_notional),
            )
            actual_notional = abs(clamped)
            if clamped == 0:
                target = 0

        if actual_notional <= 0:
            target = 0

        if target != position:
            if position != 0 and entry_price is not None:
                exit_price = _fill_price(
                    execution_point,
                    side=position,
                    is_entry=False,
                    default_spread_bps=default_spread_bps,
                )
                gross = position * entry_notional * (exit_price / entry_price - 1.0)
                cost = cost_model.cost_amount(
                    entry_notional,
                    order_type="market",
                    participation_rate=_participation_rate(entry_notional, execution_point),
                )
                pnl = gross - cost
                equity += pnl
                trade_pnls.append(pnl)
                trade_count += 1
                total_cost_paid += cost

            if target != 0:
                entry_price = _fill_price(
                    execution_point,
                    side=target,
                    is_entry=True,
                    default_spread_bps=default_spread_bps,
                )
                entry_notional = actual_notional
                entry_cost = cost_model.cost_amount(
                    actual_notional,
                    order_type="market",
                    participation_rate=_participation_rate(actual_notional, execution_point),
                )
                equity -= entry_cost
                total_cost_paid += entry_cost
                holding_counter = 0
            else:
                entry_price = None
                entry_notional = 0.0
                holding_counter = 0
            position = target

        positions.append(position)
        equity_curve.append(equity)

    return BacktestResult(
        equity_curve=equity_curve,
        trade_pnls=trade_pnls,
        positions=positions,
        trade_count=trade_count,
        forced_exits=forced_exits,
        total_cost_paid=total_cost_paid,
        latency_periods=latency_periods,
    )
