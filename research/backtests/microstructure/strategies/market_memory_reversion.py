from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .base import TradeBarLike, flatten_last_signal, validate_signal_threshold
from .manual_memory_box import ManualMemoryBox


@dataclass(frozen=True, slots=True)
class MarketMemoryReversionConfig:
    lines: tuple[float, ...]
    line_tolerance_bps: float = 8.0
    flow_threshold: float = 0.05
    min_rejection_bps: float = 1.0
    breakout_tolerance_bps: float = 20.0
    cooldown_bars: int = 3
    force_flat_last: bool = True

    @classmethod
    def from_lines(
        cls,
        lines: Sequence[float],
        *,
        line_tolerance_bps: float = 8.0,
        flow_threshold: float = 0.05,
        min_rejection_bps: float = 1.0,
        breakout_tolerance_bps: float = 20.0,
        cooldown_bars: int = 3,
        force_flat_last: bool = True,
    ) -> MarketMemoryReversionConfig:
        box = ManualMemoryBox.from_lines(lines)
        if line_tolerance_bps < 0:
            raise ValueError("line_tolerance_bps must be non-negative")
        if min_rejection_bps < 0:
            raise ValueError("min_rejection_bps must be non-negative")
        if breakout_tolerance_bps < 0:
            raise ValueError("breakout_tolerance_bps must be non-negative")
        if cooldown_bars < 0:
            raise ValueError("cooldown_bars must be non-negative")
        validate_signal_threshold(flow_threshold, name="flow_threshold")
        return cls(
            lines=box.lines,
            line_tolerance_bps=line_tolerance_bps,
            flow_threshold=flow_threshold,
            min_rejection_bps=min_rejection_bps,
            breakout_tolerance_bps=breakout_tolerance_bps,
            cooldown_bars=cooldown_bars,
            force_flat_last=force_flat_last,
        )


@dataclass(frozen=True, slots=True)
class MarketMemorySignalDetail:
    timestamp: int
    price: float
    signal: int
    nearest_line: float
    line_role: str
    taker_imbalance: float
    rejection_bps: float
    reason: str


def _distance_bps(price: float, line: float) -> float:
    return abs(price / line - 1.0) * 10_000.0


def _inside_box(price: float, box: ManualMemoryBox, *, breakout_tolerance_bps: float) -> bool:
    lower_bound = box.min_line * (1.0 - breakout_tolerance_bps / 10_000.0)
    upper_bound = box.max_line * (1.0 + breakout_tolerance_bps / 10_000.0)
    return lower_bound <= price <= upper_bound


def _nearest_line(price: float, box: ManualMemoryBox) -> float:
    return min(box.lines, key=lambda line: abs(price - line))


def _line_role(line: float, box: ManualMemoryBox) -> str:
    if line == box.min_line:
        return "support"
    if line == box.max_line:
        return "resistance"
    return "internal"


def _price_change_bps(previous: float, current: float) -> float:
    if previous <= 0:
        return 0.0
    return (current / previous - 1.0) * 10_000.0


def generate_market_memory_reversion_details(
    bars: Sequence[TradeBarLike],
    *,
    config: MarketMemoryReversionConfig,
) -> list[MarketMemorySignalDetail]:
    box = ManualMemoryBox.from_lines(config.lines)
    details: list[MarketMemorySignalDetail] = []
    cooldown_remaining = 0

    for idx, bar in enumerate(bars):
        price = bar.close_price
        nearest_line = _nearest_line(price, box)
        role = _line_role(nearest_line, box)
        flow = bar.taker_imbalance
        rejection_bps = 0.0 if idx == 0 else _price_change_bps(bars[idx - 1].close_price, price)
        signal = 0
        reason = "no_edge_reversion"

        if not _inside_box(price, box, breakout_tolerance_bps=config.breakout_tolerance_bps):
            reason = "outside_memory_box"
        elif cooldown_remaining > 0:
            reason = "cooldown"
            cooldown_remaining -= 1
        elif _distance_bps(price, nearest_line) > config.line_tolerance_bps:
            reason = "not_near_memory_line"
        elif role == "support" and flow >= config.flow_threshold and rejection_bps >= config.min_rejection_bps:
            signal = 1
            reason = "support_rejection_with_buy_flow"
            cooldown_remaining = config.cooldown_bars
        elif role == "resistance" and flow <= -config.flow_threshold and rejection_bps <= -config.min_rejection_bps:
            signal = -1
            reason = "resistance_rejection_with_sell_flow"
            cooldown_remaining = config.cooldown_bars

        details.append(
            MarketMemorySignalDetail(
                timestamp=bar.bucket_start_ms,
                price=price,
                signal=signal,
                nearest_line=nearest_line,
                line_role=role,
                taker_imbalance=flow,
                rejection_bps=rejection_bps,
                reason=reason,
            )
        )

    if config.force_flat_last and details:
        last = details[-1]
        details[-1] = MarketMemorySignalDetail(
            timestamp=last.timestamp,
            price=last.price,
            signal=0,
            nearest_line=last.nearest_line,
            line_role=last.line_role,
            taker_imbalance=last.taker_imbalance,
            rejection_bps=last.rejection_bps,
            reason="force_flat_last",
        )
    return details


def generate_market_memory_reversion_signals(
    bars: Sequence[TradeBarLike],
    *,
    config: MarketMemoryReversionConfig,
) -> list[int]:
    details = generate_market_memory_reversion_details(bars, config=config)
    return flatten_last_signal([detail.signal for detail in details], force_flat_last=False)
