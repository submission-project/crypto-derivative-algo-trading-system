from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .base import TradeBarLike, flatten_last_signal, validate_signal_threshold


@dataclass(frozen=True, slots=True)
class ManualMemoryBox:
    lines: tuple[float, ...]

    @classmethod
    def from_lines(cls, lines: Sequence[float]) -> ManualMemoryBox:
        unique_lines = tuple(sorted(set(float(line) for line in lines)))
        if len(unique_lines) < 2:
            raise ValueError("manual memory box needs at least two price lines")
        if any(line <= 0 for line in unique_lines):
            raise ValueError("manual memory box lines must be positive")
        return cls(lines=unique_lines)

    @property
    def min_line(self) -> float:
        return self.lines[0]

    @property
    def max_line(self) -> float:
        return self.lines[-1]

    @property
    def internal_lines(self) -> tuple[float, ...]:
        return self.lines[1:-1]


def _is_near_line(price: float, line: float, *, tolerance_bps: float) -> bool:
    distance_bps = abs(price / line - 1.0) * 10_000.0
    return distance_bps <= tolerance_bps


def _inside_box(price: float, box: ManualMemoryBox, *, breakout_tolerance_bps: float) -> bool:
    lower_bound = box.min_line * (1.0 - breakout_tolerance_bps / 10_000.0)
    upper_bound = box.max_line * (1.0 + breakout_tolerance_bps / 10_000.0)
    return lower_bound <= price <= upper_bound


def nearest_memory_line(price: float, lines: Sequence[float]) -> float:
    box = ManualMemoryBox.from_lines(lines)
    return min(box.lines, key=lambda line: abs(price - line))


def generate_manual_memory_box_signals(
    bars: Sequence[TradeBarLike],
    *,
    lines: Sequence[float],
    line_tolerance_bps: float = 8.0,
    flow_threshold: float = 0.05,
    breakout_tolerance_bps: float = 20.0,
    trade_internal_lines: bool = False,
    force_flat_last: bool = True,
) -> list[int]:
    if line_tolerance_bps < 0:
        raise ValueError("line_tolerance_bps must be non-negative")
    if breakout_tolerance_bps < 0:
        raise ValueError("breakout_tolerance_bps must be non-negative")
    validate_signal_threshold(flow_threshold, name="flow_threshold")

    box = ManualMemoryBox.from_lines(lines)
    signals: list[int] = []
    for bar in bars:
        price = bar.close_price
        if not _inside_box(price, box, breakout_tolerance_bps=breakout_tolerance_bps):
            signals.append(0)
            continue

        flow = bar.taker_imbalance
        if _is_near_line(price, box.min_line, tolerance_bps=line_tolerance_bps):
            signals.append(1 if flow >= flow_threshold else 0)
        elif _is_near_line(price, box.max_line, tolerance_bps=line_tolerance_bps):
            signals.append(-1 if flow <= -flow_threshold else 0)
        elif trade_internal_lines:
            if not box.internal_lines:
                signals.append(0)
                continue
            nearest = nearest_memory_line(price, box.internal_lines)
            if _is_near_line(price, nearest, tolerance_bps=line_tolerance_bps):
                signals.append(1 if flow >= flow_threshold else (-1 if flow <= -flow_threshold else 0))
            else:
                signals.append(0)
        else:
            signals.append(0)

    return flatten_last_signal(signals, force_flat_last=force_flat_last)
