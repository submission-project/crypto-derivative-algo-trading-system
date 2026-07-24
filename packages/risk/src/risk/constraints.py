from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PositionLimit:
    max_abs_notional: float

    def clamp(self, target_notional: float) -> float:
        if self.max_abs_notional < 0:
            raise ValueError("max_abs_notional must be non-negative")
        return max(-self.max_abs_notional, min(target_notional, self.max_abs_notional))


@dataclass(frozen=True, slots=True)
class RiskConstraints:
    position_limit: PositionLimit
    max_drawdown: float


def apply_position_limit(target_notional: float, limit: PositionLimit) -> float:
    return limit.clamp(target_notional)
