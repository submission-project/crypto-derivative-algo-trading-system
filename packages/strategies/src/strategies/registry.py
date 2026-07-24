from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from schemas.signal import Signal


class LiveStrategy(Protocol):
    """Stateful live strategy interface used by the signal pipeline."""

    name: str

    def on_market_event(self, event: dict) -> list[Signal]:
        """Consume one normalized market event and return zero or more signals."""


@dataclass(slots=True)
class StrategyRegistry:
    strategies: list[LiveStrategy] = field(default_factory=list)

    def register(self, strategy: LiveStrategy) -> None:
        self.strategies.append(strategy)

    def on_market_event(self, event: dict) -> list[Signal]:
        signals: list[Signal] = []
        for strategy in self.strategies:
            signals.extend(strategy.on_market_event(event))
        return signals


def build_default_strategy_registry() -> StrategyRegistry:
    from .btc_oi_trend import BtcOiTrendStrategy
    from .btc_price_oi_box import BtcPriceOiBoxStrategy

    registry = StrategyRegistry()
    registry.register(BtcOiTrendStrategy())
    registry.register(BtcPriceOiBoxStrategy())
    return registry
