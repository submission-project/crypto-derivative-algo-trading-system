from __future__ import annotations

from collections.abc import Sequence

from .base import TradeBarLike, flatten_last_signal, validate_signal_threshold


def _box_position(price: float, low: float, high: float) -> float | None:
    width = high - low
    if width <= 0:
        return None
    return (price - low) / width


def generate_box_reversion_signals(
    bars: Sequence[TradeBarLike],
    *,
    lookback: int = 20,
    edge_threshold: float = 0.15,
    flow_threshold: float = 0.05,
    force_flat_last: bool = True,
) -> list[int]:
    if lookback < 2:
        raise ValueError("lookback must be at least 2")
    if not 0 < edge_threshold < 0.5:
        raise ValueError("edge_threshold must be between 0 and 0.5")
    validate_signal_threshold(flow_threshold, name="flow_threshold")

    signals: list[int] = []
    for idx, bar in enumerate(bars):
        if idx + 1 < lookback:
            signals.append(0)
            continue

        window = bars[idx + 1 - lookback : idx + 1]
        prices = [item.close_price for item in window]
        low = min(prices)
        high = max(prices)
        position = _box_position(bar.close_price, low, high)
        if position is None:
            signals.append(0)
            continue

        flow = bar.taker_imbalance
        if position <= edge_threshold and flow >= flow_threshold:
            signals.append(1)
        elif position >= 1.0 - edge_threshold and flow <= -flow_threshold:
            signals.append(-1)
        else:
            signals.append(0)

    return flatten_last_signal(signals, force_flat_last=force_flat_last)
