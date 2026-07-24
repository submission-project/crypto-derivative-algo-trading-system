from __future__ import annotations

from collections.abc import Iterable

from .base import TradeBarLike, flatten_last_signal, validate_signal_threshold


def signal_from_taker_imbalance(bar: TradeBarLike, *, threshold: float) -> int:
    validate_signal_threshold(threshold)

    imbalance = bar.taker_imbalance
    if imbalance >= threshold:
        return 1
    if imbalance <= -threshold:
        return -1
    return 0


def generate_taker_imbalance_signals(
    bars: Iterable[TradeBarLike],
    *,
    threshold: float = 0.20,
    force_flat_last: bool = True,
) -> list[int]:
    signals = [signal_from_taker_imbalance(bar, threshold=threshold) for bar in bars]
    return flatten_last_signal(signals, force_flat_last=force_flat_last)
