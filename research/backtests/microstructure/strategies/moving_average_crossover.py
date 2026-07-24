from __future__ import annotations

from collections.abc import Sequence

from .base import TradeBarLike, flatten_last_signal


def _sma(values: Sequence[float]) -> float:
    return sum(values) / len(values)

# 이동평균 교차 전략
# 시계열 모멘텀 / 트랜드 추종 계열의 가장 기본적인 전략
# 해당 전략의 단점은 횡보장에 약함
# 핵심
# - 최근 단기 이평선이 더 긴 기간의 장기 이평선보다 충분히 위에 있으면 상승 추세로 보고 Long.
# - 최근 단기 이평선이 더 긴 기간의 장기 이평선보다 충분히 아래 있으면 하락 추세로 보고 Short.


def generate_moving_average_crossover_signals(
    bars: Sequence[TradeBarLike],
    *,
    fast_window: int = 20,
    slow_window: int = 80,
    neutral_band_bps: float = 2.0,
    exit_on_neutral: bool = False,
    force_flat_last: bool = True,
) -> list[int]:
    if fast_window < 2:
        raise ValueError("fast_window must be at least 2")
    if slow_window <= fast_window:
        raise ValueError("slow_window must be greater than fast_window")
    if neutral_band_bps < 0:
        raise ValueError("neutral_band_bps must be non-negative")

    prices = [bar.close_price for bar in bars]
    signals: list[int] = []
    current_position = 0
    band = neutral_band_bps / 10_000.0

    for idx in range(len(prices)):
        if idx + 1 < slow_window:
            signals.append(0)
            continue

        fast_ma = _sma(prices[idx + 1 - fast_window : idx + 1])
        slow_ma = _sma(prices[idx + 1 - slow_window : idx + 1])

        if fast_ma > slow_ma * (1.0 + band):
            current_position = 1
        elif fast_ma < slow_ma * (1.0 - band):
            current_position = -1
        elif exit_on_neutral:
            current_position = 0

        signals.append(current_position)

    return flatten_last_signal(signals, force_flat_last=force_flat_last)
