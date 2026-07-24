from __future__ import annotations

from typing import Protocol


class TradeBarLike(Protocol):
    @property
    def bucket_start_ms(self) -> int: ...

    @property
    def close_price(self) -> float: ...

    @property
    def quote_volume(self) -> float: ...

    @property
    def taker_buy_quote_volume(self) -> float: ...

    @property
    def taker_sell_quote_volume(self) -> float: ...

    @property
    def trade_count(self) -> int: ...

    @property
    def taker_imbalance(self) -> float: ...


def validate_signal_threshold(value: float, *, name: str = "threshold") -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def flatten_last_signal(signals: list[int], *, force_flat_last: bool) -> list[int]:
    if force_flat_last and signals:
        signals[-1] = 0
    return signals
