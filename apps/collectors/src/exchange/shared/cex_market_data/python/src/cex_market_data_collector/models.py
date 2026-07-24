from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def ms_to_iso(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: str
    size: str


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    exchange: str
    market_type: str
    symbol: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    exchange_ts: int | None
    local_ts: int
    sequence: str | None
    raw: Any
    error: str | None = None

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["datetime"] = ms_to_iso(self.exchange_ts)
        return record


@dataclass(frozen=True, slots=True)
class OpenInterestSnapshot:
    exchange: str
    market_type: str
    symbol: str
    open_interest: str | None
    open_interest_unit: str | None
    open_interest_value_usd: str | None
    exchange_ts: int | None
    local_ts: int
    raw: Any
    note: str | None = None
    error: str | None = None

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["datetime"] = ms_to_iso(self.exchange_ts)
        return record


@dataclass(frozen=True, slots=True)
class ExchangeSnapshot:
    exchange: str
    orderbook: OrderBookSnapshot | None
    open_interest: OpenInterestSnapshot | None
    error: str | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "orderbook": self.orderbook.to_record() if self.orderbook else None,
            "open_interest": (
                self.open_interest.to_record() if self.open_interest else None
            ),
            "error": self.error,
        }
