from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Protocol

from .models import OpenInterestSnapshot, OrderBookLevel, OrderBookSnapshot, now_ms
from .utils import to_str


class JsonGetter(Protocol):
    async def get_json(
        self,
        url: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any: ...


class ExchangeAdapter(ABC):
    exchange: str
    market_type = "perp"
    symbol: str

    @abstractmethod
    async def fetch_orderbook(
        self,
        client: JsonGetter,
        *,
        depth: int = 20,
    ) -> OrderBookSnapshot: ...

    @abstractmethod
    async def fetch_open_interest(self, client: JsonGetter) -> OpenInterestSnapshot: ...

    def _orderbook(
        self,
        *,
        bids: list[Any],
        asks: list[Any],
        exchange_ts: int | None,
        sequence: str | None,
        raw: Any,
    ) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol,
            bids=_levels(bids),
            asks=_levels(asks),
            exchange_ts=exchange_ts,
            local_ts=now_ms(),
            sequence=sequence,
            raw=raw,
        )

    def _oi(
        self,
        *,
        open_interest: str | None,
        unit: str | None,
        value_usd: str | None,
        exchange_ts: int | None,
        raw: Any,
        note: str | None = None,
    ) -> OpenInterestSnapshot:
        return OpenInterestSnapshot(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol,
            open_interest=open_interest,
            open_interest_unit=unit,
            open_interest_value_usd=value_usd,
            exchange_ts=exchange_ts,
            local_ts=now_ms(),
            raw=raw,
            note=note,
        )


def _levels(rows: list[Any]) -> list[OrderBookLevel]:
    levels: list[OrderBookLevel] = []
    for row in rows:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            price = to_str(row[0])
            size = to_str(row[1])
            if price is not None and size is not None:
                levels.append(OrderBookLevel(price=price, size=size))
    return levels
