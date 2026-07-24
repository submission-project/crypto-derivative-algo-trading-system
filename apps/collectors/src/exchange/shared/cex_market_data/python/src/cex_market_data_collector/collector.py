from __future__ import annotations

import asyncio

from .adapter_base import JsonGetter
from .adapters import DEFAULT_EXCHANGES, build_adapter
from .models import ExchangeSnapshot, OpenInterestSnapshot, OrderBookSnapshot, now_ms


def _error_orderbook(exchange: str, symbol: str, error: Exception) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        exchange=exchange,
        market_type="perp",
        symbol=symbol,
        bids=[],
        asks=[],
        exchange_ts=None,
        local_ts=now_ms(),
        sequence=None,
        raw={},
        error=str(error),
    )


def _error_open_interest(exchange: str, symbol: str, error: Exception) -> OpenInterestSnapshot:
    return OpenInterestSnapshot(
        exchange=exchange,
        market_type="perp",
        symbol=symbol,
        open_interest=None,
        open_interest_unit=None,
        open_interest_value_usd=None,
        exchange_ts=None,
        local_ts=now_ms(),
        raw={},
        error=str(error),
    )


async def collect_exchange_snapshot(
    exchange: str,
    client: JsonGetter,
    *,
    depth: int = 20,
) -> ExchangeSnapshot:
    adapter = build_adapter(exchange)

    orderbook, open_interest = await asyncio.gather(
        adapter.fetch_orderbook(client, depth=depth),
        adapter.fetch_open_interest(client),
        return_exceptions=True,
    )

    if isinstance(orderbook, Exception):
        orderbook = _error_orderbook(adapter.exchange, adapter.symbol, orderbook)
    if isinstance(open_interest, Exception):
        open_interest = _error_open_interest(adapter.exchange, adapter.symbol, open_interest)

    error = None
    if orderbook.error and open_interest.error:
        error = f"orderbook={orderbook.error}; open_interest={open_interest.error}"

    return ExchangeSnapshot(
        exchange=adapter.exchange,
        orderbook=orderbook,
        open_interest=open_interest,
        error=error,
    )


async def collect_market_snapshots(
    exchanges: tuple[str, ...] = DEFAULT_EXCHANGES,
    *,
    client: JsonGetter,
    depth: int = 20,
) -> list[ExchangeSnapshot]:
    tasks = [
        collect_exchange_snapshot(exchange, client, depth=depth)
        for exchange in exchanges
    ]
    return list(await asyncio.gather(*tasks))
