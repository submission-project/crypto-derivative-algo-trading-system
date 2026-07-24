from __future__ import annotations

from typing import Mapping

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str
from cex_market_data_collector.http import HttpJsonClient

class KrakenPerpRestAdapter(ExchangeAdapter):
    exchange = "kraken"
    symbol = "PF_XBTUSD"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://futures.kraken.com/derivatives/api/v3/orderbook",
            {"symbol": self.symbol},
        )
        book = first_mapping(data.get("orderBook"))
        return self._orderbook(
            bids=book.get("bids", [])[:depth],
            asks=book.get("asks", [])[:depth],
            exchange_ts=now_ms(),
            sequence=None,
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        ticker = await client.get_json(
            f"https://futures.kraken.com/derivatives/api/v3/tickers/{self.symbol}"
        )
        instruments = await client.get_json(
            "https://futures.kraken.com/derivatives/api/v3/instruments"
        )
        item = first_mapping(ticker.get("ticker"))
        instrument = next(
            (
                row
                for row in instruments.get("instruments", [])
                if isinstance(row, Mapping) and row.get("symbol") == self.symbol
            ),
            {},
        )
        amount = first_str(item, "openInterest")
        price = first_str(item, "markPrice", "last", "bid", "ask")
        multiplier = first_float(instrument, "contractSize") or 1.0
        return self._oi(
            open_interest=amount,
            unit="CONTRACTS",
            value_usd=notional_str(amount, price, multiplier),
            exchange_ts=now_ms(),
            raw={"ticker": ticker, "instrument": instrument},
            note="Kraken futures openInterest is multiplied by contractSize and mark/last price for estimated USD notional.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return KrakenPerpRestAdapter()
