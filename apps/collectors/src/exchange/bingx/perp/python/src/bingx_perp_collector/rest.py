from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str
from cex_market_data_collector.http import HttpJsonClient


class BingxPerpRestAdapter(ExchangeAdapter):
    exchange = "bingx"
    symbol = "BTC-USDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://open-api.bingx.com/openApi/swap/v2/quote/depth",
            {"symbol": self.symbol, "limit": depth},
        )
        item = first_mapping(data.get("data"))
        return self._orderbook(
            bids=item.get("bids", []),
            asks=item.get("asks", []),
            exchange_ts=int(first_float(item, "T", "time", "ts") or now_ms()),
            sequence=None,
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        data = await client.get_json(
            "https://open-api.bingx.com/openApi/swap/v2/quote/openInterest",
            {"symbol": self.symbol},
        )
        item = first_mapping(data.get("data"))
        return self._oi(
            open_interest=None,
            unit=None,
            value_usd=first_str(item, "openInterestValue", "value", "openInterest"),
            exchange_ts=int(first_float(item, "time", "timestamp") or now_ms()),
            raw=data,
            note="BingX BTC-USDT openInterest has been observed as notional/value; do not multiply by price again.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return BingxPerpRestAdapter()
