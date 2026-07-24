from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str
from cex_market_data_collector.http import HttpJsonClient

class OkxPerpRestAdapter(ExchangeAdapter):
    exchange = "okx"
    symbol = "BTC-USDT-SWAP"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://www.okx.com/api/v5/market/books",
            {"instId": self.symbol, "sz": depth},
        )
        item = first_mapping(data.get("data"))
        return self._orderbook(
            bids=item.get("bids", []),
            asks=item.get("asks", []),
            exchange_ts=int(first_float(item, "ts") or now_ms()),
            sequence=first_str(item, "seqId"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        data = await client.get_json(
            "https://www.okx.com/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": self.symbol},
        )
        item = first_mapping(data.get("data"))
        return self._oi(
            open_interest=first_str(item, "oiCcy", "oi"),
            unit="BTC_OR_CONTRACTS",
            value_usd=first_str(item, "oiUsd"),
            exchange_ts=int(first_float(item, "ts") or now_ms()),
            raw=data,
            note="OKX exposes oi/oiCcy and may expose oiUsd depending on instrument.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return OkxPerpRestAdapter()
