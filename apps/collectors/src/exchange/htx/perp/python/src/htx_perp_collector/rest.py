from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str
from cex_market_data_collector.http import HttpJsonClient

class HtxPerpRestAdapter(ExchangeAdapter):
    exchange = "htx"
    symbol = "BTC-USDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://api.hbdm.com/linear-swap-ex/market/depth",
            {"contract_code": self.symbol, "type": "step0"},
        )
        tick = first_mapping(data.get("tick"))
        return self._orderbook(
            bids=tick.get("bids", [])[:depth],
            asks=tick.get("asks", [])[:depth],
            exchange_ts=int(first_float(tick, "ts") or first_float(data, "ts") or now_ms()),
            sequence=first_str(tick, "version"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        data = await client.get_json(
            "https://api.hbdm.com/linear-swap-api/v1/swap_open_interest",
            {"contract_code": self.symbol},
        )
        item = first_mapping(data.get("data"))
        return self._oi(
            open_interest=first_str(item, "amount", "volume"),
            unit="BTC_OR_CONTRACTS",
            value_usd=first_str(item, "value"),
            exchange_ts=int(first_float(data, "ts") or now_ms()),
            raw=data,
            note="HTX provides amount/volume and value; use value for cross-exchange notional aggregation.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return HtxPerpRestAdapter()
