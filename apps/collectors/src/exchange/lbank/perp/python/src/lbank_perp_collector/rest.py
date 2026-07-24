from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str
from cex_market_data_collector.http import HttpJsonClient

class LbankPerpRestAdapter(ExchangeAdapter):
    exchange = "lbank"
    symbol = "btc_usdt"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://www.lbkex.net/v2/depth.do",
            {"symbol": self.symbol, "size": depth},
        )
        return self._orderbook(
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            exchange_ts=int(first_float(data, "timestamp", "ts") or now_ms()),
            sequence=None,
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        data = await client.get_json(
            "https://www.lbkex.net/cfd/openApi/v1/pub/marketData",
            {"symbol": self.symbol},
        )
        item = first_mapping(data.get("data"))
        amount = first_str(item, "openInterest", "holdVol", "position")
        price = first_str(item, "markPrice", "lastPrice", "last")
        return self._oi(
            open_interest=amount,
            unit="BTC_OR_CONTRACTS",
            value_usd=first_str(item, "openInterestValue") or notional_str(amount, price),
            exchange_ts=int(first_float(item, "timestamp", "ts") or first_float(data, "timestamp", "ts") or now_ms()),
            raw=data,
            note="LBank futures field names differ by market; raw payload is retained for audit.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return LbankPerpRestAdapter()
