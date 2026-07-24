from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_str, notional_str, to_str

from cex_market_data_collector.http import HttpJsonClient


class BinancePerpRestAdapter(ExchangeAdapter):
    exchange = "binance"
    symbol = "BTCUSDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://fapi.binance.com/fapi/v1/depth",
            {"symbol": self.symbol, "limit": _nearest_depth_limit(depth)},
        )
        return self._orderbook(
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            exchange_ts=int(first_float(data, "T", "E") or now_ms()),
            sequence=to_str(data.get("lastUpdateId")),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        oi = await client.get_json(
            "https://fapi.binance.com/fapi/v1/openInterest",
            {"symbol": self.symbol},
        )
        mark = await client.get_json(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            {"symbol": self.symbol},
        )
        amount = to_str(oi.get("openInterest"))
        mark_price = first_str(mark, "markPrice", "indexPrice")
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=notional_str(amount, mark_price),
            exchange_ts=int(first_float(oi, "time") or first_float(mark, "time") or now_ms()),
            raw={"open_interest": oi, "premium_index": mark},
            note="openInterest is base amount; USD notional is estimated with markPrice.",
        )


def _nearest_depth_limit(depth: int) -> int:
    for allowed in (5, 10, 20, 50, 100, 500, 1000):
        if depth <= allowed:
            return allowed
    return 1000


def build_rest_adapter() -> ExchangeAdapter:
    return BinancePerpRestAdapter()
