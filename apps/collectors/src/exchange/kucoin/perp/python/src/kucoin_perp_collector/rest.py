from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str
from cex_market_data_collector.http import HttpJsonClient

class KucoinPerpRestAdapter(ExchangeAdapter):
    exchange = "kucoin"
    symbol = "XBTUSDTM"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        path = "level2/depth20" if depth <= 20 else "level2/depth100"
        data = await client.get_json(
            f"https://api-futures.kucoin.com/api/v1/{path}",
            {"symbol": self.symbol},
        )
        item = first_mapping(data.get("data"))
        return self._orderbook(
            bids=item.get("bids", []),
            asks=item.get("asks", []),
            exchange_ts=int(first_float(item, "ts") or first_float(data, "time") or now_ms()),
            sequence=first_str(item, "sequence"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        data = await client.get_json(
            f"https://api-futures.kucoin.com/api/v1/contracts/{self.symbol}"
        )
        item = first_mapping(data.get("data"))
        open_interest = first_float(item, "openInterest")
        multiplier = first_float(item, "multiplier") or 1.0
        lot_size = first_float(item, "lotSize") or 1.0
        amount = str(open_interest * multiplier * lot_size) if open_interest is not None else None
        price = first_str(item, "markPrice", "indexPrice", "lastTradePrice")
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=notional_str(amount, price),
            exchange_ts=int(first_float(item, "timestamp") or first_float(data, "time") or now_ms()),
            raw=data,
            note="KuCoin futures openInterest is contract count; amount is multiplied by multiplier and lotSize.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return KucoinPerpRestAdapter()
