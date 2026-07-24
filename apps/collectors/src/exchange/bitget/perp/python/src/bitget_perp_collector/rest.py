from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str
from cex_market_data_collector.http import HttpJsonClient

class BitgetPerpRestAdapter(ExchangeAdapter):
    exchange = "bitget"
    symbol = "BTCUSDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://api.bitget.com/api/v2/mix/market/merge-depth",
            {"productType": "USDT-FUTURES", "symbol": self.symbol, "precision": "scale0", "limit": str(depth)},
        )
        item = first_mapping(data.get("data"))
        return self._orderbook(
            bids=item.get("bids", []),
            asks=item.get("asks", []),
            exchange_ts=int(first_float(item, "ts") or first_float(data, "requestTime") or now_ms()),
            sequence=first_str(item, "precision"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        oi = await client.get_json(
            "https://api.bitget.com/api/v2/mix/market/open-interest",
            {"productType": "USDT-FUTURES", "symbol": self.symbol},
        )
        ticker = await client.get_json(
            "https://api.bitget.com/api/v2/mix/market/ticker",
            {"productType": "USDT-FUTURES", "symbol": self.symbol},
        )
        item = first_mapping(first_mapping(oi.get("data")).get("openInterestList"))
        ticker_item = first_mapping(ticker.get("data"))
        amount = first_str(item, "size")
        price = first_str(ticker_item, "markPrice", "lastPr", "last")
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=notional_str(amount, price),
            exchange_ts=int(first_float(first_mapping(oi.get("data")), "ts") or now_ms()),
            raw={"open_interest": oi, "ticker": ticker},
            note="Bitget open interest size is base coin amount; USD notional is estimated with mark/last price.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return BitgetPerpRestAdapter()
