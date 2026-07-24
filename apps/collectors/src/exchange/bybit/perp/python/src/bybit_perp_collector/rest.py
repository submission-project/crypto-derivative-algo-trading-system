from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str
from cex_market_data_collector.http import HttpJsonClient

class BybitPerpRestAdapter(ExchangeAdapter):
    exchange = "bybit"
    symbol = "BTCUSDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://api.bybit.com/v5/market/orderbook",
            {"category": "linear", "symbol": self.symbol, "limit": depth},
        )
        result = first_mapping(data.get("result"))
        return self._orderbook(
            bids=result.get("b", []),
            asks=result.get("a", []),
            exchange_ts=int(first_float(result, "cts", "ts") or now_ms()),
            sequence=first_str(result, "u", "seq"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        oi = await client.get_json(
            "https://api.bybit.com/v5/market/open-interest",
            {"category": "linear", "symbol": self.symbol, "intervalTime": "5min", "limit": 1},
        )
        ticker = await client.get_json(
            "https://api.bybit.com/v5/market/tickers",
            {"category": "linear", "symbol": self.symbol},
        )
        item = first_mapping(first_mapping(oi.get("result")).get("list"))
        ticker_item = first_mapping(first_mapping(ticker.get("result")).get("list"))
        amount = first_str(item, "openInterest")
        price = first_str(ticker_item, "markPrice", "lastPrice")
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=notional_str(amount, price),
            exchange_ts=int(first_float(item, "timestamp") or now_ms()),
            raw={"open_interest": oi, "ticker": ticker},
            note="Bybit linear openInterest is base coin amount; USD notional is estimated with markPrice.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return BybitPerpRestAdapter()
