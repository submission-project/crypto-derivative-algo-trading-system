from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str, to_float
from cex_market_data_collector.http import HttpJsonClient

class MexcPerpRestAdapter(ExchangeAdapter):
    exchange = "mexc"
    symbol = "BTC_USDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            f"https://contract.mexc.com/api/v1/contract/depth/{self.symbol}",
            {"limit": depth},
        )
        return self._orderbook(
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            exchange_ts=int(first_float(data, "timestamp") or now_ms()),
            sequence=first_str(data, "version"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        ticker = await client.get_json(
            "https://contract.mexc.com/api/v1/contract/ticker",
            {"symbol": self.symbol},
        )
        detail = await client.get_json(
            "https://contract.mexc.com/api/v1/contract/detail",
            {"symbol": self.symbol},
        )
        item = first_mapping(ticker.get("data"))
        detail_item = first_mapping(detail.get("data"))
        hold_vol = first_float(item, "holdVol", "openInterest")
        contract_size = first_float(detail_item, "contractSize") or 1.0
        amount = str(hold_vol * contract_size) if hold_vol is not None else None
        price = first_str(item, "fairPrice", "indexPrice", "lastPrice")
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=notional_str(amount, price),
            exchange_ts=int(first_float(item, "timestamp") or now_ms()),
            raw={"ticker": ticker, "detail": detail},
            note="MEXC ticker holdVol is contract volume; amount is multiplied by contractSize.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return MexcPerpRestAdapter()
