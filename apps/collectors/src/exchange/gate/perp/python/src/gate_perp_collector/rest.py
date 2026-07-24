from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import first_float, first_mapping, first_str, notional_str, to_float
from cex_market_data_collector.http import HttpJsonClient   

class GatePerpRestAdapter(ExchangeAdapter):
    exchange = "gate"
    symbol = "BTC_USDT"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            "https://api.gateio.ws/api/v4/futures/usdt/order_book",
            {"contract": self.symbol, "limit": depth},
        )
        return self._orderbook(
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            exchange_ts=_time_ms(data),
            sequence=first_str(data, "id"),
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        stats = await client.get_json(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            {"contract": self.symbol, "limit": 1},
        )
        contract = await client.get_json(
            "https://api.gateio.ws/api/v4/futures/usdt/contracts/BTC_USDT"
        )
        item = first_mapping(stats)
        contracts = first_float(item, "open_interest")
        multiplier = first_float(contract, "quanto_multiplier", "multiplier") or 1.0
        amount = str(contracts * multiplier) if contracts is not None else None
        return self._oi(
            open_interest=amount,
            unit="BTC",
            value_usd=first_str(item, "open_interest_usd") or notional_str(amount, first_str(item, "mark_price", "last")),
            exchange_ts=_time_ms(item),
            raw={"contract_stats": stats, "contract": contract},
            note="Gate stats report contracts; amount is contracts multiplied by quanto_multiplier.",
        )


def _time_ms(item) -> int:
    timestamp = first_float(item, "time_ms", "time", "current")
    if timestamp is None:
        return now_ms()
    timestamp_ms = int(timestamp)
    if timestamp_ms < 10_000_000_000:
        timestamp_ms *= 1000
    return timestamp_ms


def build_rest_adapter() -> ExchangeAdapter:
    return GatePerpRestAdapter()
