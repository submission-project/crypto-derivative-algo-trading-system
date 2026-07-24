from __future__ import annotations

from cex_market_data_collector.adapter_base import ExchangeAdapter
from cex_market_data_collector.models import now_ms
from cex_market_data_collector.utils import to_float
from cex_market_data_collector.http import HttpJsonClient


class BitfinexPerpRestAdapter(ExchangeAdapter):
    exchange = "bitfinex"
    symbol = "tBTCF0:USTF0"

    async def fetch_orderbook(self, client: HttpJsonClient, *, depth: int = 20):
        data = await client.get_json(
            f"https://api-pub.bitfinex.com/v2/book/{self.symbol}/P0",
            {"len": depth},
        )
        bids = []
        asks = []
        for price, _count, amount in data:
            level = [price, abs(amount)]
            if amount > 0:
                bids.append(level)
            else:
                asks.append(level)
        return self._orderbook(
            bids=bids,
            asks=asks,
            exchange_ts=now_ms(),
            sequence=None,
            raw=data,
        )

    async def fetch_open_interest(self, client: HttpJsonClient):
        long_stats = await client.get_json(
            f"https://api-pub.bitfinex.com/v2/stats1/pos.size:1m:{self.symbol}:long/last"
        )
        short_stats = await client.get_json(
            f"https://api-pub.bitfinex.com/v2/stats1/pos.size:1m:{self.symbol}:short/last"
        )
        long_size = to_float(long_stats[1] if isinstance(long_stats, list) and len(long_stats) > 1 else None)
        short_size = to_float(short_stats[1] if isinstance(short_stats, list) and len(short_stats) > 1 else None)
        total = None if long_size is None and short_size is None else str((long_size or 0.0) + (short_size or 0.0))
        return self._oi(
            open_interest=total,
            unit="BTC",
            value_usd=None,
            exchange_ts=int(to_float(long_stats[0]) or now_ms()) if isinstance(long_stats, list) else now_ms(),
            raw={"long": long_stats, "short": short_stats},
            note="Bitfinex derivatives OI is approximated from public long/short position size stats; USD notional needs a separate mark price.",
        )


def build_rest_adapter() -> ExchangeAdapter:
    return BitfinexPerpRestAdapter()
