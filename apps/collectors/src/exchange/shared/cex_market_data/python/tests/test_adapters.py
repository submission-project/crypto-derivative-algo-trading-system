from __future__ import annotations

from typing import Any, Mapping

import pytest

from binance_perp_collector.rest import BinancePerpRestAdapter
from bitfinex_perp_collector.rest import BitfinexPerpRestAdapter
from bitget_perp_collector.rest import BitgetPerpRestAdapter
from bybit_perp_collector.rest import BybitPerpRestAdapter
from cex_market_data_collector.collector import collect_exchange_snapshot
from cex_market_data_collector.adapters import supported_exchanges
from gate_perp_collector.rest import GatePerpRestAdapter


class FakeClient:
    def __init__(self, payloads: dict[str, Any]):
        self.payloads = payloads
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    async def get_json(self, url: str, params: Mapping[str, Any] | None = None) -> Any:
        self.calls.append((url, params))
        for needle, payload in self.payloads.items():
            if needle in url:
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"unexpected URL: {url}")


@pytest.mark.asyncio
async def test_binance_normalizes_orderbook_and_open_interest() -> None:
    client = FakeClient(
        {
            "/depth": {
                "lastUpdateId": 123,
                "bids": [["65000.0", "1.5"]],
                "asks": [["65001.0", "2.0"]],
            },
            "/openInterest": {
                "openInterest": "100.0",
                "time": 1_700_000_000_000,
            },
            "/premiumIndex": {
                "markPrice": "65000.0",
                "time": 1_700_000_000_001,
            },
        }
    )

    adapter = BinancePerpRestAdapter()
    orderbook = await adapter.fetch_orderbook(client, depth=20)
    oi = await adapter.fetch_open_interest(client)

    assert orderbook.sequence == "123"
    assert orderbook.bids[0].price == "65000.0"
    assert orderbook.asks[0].size == "2.0"
    assert oi.open_interest == "100.0"
    assert oi.open_interest_value_usd == "6500000.0"


@pytest.mark.asyncio
async def test_bybit_normalizes_v5_shapes() -> None:
    client = FakeClient(
        {
            "/orderbook": {
                "result": {
                    "s": "BTCUSDT",
                    "b": [["65000.0", "1"]],
                    "a": [["65001.0", "1"]],
                    "cts": 1_700_000_000_100,
                    "u": 10,
                }
            },
            "/open-interest": {
                "result": {
                    "list": [
                        {
                            "openInterest": "10.0",
                            "timestamp": "1700000000000",
                        }
                    ]
                }
            },
            "/tickers": {
                "result": {
                    "list": [
                        {
                            "markPrice": "65000.0",
                        }
                    ]
                }
            },
        }
    )

    adapter = BybitPerpRestAdapter()
    orderbook = await adapter.fetch_orderbook(client)
    oi = await adapter.fetch_open_interest(client)

    assert orderbook.exchange_ts == 1_700_000_000_100
    assert orderbook.sequence == "10"
    assert oi.open_interest_unit == "BTC"
    assert oi.open_interest_value_usd == "650000.0"


@pytest.mark.asyncio
async def test_bitget_uses_size_as_base_amount() -> None:
    client = FakeClient(
        {
            "/merge-depth": {
                "data": {
                    "bids": [[65000.0, 1.0]],
                    "asks": [[65001.0, 1.0]],
                    "ts": "1700000000000",
                    "precision": "scale0",
                }
            },
            "/open-interest": {
                "data": {
                    "openInterestList": [{"symbol": "BTCUSDT", "size": "2.5"}],
                    "ts": "1700000000000",
                }
            },
            "/ticker": {
                "data": [
                    {
                        "markPrice": "65000.0",
                    }
                ]
            },
        }
    )

    adapter = BitgetPerpRestAdapter()
    orderbook = await adapter.fetch_orderbook(client)
    oi = await adapter.fetch_open_interest(client)

    assert orderbook.bids[0].price == "65000.0"
    assert oi.open_interest == "2.5"
    assert oi.open_interest_value_usd == "162500.0"


@pytest.mark.asyncio
async def test_gate_converts_contracts_with_multiplier() -> None:
    client = FakeClient(
        {
            "/order_book": {
                "bids": [["65000.0", "10"]],
                "asks": [["65001.0", "20"]],
                "current": 1_700_000_000_000,
                "id": 100,
            },
            "/contract_stats": [
                {
                    "open_interest": "1000",
                    "mark_price": "65000.0",
                    "time": 1_700_000_000,
                }
            ],
            "/contracts/BTC_USDT": {
                "quanto_multiplier": "0.0001",
            },
        }
    )

    adapter = GatePerpRestAdapter()
    orderbook = await adapter.fetch_orderbook(client)
    oi = await adapter.fetch_open_interest(client)

    assert orderbook.sequence == "100"
    assert oi.open_interest == "0.1"
    assert oi.open_interest_value_usd == "6500.0"


@pytest.mark.asyncio
async def test_bitfinex_splits_signed_book_amounts() -> None:
    client = FakeClient(
        {
            "/book/": [
                [65000.0, 1, 0.5],
                [65001.0, 1, -0.25],
            ],
            ":long/last": [1_700_000_000_000, 10.0],
            ":short/last": [1_700_000_000_000, 12.0],
        }
    )

    adapter = BitfinexPerpRestAdapter()
    orderbook = await adapter.fetch_orderbook(client)
    oi = await adapter.fetch_open_interest(client)

    assert orderbook.bids[0].size == "0.5"
    assert orderbook.asks[0].size == "0.25"
    assert oi.open_interest == "22.0"


@pytest.mark.asyncio
async def test_collector_preserves_partial_failures() -> None:
    client = FakeClient(
        {
            "/depth": RuntimeError("depth unavailable"),
            "/openInterest": {
                "openInterest": "100.0",
                "time": 1_700_000_000_000,
            },
            "/premiumIndex": {
                "markPrice": "65000.0",
            },
        }
    )

    snapshot = await collect_exchange_snapshot("binance", client)

    assert snapshot.orderbook is not None
    assert snapshot.orderbook.error == "depth unavailable"
    assert snapshot.open_interest is not None
    assert snapshot.open_interest.open_interest_value_usd == "6500000.0"
    assert snapshot.error is None


def test_supported_exchange_list_includes_requested_portfolio_venues() -> None:
    assert set(supported_exchanges()) >= {
        "binance",
        "bybit",
        "okx",
        "bitget",
        "gate",
        "mexc",
        "kucoin",
        "bingx",
        "htx",
        "kraken",
        "bitfinex",
        "lbank",
    }
