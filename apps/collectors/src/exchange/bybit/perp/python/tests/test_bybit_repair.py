from __future__ import annotations

from typing import Any, Mapping

import pytest

from bybit_perp_collector.repair import BybitTradeRepairAdapter
from cex_market_data_collector.trade_repair import TradeRepairState


class FakeClient:
    def __init__(self, payloads: dict[str, Any]):
        self.payloads = payloads
        self.calls: list[tuple[str, Mapping[str, Any] | None]] = []

    async def get_json(self, url: str, params: Mapping[str, Any] | None = None) -> Any:
        self.calls.append((url, params))
        for needle, payload in self.payloads.items():
            if needle in url:
                return payload
        raise AssertionError(f"unexpected URL: {url}")


@pytest.mark.asyncio
async def test_bybit_repair_adapter_uses_recent_trade_endpoint() -> None:
    state = TradeRepairState()
    state.observe({"data_type": "trade", "exchange": "bybit", "symbol": "BTCUSDT", "trade_id": "10"})
    gap = state.observe({"data_type": "trade", "exchange": "bybit", "symbol": "BTCUSDT", "trade_id": "12"})
    assert gap is not None
    adapter = BybitTradeRepairAdapter()
    client = FakeClient(
        {
            "/v5/market/recent-trade": {
                "result": {
                    "list": [
                        {"execId": "11", "symbol": "BTCUSDT", "price": "65000", "size": "0.1", "side": "Buy", "time": "1"}
                    ]
                }
            }
        }
    )

    repaired = await adapter.fetch_repair_trades(client, gap)

    assert repaired[0]["exchange"] == "bybit"
    assert repaired[0]["trade_id"] == "11"
    assert repaired[0]["source"] == "rest_gap_fill"
    assert client.calls[0][1]["category"] == "linear"


@pytest.mark.asyncio
async def test_bybit_repair_adapter_filters_by_timestamp_for_resume_gap() -> None:
    state = TradeRepairState()
    state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "abc",
            "exchange_ts": 1_700_000_000_000,
        }
    )
    state.mark_stream_interrupted()
    gap = state.observe(
        {
            "data_type": "trade",
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "trade_id": "def",
            "exchange_ts": 1_700_000_003_000,
        }
    )
    assert gap is not None
    adapter = BybitTradeRepairAdapter()
    client = FakeClient(
        {
            "/v5/market/recent-trade": {
                "result": {
                    "list": [
                        {"execId": "old", "symbol": "BTCUSDT", "price": "1", "size": "0.1", "side": "Buy", "time": "1699999999999"},
                        {"execId": "mid", "symbol": "BTCUSDT", "price": "2", "size": "0.1", "side": "Buy", "time": "1700000001000"},
                        {"execId": "new", "symbol": "BTCUSDT", "price": "3", "size": "0.1", "side": "Buy", "time": "1700000003000"},
                    ]
                }
            }
        }
    )

    repaired = await adapter.fetch_repair_trades(client, gap)

    assert [event["trade_id"] for event in repaired] == ["mid"]
