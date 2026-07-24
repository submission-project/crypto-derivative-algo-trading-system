from __future__ import annotations

from typing import Any, Mapping

import pytest

from cex_market_data_collector.trade_repair import TradeRepairState
from okx_perp_collector.repair import OkxTradeRepairAdapter


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
async def test_okx_repair_adapter_filters_recent_trades_by_gap() -> None:
    state = TradeRepairState()
    state.observe({"data_type": "trade", "exchange": "okx", "symbol": "BTC-USDT-SWAP", "trade_id": "100"})
    gap = state.observe({"data_type": "trade", "exchange": "okx", "symbol": "BTC-USDT-SWAP", "trade_id": "103"})
    assert gap is not None
    adapter = OkxTradeRepairAdapter()
    client = FakeClient(
        {
            "/api/v5/market/trades": {
                "data": [
                    {"instId": "BTC-USDT-SWAP", "tradeId": "99", "px": "1", "sz": "1", "side": "buy", "ts": "1"},
                    {"instId": "BTC-USDT-SWAP", "tradeId": "101", "px": "2", "sz": "1", "side": "buy", "ts": "2"},
                    {"instId": "BTC-USDT-SWAP", "tradeId": "102", "px": "3", "sz": "1", "side": "sell", "ts": "3"},
                    {"instId": "BTC-USDT-SWAP", "tradeId": "104", "px": "4", "sz": "1", "side": "sell", "ts": "4"},
                ]
            }
        }
    )

    repaired = await adapter.fetch_repair_trades(client, gap)

    assert [event["trade_id"] for event in repaired] == ["101", "102"]
    assert all(event["source"] == "rest_gap_fill" for event in repaired)
    assert all(event["verified_by_rest"] is True for event in repaired)
