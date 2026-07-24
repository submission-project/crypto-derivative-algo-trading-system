from __future__ import annotations

from typing import Any, Mapping

import pytest

from bitget_perp_collector.repair import BitgetTradeRepairAdapter
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
async def test_bitget_repair_adapter_uses_recent_fills_endpoint() -> None:
    state = TradeRepairState()
    state.observe({"data_type": "trade", "exchange": "bitget", "symbol": "BTCUSDT", "trade_id": "20"})
    gap = state.observe({"data_type": "trade", "exchange": "bitget", "symbol": "BTCUSDT", "trade_id": "22"})
    assert gap is not None
    adapter = BitgetTradeRepairAdapter()
    client = FakeClient(
        {
            "/api/v2/mix/market/fills": {
                "data": [
                    {"tradeId": "21", "symbol": "BTCUSDT", "price": "65000", "size": "0.1", "side": "buy", "ts": "1"}
                ]
            }
        }
    )

    repaired = await adapter.fetch_repair_trades(client, gap)

    assert repaired[0]["exchange"] == "bitget"
    assert repaired[0]["trade_id"] == "21"
    assert repaired[0]["source"] == "rest_gap_fill"
    assert client.calls[0][1]["productType"] == "USDT-FUTURES"
