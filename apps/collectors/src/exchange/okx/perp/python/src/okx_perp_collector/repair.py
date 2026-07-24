from __future__ import annotations

from typing import Any, Mapping

from cex_market_data_collector.adapter_base import JsonGetter
from cex_market_data_collector.operational_helpers import trade_event
from cex_market_data_collector.trade_repair import (
    TradeGap,
    filter_trade_rows_by_gap,
    mark_repaired_trade,
)
from cex_market_data_collector.utils import first_str


class OkxTradeRepairAdapter:
    """Best-effort OKX repair via recent public trades."""

    exchange = "okx"

    async def fetch_repair_trades(
        self,
        client: JsonGetter,
        gap: TradeGap,
    ) -> list[dict[str, Any]]:
        data = await client.get_json(
            "https://www.okx.com/api/v5/market/trades",
            {"instId": gap.symbol, "limit": "500"},
        )
        rows = data.get("data", [])
        rows = rows if isinstance(rows, list) else []
        repaired = filter_trade_rows_by_gap(
            rows,
            id_getter=lambda row: row.get("tradeId"),
            ts_getter=lambda row: row.get("ts"),
            gap=gap,
        )
        return [
            mark_repaired_trade(_okx_trade(row, gap.symbol), gap)
            for row in repaired
            if isinstance(row, Mapping)
        ]


def _okx_trade(row: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    return trade_event(
        exchange="okx",
        symbol=first_str(row, "instId") or symbol,
        trade_id=first_str(row, "tradeId"),
        price=first_str(row, "px"),
        size=first_str(row, "sz"),
        side=first_str(row, "side"),
        exchange_ts=first_str(row, "ts"),
        raw=dict(row),
    )
