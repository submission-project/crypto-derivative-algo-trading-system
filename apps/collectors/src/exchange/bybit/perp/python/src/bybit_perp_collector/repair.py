from __future__ import annotations

from typing import Any, Mapping

from cex_market_data_collector.adapter_base import JsonGetter
from cex_market_data_collector.operational_helpers import trade_event
from cex_market_data_collector.trade_repair import (
    TradeGap,
    filter_trade_rows_by_gap,
    mark_repaired_trade,
)
from cex_market_data_collector.utils import first_mapping, first_str


class BybitTradeRepairAdapter:
    """Best-effort Bybit repair via V5 recent trades."""

    exchange = "bybit"

    async def fetch_repair_trades(
        self,
        client: JsonGetter,
        gap: TradeGap,
    ) -> list[dict[str, Any]]:
        data = await client.get_json(
            "https://api.bybit.com/v5/market/recent-trade",
            {"category": "linear", "symbol": gap.symbol, "limit": 1000},
        )
        rows = first_mapping(data.get("result")).get("list", [])
        rows = rows if isinstance(rows, list) else []
        repaired = filter_trade_rows_by_gap(
            rows,
            id_getter=lambda row: row.get("execId") or row.get("i"),
            ts_getter=lambda row: row.get("time") or row.get("T"),
            gap=gap,
        )
        return [
            mark_repaired_trade(_bybit_trade(row, gap.symbol), gap)
            for row in repaired
            if isinstance(row, Mapping)
        ]


def _bybit_trade(row: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    return trade_event(
        exchange="bybit",
        symbol=first_str(row, "s", "symbol") or symbol,
        trade_id=first_str(row, "execId", "i"),
        price=first_str(row, "price", "p"),
        size=first_str(row, "size", "v"),
        side=first_str(row, "side", "S"),
        exchange_ts=first_str(row, "time", "T"),
        raw=dict(row),
    )
