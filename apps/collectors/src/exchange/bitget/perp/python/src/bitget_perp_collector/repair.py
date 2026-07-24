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


class BitgetTradeRepairAdapter:
    """Best-effort Bitget repair via recent futures fills."""

    exchange = "bitget"

    async def fetch_repair_trades(
        self,
        client: JsonGetter,
        gap: TradeGap,
    ) -> list[dict[str, Any]]:
        data = await client.get_json(
            "https://api.bitget.com/api/v2/mix/market/fills",
            {"productType": "USDT-FUTURES", "symbol": gap.symbol, "limit": "100"},
        )
        rows = data.get("data", [])
        rows = rows if isinstance(rows, list) else []
        repaired = filter_trade_rows_by_gap(
            rows,
            id_getter=lambda row: row.get("tradeId"),
            ts_getter=lambda row: row.get("ts") or row.get("tradeTime"),
            gap=gap,
        )
        return [
            mark_repaired_trade(_bitget_trade(row, gap.symbol), gap)
            for row in repaired
            if isinstance(row, Mapping)
        ]


def _bitget_trade(row: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    return trade_event(
        exchange="bitget",
        symbol=first_str(row, "symbol", "instId") or symbol,
        trade_id=first_str(row, "tradeId"),
        price=first_str(row, "price", "p"),
        size=first_str(row, "size", "q"),
        side=first_str(row, "side"),
        exchange_ts=first_str(row, "ts", "tradeTime"),
        raw=dict(row),
    )
