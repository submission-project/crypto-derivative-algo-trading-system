from __future__ import annotations

from typing import Any, Mapping

from cex_market_data_collector.operational_helpers import (
    open_interest_event,
    orderbook_event,
    topic,
    trade_event,
)
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec
from cex_market_data_collector.utils import first_mapping

from .repair import BybitTradeRepairAdapter


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        topic_name = packet.get("topic", "") if isinstance(packet, dict) else ""
        data = packet.get("data", []) if isinstance(packet, dict) else []
        if topic_name.startswith("publicTrade."):
            rows = data if isinstance(data, list) else [data]
            return [
                trade_event(
                    exchange="bybit",
                    symbol=row.get("s", "BTCUSDT"),
                    trade_id=row.get("i"),
                    price=row.get("p"),
                    size=row.get("v"),
                    side=row.get("S"),
                    exchange_ts=row.get("T"),
                    raw=row,
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        if topic_name.startswith("orderbook."):
            item = first_mapping(data)
            return [
                orderbook_event(
                    exchange="bybit",
                    symbol=item.get("s", "BTCUSDT"),
                    bids=item.get("b", []),
                    asks=item.get("a", []),
                    exchange_ts=packet.get("cts") or item.get("ts") or packet.get("ts"),
                    sequence=item.get("u") or packet.get("seq"),
                    raw=packet,
                )
            ]
        if topic_name.startswith("tickers."):
            item = first_mapping(data)
            return [
                open_interest_event(
                    exchange="bybit",
                    symbol=item.get("symbol", "BTCUSDT"),
                    open_interest=item.get("openInterest"),
                    open_interest_value_usd=item.get("openInterestValue"),
                    exchange_ts=packet.get("ts") or item.get("ts"),
                    unit="BTC",
                    raw=packet,
                    note="Bybit linear ticker stream includes openInterest and openInterestValue.",
                )
            ]
        return []

    return (
        WebSocketSpec(
            "bybit",
            "trade_orderbook_oi",
            "wss://stream.bybit.com/v5/public/linear",
            topic("bybit", "mixed"),
            subscribe_messages=(
                {
                    "op": "subscribe",
                    "args": [
                        "publicTrade.BTCUSDT",
                        "orderbook.50.BTCUSDT",
                        "tickers.BTCUSDT",
                    ],
                },
            ),
            normalizer=normalize,
            trade_repair=BybitTradeRepairAdapter(),
        ),
    )
