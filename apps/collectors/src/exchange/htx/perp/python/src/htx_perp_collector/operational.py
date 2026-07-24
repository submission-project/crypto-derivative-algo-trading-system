from __future__ import annotations

from typing import Any, Mapping

from cex_market_data_collector.operational_helpers import orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        channel = packet.get("ch") if isinstance(packet, dict) else None
        tick = packet.get("tick", {}) if isinstance(packet, dict) else {}
        if channel == "market.BTC-USDT.trade.detail":
            rows = tick.get("data", [])
            return [
                trade_event(
                    exchange="htx",
                    symbol="BTC-USDT",
                    trade_id=row.get("id"),
                    price=row.get("price"),
                    size=row.get("amount"),
                    side=row.get("direction"),
                    exchange_ts=row.get("ts"),
                    raw=row,
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        if channel == "market.BTC-USDT.depth.step0":
            return [
                orderbook_event(
                    exchange="htx",
                    symbol="BTC-USDT",
                    bids=tick.get("bids", []),
                    asks=tick.get("asks", []),
                    exchange_ts=tick.get("ts"),
                    sequence=tick.get("version"),
                    raw=packet,
                )
            ]
        return []

    return (
        WebSocketSpec(
            "htx",
            "trade_orderbook",
            "wss://api.hbdm.com/linear-swap-ws",
            topic("htx", "mixed"),
            subscribe_messages=(
                {"sub": "market.BTC-USDT.trade.detail", "id": "trade"},
                {"sub": "market.BTC-USDT.depth.step0", "id": "depth"},
            ),
            gzip_binary=True,
            normalizer=normalize,
        ),
    )
