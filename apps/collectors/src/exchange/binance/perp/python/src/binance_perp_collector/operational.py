from __future__ import annotations

from typing import Any

from cex_market_data_collector.operational_helpers import orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    url = (
        "wss://fstream.binance.com/stream"
        "?streams=btcusdt@trade/btcusdt@depth20@100ms"
    )

    def normalize(packet: Any) -> list[MarketEvent]:
        data = packet.get("data", packet) if isinstance(packet, dict) else {}
        event_type = data.get("e")
        if event_type == "trade":
            return [
                trade_event(
                    exchange="binance",
                    symbol=data.get("s", "BTCUSDT"),
                    trade_id=data.get("t"),
                    price=data.get("p"),
                    size=data.get("q"),
                    side="sell" if data.get("m") else "buy",
                    exchange_ts=data.get("T") or data.get("E"),
                    raw=data,
                )
            ]
        if event_type == "depthUpdate":
            return [
                orderbook_event(
                    exchange="binance",
                    symbol=data.get("s", "BTCUSDT"),
                    bids=data.get("b", []),
                    asks=data.get("a", []),
                    exchange_ts=data.get("T") or data.get("E"),
                    sequence=data.get("u"),
                    raw=data,
                )
            ]
        return []

    return (
        WebSocketSpec(
            "binance",
            "trade_orderbook",
            url,
            topic("binance", "mixed"),
            normalizer=normalize,
        ),
    )
