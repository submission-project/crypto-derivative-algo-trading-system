from __future__ import annotations

from typing import Any

from cex_market_data_collector.models import now_ms
from cex_market_data_collector.operational_helpers import open_interest_event, orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        if not isinstance(packet, list) or len(packet) < 2 or packet[1] == "hb":
            return []
        payload = packet[1]
        if isinstance(payload, list) and len(payload) >= 11:
            return [
                open_interest_event(
                    exchange="bitfinex",
                    symbol="tBTCF0:USTF0",
                    open_interest=payload[10],
                    mark_price=payload[9] if len(payload) > 9 else None,
                    exchange_ts=payload[0] if payload else now_ms(),
                    unit="BTC",
                    raw=packet,
                    note="Bitfinex derivatives status stream OPEN_INTEREST field.",
                )
            ]
        if isinstance(payload, list) and payload and isinstance(payload[0], list):
            return [
                orderbook_event(
                    exchange="bitfinex",
                    symbol="tBTCF0:USTF0",
                    bids=[],
                    asks=[],
                    exchange_ts=now_ms(),
                    sequence=packet[0],
                    raw=packet,
                )
            ]
        return [
            trade_event(
                exchange="bitfinex",
                symbol="tBTCF0:USTF0",
                trade_id=payload[0] if isinstance(payload, list) and payload else None,
                price=payload[3] if isinstance(payload, list) and len(payload) > 3 else None,
                size=abs(payload[2]) if isinstance(payload, list) and len(payload) > 2 else None,
                side="buy" if isinstance(payload, list) and len(payload) > 2 and payload[2] > 0 else "sell",
                exchange_ts=payload[1] if isinstance(payload, list) and len(payload) > 1 else now_ms(),
                raw=packet,
            )
        ]

    return (
        WebSocketSpec(
            "bitfinex",
            "trade_orderbook_oi",
            "wss://api-pub.bitfinex.com/ws/2",
            topic("bitfinex", "mixed"),
            subscribe_messages=(
                {"event": "subscribe", "channel": "trades", "symbol": "tBTCF0:USTF0"},
                {"event": "subscribe", "channel": "book", "symbol": "tBTCF0:USTF0", "prec": "P0", "len": 25},
                {"event": "subscribe", "channel": "status", "key": "deriv:tBTCF0:USTF0"},
            ),
            normalizer=normalize,
        ),
    )
