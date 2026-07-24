from __future__ import annotations

from typing import Any

from cex_market_data_collector.models import now_ms
from cex_market_data_collector.operational_helpers import open_interest_event, orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        feed = packet.get("feed") if isinstance(packet, dict) else None
        if feed == "trade":
            return [
                trade_event(
                    exchange="kraken",
                    symbol=packet.get("product_id", "PF_XBTUSD"),
                    trade_id=packet.get("uid"),
                    price=packet.get("price"),
                    size=packet.get("qty"),
                    side=packet.get("side"),
                    exchange_ts=now_ms(),
                    raw=packet,
                )
            ]
        if feed and "book" in feed:
            return [
                orderbook_event(
                    exchange="kraken",
                    symbol=packet.get("product_id", "PF_XBTUSD"),
                    bids=packet.get("bids", []),
                    asks=packet.get("asks", []),
                    exchange_ts=now_ms(),
                    sequence=packet.get("seq"),
                    raw=packet,
                )
            ]
        if feed == "ticker":
            return [
                open_interest_event(
                    exchange="kraken",
                    symbol=packet.get("product_id", "PF_XBTUSD"),
                    open_interest=packet.get("openInterest"),
                    mark_price=packet.get("markPrice") or packet.get("last") or packet.get("index"),
                    exchange_ts=packet.get("time") or now_ms(),
                    unit="BTC",
                    raw=packet,
                    note="Kraken Futures ticker feed includes openInterest and markPrice.",
                )
            ]
        return []

    return (
        WebSocketSpec(
            "kraken",
            "trade_orderbook_oi",
            "wss://futures.kraken.com/ws/v1",
            topic("kraken", "mixed"),
            subscribe_messages=(
                {"event": "subscribe", "feed": "trade", "product_ids": ["PF_XBTUSD"]},
                {"event": "subscribe", "feed": "book", "product_ids": ["PF_XBTUSD"]},
                {"event": "subscribe", "feed": "ticker", "product_ids": ["PF_XBTUSD"]},
            ),
            normalizer=normalize,
        ),
    )
