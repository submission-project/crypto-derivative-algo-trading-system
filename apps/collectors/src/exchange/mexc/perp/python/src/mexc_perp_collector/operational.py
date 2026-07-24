from __future__ import annotations

from typing import Any, Mapping

from cex_market_data_collector.operational_helpers import open_interest_event, orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        channel = packet.get("channel") if isinstance(packet, dict) else None
        data = packet.get("data", {}) if isinstance(packet, dict) else {}
        symbol = packet.get("symbol", "BTC_USDT") if isinstance(packet, dict) else "BTC_USDT"
        if channel == "push.deal":
            rows = data if isinstance(data, list) else [data]
            return [
                trade_event(
                    exchange="mexc",
                    symbol=symbol,
                    trade_id=row.get("tradeId") or row.get("id"),
                    price=row.get("p"),
                    size=row.get("v"),
                    side=row.get("T"),
                    exchange_ts=row.get("t"),
                    raw=row,
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        if channel == "push.depth":
            return [
                orderbook_event(
                    exchange="mexc",
                    symbol=symbol,
                    bids=data.get("bids", []),
                    asks=data.get("asks", []),
                    exchange_ts=data.get("ts"),
                    sequence=data.get("version"),
                    raw=packet,
                )
            ]
        if channel == "push.ticker":
            return [
                open_interest_event(
                    exchange="mexc",
                    symbol=symbol,
                    open_interest=data.get("holdVol") or data.get("openInterest"),
                    mark_price=data.get("fairPrice") or data.get("lastPrice") or data.get("indexPrice"),
                    exchange_ts=data.get("timestamp") or data.get("ts") or packet.get("ts"),
                    unit="CONTRACTS",
                    raw=packet,
                    note="MEXC contract ticker stream holdVol is used as open-interest contracts.",
                )
            ]
        return []

    return (
        WebSocketSpec(
            "mexc",
            "trade_orderbook_oi",
            "wss://contract.mexc.com/edge",
            topic("mexc", "mixed"),
            subscribe_messages=(
                {"method": "sub.deal", "param": {"symbol": "BTC_USDT"}},
                {"method": "sub.depth", "param": {"symbol": "BTC_USDT"}},
                {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}},
            ),
            ping_message={"method": "ping"},
            normalizer=normalize,
        ),
    )
