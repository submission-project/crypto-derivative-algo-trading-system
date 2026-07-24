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

from .repair import BitgetTradeRepairAdapter


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        arg = packet.get("arg", {}) if isinstance(packet, dict) else {}
        channel = arg.get("channel")
        rows = packet.get("data", []) if isinstance(packet, dict) else []
        if channel == "trade":
            return [
                trade_event(
                    exchange="bitget",
                    symbol=arg.get("instId", "BTCUSDT"),
                    trade_id=row.get("tradeId"),
                    price=row.get("price") or row.get("p"),
                    size=row.get("size") or row.get("q"),
                    side=row.get("side"),
                    exchange_ts=row.get("ts"),
                    raw=row,
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        if channel in {"books5", "books"}:
            item = first_mapping(rows)
            return [
                orderbook_event(
                    exchange="bitget",
                    symbol=arg.get("instId", "BTCUSDT"),
                    bids=item.get("bids", []),
                    asks=item.get("asks", []),
                    exchange_ts=item.get("ts"),
                    sequence=item.get("checksum"),
                    raw=packet,
                )
            ]
        if channel in {"ticker", "tickers"}:
            return [
                open_interest_event(
                    exchange="bitget",
                    symbol=arg.get("instId", row.get("instId", "BTCUSDT")),
                    open_interest=(
                        row.get("openInterest")
                        or row.get("openInterestQty")
                        or row.get("holdingAmount")
                    ),
                    open_interest_value_usd=(
                        row.get("openInterestValue")
                        or row.get("openInterestUsd")
                        or row.get("openInterestUSDT")
                    ),
                    mark_price=row.get("markPrice") or row.get("lastPr") or row.get("last"),
                    exchange_ts=row.get("ts") or packet.get("ts"),
                    unit="BTC",
                    raw=row,
                    note="Bitget futures ticker stream includes open interest fields on supported product lines.",
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        return []

    return (
        WebSocketSpec(
            "bitget",
            "trade_orderbook_oi",
            "wss://ws.bitget.com/v2/ws/public",
            topic("bitget", "mixed"),
            subscribe_messages=(
                {
                    "op": "subscribe",
                    "args": [
                        {"instType": "USDT-FUTURES", "channel": "trade", "instId": "BTCUSDT"},
                        {"instType": "USDT-FUTURES", "channel": "books5", "instId": "BTCUSDT"},
                        {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
                    ],
                },
            ),
            normalizer=normalize,
            trade_repair=BitgetTradeRepairAdapter(),
        ),
    )
