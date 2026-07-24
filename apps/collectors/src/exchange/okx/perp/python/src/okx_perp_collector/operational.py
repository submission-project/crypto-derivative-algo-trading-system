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

from .repair import OkxTradeRepairAdapter


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        arg = packet.get("arg", {}) if isinstance(packet, dict) else {}
        channel = arg.get("channel")
        inst_id = arg.get("instId", "BTC-USDT-SWAP")
        rows = packet.get("data", []) if isinstance(packet, dict) else []
        if channel == "trades":
            return [
                trade_event(
                    exchange="okx",
                    symbol=row.get("instId", inst_id),
                    trade_id=row.get("tradeId"),
                    price=row.get("px"),
                    size=row.get("sz"),
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
                    exchange="okx",
                    symbol=inst_id,
                    bids=item.get("bids", []),
                    asks=item.get("asks", []),
                    exchange_ts=item.get("ts"),
                    sequence=item.get("seqId"),
                    raw=packet,
                )
            ]
        if channel == "open-interest":
            return [
                open_interest_event(
                    exchange="okx",
                    symbol=row.get("instId", inst_id),
                    open_interest=row.get("oiCcy") or row.get("oi"),
                    open_interest_value_usd=row.get("oiUsd"),
                    exchange_ts=row.get("ts"),
                    unit="BTC_OR_CONTRACTS",
                    raw=row,
                    note="OKX public open-interest channel for derivative instruments.",
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        return []

    return (
        WebSocketSpec(
            "okx",
            "trade_orderbook_oi",
            "wss://ws.okx.com:8443/ws/v5/public",
            topic("okx", "mixed"),
            subscribe_messages=(
                {
                    "op": "subscribe",
                    "args": [
                        {"channel": "trades", "instId": "BTC-USDT-SWAP"},
                        {"channel": "books5", "instId": "BTC-USDT-SWAP"},
                        {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
                    ],
                },
            ),
            normalizer=normalize,
            trade_repair=OkxTradeRepairAdapter(),
        ),
    )
