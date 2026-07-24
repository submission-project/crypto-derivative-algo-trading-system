from __future__ import annotations

import time
from typing import Any, Mapping

from cex_market_data_collector.operational_helpers import open_interest_event, orderbook_event, topic, trade_event
from cex_market_data_collector.operational_models import MarketEvent, WebSocketSpec
from cex_market_data_collector.utils import first_float


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    def normalize(packet: Any) -> list[MarketEvent]:
        channel = packet.get("channel") if isinstance(packet, dict) else None
        result = packet.get("result", {}) if isinstance(packet, dict) else {}
        if channel == "futures.trades":
            rows = result if isinstance(result, list) else [result]
            return [
                trade_event(
                    exchange="gate",
                    symbol=row.get("contract", "BTC_USDT"),
                    trade_id=row.get("id"),
                    price=row.get("price"),
                    size=row.get("size"),
                    side="buy" if first_float(row, "size") and first_float(row, "size") > 0 else "sell",
                    exchange_ts=(first_float(row, "create_time_ms") or 0),
                    raw=row,
                )
                for row in rows
                if isinstance(row, Mapping)
            ]
        if channel in {"futures.order_book_update", "futures.book_ticker"}:
            return [
                orderbook_event(
                    exchange="gate",
                    symbol=result.get("s", "BTC_USDT"),
                    bids=result.get("b", []),
                    asks=result.get("a", []),
                    exchange_ts=(first_float(result, "t") or time.time()) * 1000,
                    sequence=result.get("U") or result.get("u"),
                    raw=packet,
                )
            ]
        if channel == "futures.tickers":
            return [
                open_interest_event(
                    exchange="gate",
                    symbol=result.get("contract", "BTC_USDT"),
                    open_interest=result.get("total_size"),
                    mark_price=result.get("mark_price") or result.get("last"),
                    exchange_ts=(first_float(packet, "time_ms") or first_float(packet, "time") or time.time()) * 1000,
                    unit="CONTRACTS",
                    raw=packet,
                    note="Gate futures ticker stream total_size is used as open-interest contracts.",
                )
            ]
        return []

    return (
        WebSocketSpec(
            "gate",
            "trade_orderbook_oi",
            "wss://fx-ws.gateio.ws/v4/ws/usdt",
            topic("gate", "mixed"),
            subscribe_messages=(
                {"time": int(time.time()), "channel": "futures.trades", "event": "subscribe", "payload": ["BTC_USDT"]},
                {"time": int(time.time()), "channel": "futures.order_book_update", "event": "subscribe", "payload": ["BTC_USDT", "100ms", "20"]},
                {"time": int(time.time()), "channel": "futures.tickers", "event": "subscribe", "payload": ["BTC_USDT"]},
            ),
            normalizer=normalize,
        ),
    )
