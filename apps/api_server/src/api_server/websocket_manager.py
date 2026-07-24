from __future__ import annotations

import logging
import re
from typing import Any
import orjson
from fastapi import WebSocket

logger = logging.getLogger(__name__)


def normalize_market_symbol(symbol: str) -> str:
    """Normalize exchange-specific BTC/USDT perpetual symbols for subscription matching."""
    raw = str(symbol or "").strip().upper()
    if raw == "*":
        return "*"

    if ":" in raw:
        base = raw.split(":", 1)[0]
        if base.startswith("T"):
            base = base[1:]
        if base.endswith("F0"):
            base = base[:-2]
        base = base.replace("XBT", "BTC")
        return f"{base}USDT" if base else raw

    cleaned = re.sub(r"[^A-Z0-9]", "", raw)
    if cleaned.startswith("PF"):
        cleaned = cleaned[2:]
    cleaned = cleaned.replace("XBT", "BTC")

    for suffix in ("FUTURES", "SWAP", "PERP"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]

    if cleaned.endswith("M") and cleaned[:-1].endswith("USDT"):
        cleaned = cleaned[:-1]

    for quote in ("USDT", "USDC", "USD"):
        if cleaned.endswith(quote) and len(cleaned) > len(quote):
            return f"{cleaned[: -len(quote)]}USDT"
    return cleaned


class WebSocketManager:
    def __init__(self) -> None:
        self.active_connections: set[WebSocket] = set()
        # Mapping from connection to a set of (exchange, symbol, data_type) subscription filters
        # e.g., ("binance", "BTCUSDT", "trade")
        self.subscriptions: dict[WebSocket, set[tuple[str, str, str]]] = {}

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        self.subscriptions[websocket] = set()
        logger.info(f"WebSocket client connected: {websocket.client}")

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        self.subscriptions.pop(websocket, None)
        logger.info(f"WebSocket client disconnected: {websocket.client}")

    def subscribe(self, websocket: WebSocket, exchange: str, symbol: str, data_type: str) -> None:
        if websocket in self.subscriptions:
            self.subscriptions[websocket].add((exchange.lower(), normalize_market_symbol(symbol), data_type.lower()))
            logger.debug(f"Client {websocket.client} subscribed to {exchange}:{symbol}:{data_type}")

    def unsubscribe(self, websocket: WebSocket, exchange: str, symbol: str, data_type: str) -> None:
        if websocket in self.subscriptions:
            self.subscriptions[websocket].discard((exchange.lower(), normalize_market_symbol(symbol), data_type.lower()))
            logger.debug(f"Client {websocket.client} unsubscribed from {exchange}:{symbol}:{data_type}")

    async def broadcast(self, payload: dict[str, Any]) -> None:
        if not self.active_connections:
            return

        val = payload.get("value") or {}
        exchange = str(val.get("exchange", "")).lower()
        symbol = normalize_market_symbol(str(val.get("symbol", "")))
        data_type = str(val.get("data_type", "")).lower()

        message_bytes = None
        for ws in list(self.active_connections):
            subs = self.subscriptions.get(ws, set())
            
            # If the client has no specific subscriptions, they receive all events.
            # Otherwise, check if the incoming event matches any of their filters.
            # A wildcard "*" can be used for exchange, symbol, or data_type.
            should_send = False
            if not subs:
                should_send = True
            else:
                for sub_exchange, sub_symbol, sub_data_type in subs:
                    match_exchange = (sub_exchange == "*" or sub_exchange == exchange)
                    match_symbol = (sub_symbol == "*" or sub_symbol == symbol)
                    match_data_type = (sub_data_type == "*" or sub_data_type == data_type)
                    if match_exchange and match_symbol and match_data_type:
                        should_send = True
                        break

            if should_send:
                try:
                    if message_bytes is None:
                        message_bytes = orjson.dumps(payload)
                    await ws.send_bytes(message_bytes)
                except Exception as e:
                    logger.debug(f"Failed to send websocket message to {ws.client}: {e}")
                    self.disconnect(ws)


class WebSocketManagerSink:
    """
    An EventSink implementation that routes events from the CEX market data collector
    to the WebSocketManager.
    """
    def __init__(self, manager: WebSocketManager) -> None:
        self.manager = manager

    async def emit(self, topic: str, key: str, event: Any) -> None:
        payload = {
            "topic": topic,
            "key": key,
            "value": event,
        }
        await self.manager.broadcast(payload)

    async def close(self) -> None:
        pass
