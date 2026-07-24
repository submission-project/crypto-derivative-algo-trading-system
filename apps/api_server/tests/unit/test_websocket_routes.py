from __future__ import annotations

import pytest
import asyncio
import orjson
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient
from api_server.websocket_manager import WebSocketManager, WebSocketManagerSink, normalize_market_symbol
from api_server.routes.websocket import router
from api_server.runtime import state

@pytest.mark.asyncio
async def test_websocket_manager_subscriptions() -> None:
    manager = WebSocketManager()
    
    class MockWebSocket:
        def __init__(self) -> None:
            self.client = "test_client"
            self.sent_bytes: list[bytes] = []
            
        async def accept(self) -> None:
            pass
            
        async def send_bytes(self, data: bytes) -> None:
            self.sent_bytes.append(data)
            
    ws = MockWebSocket()
    await manager.connect(ws)  # type: ignore
    
    # 1. 특정 필터 구독
    manager.subscribe(ws, "binance", "BTCUSDT", "trade")  # type: ignore
    
    # 2. 관련 없는 이벤트 브로드캐스트 (전송되지 않아야 함)
    await manager.broadcast({
        "topic": "market.mixed.bybit.perp",
        "key": "BTCUSDT",
        "value": {
            "exchange": "bybit",
            "symbol": "BTCUSDT",
            "data_type": "trade",
            "price": "60000",
        }
    })
    assert len(ws.sent_bytes) == 0
    
    # 3. 매칭되는 이벤트 브로드캐스트 (전송되어야 함)
    matching_event = {
        "topic": "market.mixed.binance.perp",
        "key": "BTCUSDT",
        "value": {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "data_type": "trade",
            "price": "65000",
        }
    }
    await manager.broadcast(matching_event)
    assert len(ws.sent_bytes) == 1
    
    decoded = orjson.loads(ws.sent_bytes[0])
    assert decoded["value"]["price"] == "65000"
    
    manager.disconnect(ws)  # type: ignore
    assert ws not in manager.active_connections


def test_normalize_market_symbol_handles_exchange_specific_btc_perp_symbols() -> None:
    assert normalize_market_symbol("BTCUSDT") == "BTCUSDT"
    assert normalize_market_symbol("BTC-USDT-SWAP") == "BTCUSDT"
    assert normalize_market_symbol("BTC_USDT") == "BTCUSDT"
    assert normalize_market_symbol("PF_XBTUSD") == "BTCUSDT"
    assert normalize_market_symbol("tBTCF0:USTF0") == "BTCUSDT"
    assert normalize_market_symbol("*") == "*"


@pytest.mark.asyncio
async def test_websocket_manager_symbol_alias_subscriptions() -> None:
    manager = WebSocketManager()

    class MockWebSocket:
        def __init__(self) -> None:
            self.client = "test_client"
            self.sent_bytes: list[bytes] = []

        async def accept(self) -> None:
            pass

        async def send_bytes(self, data: bytes) -> None:
            self.sent_bytes.append(data)

    ws = MockWebSocket()
    await manager.connect(ws)  # type: ignore
    manager.subscribe(ws, "*", "BTCUSDT", "*")  # type: ignore

    for exchange, symbol in (
        ("okx", "BTC-USDT-SWAP"),
        ("gate", "BTC_USDT"),
        ("mexc", "BTC_USDT"),
        ("kraken", "PF_XBTUSD"),
        ("bitfinex", "tBTCF0:USTF0"),
    ):
        await manager.broadcast(
            {
                "topic": f"market.mixed.{exchange}.perp",
                "key": symbol,
                "value": {
                    "exchange": exchange,
                    "symbol": symbol,
                    "data_type": "open_interest",
                },
            }
        )

    assert len(ws.sent_bytes) == 5


def test_websocket_endpoint() -> None:
    app_mini = FastAPI()
    manager = WebSocketManager()
    
    # 임시로 state.ws_manager에 바인딩
    state.ws_manager = manager
    
    app_mini.include_router(router)
    
    client = TestClient(app_mini)
    with client.websocket_connect("/api/ws/market?exchange=binance&symbol=BTCUSDT&data_type=trade") as websocket:
        assert len(manager.active_connections) == 1
        ws = list(manager.active_connections)[0]
        assert ("binance", "BTCUSDT", "trade") in manager.subscriptions[ws]
        
        # 백엔드 브로드캐스트 모사
        payload = {
            "topic": "market.mixed.binance.perp",
            "key": "BTCUSDT",
            "value": {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "data_type": "trade",
                "price": "67000",
            }
        }
        
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(manager.broadcast(payload))
        finally:
            loop.close()
        
        data = websocket.receive_bytes()
        decoded = orjson.loads(data)
        assert decoded["value"]["price"] == "67000"


def test_signal_websocket_endpoint_subscribes_to_signal_events() -> None:
    app_mini = FastAPI()
    manager = WebSocketManager()
    state.ws_manager = manager

    app_mini.include_router(router)

    client = TestClient(app_mini)
    with client.websocket_connect("/api/ws/signals?exchange=binance&symbol=BTCUSDT") as websocket:
        assert len(manager.active_connections) == 1
        ws = list(manager.active_connections)[0]
        assert ("binance", "BTCUSDT", "signal") in manager.subscriptions[ws]

        payload = {
            "topic": "strategy.signals",
            "key": "S-BINANCE-PERP-TEST",
            "value": {
                "exchange": "BINANCE",
                "symbol": "BTCUSDT",
                "data_type": "signal",
                "signal_id": "S-BINANCE-PERP-TEST",
            },
        }

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(manager.broadcast(payload))
        finally:
            loop.close()

        data = websocket.receive_bytes()
        decoded = orjson.loads(data)
        assert decoded["value"]["signal_id"] == "S-BINANCE-PERP-TEST"
