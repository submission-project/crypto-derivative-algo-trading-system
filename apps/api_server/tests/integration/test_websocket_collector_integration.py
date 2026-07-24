from __future__ import annotations

import asyncio
import contextlib
import pytest
import orjson

from cex_market_data_collector.module_loader import ensure_exchange_package_paths
from cex_market_data_collector.operational_adapters import build_operational_specs
from cex_market_data_collector.operational_runtime import run_operational_specs
from api_server.websocket_manager import WebSocketManager, WebSocketManagerSink

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("exchange", [
    "binance",
    "bybit",
    "okx",
    "bitget",
    "gate",
    "mexc",
    "kraken",
    "htx",
    "lbank",
    "bitfinex",
    "bingx",
    "kucoin",
])
@pytest.mark.asyncio
async def test_real_collector_to_websocket_integration(exchange: str) -> None:
    # 1. 수집기 검색 경로 설정
    ensure_exchange_package_paths()
    
    manager = WebSocketManager()
    
    # 비동기 환경에서 안전하게 수신 결과를 캡처하기 위한 Mock WebSocket
    class MockLiveWebSocket:
        def __init__(self) -> None:
            self.client = "test_live_client"
            self.received_messages: list[bytes] = []
            self.event = asyncio.Event()

        async def accept(self) -> None:
            pass

        async def send_bytes(self, data: bytes) -> None:
            self.received_messages.append(data)
            self.event.set()

    ws = MockLiveWebSocket()
    await manager.connect(ws)  # type: ignore
    manager.subscribe(ws, exchange, "BTCUSDT", "*")  # type: ignore
    
    # 2. 거래소에 대한 스펙 구축 및 Sink 설정
    specs = build_operational_specs((exchange,))
    
    # 만약 해당 거래소에 대한 ws 스펙이 정의되어 있지 않다면 스킵
    if not any(spec.websocket_specs for spec in specs):
        pytest.skip(f"No websocket spec defined for exchange: {exchange}")
        
    sink = WebSocketManagerSink(manager)
    
    # 수집기 기동 (동일한 이벤트 루프 내의 백그라운드 태스크)
    collector_task = asyncio.create_task(
        run_operational_specs(specs, sink=sink)
    )
    
    try:
        # 실시간 웹소켓으로부터 최초 1개 이상의 데이터 수집 대기 (최대 30초)
        await asyncio.wait_for(ws.event.wait(), timeout=30.0)
    except asyncio.TimeoutError:
        # 지오블락(Geo-blocking)이나 일시적인 네트워크 순단 등으로 인해 30초 동안 수집되지 않은 경우
        # 실패 처리하지 않고 skip하도록 함으로써 원활한 CI/CD 및 로컬 검증 보장
        pytest.skip(f"Timeout waiting for data from {exchange} (might be geoblocked or offline)")
    except Exception as e:
        pytest.skip(f"Connection failed for {exchange}: {e}")
    finally:
        # 수집기 태스크 종료 및 정리
        collector_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await collector_task
        
    # 3. 데이터 및 형식 검증
    assert len(ws.received_messages) > 0, f"{exchange} 실시간 데이터를 수신하지 못했습니다."
    
    decoded = orjson.loads(ws.received_messages[0])
    assert "topic" in decoded
    assert "value" in decoded
    
    val = decoded["value"]
    assert val["exchange"] == exchange
    assert val["symbol"].upper() in ("BTCUSDT", "BTC-USDT", "BTC-USDT-SWAP", "BTC_USDT")
    assert val["data_type"] in ("trade", "orderbook", "open_interest")
