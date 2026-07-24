from __future__ import annotations

import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends

from api_server.runtime import state
from api_server.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ws", tags=["WebSocket"])

def get_websocket_manager() -> WebSocketManager:
    if state.ws_manager is None:
        raise RuntimeError("WebSocketManager is not initialized")
    return state.ws_manager

@router.websocket("/market")
async def websocket_market(
    websocket: WebSocket,
    exchange: str | None = None,
    symbol: str | None = None,
    data_type: str | None = None,
    manager: WebSocketManager = Depends(get_websocket_manager),
) -> None:
    """
    실시간 시장 데이터 스트리밍을 위한 WebSocket 엔드포인트
    Query Parameter 필터링 지원:
      - exchange: 거래소 (예: binance, bybit, okx)
      - symbol: 심볼 (예: BTCUSDT)
      - data_type: 데이터 종류 (예: trade, orderbook, open_interest)
    """
    await manager.connect(websocket)
    
    # 쿼리 파라미터가 있는 경우 초기 구독 추가
    if exchange or symbol or data_type:
        manager.subscribe(
            websocket,
            exchange=exchange or "*",
            symbol=symbol or "*",
            data_type=data_type or "*",
        )

    try:
        while True:
            # 클라이언트로부터 메시지(구독 추가/취소 명령)를 대기
            data = await websocket.receive_json()
            if isinstance(data, dict):
                action = data.get("action")
                ex = data.get("exchange", "*")
                sym = data.get("symbol", "*")
                dt = data.get("data_type", "*")
                
                if action == "subscribe":
                    manager.subscribe(websocket, ex, sym, dt)
                elif action == "unsubscribe":
                    manager.unsubscribe(websocket, ex, sym, dt)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"Error handling websocket communication: {e}")
    finally:
        manager.disconnect(websocket)


@router.websocket("/signals")
async def websocket_signals(
    websocket: WebSocket,
    exchange: str = "*",
    symbol: str = "*",
    manager: WebSocketManager = Depends(get_websocket_manager),
):
    """실시간 전략 시그널 스트리밍 WebSocket 엔드포인트"""
    await manager.connect(websocket)
    manager.subscribe(websocket, exchange, symbol, "signal")
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "subscribe":
                manager.subscribe(
                    websocket,
                    data.get("exchange", exchange),
                    data.get("symbol", symbol),
                    "signal",
                )
            elif action == "unsubscribe":
                manager.unsubscribe(
                    websocket,
                    data.get("exchange", exchange),
                    data.get("symbol", symbol),
                    "signal",
                )
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"Error handling signal websocket communication: {e}")
    finally:
        manager.disconnect(websocket)
