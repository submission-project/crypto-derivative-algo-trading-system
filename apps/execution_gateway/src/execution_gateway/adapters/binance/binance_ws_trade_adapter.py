"""
Binance WebSocket Trade API 어댑터 (Phase 3b).

초저지연 단건 주문을 위한 Persistent WebSocket 연결 관리.

특징:
  - REST batchOrders 대신 초저지연이 필요한 단건 주문에 사용
  - Ed25519 전용 (self-generated API key)
  - Persistent connection — 연결 유지 + 헬스체크 (ping/pong)
  - 미래 응답을 asyncio.Future로 매핑 (request_id 기반)
  - 오류 응답 / 타임아웃 처리
  - 재연결 시 inflight 요청에 TimeoutError 통보

Binance WS Trade API 문서:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info

메시지 구조:
  요청: {"id": "<uuid>", "method": "order.place", "params": {...signed...}}
  응답: {"id": "<uuid>", "status": 200, "result": {...}} or {"status": 4xx, "error": {...}}
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed
try:
    from websockets.protocol import State as WsState
except ImportError:
    WsState = None  # type: ignore

from common.logging import setup_logger
from .auth.binance_ws_auth import sign_ws_ed25519, sign_ws_hmac

logger = setup_logger(__name__)

# 요청 응답 대기 타임아웃 (ms → s)
_REQUEST_TIMEOUT_SEC = 5.0

# 재연결 백오프
_RECONNECT_INITIAL_DELAY = 1.0
_RECONNECT_MAX_DELAY = 30.0
_RECONNECT_MULTIPLIER = 2.0

# Binance WS keep-alive ping (매 3분 — 서버가 10분 무응답 시 연결 끊음)
_PING_INTERVAL_SEC = 180


class WsTradeError(Exception):
    """WebSocket Trade API 에러 응답."""
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(f"WsTradeError(code={code}, msg={msg})")


class BinanceWsTradeAdapter:
    """
    Binance Futures WebSocket Trade API Adapter.
    """

    def __init__(
        self,
        ws_trade_url: str,
        api_key: str,
        private_key_pem: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        if not private_key_pem and not api_secret:
            raise ValueError("Either private_key_pem (Ed25519) or api_secret (HMAC) must be provided")

        self._url = ws_trade_url
        self._api_key = api_key
        self._private_key_pem = private_key_pem
        self._api_secret = api_secret

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False

        # request_id → Future 매핑
        self._pending: dict[str, asyncio.Future] = {}

        # 수신 루프 태스크
        self._receive_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None

    # ──────────────────────── Public API ────────────────────────

    async def connect(self) -> None:
        """WebSocket 연결 수립 및 수신 루프 시작."""
        if self._running:
            return

        self._running = True
        await self._do_connect()

    async def close(self) -> None:
        """연결 종료."""
        self._running = False
        if self._ping_task:
            self._ping_task.cancel()
        if self._receive_task:
            self._receive_task.cancel()
        if self._ws:
            await self._ws.close()
        # 남아있는 inflight 요청에 CancelledError 통보
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()

    async def place_order(self, params: dict) -> dict:
        """
        WS Trade API로 단건 지정가/시장가 주문.

        Args:
            params: Binance order params (symbol, side, type, etc.)
                    newClientOrderId가 없으면 자동 주입됨

        Returns:
            Binance 응답 result dict
        """
        return await self._request("order.place", params)

    async def cancel_order(self, params: dict) -> dict:
        """
        WS Trade API로 단건 주문 취소.

        Args:
            params: {"symbol": ..., "origClientOrderId": ...} 또는 {"symbol":..., "orderId":...}
        """
        return await self._request("order.cancel", params)

    async def modify_order(self, params: dict) -> dict:
        """WS Trade API로 주문 수정."""
        return await self._request("order.modify", params)

    async def get_order(self, params: dict) -> dict:
        """WS Trade API로 주문 조회."""
        return await self._request("order.status", params)

    async def get_account(self) -> dict:
        """WS Trade API로 계정 정보 조회."""
        return await self._request("account.status", {})

    # ──────────────────────── Connection ────────────────────────

    async def _do_connect(self) -> None:
        """실제 WebSocket 연결 및 태스크 시작."""
        self._ws = await websockets.connect(
            self._url,
            ping_interval=None,  # 직접 ping 관리
            ping_timeout=None,
        )
        logger.info(f"WS Trade 연결 완료: {self._url}")

        self._receive_task = asyncio.create_task(self._receive_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _reconnect(self) -> None:
        """재연결 (inflight 요청에 에러 통보 후 재연결)."""
        # 현재 inflight 요청에 연결 끊김 알림
        err = ConnectionError("WebSocket 연결이 끊어졌습니다. 재연결 중...")
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

        delay = _RECONNECT_INITIAL_DELAY
        while self._running:
            try:
                logger.info(f"WS Trade 재연결 시도 중... ({delay:.0f}초 대기 후)")
                await asyncio.sleep(delay)
                await self._do_connect()
                logger.info("WS Trade 재연결 성공")
                return
            except Exception as e:
                logger.error(f"WS Trade 재연결 실패: {e}")
                delay = min(delay * _RECONNECT_MULTIPLIER, _RECONNECT_MAX_DELAY)

    # ──────────────────────── Request / Response ────────────────────────

    def _sign(self, params: dict) -> dict:
        """서명 방식 선택: Ed25519 우선, 없으면 HMAC."""
        if self._private_key_pem:
            return sign_ws_ed25519(params, self._api_key, self._private_key_pem)
        return sign_ws_hmac(params, self._api_key, self._api_secret)  # type: ignore[arg-type]

    async def _request(self, method: str, params: dict) -> dict:
        """
        서명된 요청을 WS로 전송하고 응답을 Future로 대기.

        Raises:
            WsTradeError: Binance 에러 응답
            asyncio.TimeoutError: 응답 타임아웃
            ConnectionError: 연결 끊김
        """
        is_open = False
        if self._ws:
            if WsState:
                is_open = self._ws.protocol.state is WsState.OPEN
            else:
                is_open = getattr(self._ws, "open", False)

        if not is_open:
            raise ConnectionError("WS Trade 연결이 없습니다. connect()를 먼저 호출하세요.")

        request_id = str(uuid.uuid4())
        signed_params = self._sign(dict(params))

        message = json.dumps({
            "id": request_id,
            "method": method,
            "params": signed_params,
        })

        loop = asyncio.get_event_loop()
        fut: asyncio.Future[dict] = loop.create_future()
        self._pending[request_id] = fut

        try:
            await self._ws.send(message)
            logger.debug(f"WS 요청 전송: method={method}, id={request_id}")

            result = await asyncio.wait_for(fut, timeout=_REQUEST_TIMEOUT_SEC)
            return result

        except asyncio.TimeoutError:
            logger.error(f"WS 요청 타임아웃: method={method}, id={request_id}")
            self._pending.pop(request_id, None)
            raise

        except Exception:
            self._pending.pop(request_id, None)
            raise

    # ──────────────────────── Loops ────────────────────────

    async def _receive_loop(self) -> None:
        """수신 루프: 응답 메시지를 파싱하여 대기 중인 Future에 설정."""
        try:
            async for raw in self._ws:  # type: ignore[union-attr]
                try:
                    msg = json.loads(raw)
                    await self._handle_message(msg)
                except json.JSONDecodeError:
                    logger.warning(f"JSON 파싱 실패: {raw[:100]}")
        except ConnectionClosed as e:
            logger.warning(f"WS Trade 연결 종료: code={e.code}")
        except Exception as e:
            logger.error(f"WS Trade 수신 루프 오류: {e}", exc_info=True)
        finally:
            if self._running:
                asyncio.create_task(self._reconnect())

    async def _ping_loop(self) -> None:
        """3분마다 ping 전송 (서버 연결 유지)."""
        while self._running:
            await asyncio.sleep(_PING_INTERVAL_SEC)
            
            is_open = False
            if self._ws:
                if WsState:
                    is_open = self._ws.protocol.state is WsState.OPEN
                else:
                    is_open = getattr(self._ws, "open", False)

            if is_open:
                try:
                    await self._ws.ping()
                    logger.debug("WS Trade ping 전송")
                except Exception as e:
                    logger.warning(f"WS Trade ping 실패: {e}")

    async def _handle_message(self, msg: dict) -> None:
        """
        수신 메시지 처리.

        성공: {"id": ..., "status": 200, "result": {...}}
        실패: {"id": ..., "status": 4xx, "error": {"code": ..., "msg": ...}}
        """
        request_id = msg.get("id")

        if not request_id:
            # id 없는 메시지 (서버 push 등) — 무시
            logger.debug(f"id 없는 WS 메시지: {str(msg)[:100]}")
            return

        fut = self._pending.pop(request_id, None)
        if not fut:
            logger.warning(f"대응하는 요청 없음: id={request_id}")
            return

        if fut.done():
            return

        status = msg.get("status", 0)

        if status == 200:
            fut.set_result(msg.get("result", {}))
        else:
            error = msg.get("error", {})
            code = error.get("code", status)
            msg_str = error.get("msg", f"HTTP {status}")
            fut.set_exception(WsTradeError(code, msg_str))
            # 바이낙스 API 에러 응답(-1102, -2010 등)은 예상된 비즈니스 오류이므로 warning
            # 시스템/에이전트 수준의 오류는 호출 측 (submit_order_ws)에서 처리
            logger.warning(f"WS Trade API 오류 응답: code={code}, msg={msg_str}")
