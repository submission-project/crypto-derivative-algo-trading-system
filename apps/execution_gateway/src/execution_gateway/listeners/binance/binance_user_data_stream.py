"""
Binance User Data Stream WebSocket 리스너.

개선 사항 (v2):
  1. asyncio.gather → asyncio.wait(FIRST_COMPLETED) 구조로 교체
     receive_loop 종료 시 keepalive_loop도 즉시 취소, 재연결로 복귀
  2. 콜백 직접 await 대신 asyncio.Queue 경유 — receive_loop가 느린 콜백에 블록되지 않음
  3. ORDER_TRADE_UPDATE 콜백에 전체 이벤트(event_time, transaction_time, order, raw) 전달
  4. ws_base_url에 /private 미포함 시 경고 로그
  5. listenKey를 keepalive PUT 요청에 명시적으로 포함

Lifecycle:
  1. REST POST /fapi/v1/listenKey → listenKey 발급 (유효 60분)
  2. wss://fstream.binance.com/private/ws/{listenKey} 연결
  3. 30분마다 PUT /fapi/v1/listenKey keepalive
  4. ORDER_TRADE_UPDATE / ACCOUNT_UPDATE 이벤트 → Queue → worker
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any, Callable, Coroutine, Optional, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed

from common.logging import setup_logger

from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter

from execution_gateway.adapters.binance.mapper.binance_algo_event_mapper import (
    normalize_binance_algo_update,
)
from execution_gateway.adapters.binance.mapper.binance_order_event_mapper import (
    normalize_binance_order_update,
)
from execution_gateway.adapters.binance.mapper.binance_position_event_mapper import (
    normalize_binance_account_update_positions,
)

from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.position_update_event import NormalizedPositionSnapshot
from schemas.conditional_order_event import NormalizedConditionalOrderEvent

from schemas.market import Exchange, MarketType

from execution_gateway.listeners.user_data_stream import UserDataStreamListener

logger = setup_logger(__name__)

# listenKey keepalive 간격 (권장: 30분, 만료: 60분)
_KEEPALIVE_INTERVAL_SEC = 30 * 60

# 재연결 백오프
_RECONNECT_INITIAL_DELAY = 1.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_MULTIPLIER = 2.0

# 이벤트 큐 최대 크기
# 가득 차면 receive loop가 await put()에서 대기하여 이벤트 유실을 막는다. => backpressure 정책
_EVENT_QUEUE_MAXSIZE = 10_000

# 콜백 타입: async def handler(event_envelope: dict) -> None
EventCallback = Callable[[dict], Coroutine[Any, Any, None]]

OrderUpdateCallback = Callable[
    [NormalizedOrderUpdateEvent],
    Awaitable[None],
]

PositionUpdateCallback = Callable[
    [list[NormalizedPositionSnapshot]],
    Awaitable[None],
]

ConditionalUpdateCallback = Callable[
    [NormalizedConditionalOrderEvent],
    Awaitable[None],
]

class BinanceUserDataStreamListener(UserDataStreamListener):
    """
    Binance User Data Stream WebSocket 리스너 (이벤트 큐 기반).

    사용 예:
        listener = BinanceUserDataStreamListener(adapter, ws_base_url)
        listener.on_order_update(handle_normalized_order_update)
        await listener.start()
    """

    exchange = Exchange.BINANCE
    market_type = MarketType.PERP

    def __init__(
        self,
        *,
        rest_adapter: BinanceRestAdapter,
        ws_base_url: str,
    ):
        self._listen_key_closed = False
        self._close_lock = asyncio.Lock()
        self._adapter = rest_adapter
        self._ws_base_url = ws_base_url.rstrip("/")

        self._listen_key: Optional[str] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        self._running = False
        self._stop_event = asyncio.Event()

        # 이벤트 큐 (receive_loop → worker_loop)
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=_EVENT_QUEUE_MAXSIZE
        )

        self._worker_task: Optional[asyncio.Task[None]] = None

        # 이벤트별 콜백 목록
        self._order_callbacks: list[OrderUpdateCallback] = []
        # self._account_callbacks: list[AccountUpdateCallback] = []
        self._position_update_callbacks: list[PositionUpdateCallback] = []

        self._on_algo_update: Callable[
            [NormalizedConditionalOrderEvent],
            Awaitable[None],
        ] | None = None

        if not self._ws_base_url.endswith("/private"):
            raise ValueError(
                "USDⓈ-M User Data Stream은 .../private base URL을 사용합니다. "
                f"현재 값: {self._ws_base_url}"
            )

    # ────────────────────── Public API ──────────────────────

    def on_order_update(
        self,
        callback: OrderUpdateCallback,
    ) -> OrderUpdateCallback:
        self._order_callbacks.append(callback)
        return callback

    # def on_account_update(
    #     self,
    #     callback: AccountUpdateCallback,
    # ) -> AccountUpdateCallback:
    #     self._account_callbacks.append(callback)
    #     return callback

    def on_position_update(
        self,
        callback: PositionUpdateCallback,
    ) -> PositionUpdateCallback:
        self._position_update_callbacks.append(callback)
        return callback

    def on_algo_update(
        self,
        callback: ConditionalUpdateCallback
    ) -> None:
        self._on_algo_update = callback

    async def start(self) -> None:
        """
        listenKey 발급 후 WebSocket 연결 시작.

        이 메서드는 종료될 때까지 반환되지 않는다.
        보통 asyncio.create_task(listener.start())로 실행한다.
        """

        # 이미 실행 중이면 스킵
        if self._running:
            logger.warning("BinanceUserDataStreamListener가 이미 실행 중입니다.")
            return

        self._running = True
        self._stop_event.clear()

        reconnect_delay = _RECONNECT_INITIAL_DELAY

        self._worker_task = asyncio.create_task(
            self._event_worker(),
            name="user-data-stream-event-worker",
        )

        try:
            while self._running:
                try:
                    await self._connect_and_listen()
                    reconnect_delay = _RECONNECT_INITIAL_DELAY  # 성공 시 리셋

                except asyncio.CancelledError:
                    logger.info("User Data Stream listener task cancelled")
                    raise

                except ConnectionClosed as e:
                    if self._running:
                        logger.warning(
                            f"User Data Stream 연결 종료 "
                            f"(code={e.code}, reason={e.reason}). "
                            f"{reconnect_delay:.0f}초 후 재연결..."
                        )

                except Exception as e:
                    if self._running:
                        logger.error(
                            f"User Data Stream 오류: {e}. "
                            f"{reconnect_delay:.0f}초 후 재연결...",
                            exc_info=True,
                        )


                if self._running:
                    stopped = await self._sleep_or_stop(reconnect_delay)
                    if stopped:
                        break

                    reconnect_delay = min(
                        reconnect_delay * _RECONNECT_MULTIPLIER,
                        _RECONNECT_MAX_DELAY,
                    )
        finally:
            self._running = False
            self._stop_event.set()

            await self._close_ws()
            await self._close_listen_key_once()

            if self._worker_task:
                self._worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._worker_task
                self._worker_task = None

            logger.info("User Data Stream listener stopped")


    async def stop(self) -> None:
        """
        리스너 종료.

        start()가 reconnect sleep 중이어도 _stop_event로 깨운다.
        """
        self._running = False
        self._stop_event.set()

        await self._close_ws()
        
        await self._close_listen_key_once()

        logger.info("User Data Stream listener stopped")

    # ────────────────────── Connect & Listen ──────────────────────

    async def _connect_and_listen(self) -> None:
        """
        listenKey 발급 후 WebSocket 연결.
        receive loop와 keepalive loop 중 하나가 끝나면 재연결한다.
        """
        await self._close_listen_key_once()

        resp = await self._adapter.create_listen_key()
        self._listen_key = resp.listenKey
        self._listen_key_closed = False

        logger.info(f"listenKey 발급 완료: {self._listen_key[:16]}...")

        ws_url = f"{self._ws_base_url}/ws/{self._listen_key}"
        logger.info(f"User Data Stream 연결 중: {ws_url[:60]}...")

        async with websockets.connect(ws_url, ping_interval=None) as ws:
            self._ws = ws
            logger.info("User Data Stream 연결 완료")

            receive_task = asyncio.create_task(
                self._receive_loop(ws),
                name="user-data-stream-receive-loop",
            )
            keepalive_task = asyncio.create_task(
                self._keepalive_loop(),
                name="user-data-stream-keepalive-loop",
            )

            done, pending = await asyncio.wait(
                {receive_task, keepalive_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            self._ws = None

            for task in done:
                if task.cancelled():
                    continue

                exc = task.exception()
                if exc:
                    raise exc

    # ────────────────────── Internal loops ──────────────────────

    async def _receive_loop(
        self, ws: websockets.WebSocketClientProtocol
    ) -> None:
        """
        WebSocket 메시지 수신 후 queue에 넣는다.

        주문 이벤트는 유실되면 위험하므로 put_nowait/drop을 쓰지 않는다.
        queue가 가득 차면 receive loop에 backpressure가 걸린다.
        """
        async for raw in ws:
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"JSON 파싱 실패: {raw[:100]}")
                continue

            await self._event_queue.put(event)

    async def _keepalive_loop(self) -> None:
        """
        30분마다 listenKey keepalive.

        실패하면 예외를 올려서 _connect_and_listen()이 종료되고
        start()의 재연결 루프로 복귀한다.
        """
        while self._running:
            stopped = await self._sleep_or_stop(_KEEPALIVE_INTERVAL_SEC)
            if stopped:
                return

            if not self._listen_key:
                continue

            try:
                await self._adapter.keepalive_listen_key(self._listen_key)
                logger.debug("listenKey keepalive 완료")

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.warning(f"listenKey keepalive 실패: {e}")
                raise
            
    async def _event_worker(self) -> None:
        """
        큐에서 이벤트를 꺼내 콜백으로 디스패치.
        """
        while True:
            event = await self._event_queue.get()

            try:
                await self._dispatch(event)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.error(f"이벤트 처리 오류: {e}", exc_info=True)

            finally:
                self._event_queue.task_done()

    async def _dispatch(self, event: dict[str, Any]) -> None:
        """
        이벤트 타입별 콜백 호출.
        """
        event_type = event.get("e")

        if event_type == "ORDER_TRADE_UPDATE":
            # envelope = OrderUpdateEnvelope(
            #     event_time=event.get("E"),
            #     transaction_time=event.get("T"),
            #     order=event.get("o", {}),
            #     raw=event,
            # )
            normalized:NormalizedOrderUpdateEvent = normalize_binance_order_update(
                event,
                market_type=self.market_type,
            )

            if not self._order_callbacks:
                logger.debug("ORDER_TRADE_UPDATE 콜백이 등록되어 있지 않습니다.")
                return

            for cb in self._order_callbacks:
                try:
                    # await cb(envelope)
                    await cb(normalized)
                except Exception as e:
                    logger.error(
                        f"ORDER_TRADE_UPDATE 콜백 오류: {e}", exc_info=True
                    )
            return

        if event_type == "ACCOUNT_UPDATE":
            # envelope = AccountUpdateEnvelope(
            #     event_time=event.get("E"),
            #     raw=event,
            # )
            snapshots = normalize_binance_account_update_positions(event)
            if not snapshots:
                logger.debug("ACCOUNT_UPDATE position snapshot 없음")
                return

            # if not self._account_callbacks:
            #     logger.debug("ACCOUNT_UPDATE 콜백이 등록되어 있지 않습니다.")
            #     return

            # for cb in self._account_callbacks:
            #     try:
            #         # await cb(envelope)
            #         await cb(snapshots)
            #     except Exception as e:
            #         logger.error(
            #             f"ACCOUNT_UPDATE 콜백 오류: {e}",
            #             exc_info=True,
            #         )
            if not self._position_update_callbacks:
                logger.debug("ACCOUNT_UPDATE position 콜백이 등록되어 있지 않습니다.")
                return

            for cb in self._position_update_callbacks:
                try:
                    await cb(snapshots)
                except Exception as e:
                    logger.error(
                        f"ACCOUNT_UPDATE 콜백 오류: {e}",
                        exc_info=True,
                    )

            return

        if event_type == "ALGO_UPDATE":
            if not self._on_algo_update:
                logger.debug(f"ALGO_UPDATE callback 미등록: {event}")
                return

            # algo_payload = event.get("o", {})

            # if not isinstance(algo_payload, dict):
            #     logger.warning(f"잘못된 ALGO_UPDATE payload: {event}")
            #     return

            # envelope = AlgoUpdateEnvelope(
            #     event_time=int(event.get("E") or 0),
            #     transaction_time=(
            #         int(event["T"])
            #         if event.get("T") is not None
            #         else None
            #     ),
            #     algo=algo_payload,
            #     raw=event,)
            # await self._on_algo_update(envelope)

            normalized:NormalizedConditionalOrderEvent = normalize_binance_algo_update(
                raw_event=event,
                market_type=self.market_type,
            )
            await self._on_algo_update(normalized)
            return

        if event_type == "listenKeyExpired":
            logger.warning("listenKey 만료됨. 재연결 트리거...")
            await self._close_ws()

            return

        logger.debug(f"무시된 이벤트 타입: {event_type}")
        return

    # ────────────────────── Cleanup helpers ──────────────────────

    async def _sleep_or_stop(self, delay_sec: float) -> bool:
        """
        delay_sec 동안 sleep하되 stop_event가 set되면 즉시 깨어난다.

        Returns:
            True  = stop_event로 깨어남
            False = timeout까지 기다림
        """
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_sec)
            return True
        except asyncio.TimeoutError:
            return False

    async def _close_ws(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                logger.warning(f"WebSocket close 실패: {e}")
            finally:
                self._ws = None

    async def _close_current_listen_key(self) -> None:
        """현재 listenKey를 Binance에서 해제하고 내부 참조 초기화."""
        if not self._listen_key:
            return

        try:
            await self._adapter.close_listen_key(self._listen_key)
            logger.debug(f"listenKey 해제 완료: {self._listen_key[:16]}...")
        except Exception as e:
            logger.warning(f"listenKey 해제 실패: {e}")
        finally:
            self._listen_key = None

    async def _close_listen_key_once(self) -> None:
        async with self._close_lock:
            if self._listen_key_closed:
                return

            self._listen_key_closed = True

            await self._close_current_listen_key()
