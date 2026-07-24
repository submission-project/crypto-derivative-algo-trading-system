from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import contextlib

import pytest

from execution_gateway.config import settings as gw_settings
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from execution_gateway.listeners.binance.binance_user_data_stream import (
    BinanceUserDataStreamListener,
)
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
)
from schemas.position import PositionSide

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState
from execution_gateway.adapters.binance.dto.resp.AlgoOrderResponseDto import AlgoOrderRespDto


pytestmark = pytest.mark.integration


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _require_real_binance_tests_enabled() -> None:
    if os.getenv("RUN_BINANCE_REAL_TESTS") != "1":
        pytest.skip(
            "Real Binance testnet tests are disabled. "
            "Set RUN_BINANCE_REAL_TESTS=1 to run."
        )


def _load_pem() -> str:
    pem_path = getattr(gw_settings, "active_ed25519_key_pem", None)

    if not pem_path:
        pytest.skip("settings.active_ed25519_key_pem is not configured")

    path = Path(str(pem_path))

    if not path.exists():
        pytest.skip(f"ED25519 private key file does not exist: {path}")

    return path.read_text()


def _make_adapter() -> BinanceRestAdapter:
    pem_data = _load_pem()

    return BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem_data,
    )


def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

async def _get_reference_price(
    adapter: BinanceRestAdapter,
    symbol: str,
) -> Decimal:
    ticker = await adapter.get_symbol_price_ticker(symbol)
    return Decimal(str(ticker.price))

def _make_test_stop_market_order(
    *,
    symbol: str,
    quantity: str,
    trigger_price: str,
) -> Order:
    now = _now_ms()

    order_id = f"TKALGO{int(time.time() * 1000)}"

    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity=quantity,
        price=None,
        trigger_price=trigger_price,
        reduce_only=False,
        close_position=False,
        client_conditional_id=order_id,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.SUBMITTED,
        created_ts=now,
        updated_ts=now,
        version=1,
    )


async def _wait_for_algo_event(
    queue: asyncio.Queue[NormalizedConditionalOrderEvent],
    *,
    client_algo_id: str,
    expected_statuses: set[str],
    timeout_sec: float = 15.0,
) -> NormalizedConditionalOrderEvent:
    deadline = asyncio.get_running_loop().time() + timeout_sec
    seen: list[dict] = []

    while True:
        remaining = deadline - asyncio.get_running_loop().time()

        if remaining <= 0:
            raise AssertionError(
                {
                    "message": "Timed out waiting for ALGO_UPDATE",
                    "client_algo_id": client_algo_id,
                    "expected_statuses": sorted(expected_statuses),
                    "seen": seen,
                }
            )

        event = await asyncio.wait_for(queue.get(), timeout=remaining)

        seen.append(event.raw)

        if event.client_conditional_id != client_algo_id:
            continue

        if event.exchange_conditional_status in expected_statuses:
            return event

@pytest.mark.asyncio
async def test_real_user_data_stream_receives_algo_update_create_and_cancel() -> None:
    """
    실제 Binance Futures Testnet User Data Stream ALGO_UPDATE 테스트.

    검증:
      1. listener.start()로 실제 user stream 연결
      2. 실제 /fapi/v1/algoOrder 생성
      3. ALGO_UPDATE NEW 수신
      4. 실제 /fapi/v1/algoOrder 취소
      5. ALGO_UPDATE CANCELED 수신
      6. listener가 raw event를 NormalizedConditionalOrderEvent로 변환해서 전달
    """
    # _require_real_binance_tests_enabled()

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    adapter = _make_adapter()
    router = BinanceOrderRouter(adapter)

    # User Data Stream은 Testnet에서 wss://stream.binancefuture.com 를 사용한다.
    ws_base_url = gw_settings.binance_testnet_ws_url

    assert ws_base_url

    listener = BinanceUserDataStreamListener(
        rest_adapter=adapter,
        ws_base_url=ws_base_url,
    )

    queue: asyncio.Queue[NormalizedConditionalOrderEvent] = asyncio.Queue()

    async def on_algo_update(event: NormalizedConditionalOrderEvent) -> None:
        await queue.put(event)

    listener.on_algo_update(on_algo_update)

    listener_task: asyncio.Task | None = None
    order: Order | None = None
    exchange_algo_id: str | None = None

    try:
        listener_task = asyncio.create_task(
            listener.start(),
            name="real-test-user-data-stream-listener",
        )

        # listener가 listenKey 생성 + websocket 연결할 시간을 조금 준다.
        await asyncio.sleep(2.0)

        ref_price = await _get_reference_price(adapter, symbol)

        # BUY STOP_MARKET: 현재가 2배를 trigger로 두면 즉시 발동 가능성이 낮다.
        trigger_price = _round_down_to_step(
            ref_price * Decimal("2"),
            Decimal("0.1"),
        )

        order = _make_test_stop_market_order(
            symbol=symbol,
            quantity=quantity,
            trigger_price=str(trigger_price),
        )


        resp = await router.place_conditional_order(order)

        assert isinstance(resp, AlgoOrderRespDto)

        exchange_algo_id_raw = resp.algoId
        if exchange_algo_id_raw is not None:
            exchange_algo_id = str(exchange_algo_id_raw)

        # 생성 이벤트 NEW 대기
        created_event = await _wait_for_algo_event(
            queue,
            # pyrefly: ignore [bad-argument-type]
            client_algo_id=order.client_conditional_id,
            expected_statuses={BinanceConditionalOrderState.new, },
            timeout_sec=20.0,
        )

        assert created_event.client_conditional_id == order.client_conditional_id
        assert created_event.symbol == symbol
        assert created_event.target_status in {
            ConditionalStatus.NEW,
            ConditionalStatus.ACTIVE,
        }

        # 실제 취소
        await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=order.client_conditional_id,
            algo_id=exchange_algo_id,
        )

        canceled_event = await _wait_for_algo_event(
            queue,
            # pyrefly: ignore [bad-argument-type]
            client_algo_id=order.client_conditional_id,
            expected_statuses={BinanceConditionalOrderState.canceled},
            timeout_sec=20.0,
        )

        assert canceled_event.client_conditional_id == order.client_conditional_id
        assert canceled_event.target_status in {
            ConditionalStatus.CANCELLED,
        }

    finally:
        # 테스트 실패 시에도 실제 테스트넷 주문 정리
        if order is not None:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=order.client_conditional_id,
                    algo_id=exchange_algo_id,
                )
            except Exception:
                pass

        try:
            await listener.stop()
        except Exception:
            pass

        if listener_task is not None:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task

        await adapter.close()
