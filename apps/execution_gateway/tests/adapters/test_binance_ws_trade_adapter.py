"""
BinanceWsTradeAdapter 단위 테스트.

검증 항목:
- Ed25519 / HMAC 둘 다 없으면 ValueError
- _request: 정상 응답 → result 반환
- _request: 에러 응답 → WsTradeError
- _request: 타임아웃 → asyncio.TimeoutError
- _handle_message: id 없는 메시지 무시
"""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import os

from execution_gateway.adapters.binance.binance_ws_trade_adapter import (
    BinanceWsTradeAdapter,
    WsTradeError,
)

from execution_gateway.config import settings

from common.logging import setup_logger

logger = setup_logger(__name__)



def make_adapter(
    private_key_pem: str = None,
    api_secret: str = None,
):
    return BinanceWsTradeAdapter(
        ws_trade_url="wss://testnet.binancefuture.com/ws-fapi/v1",
        api_key="test_api_key",
        private_key_pem=private_key_pem,
        api_secret=api_secret or "test_secret",
    )


# ── 초기화 ──

def test_init_raises_without_credentials():
    with pytest.raises(ValueError, match="Either private_key_pem"):
        BinanceWsTradeAdapter(
            ws_trade_url="wss://test",
            api_key="key",
        )


def test_init_ok_with_secret():
    adapter = make_adapter(api_secret="my_secret")
    assert adapter._api_key == "test_api_key"


# ── 응답 처리 ──

@pytest.mark.asyncio
async def test_handle_message_success():
    """200 응답 → Future에 result 설정."""
    adapter = make_adapter()
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    adapter._pending["req-123"] = fut

    await adapter._handle_message({
        "id": "req-123",
        "status": 200,
        "result": {"orderId": 9999, "status": "NEW"},
    })

    assert fut.done()
    assert fut.result()["orderId"] == 9999


@pytest.mark.asyncio
async def test_handle_message_error(caplog):
    """4xx 응답 → Future에 WsTradeError 설정, WARNING 로그 (ERROR 아님 → Discord 알람 미발송)."""
    import logging
    adapter = make_adapter()
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    adapter._pending["req-456"] = fut

    with caplog.at_level(logging.WARNING, logger="execution_gateway.adapters.binance_ws_trade_adapter"):
        await adapter._handle_message({
            "id": "req-456",
            "status": 400,
            "error": {"code": -1102, "msg": "Mandatory parameter missing"},
        })

    assert fut.done()
    with pytest.raises(WsTradeError) as exc_info:
        fut.result()
    assert exc_info.value.code == -1102

    # WARNING으로 로깅되는지 확인 (알람 발송 수준인 ERROR가 아님을 보장)
    error_records = [r for r in caplog.records if r.levelname == "ERROR"]
    assert len(error_records) == 0, f"ERROR 레벨 로그 발생 — Discord 알람 트리거 가능: {error_records}"


@pytest.mark.asyncio
async def test_handle_message_no_id_ignored():
    """id 없는 메시지는 조용히 무시."""
    adapter = make_adapter()
    # 아무 Future도 없고 예외도 발생하지 않아야 함
    await adapter._handle_message({"e": "SOME_SERVER_PUSH", "data": {}})


@pytest.mark.asyncio
async def test_handle_message_unknown_id_ignored():
    """대응하는 Future 없는 id는 무시."""
    adapter = make_adapter()
    await adapter._handle_message({"id": "nonexistent", "status": 200, "result": {}})


# ── WsTradeError ──

def test_ws_trade_error_str():
    err = WsTradeError(-1102, "Mandatory parameter missing")
    assert "-1102" in str(err)
    assert "Mandatory parameter missing" in str(err)


def load_pem():
    pem_path = settings.active_ed25519_key_pem
    if not pem_path or not os.path.exists(pem_path):
        pytest.skip(f"PEM 파일이 존재하지 않습니다: {pem_path}")
    with open(pem_path, "r") as f:
        return f.read()


@pytest.mark.asyncio
async def test_ws_trade_real_communication():
    """실제 WebSocket Trade API로 주문/취소를 수행."""
    pem_data = load_pem()
    ws_adapter = BinanceWsTradeAdapter(
        ws_trade_url=settings.binance_testnet_ws_trade_url,
        api_key=settings.active_api_key,
        private_key_pem=pem_data
    )
    await ws_adapter.connect()
    
    try:
        logger.info("WS Trade: 주문 생성 테스트...")
        
        # 1. 주문 생성
        order = await ws_adapter.place_order({
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "30000",
            "quantity": "0.01"
        })
        order_id = order["orderId"]
        logger.info(f"WS Trade: 주문 생성 성공 (ID: {order_id})")

        # 2. 주문 취소
        logger.info("WS Trade: 주문 취소 테스트...")
        cancel = await ws_adapter.cancel_order({
            "symbol": "BTCUSDT",
            "orderId": order_id
        })
        logger.info(f"WS Trade: 주문 취소 결과 = {cancel}")
        assert cancel.get("status") == "CANCELED"
        logger.info("WS Trade: 주문 취소 성공")
    finally:
        await ws_adapter.close()