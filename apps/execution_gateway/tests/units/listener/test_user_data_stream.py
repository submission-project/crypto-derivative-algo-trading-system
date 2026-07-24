"""
UserDataStreamListener 단위 테스트 (v2).

검증 항목:
- ORDER_TRADE_UPDATE 콜백에 정규화된 주문 이벤트 전달
- ACCOUNT_UPDATE 콜백에 정규화된 position snapshot list 전달
- 미알려진 이벤트 타입 처리 (에러 없이 무시)
- listenKeyExpired 시 ws.close() 호출
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

from execution_gateway.listeners.binance.binance_user_data_stream import (
    BinanceUserDataStreamListener,
)
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.order import ConditionalStatus, OrderStatus
from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.position import PositionSide, PositionStatus
from schemas.position_update_event import NormalizedPositionSnapshot
from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter


@pytest.fixture
def mock_adapter():
    adapter = MagicMock(spec=BinanceRestAdapter)
    adapter.create_listen_key = AsyncMock(return_value="test_listen_key")
    adapter.keepalive_listen_key = AsyncMock()
    adapter.close_listen_key = AsyncMock()
    return adapter


@pytest.fixture
def listener(mock_adapter):
    return BinanceUserDataStreamListener(
        rest_adapter=mock_adapter,
        ws_base_url="wss://stream.binancefuture.com/private",  # /private 포함
    )


@pytest.mark.asyncio
async def test_order_update_delivers_normalized_event(listener):
    """ORDER_TRADE_UPDATE 콜백에 정규화된 주문 이벤트가 전달되는지 확인."""
    received = None

    @listener.on_order_update
    async def handler(event: NormalizedOrderUpdateEvent):
        nonlocal received
        received = event

    test_event = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1568879465651,
        "T": 1568879465650,
        "o": {
            "s": "BTCUSDT",
            "c": "TEST_CLIENT_ID",
            "S": "BUY",
            "X": "FILLED",
            "x": "TRADE",
            "i": 88888,
            "l": "0.001",
            "L": "60000",
            "t": 55555,
            "z": "0.001",
            "ap": "60000",
        },
    }

    await listener._dispatch(test_event)

    assert received is not None
    assert received.symbol == "BTCUSDT"
    assert received.client_order_id == "TEST_CLIENT_ID"
    assert received.exchange_order_id == "88888"
    assert received.target_status == OrderStatus.FILLED
    assert received.exchange_status == "FILLED"
    assert received.execution_type == "TRADE"
    assert received.last_fill_quantity == "0.001"
    assert received.trade_id == "55555"
    assert received.event_time == 1568879465651
    assert received.transaction_time == 1568879465650
    assert received.raw == test_event


@pytest.mark.asyncio
async def test_account_update_delivers_position_snapshots(listener):
    """ACCOUNT_UPDATE 콜백에 position snapshot list가 전달되는지 확인."""
    received = None

    @listener.on_position_update
    async def handler(event: list[NormalizedPositionSnapshot]):
        nonlocal received
        received = event

    test_event = {
        "e": "ACCOUNT_UPDATE",
        "E": 1591888236894,
        "T": 1591888236895,
        "a": {
            "m": "ORDER",
            "B": [],
            "P": [
                {
                    "s": "BTCUSDT",
                    "pa": "0.001",
                    "ep": "60000",
                    "bep": "60000",
                    "up": "1.23",
                    "mt": "cross",
                    "iw": "0",
                    "ps": "BOTH",
                }
            ],
        },
    }

    await listener._dispatch(test_event)

    assert received is not None
    assert len(received) == 1
    snapshot = received[0]
    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.position_side == PositionSide.BOTH
    assert snapshot.status == PositionStatus.OPEN
    assert snapshot.position_amt == "0.001"
    assert snapshot.event_time == 1591888236894
    assert snapshot.transaction_time == 1591888236895
    assert snapshot.raw == test_event["a"]["P"][0]


@pytest.mark.asyncio
async def test_algo_update_delivers_normalized_conditional_event(listener):
    """ALGO_UPDATE 콜백에는 거래소 공통 조건부 주문 이벤트가 전달된다."""
    received = None

    async def handler(event: NormalizedConditionalOrderEvent):
        nonlocal received
        received = event

    listener.on_algo_update(handler)

    test_event = {
        "e": "ALGO_UPDATE",
        "E": 1_700_000_000_000,
        "T": 1_700_000_000_001,
        "o": {
            "s": "BTCUSDT",
            "caid": "CLIENT-ALGO-001",
            "aid": 12345,
            "X": "NEW",
        },
    }

    await listener._dispatch(test_event)

    assert received is not None
    assert received.symbol == "BTCUSDT"
    assert received.client_conditional_id == "CLIENT-ALGO-001"
    assert received.exchange_conditional_id == "12345"
    assert received.target_status == ConditionalStatus.NEW
    assert received.raw == test_event


@pytest.mark.asyncio
async def test_unknown_event_ignored(listener):
    """알 수 없는 이벤트는 조용히 무시되는지 확인."""
    @listener.on_order_update
    async def handler(event: NormalizedOrderUpdateEvent):
        pytest.fail("알 수 없는 이벤트로 order 콜백이 호출됨")

    await listener._dispatch({"e": "MARGIN_CALL", "E": 123})


@pytest.mark.asyncio
async def test_listen_key_expired_closes_ws(listener):
    """listenKeyExpired 시 ws.close()가 호출되는지 확인."""
    mock_ws = AsyncMock()
    listener._ws = mock_ws

    await listener._dispatch({"e": "listenKeyExpired"})

    mock_ws.close.assert_called_once()
