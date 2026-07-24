from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
    PositionAction,
)
from schemas.order_update_event import NormalizedOrderUpdateEvent
from storage.redis_client import RedisStreamClient

from api_server.services.execution_log_service import ExecutionLogService

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def redis_stream_client() -> RedisStreamClient:
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=15,
    )

    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()
    yield client
    await client.client.flushdb()
    await client.close()


def make_order() -> Order:
    return Order(
        order_id="ORD-FILL-IT-001",
        source=OrderSource.MANUAL,
        signal_id=None,
        strategy_name=None,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        stop_price=None,
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=1_700_000_000_000,
        submitted_ts=1_700_000_000_100,
        filled_ts=None,
        updated_ts=1_700_000_000_200,
        status=OrderStatus.PARTIALLY_FILLED,
        version=3,
    )


def make_order_event() -> dict:
    return {
        "x": "TRADE",
        "l": "0.01",
        "t": 987654321,
        "L": "60000",
        "n": "0.01",
        "N": "USDT",
        "m": False,
        "i": 123456,
    }


def make_normalized_event(order_event: dict) -> NormalizedOrderUpdateEvent:
    trade_id = order_event.get("t")
    if trade_id in (None, "", 0, "0", -1, "-1"):
        trade_id = None

    return NormalizedOrderUpdateEvent(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id="ORD-FILL-IT-001",
        exchange_order_id=(
            str(order_event["i"]) if order_event.get("i") is not None else None
        ),
        execution_type=str(order_event.get("x")) if order_event.get("x") else None,
        last_fill_quantity=(
            str(order_event["l"]) if order_event.get("l") is not None else None
        ),
        last_fill_price=(
            str(order_event["L"]) if order_event.get("L") is not None else None
        ),
        trade_id=str(trade_id) if trade_id is not None else None,
        commission=(
            str(order_event["n"]) if order_event.get("n") is not None else None
        ),
        commission_asset=(
            str(order_event["N"]) if order_event.get("N") is not None else None
        ),
        is_maker=order_event.get("m") if isinstance(order_event.get("m"), bool) else None,
        event_time=1_700_000_000_300,
        transaction_time=1_700_000_000_350,
        raw={"o": order_event},
    )


@pytest.mark.asyncio
async def test_fill_dedup_allows_only_first_save(
    redis_stream_client: RedisStreamClient,
) -> None:
    order = make_order()
    order_event = make_order_event()
    event_data = make_normalized_event(order_event)

    exec_repo = MagicMock()
    exec_repo.save = AsyncMock()

    svc = ExecutionLogService(exec_repo=exec_repo, redis=redis_stream_client)

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    assert exec_repo.save.await_count == 1
