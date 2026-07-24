from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

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

from api_server.services.execution_log_service import ExecutionLogService


def make_order() -> Order:
    return Order(
        order_id="ORD-FILL-001",
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
        client_order_id="ORD-FILL-001",
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
async def test_save_execution_log_saves_when_fill_is_new(monkeypatch) -> None:
    order = make_order()
    order_event = make_order_event()
    event_data = make_normalized_event(order_event)

    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=True)

    exec_repo = MagicMock()
    exec_repo.save = AsyncMock()

    mock_redis = MagicMock(client=redis_client)

    svc = ExecutionLogService(exec_repo=exec_repo, redis=mock_redis)

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    redis_client.set.assert_awaited_once()
    exec_repo.save.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_execution_log_skips_duplicate_fill(monkeypatch) -> None:
    order = make_order()
    order_event = make_order_event()
    event_data = make_normalized_event(order_event)

    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=False)

    exec_repo = MagicMock()
    exec_repo.save = AsyncMock()

    mock_redis = MagicMock(client=redis_client)

    svc = ExecutionLogService(exec_repo=exec_repo, redis=mock_redis)

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    redis_client.set.assert_awaited_once()
    exec_repo.save.assert_not_awaited()


@pytest.mark.asyncio
async def test_save_execution_log_deletes_dedup_key_when_questdb_save_fails(
    monkeypatch,
) -> None:
    order = make_order()
    order_event = make_order_event()
    event_data = make_normalized_event(order_event)

    redis_client = MagicMock()
    redis_client.set = AsyncMock(return_value=True)
    redis_client.delete = AsyncMock()

    exec_repo = MagicMock()
    exec_repo.save = AsyncMock(side_effect=RuntimeError("questdb down"))

    mock_redis = MagicMock(client=redis_client)

    svc = ExecutionLogService(exec_repo=exec_repo, redis=mock_redis)

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    redis_client.set.assert_awaited_once()
    exec_repo.save.assert_awaited_once()
    redis_client.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_execution_log_ignores_non_trade_event(monkeypatch) -> None:
    order = make_order()
    order_event = {
        "x": "NEW",
        "l": "0",
        "t": 0,
    }
    event_data = make_normalized_event(order_event)

    redis_client = MagicMock()
    redis_client.set = AsyncMock()

    exec_repo = MagicMock()
    exec_repo.save = AsyncMock()

    mock_redis = MagicMock(client=redis_client)

    svc = ExecutionLogService(exec_repo=exec_repo, redis=mock_redis)

    await svc.save_if_needed(
        order=order,
        event_data=event_data,
    )

    redis_client.set.assert_not_awaited()
    exec_repo.save.assert_not_awaited()
