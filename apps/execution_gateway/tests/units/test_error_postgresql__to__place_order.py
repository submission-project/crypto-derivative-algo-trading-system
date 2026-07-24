from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from schemas.market import Exchange, MarketType
from schemas.order import (
    OrderRequest,
    OrderSide,
    OrderType,
    PositionAction,
    TimeInForce,
)
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

# PostgreSQL 저장 실패 시 Binance까지 안 가는지
# state_service.create_order()가 실패했는데도 adapter.place_order()가 불리면 설계가 깨짐
@pytest.mark.asyncio
async def test_submit_order_does_not_call_binance_when_create_order_fails() -> None:
    mock_client = MagicMock()
    mock_client.exchange = Exchange.BINANCE
    mock_client.market_type = MarketType.PERP
    mock_client.place_order = AsyncMock()

    registry = ExchangeExecutionClientRegistry()
    registry.register(mock_client)

    redis_repo = MagicMock(spec=OrderStateRedisRepository)
    redis_repo.save = AsyncMock()
    redis_repo.update_status = AsyncMock()

    state_service = MagicMock()
    state_service.create_order = AsyncMock(
        side_effect=RuntimeError("pg create failed")
    )

    gateway = ExecutionGateway(
        state_repo=redis_repo,
        state_service=state_service,
        exchange_clients=registry,
    )

    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        price="60000",
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )

    with pytest.raises(RuntimeError, match="pg create failed"):
        await gateway.submit_order(req)

    state_service.create_order.assert_awaited_once()
    mock_client.place_order.assert_not_awaited()