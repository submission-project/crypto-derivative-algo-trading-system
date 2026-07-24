from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
from execution_gateway.gateway import ExecutionGateway
from schemas.market import Exchange, MarketType
from schemas.order import (
    OrderRequest,
    OrderSource,
    OrderSide,
    OrderType,
    PositionAction,
    TimeInForce,
)
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository


@pytest.fixture
def mock_adapter():
    return MagicMock(spec=BinanceRestAdapter)


@pytest.fixture
def mock_redis_repo():
    repo = MagicMock(spec=OrderStateRedisRepository)
    repo.save = AsyncMock()
    return repo


@pytest.fixture
def mock_state_service():
    service = MagicMock()
    service.create_order = AsyncMock(side_effect=lambda order: order)
    return service


@pytest.fixture
def gateway(mock_redis_repo, mock_state_service):
    return ExecutionGateway(
        state_repo=mock_redis_repo,
        state_service=mock_state_service,
        exchange_clients=MagicMock(),
    )


def make_limit_req() -> OrderRequest:
    return OrderRequest(
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


@pytest.mark.asyncio
async def test_create_internal_order_uses_state_service_not_direct_redis_save(
    gateway,
    mock_redis_repo,
    mock_state_service,
) -> None:
    order = await gateway.transitions.create_internal_order(
        req=make_limit_req(),
        source=OrderSource.MANUAL,
    )

    mock_state_service.create_order.assert_awaited_once_with(order)
    mock_redis_repo.save.assert_not_awaited()