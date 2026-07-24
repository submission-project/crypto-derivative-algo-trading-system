from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
from execution_gateway.exchange import ExchangeLeverageResult
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from schemas.market import Exchange, MarketType
from schemas.order import OrderRequest, OrderSide, OrderType, PositionAction
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository


class DummyExecutionClient:
    exchange = Exchange.BINANCE
    market_type = MarketType.PERP

    def __init__(self) -> None:
        self.change_leverage = AsyncMock(
            return_value=ExchangeLeverageResult(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol="BTCUSDT",
                leverage=7,
                raw={
                    "symbol": "BTCUSDT",
                    "leverage": 7,
                    "maxNotionalValue": "1000000",
                },
            )
        )

    async def close(self) -> None:
        return None


@pytest.fixture
def gateway() -> tuple[ExecutionGateway, DummyExecutionClient]:
    adapter = MagicMock(spec=BinanceRestAdapter)
    adapter.change_leverage = AsyncMock()

    state_repo = MagicMock(spec=OrderStateRedisRepository)
    state_service = MagicMock()

    client = DummyExecutionClient()
    registry = ExchangeExecutionClientRegistry()
    # pyrefly: ignore [bad-argument-type]
    registry.register(client)

    return (
        ExecutionGateway(
            state_repo=state_repo,
            state_service=state_service,
            exchange_clients=registry,
        ),
        client,
    )


@pytest.mark.asyncio
async def test_change_leverage_uses_exchange_client_registry(
    gateway: tuple[ExecutionGateway, DummyExecutionClient],
) -> None:
    gw, client = gateway

    result = await gw.change_leverage(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        leverage=7,
    )

    client.change_leverage.assert_awaited_once_with(
        symbol="btcusdt",
        leverage=7,
    )
    assert result.exchange is Exchange.BINANCE
    assert result.market_type is MarketType.PERP
    assert result.symbol == "BTCUSDT"
    assert result.leverage == 7
    assert result.raw["maxNotionalValue"] == "1000000"


@pytest.mark.asyncio
async def test_order_request_leverage_uses_request_exchange_and_market_type(
    gateway: tuple[ExecutionGateway, DummyExecutionClient],
) -> None:
    gw, client = gateway
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="ETHUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        leverage=3,
        position_action=PositionAction.OPEN,
    )

    await gw.submission_service._apply_order_request_leverage_if_present(req)

    client.change_leverage.assert_awaited_once_with(
        symbol="ETHUSDT",
        leverage=3,
    )
