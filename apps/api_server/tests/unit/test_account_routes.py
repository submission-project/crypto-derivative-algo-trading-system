from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api_server.routes.account import ChangeLeverageRequest, change_leverage
from execution_gateway.exchange import ExchangeLeverageResult
from schemas.market import Exchange, MarketType


class DummyAccountService:
    def __init__(self) -> None:
        self.change_leverage = AsyncMock(
            return_value=ExchangeLeverageResult(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol="BTCUSDT",
                leverage=5,
                raw={
                    "symbol": "BTCUSDT",
                    "leverage": 5,
                    "maxNotionalValue": "1000000",
                },
            )
        )


@pytest.mark.asyncio
async def test_change_leverage_route_returns_raw_exchange_response() -> None:
    service = DummyAccountService()
    req = ChangeLeverageRequest(
        exchange="binance",
        market_type="perp",
        symbol="btcusdt",
        leverage=5,
    )

    response = await change_leverage(req=req, service=service)

    service.change_leverage.assert_awaited_once_with(
        exchange_str="binance",
        market_type_str="perp",
        symbol_str="btcusdt",
        leverage=5,
    )
    assert response.ok is True
    assert response.exchange == "BINANCE"
    assert response.market_type == "PERP"
    assert response.symbol == "BTCUSDT"
    assert response.leverage == 5
    assert response.response == {
        "symbol": "BTCUSDT",
        "leverage": 5,
        "maxNotionalValue": "1000000",
    }
