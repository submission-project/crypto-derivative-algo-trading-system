from __future__ import annotations

from fastapi import HTTPException

from execution_gateway.gateway import ExecutionGateway
from execution_gateway.exchange import ExchangeApiError, ExchangeLeverageResult
from schemas.market import Exchange, MarketType


class AccountService:
    """계정 및 레버리지 관련 비즈니스 로직을 처리하는 서비스."""

    def __init__(self, gateway: ExecutionGateway) -> None:
        self._gateway = gateway

    async def change_leverage(
        self, exchange_str: str, market_type_str: str, symbol_str: str, leverage: int
    ) -> ExchangeLeverageResult:
        """Binance USD-M Futures symbol leverage 변경"""
        from api_server.helper import exchange_error_to_http

        exchange = Exchange(exchange_str.upper())
        market_type = MarketType(market_type_str.upper())
        symbol = symbol_str.upper()

        try:
            return await self._gateway.change_leverage(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                leverage=leverage,
            )
        except ExchangeApiError as e:
            raise exchange_error_to_http(e) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
