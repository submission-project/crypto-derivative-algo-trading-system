from __future__ import annotations

from fastapi import HTTPException

from execution_gateway.services.position_order_service import (
    PositionCloseError,
    PositionOrderService,
)
from schemas.market import Exchange, MarketType
from schemas.order import Order, OrderSource, TimeInForce
from storage.repositories.redis.position_state_repo import PositionRedisRepository
from schemas.position import PositionSide, PositionCloseOrderType, Position


class PositionService:
    """포지션 관리(종료, 축소, 조회) 비즈니스 로직을 처리하는 서비스."""

    def __init__(
        self,
        position_order_service: PositionOrderService,
        position_repo: PositionRedisRepository,
    ) -> None:
        self._position_order_service = position_order_service
        self._repo = position_repo

    async def get_open_positions(self, exchange: Exchange, market_type: MarketType) -> list[Position]:
        """현재 열려 있는 포지션 목록 조회 (Redis projection 조회)"""
        rows = await self._repo.list_open_positions(
            exchange=exchange.value,
            market_type=market_type.value,
        )
        return [Position.model_validate(row) for row in rows]



    async def close_position(
        self,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        position_side: PositionSide,
        source: OrderSource,
        close_type: PositionCloseOrderType,
        price: str | None = None,
        trigger_price: str | None = None,
        time_in_force: TimeInForce | None = None
    ) -> Order:
        """포지션 전량 종료"""
        try:
            if close_type == PositionCloseOrderType.MARKET:
                return await self._position_order_service.close_position_market(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    position_side=position_side,
                    source=source,
                )
            
            if close_type == PositionCloseOrderType.LIMIT:
                if not price:
                    raise ValueError("The price must be provided for limit order")

                if not time_in_force:
                    raise ValueError("The time_in_force must be provided for limit order")
                    
                return await self._position_order_service.close_position_limit(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    position_side=position_side,
                    price=price,
                    time_in_force=time_in_force,
                    source=source,
                )

            if close_type == PositionCloseOrderType.STOP_MARKET:
                if not trigger_price:
                    raise ValueError("The trigger_price must be provided for stop market order")

                return await self._position_order_service.close_position_stop_market(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    position_side=position_side,
                    source=source,
                    trigger_price=trigger_price
                )
            
            if close_type == PositionCloseOrderType.STOP_LIMIT:
                if not price:
                    raise ValueError("price must be provided for limit order")

                if not time_in_force:
                    raise ValueError("The time_in_force must be provided for limit order")

                if not trigger_price:
                    raise ValueError("trigger_price must be provided for stop limit order")

                return await self._position_order_service.close_position_stop_limit(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    position_side=position_side,
                    source=source,
                    price=price,
                    trigger_price=trigger_price,
                    time_in_force=time_in_force
                )

            raise ValueError(f"Invalid close type {close_type}")

        except PositionCloseError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def reduce_position(
        self,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        position_side: PositionSide,
        quantity: str,
        source: OrderSource,
    ) -> Order:
        """포지션 일부 축소"""
        try:
            return await self._position_order_service.reduce_position_market(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                position_side=position_side,
                quantity=quantity,
                source=source
            )
        except PositionCloseError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
