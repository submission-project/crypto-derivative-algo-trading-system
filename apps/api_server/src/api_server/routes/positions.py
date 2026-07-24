from __future__ import annotations

from fastapi import APIRouter, Depends
from schemas.order import Order, OrderSource
from schemas.position import Position
from schemas.market import Exchange, MarketType
from api_server.services.position.position_service import PositionService

from api_server.dto.position import ClosePositionRequest, ReducePositionRequest

router = APIRouter(prefix="/positions", tags=["positions"])

def get_position_service() -> PositionService:
    raise RuntimeError("dependency override required")


@router.get("/open/{exchange}/{market_type}", response_model=list[Position])
async def get_open_positions(
    exchange: Exchange,
    market_type: MarketType,
    service: PositionService = Depends(get_position_service),
):
    """현재 열려 있는 포지션 목록 조회"""
    return await service.get_open_positions(exchange, market_type)



# [claim] 현재 포지션 종료시, 시장가 주문만 가능한거 같음(close_postion_market)
@router.post("/close", response_model=Order)
async def close_position(
    req: ClosePositionRequest,
    service: PositionService = Depends(get_position_service),
):
    """포지션 전량 종료"""
    return await service.close_position(
        exchange=req.exchange,
        market_type=req.market_type,
        symbol=req.symbol,
        position_side=req.position_side,
        source=OrderSource.MANUAL,
        close_type=req.close_type,
        price=req.price,
        trigger_price=req.trigger_price,
        time_in_force=req.time_in_force,
    )



@router.post("/reduce", response_model=Order)
async def reduce_position(
    req: ReducePositionRequest,
    service: PositionService = Depends(get_position_service),
):
    """포지션 일부 축소"""
    return await service.reduce_position(
        exchange=req.exchange,
        market_type=req.market_type,
        symbol=req.symbol,
        position_side=req.position_side,
        quantity=req.quantity,
        source=OrderSource.MANUAL
    )