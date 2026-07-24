from fastapi import APIRouter, Depends

from schemas.order import (
    Order,
    OrderRequest,
    BatchOrderRequest,
    BatchCancelRequest,
)
from schemas.market import Exchange, MarketType
from api_server.services.order.order_service import OrderService

router = APIRouter(prefix="/api/orders", tags=["Orders"])


def get_order_service() -> OrderService:
    raise NotImplementedError()


@router.post("", response_model=Order)
async def submit_order(
    req: OrderRequest, service: OrderService = Depends(get_order_service)
):
    """단건 주문 생성"""
    return await service.submit_order(req)


@router.post("/batch", response_model=list[Order])
async def submit_batch_orders(
    req: BatchOrderRequest, service: OrderService = Depends(get_order_service)
):
    """일괄 주문 생성 (최대 5건)"""
    return await service.submit_batch_orders(req)


@router.delete("/batch", response_model=list[dict])
async def cancel_batch_orders(
    req: BatchCancelRequest, service: OrderService = Depends(get_order_service)
):
    """일괄 주문 취소 (최대 10건)"""
    return await service.cancel_batch_orders(req)


@router.delete("/all-regular/{exchange}/{market_type}/{symbol}")
async def cancel_all_regular_orders(
    exchange: Exchange,
    market_type: MarketType,
    symbol: str,
    service: OrderService = Depends(get_order_service),
):
    """특정 심볼 전체 취소"""
    return await service.cancel_all_regular_orders(exchange, market_type, symbol)


@router.delete("/all/{exchange}/{market_type}/{symbol}")
async def cancel_all_orders(
    exchange: Exchange,
    market_type: MarketType,
    symbol: str,
    service: OrderService = Depends(get_order_service),
):
    """특정 심볼 일반/조건부 전체 주문 취소"""
    return await service.cancel_all_orders(exchange, market_type, symbol)


@router.delete("/all-conditional/{exchange}/{market_type}/{symbol}")
async def cancel_all_conditional_orders(
    exchange: Exchange,
    market_type: MarketType,
    symbol: str,
    service: OrderService = Depends(get_order_service),
):
    """특정 심볼 전체 조건부 주문 취소"""
    return await service.cancel_all_conditional_orders(exchange, market_type, symbol)


@router.delete("/{order_id}")
async def cancel_order(
    order_id: str,
    # symbol: str,
    service: OrderService = Depends(get_order_service),
):
    """단건 주문 취소"""
    return await service.cancel_order(order_id)


@router.get("/open/{exchange}/{market_type}", response_model=list[Order])
async def get_open_orders(
    exchange: Exchange,
    market_type: MarketType,
    symbol: str | None = None,
    service: OrderService = Depends(get_order_service),
):
    """미체결 주문 목록 조회 (Redis 실시간 상태)"""
    return await service.get_open_orders(exchange, market_type, symbol)


@router.get("/{order_id}", response_model=Order)
async def get_order(
    order_id: str, service: OrderService = Depends(get_order_service)
):
    """단건 주문 상태 조회 (Redis)"""
    return await service.get_order(order_id)


@router.get("/history/{exchange}/{market_type}", response_model=list[Order])
async def get_order_history(
    exchange: Exchange,
    market_type: MarketType,
    status: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
    service: OrderService = Depends(get_order_service),
):
    """주문 이력 조회 (DB 기준, 필터링 지원)"""
    return await service.get_order_history(
        exchange=exchange,
        market_type=market_type,
        status=status,
        symbol=symbol,
        limit=limit,
    )

