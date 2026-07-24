from __future__ import annotations

from typing import Any
from fastapi import HTTPException

from execution_gateway.gateway import ExecutionGateway
from execution_gateway.exchange import ExchangeApiError
from schemas.order import (
    Order,
    OrderRequest,
    BatchOrderRequest,
    BatchCancelRequest,
    OrderSource,
)
from schemas.market import Exchange, MarketType

from execution_gateway.gateway.cancellation_service import CancelOrderSkipped


class OrderService:
    """주문 관련 비즈니스 로직을 처리하는 서비스."""

    def __init__(self, gateway: ExecutionGateway) -> None:
        self._gateway = gateway

    async def submit_order(self, req: OrderRequest) -> Order:
        """단건 주문 생성"""
        from api_server.helper import exchange_error_to_http

        try:
            return await self._gateway.submit_order(req, source=OrderSource.MANUAL)
        except ExchangeApiError as e:
            raise exchange_error_to_http(e) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def submit_batch_orders(self, req: BatchOrderRequest) -> list[Order]:
        """일괄 주문 생성 (최대 5건)"""
        from api_server.helper import exchange_error_to_http

        try:
            return await self._gateway.submit_batch_orders(
                exchange=req.exchange,
                market_type=req.market_type,
                requests=req.orders,
                source=OrderSource.MANUAL,
            )
        except ExchangeApiError as e:
            raise exchange_error_to_http(e) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def cancel_batch_orders(self, req: BatchCancelRequest):
        """일괄 주문 취소 (최대 10건)"""
        return await self._gateway.cancel_batch_orders(
            exchange=req.exchange,
            market_type=req.market_type,
            symbol=req.symbol,
            order_ids=req.order_ids,
        )

    async def cancel_all_regular_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> Any:
        """특정 심볼 전체 취소"""
        return await self._gateway.cancel_all_regular_open_orders(
            exchange, market_type, symbol
        )

    async def cancel_all_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> dict:
        """특정 심볼 일반/조건부 전체 주문 취소"""
        return await self._gateway.cancel_all_open_orders(exchange, market_type, symbol)

    async def cancel_all_conditional_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> dict:
        """특정 심볼 전체 조건부 주문 취소"""
        return await self._gateway.cancel_all_conditional_open_orders(
            exchange, market_type, symbol
        )

    async def cancel_order(self, order_id: str) -> Any:
        """단건 주문 취소"""
        from api_server.helper import exchange_error_to_http

        try:
            return await self._gateway.cancel_order(order_id)
        except CancelOrderSkipped as e:
            if e.http_status == 200:
                return e.to_payload()

            raise HTTPException(
                status_code=e.http_status,
                detail=e.to_payload(),
            )
        except ExchangeApiError as e:
            raise exchange_error_to_http(e) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    async def get_open_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str | None = None
    ) -> list[Order]:
        """미체결 주문 목록 조회 (Redis 실시간 상태)"""
        orders_dict = await self._gateway.state_repo.list_open_regular_orders(
            exchange=exchange, market_type=market_type
        )
        if symbol:
            sym = symbol.strip().upper()
            orders_dict = [
                o for o in orders_dict if (o.get("symbol") or "").upper() == sym
            ]
        return [Order.model_validate(o) for o in orders_dict]

    async def get_order(self, order_id: str) -> Order:
        """단건 주문 상태 조회 (Redis)"""
        order_dict = await self._gateway.state_repo.get(order_id)
        if not order_dict:
            raise HTTPException(status_code=404, detail="Order not found")
        return Order.model_validate(order_dict)

    async def get_order_history(
        self,
        exchange: Exchange,
        market_type: MarketType,
        status: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[Order]:
        """주문 이력 조회 (DB 기준, 필터링 지원)"""
        pool = self._gateway.state_service.postgres.require_pool()
        async with pool.acquire() as conn:
            return await self._gateway.state_service.postgres_order_repo.list_orders(
                conn=conn,
                exchange=exchange,
                market_type=market_type,
                status=status,
                symbol=symbol,
                limit=limit,
            )

