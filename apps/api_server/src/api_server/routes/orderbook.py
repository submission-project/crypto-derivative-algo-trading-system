from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends, HTTPException

from schemas.market import Exchange, MarketType
from execution_gateway.gateway import ExecutionGateway

router = APIRouter(prefix="/api/orderbook", tags=["Orderbook"])


def get_gateway() -> ExecutionGateway:
    raise RuntimeError("dependency override required")


@router.get("/price/{exchange}/{market_type}/{symbol}")
async def get_symbol_price(
    exchange: Exchange,
    market_type: MarketType,
    symbol: str,
    gateway: ExecutionGateway = Depends(get_gateway),
) -> dict[str, Any]:
    """특정 상품의 실시간 현재가 조회"""
    try:
        return await gateway.get_symbol_price_ticker(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:

        print(e)
        raise HTTPException(status_code=500, detail=str(e))
