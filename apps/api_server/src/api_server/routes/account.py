from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from api_server.services.account.account_service import AccountService

router = APIRouter(
    prefix="/api/account",
    tags=["account"],
)


class ChangeLeverageRequest(BaseModel):
    exchange: str
    market_type: str
    symbol: str
    leverage: int = Field(ge=1, le=125)


class ChangeLeverageResponse(BaseModel):
    ok: bool
    exchange: str
    market_type: str
    symbol: str
    leverage: int
    response: dict[str, Any]


def get_account_service() -> AccountService:
    raise RuntimeError("dependency override required")


@router.post("/leverage", response_model=ChangeLeverageResponse)
async def change_leverage(
    req: ChangeLeverageRequest,
    service: AccountService = Depends(get_account_service),
) -> ChangeLeverageResponse:
    """
    Binance USD-M Futures symbol leverage 변경.

    예:
      BTCUSDT leverage=5 설정 후,
      이후 /api/orders 주문은 BTCUSDT 5배 레버리지 설정 상태에서 실행된다.
    """
    resp = await service.change_leverage(
        exchange_str=req.exchange,
        market_type_str=req.market_type,
        symbol_str=req.symbol,
        leverage=req.leverage,
    )

    return ChangeLeverageResponse(
        ok=True,
        exchange=resp.exchange.value,
        market_type=resp.market_type.value,
        symbol=req.symbol.upper(),
        leverage=resp.leverage,
        response=resp.raw,
    )