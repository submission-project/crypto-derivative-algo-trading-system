from __future__ import annotations

from pydantic import BaseModel

from schemas.market import Exchange, MarketType
from schemas.order import TimeInForce
from schemas.position import PositionSide, PositionCloseOrderType


class ClosePositionRequest(BaseModel):
    exchange: Exchange = Exchange.BINANCE
    market_type: MarketType = MarketType.PERP
    symbol: str
    position_side: PositionSide = PositionSide.BOTH
    close_type: PositionCloseOrderType = PositionCloseOrderType.MARKET
    price: str | None = None
    trigger_price: str | None = None
    time_in_force: TimeInForce | None = None


class ReducePositionRequest(BaseModel):
    exchange: Exchange = Exchange.BINANCE
    market_type: MarketType = MarketType.PERP
    symbol: str
    position_side: PositionSide = PositionSide.BOTH
    quantity: str