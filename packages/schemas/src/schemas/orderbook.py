from pydantic import BaseModel, Field
from typing import List, Tuple
from .market import Exchange, MarketType

# Price, Size tuple
OrderbookLevel = Tuple[float, float]

class OrderbookUpdate(BaseModel):
    exchange: Exchange
    market_type: MarketType
    symbol: str
    bids: List[OrderbookLevel]
    asks: List[OrderbookLevel]
    exchange_ts: int
    local_ts: int
    update_id: int = Field(default=0, description="Sequence ID from exchange")

class OrderbookSnapshot(BaseModel):
    exchange: Exchange
    market_type: MarketType
    symbol: str
    bids: List[OrderbookLevel]
    asks: List[OrderbookLevel]
    exchange_ts: int
    local_ts: int
    last_update_id: int
