from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from schemas.market import Exchange, MarketType
from schemas.order import OrderStatus


class NormalizedOrderUpdateEvent(BaseModel):
    exchange: Exchange
    market_type: MarketType
    symbol: str

    client_order_id: str
    exchange_order_id: str | None = None

    target_status: OrderStatus | None = None
    exchange_status: str | None = None
    execution_type: str | None = None

    filled_quantity: str | None = None
    avg_fill_price: str | None = None

    last_fill_quantity: str | None = None
    last_fill_price: str | None = None
    trade_id: str | None = None
    commission: str | None = None
    commission_asset: str | None = None
    is_maker: bool | None = None

    reject_reason_text: str | None = None

    event_time: int | None = None
    transaction_time: int | None = None

    raw: dict[str, Any]