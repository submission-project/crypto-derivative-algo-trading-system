from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel

from schemas.market import Exchange, MarketType
from schemas.order import ConditionalStatus


class NormalizedConditionalOrderEvent(BaseModel):
    """
    거래소별 조건부 주문 이벤트를 Takora 내부 표준 이벤트로 정규화한 모델.

    Binance:
      ALGO_UPDATE

    OKX:
      algo order event

    Bitget:
      plan / trigger / tpsl event

    Bybit:
      conditional / trigger event
    """

    exchange: Exchange
    market_type: MarketType
    symbol: str

    client_conditional_id: Optional[str] = None
    exchange_conditional_id: Optional[str] = None

    target_status: ConditionalStatus
    exchange_conditional_status: Optional[str] = None

    triggered_order_id: Optional[str] = None
    triggered_client_order_id: Optional[str] = None

    filled_quantity: Optional[str] = None
    avg_fill_price: Optional[str] = None

    reject_reason_text: Optional[str] = None

    event_time: int
    transaction_time: Optional[int] = None

    raw: dict[str, Any]