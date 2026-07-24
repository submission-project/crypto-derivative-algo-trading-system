from typing import Optional

from pydantic import BaseModel, Field, model_validator

from common.ids import generate_execution_id
from .market import Exchange, MarketType, DecimalString
from .order import OrderSide, OrderSource


class ExecutionReport(BaseModel):
    """
    개별 체결(fill) 이벤트.

    Binance User Data Stream의 ORDER_TRADE_UPDATE 이벤트에서 생성되며,
    QuestDB execution_log 테이블에 시계열로 저장됩니다.

    하나의 Order가 여러 개의 ExecutionReport를 가질 수 있습니다 (부분 체결).
    """
    execution_id: Optional[str] = Field(
        default=None,
        description="Snowflake ID (prefix: X-EXCHANGE-MARKETTYPE)"
    )
    order_id: str
    source: OrderSource = Field(description="주문 발생 출처")
    signal_id: Optional[str] = None
    strategy_name: Optional[str] = None

    exchange: Exchange
    market_type: MarketType
    symbol: str
    side: OrderSide

    # Fill details
    fill_price: DecimalString
    fill_quantity: DecimalString
    commission: DecimalString = "0"
    commission_asset: str = "USDT"

    is_maker: bool = False
    exchange_trade_id: Optional[str] = None
    exchange_order_id: Optional[str] = None

    # Timestamps (ms)
    exchange_ts: int = Field(description="거래소 체결 시각")
    local_ts: int = Field(description="로컬 수신 시각")
    latency_ms: Optional[float] = Field(
        default=None,
        description="주문 전송 → 체결 레이턴시",
    )

    @model_validator(mode='after')
    def generate_id_if_missing(self) -> 'ExecutionReport':
        if not self.execution_id:
            self.execution_id = generate_execution_id(self.exchange.value, self.market_type.value)
        return self
