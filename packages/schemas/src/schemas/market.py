from decimal import Decimal
from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, Field, StringConstraints


class Exchange(str, Enum):
    BINANCE = "BINANCE"
    BYBIT = "BYBIT"
    OKX = "OKX"
    BITGET = "BITGET"
    GATE = "GATE"
    MEXC = "MEXC"
    KUCOIN = "KUCOIN"
    HTX = "HTX"
    KRAKEN = "KRAKEN"


class MarketType(str, Enum):
    SPOT = "SPOT"  # Spot
    PERP = "PERP"  # Perpetual futures
    FUTURES = "FUTURES"  # Dated futures


class TradeSource(str, Enum):
    """데이터 출처를 식별하는 열거형"""

    UNDOCUMENTED_TRADE = "fstream_undocumented_trade"  # @trade (비공식)
    AGGTRADE_EXPANDED = "fstream_aggtrade_rest_expanded"  # @aggTrade → REST 분해
    REST_GAP_FILL = "rest_gap_fill"  # Gap Detection 복원
    REST_VALIDATION = "rest_periodic_validation"  # 주기적 REST 검증


# 거래소가 보내는 가격/수량은 정밀도 손실을 막기 위해 항상 십진 문자열("70000.5")로 운반합니다.
# 직접 산술이 필요한 다운스트림은 .price_decimal() 헬퍼로 Decimal로 파싱하세요.
DecimalString = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        pattern=r"^-?\d+(\.\d+)?$",
    ),
]


class Trade(BaseModel):
    trade_id: int = Field(
        description="Unique trade ID from exchange (monotonically increasing per symbol)"
    )
    exchange: Exchange
    market_type: MarketType
    symbol: str = Field(description="Trading pair symbol, e.g., BTCUSDT")
    price: DecimalString = Field(
        description="Execution price (decimal string from exchange, precision-preserving)"
    )
    size: DecimalString = Field(
        description="Execution quantity (decimal string from exchange, precision-preserving)"
    )
    is_buyer_maker: bool = Field(
        description="True if the buyer is the maker (sell order hit the book)"
    )
    exchange_ts: int = Field(description="Exchange timestamp in ms")
    local_ts: int = Field(description="Local receipt timestamp in ms")

    def price_decimal(self) -> Decimal:
        """비즈니스 로직(PnL/risk/회계)에서 안전한 산술이 필요할 때 사용."""
        return Decimal(self.price)

    def size_decimal(self) -> Decimal:
        return Decimal(self.size)


class CanonicalTrade(Trade):
    """정규화된 trade 이벤트 (source + 검증 여부 포함)"""

    source: TradeSource
    verified_by_rest: bool = False
    lag_ms: Optional[float] = None


class Ticker(BaseModel):
    exchange: Exchange
    market_type: MarketType
    symbol: str
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    exchange_ts: int
    local_ts: int
