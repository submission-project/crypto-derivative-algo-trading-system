from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator

from common.ids import generate_signal_id
from .market import Exchange, MarketType, DecimalString


class SignalDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class SignalStatus(str, Enum):
    """반자동 주문 워크플로우에서의 시그널 상태"""
    PENDING = "PENDING"       # 승인 대기 중 (Discord/Slack 알림 전송됨)
    APPROVED = "APPROVED"     # 사용자 승인 → 주문 생성됨
    DISMISSED = "DISMISSED"   # 사용자가 무시/거부
    EXPIRED = "EXPIRED"       # TTL 만료 (미응답)


class Signal(BaseModel):
    """
    전략 엔진이 생성하는 매매 시그널.

    반자동 모드에서는:
      1. Stream Processor가 Signal 생성 → Redis에 PENDING 상태로 저장
      2. Discord/Slack 알림 전송
      3. 사용자가 /api/signals/{id}/approve → APPROVED → 주문 생성
      4. 미응답 시 expires_ts 경과 후 EXPIRED 처리
    """
    signal_id: Optional[str] = Field(default=None)
    exchange: Exchange
    market_type: MarketType
    symbol: str
    strategy_name: str
    direction: SignalDirection
    confidence: float
    generated_ts: int

    # ─── 반자동 워크플로우 ───
    status: SignalStatus = SignalStatus.PENDING

    # 전략이 추천하는 주문 파라미터 (사용자가 승인 시 수정 가능)
    suggested_side: Optional[str] = Field(
        default=None,
        description="추천 주문 방향: BUY / SELL",
    )
    suggested_quantity: Optional[DecimalString] = Field(
        default=None,
        description="추천 주문 수량",
    )
    suggested_price: Optional[DecimalString] = Field(
        default=None,
        description="추천 지정가 (LIMIT일 때)",
    )
    suggested_order_type: Optional[str] = Field(
        default=None,
        description="추천 주문 유형: MARKET / LIMIT",
    )
    suggested_entry_price: Optional[DecimalString] = Field(
        default=None,
        description="전략 판단 시점의 기준 진입 가격",
    )
    suggested_stop_loss: Optional[DecimalString] = Field(
        default=None,
        description="전략이 제안하는 손절 가격",
    )
    suggested_take_profit: Optional[DecimalString] = Field(
        default=None,
        description="전략이 제안하는 익절 가격",
    )

    # 승인 결과
    approved_order_id: Optional[str] = Field(
        default=None,
        description="승인 시 생성된 주문 ID",
    )
    approved_ts: Optional[int] = Field(
        default=None,
        description="승인 시각 (ms)",
    )

    # 만료 설정
    expires_ts: Optional[int] = Field(
        default=None,
        description="시그널 만료 시각 (ms). 이 시각 이후 EXPIRED 처리",
    )

    @model_validator(mode='after')
    def generate_id_if_missing(self) -> 'Signal':
        if not self.signal_id:
            self.signal_id = generate_signal_id(self.exchange.value, self.market_type.value)
        return self
