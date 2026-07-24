from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator, field_validator

from common.ids import generate_order_id
from schemas.market import Exchange, MarketType, DecimalString
from schemas.position import PositionSide
import json

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

# [claim] -> 수정 필요, 좀 단순함.
class OrderType(str, Enum):
    MARKET = "MARKET"  # 시장가 주문, 필요 조건: quantity 필요, price 없음, trigger_price 없음
    LIMIT = "LIMIT"  # 지정가 주문, 필요 조건: quantity 필요, price 필요, trigger_price 없음, time_in_force필요
    STOP_MARKET = "STOP_MARKET"  # 조건부 시장가 주문. trigger_price 도달 시 MARKET 주문 실행, 필요 조건: quantity 필요, price 없음, trigger_price 필요
    STOP_LIMIT = "STOP_LIMIT"  # 조건부 지정가 주문. trigger_price 도달 시 LIMIT 주문 실행, 필요 조건: quantity 필요, price 필요, trigger_price 필요, time_in_force필요
    # TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET" # 익절 시장가 주문, 필요 조건: quantity 필요, price 없음, trigger_price 필요, time_in_force필요
    # TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT" # 익절 지정가 주문, 필요 조건: quantity 필요, price 필요, trigger_price 필요, time_in_force필요

    # LIQUIDATION = "LIQUIDATION" # 거래소 강제 청산

class OrderRoute(str, Enum):
    REGULAR = "REGULAR"
    CONDITIONAL = "CONDITIONAL"

class TimeInForce(str, Enum):
    GTC = "GTC"  # Good Till Cancel, 사용자가 직접 주문을 취소하거나, 주문이 완전히 체결될 때까지 시장에 계속 남아 있음.
    IOC = "IOC"  # Immediate or Cancel, 주문이 전송된 즉시 체결 가능한 수량만 체결하고, 나머지는 즉시 취소됨.
    FOK = "FOK"  # Fill or Kill, 주문이 완전히 체결되지 않으면 나머지는 취소됨.
    GTX = "GTX"  # Post-Only (Maker only), 주로 수수료를 아끼기 위해 사용, 내 주문이 즉시 체결되어 테이커(Taker)가 되는 것을 막습니다. 즉, 호가창에 내 주문이 '매물'로 등록되어야만 주문이 성립됩니다. 만약 넣자마자 바로 체결될 가격이라면 주문이 자동으로 취소

# [claim] -> 수정 필요, 좀 단순함.
class OrderSource(str, Enum):
    MANUAL = "MANUAL"  # 사용자 직접 주문 (API)
    SIGNAL_APPROVED = "SIGNAL_APPROVED"  # 시그널 승인에 의한 주문
    STRATEGY = "STRATEGY"  # 전략에 의한 자동 주문
    SYSTEM = "SYSTEM"  # 시스템 내부 로직에 의한 주문
    RECOVERY = "RECOVERY"  # 복구 프로세스에 의한 주문

class OrderStatus(str, Enum):
    PENDING_NEW = "PENDING_NEW"  # Gateway가 주문 요청을 받았지만, 아직 거래소에 전송하지 않은 상태.[초기값 상태]
    SUBMITTED = "SUBMITTED"  # 거래소로 주문 실제 요청을 보낸 상태 -> 이 상태는 “거래소에 보냈다”이지, “거래소가 받아줬다”는 아님.
    ACKNOWLEDGED = "ACKNOWLEDGED"  # 거래소가 주문을 정상 접수했고, exchange order id를 부여한 상태.
    PARTIALLY_FILLED = "PARTIALLY_FILLED"  # 주문 수량 중 일부만 체결된 상태.
    FILLED = "FILLED"  # 주문 전체 수량이 완전히 체결된 상태.
    PENDING_CANCEL = "PENDING_CANCEL"  # 취소 요청을 보냈거나 보내기 직전인 상태.
    CANCELLED = "CANCELLED"  # 주문이 취소 완료된 상태.
    REJECTED = "REJECTED"  # 거래소 또는 내부 시스템이 주문을 거부한 상태.
    EXPIRED = "EXPIRED"  # 주문이 거래소 규칙에 의해 만료된 상태.
    UNKNOWN = "UNKNOWN"  # 주문 실행 결과를 모르는 상태.

    # reconciliation이 반복 확인했지만 거래소에서 주문을 찾지 못한 상태
    RECONCILE_UNRESOLVED = "RECONCILE_UNRESOLVED"

class ConditionalStatus(str, Enum):
    # None: 아직 조건부 주문 상태가 정해지지 않음. -> 로컬에서 주문 객체는 만들었지만, 거래소 응답/이벤트를 아직 받기 전일 수 있음.
    NEW = "NEW" # 거래소에 조건부 주문이 생성됨 -> 아직 트리거되지 않았고 대기 중
    ACTIVE = "ACTIVE" # 로컬 기준 활성 조건부 주문 -> 거래소에 살아 있고 계속 추적해야 하는 상태
    TRIGGERED = "TRIGGERED" # 조건부 주문의 역할이 완료 -> 이 시점 이후 실제 regular order가 생성될 수 있음
    FINISHED = "FINISHED" # 조건부 주문의 역할이 완료 -> 보통 trigger 이후 생성된 실제 주문까지 마무리됐거나, 거래소가 조건부 주문을 최종 완료 상태로 표시할 때 사용(거래소별 의미 차이가 있을 수 있음)
    CANCELLED = "CANCELLED" # 조건부 주문이 취소됨 -> 사용자 취소, 시스템 취소, cancel_all_conditional 결과 등이 여기에 해당
    EXPIRED = "EXPIRED" # 조건부 주문이 만료됨 -> 유효기간 만료, 거래소 정책상 만료 등
    REJECTED = "REJECTED" # 조건부 주문 생성 또는 처리 자체가 거부됨 -> 예: 잘못된 trigger price, insufficient margin, invalid symbol, exchange rule 위반 등
    UNKNOWN = "UNKNOWN" # 조건부 주문 상태를 확정하지 못함 -> REST/WS 오류, 거래소 조회 실패, 로컬 상태와 거래소 상태 불일치, 이벤트 누락 등이 있을 때 사용. => 끝난 상태가 아니라 “다시 확인해야 하는 상태”에 가까움.

class PositionAction(str, Enum):
    """
    이 주문이 포지션에 대해 가지는 '의도'
      - 실제 결과가 아니라 주문 생성 당시의 의도다.
      - 실제 포지션 결과는 ACCOUNT_UPDATE / positionRisk로 확인한다.

      position_amt = 0, BUY 0.01 → OPEN
      position_amt = +0.01, BUY 0.01 → INCREASE
      position_amt = +0.02, SELL 0.01 → REDUCE
      position_amt = +0.02, SELL 0.02 → CLOSE
      position_amt = +0.02, SELL 0.03 → FLIP
    """

    OPEN = "OPEN"  # 새 포지션을 여는 의도.
    INCREASE = "INCREASE"  # 기존 같은 방향 포지션을 늘리는 의도.
    REDUCE = "REDUCE"  # 기존포지션을 일부 줄이는 의도.
    CLOSE = "CLOSE"  # 기존 포지션을 완전히 닫는 의도.
    FLIP = "FLIP"  # 포지션 방향을 반대로 전환하는 의도.
    UNKNOWN = "UNKNOWN"  # PERP/FUTURES 주문이지만 포지션 의도를 모름
    NOT_APPLICABLE = "NOT_APPLICABLE"  # SPOT처럼 position_action 개념이 적용되지 않음

class RejectReason(str, Enum):
    DUPLICATE = "DUPLICATE"
    RISK_LIMIT_EXCEEDED = "RISK_LIMIT_EXCEEDED"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    EXCHANGE_REJECTED = "EXCHANGE_REJECTED"
    RATE_LIMITED = "RATE_LIMITED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNKNOWN_EXECUTION = "UNKNOWN_EXECUTION"  # 503: 주문 실행 결과 불명, get_order()로 확인 필요
    
# 종료 상태 집합 — 상태 전이 검증 및 TTL 설정에 사용
TERMINAL_STATUSES = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,

        # reconciliation이 반복 확인했지만 거래소에서 주문을 찾지 못한 상태
        OrderStatus.RECONCILE_UNRESOLVED,
    }
)

REGULAR_OPEN_STATUSES = frozenset({
    OrderStatus.PENDING_NEW,
    OrderStatus.SUBMITTED,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.PENDING_CANCEL,
    OrderStatus.UNKNOWN,
})

CONDITIONAL_TRACKABLE_STATUSES = frozenset({
    None,
    "", #아직 조건부 상태를 못 받음
    ConditionalStatus.NEW, #거래소에 조건부 주문이 생성됨
    ConditionalStatus.ACTIVE, #로컬 기준 활성 상태
    ConditionalStatus.UNKNOWN, #상태 불명, 계속 조회/복구 필요
})

# [claim] 해당 Recovery가 상태를 잘 체크한다. 
RECOVERY_STATUSES = frozenset({
    OrderStatus.SUBMITTED,
    OrderStatus.PENDING_CANCEL,
    OrderStatus.UNKNOWN,
})

UNKNOWN_STATUSES = frozenset({
    OrderStatus.UNKNOWN
})


_CONDITIONAL_ORDER_TYPES: set[OrderType] = {
    OrderType.STOP_MARKET,
    OrderType.STOP_LIMIT,
}


CONDITIONAL_TERMINAL_STATUSES = frozenset({
    ConditionalStatus.FINISHED,
    ConditionalStatus.CANCELLED,
    ConditionalStatus.EXPIRED,
    ConditionalStatus.REJECTED,
})

def infer_order_route(order_type: OrderType) -> OrderRoute:
    if order_type in _CONDITIONAL_ORDER_TYPES:
        return OrderRoute.CONDITIONAL

    return OrderRoute.REGULAR


# ─── API Request Models ───

class OrderRequest(BaseModel):
    """
    사용자가 API로 주문을 생성할 때 보내는 요청.
    POST /api/orders
    """

    exchange: Exchange
    market_type: MarketType
    symbol: str

    side: OrderSide
    order_type: OrderType

    order_route: Optional[OrderRoute] = None
    time_in_force: Optional[TimeInForce] = None

    quantity: DecimalString
    price: Optional[DecimalString] = None

    # 내부 표준 필드
    trigger_price: Optional[DecimalString] = None
    # deprecated: 외부 호환 입력. 내부에서는 trigger_price로 통합.
    stop_price: Optional[DecimalString] = None
    
    reduce_only: bool = False
    close_position: bool = False
    leverage: Optional[int] = Field(default=None, ge=1, le=125)

    # position metadata
    position_side: PositionSide = PositionSide.BOTH
    position_action: PositionAction = PositionAction.UNKNOWN

    @model_validator(mode="after")
    def normalize_request_after_validation(self) -> "OrderRequest":
        self.symbol = self.symbol.strip().upper()

        if self.trigger_price is not None and self.stop_price is not None:
            if Decimal(str(self.trigger_price)) != Decimal(str(self.stop_price)):
                raise ValueError(
                    "trigger_price and stop_price are both set but different"
                )

        if self.trigger_price is None and self.stop_price is not None:
            self.trigger_price = self.stop_price

        inferred_route = infer_order_route(self.order_type)

        if self.order_route is None:
            self.order_route = inferred_route
        elif self.order_route != inferred_route:
            raise ValueError(
                f"order_route mismatch: order_type={self.order_type.value}, "
                f"expected={inferred_route.value}, got={self.order_route.value}"
            )

        self.position_side, self.position_action = (
            normalize_position_metadata_for_order(
                market_type=self.market_type,
                position_side=self.position_side,
                position_action=self.position_action,
            )
        )
        validate_order_type_required_fields(
            order_type=self.order_type,
            price=self.price,
            trigger_price=self.trigger_price,
            time_in_force=self.time_in_force,
            reduce_only=self.reduce_only,
            close_position=self.close_position,
        )
        return self


class CancelRequest(BaseModel):
    """
    POST /api/orders/{order_id}/cancel
    """

    order_id: str
    reason: Optional[str] = None


class BatchOrderRequest(BaseModel):
    """POST /api/orders/batch — 최대 5건 일괄 주문"""

    exchange: Exchange
    market_type: MarketType
    orders: list[OrderRequest] = Field(..., max_length=5)


class BatchCancelRequest(BaseModel):
    """DELETE /api/orders/batch — 최대 10건 일괄 취소"""
    exchange: Exchange
    market_type: MarketType
    symbol: str
    order_ids: list[str] = Field(..., max_length=10)

ORDER_VERSION_INITIAL = 1

# ─── Domain Model ───
class Order(BaseModel):
    """
    Gateway가 관리하는 주문의 전체 상태.
    Redis Hash로 live state 유지, QuestDB에 이력 저장.

    상태 전이:
        PENDING_NEW → SUBMITTED → ACKNOWLEDGED → PARTIALLY_FILLED → FILLED
                  ↘ REJECTED             ↘ PENDING_CANCEL → CANCELLED
    """

    order_id: Optional[str] = Field(
        default=None, description="내부 Snowflake ID (prefix: O-EXCHANGE-MARKETTYPE)"
    )

    exchange: Exchange
    market_type: MarketType
    symbol: str

    side: OrderSide
    order_type: OrderType
    order_route: Optional[OrderRoute] = None

    time_in_force: Optional[TimeInForce] = None

    # State
    status: OrderStatus = OrderStatus.PENDING_NEW

    # Position info
    position_side: PositionSide = PositionSide.BOTH
    position_action: PositionAction = PositionAction.UNKNOWN

    # Price Info
    quantity: DecimalString
    price: Optional[DecimalString] = None
    trigger_price: Optional[DecimalString] = None

    reduce_only: bool = False
    close_position: bool = False
    # leverage: Optional[int] = Field(default=None, ge=1, le=125)

    # regular order ids
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None

    # conditional order ids
    client_conditional_id: Optional[str] = None
    exchange_conditional_id: Optional[str] = None

    # conditional status
    conditional_status: Optional[ConditionalStatus] = None
    exchange_conditional_status: Optional[str] = None

    # triggered actual order ids
    triggered_order_id: Optional[str] = None
    triggered_client_order_id: Optional[str] = None

    reject_reason: Optional[RejectReason] = None
    exchange_error_code: Optional[int] = None #: 거래소 에러 코드(예: Binance `-4164`). UDS 등 코드가 없으면 비움.
    detail_msg: Optional[str] = Field(default=None, max_length=8192) #: 거래소가 돌려준 사람이 읽을 수 있는 거부 메시지(REST/WebSocket 에러 등)

    # Fill summary
    filled_quantity: DecimalString = "0"
    avg_fill_price: DecimalString = "0"

    # Timestamps
    created_ts: int
    updated_ts: int
    submitted_ts: Optional[int] = None
    acknowledged_ts: Optional[int] = None
    triggered_ts: Optional[int] = None
    filled_ts: Optional[int] = None
    cancelled_ts: Optional[int] = None
    expired_ts: Optional[int] = None

    raw_exchange_response: Optional[dict] = None

    # Versioning
    version: int = ORDER_VERSION_INITIAL

    # Source metadata
    source: OrderSource
    signal_id: Optional[str] = Field(
        default=None,
        description="반자동 주문일 때, 트리거한 Signal ID",
    )
    strategy_name: Optional[str] = Field(
        default=None,
        description="반자동 주문일 때, 전략명",
    )

    @field_validator("raw_exchange_response", mode="before")
    @classmethod
    def _decode_raw_exchange_response(cls, value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                raise ValueError(
                    "raw_exchange_response must be a valid JSON string"
                ) from e

        return value

    @model_validator(mode="after")
    def normalize_order_after_validation(self) -> "Order":
        self.symbol = self.symbol.strip().upper()

        inferred_route = infer_order_route(self.order_type)

        if self.order_route is None:
            self.order_route = inferred_route
        elif self.order_route != inferred_route:
            raise ValueError(
                f"order_route mismatch: order_type={self.order_type.value}, "
                f"expected={inferred_route.value}, got={self.order_route.value}"
            )

        self.position_side, self.position_action = (
            normalize_position_metadata_for_order(
                market_type=self.market_type,
                position_side=self.position_side,
                position_action=self.position_action,
            )
        )

        validate_order_type_required_fields(
            order_type=self.order_type,
            price=self.price,
            trigger_price=self.trigger_price,
            time_in_force=self.time_in_force,
            reduce_only=self.reduce_only,
            close_position=self.close_position,
        )

        if not self.order_id:
            self.order_id = generate_order_id(
                self.exchange.value,
                self.market_type.value,
            )

        if self.order_route == OrderRoute.REGULAR:
            if not self.client_order_id:
                self.client_order_id = self.order_id

        if self.order_route == OrderRoute.CONDITIONAL:
            if not self.client_conditional_id:
                self.client_conditional_id = self.order_id

        return self

    def quantity_decimal(self) -> Decimal:
        return Decimal(self.quantity)

    def filled_quantity_decimal(self) -> Decimal:
        return Decimal(self.filled_quantity)

    def remaining_quantity(self) -> Decimal:
        return self.quantity_decimal() - self.filled_quantity_decimal()

    @property
    def is_terminal(self) -> bool:
        """주문이 종료 상태인지 확인"""
        return self.status in TERMINAL_STATUSES


# ─── Helper ───
def normalize_position_metadata_for_order(
    *,
    market_type: MarketType,
    position_side: PositionSide,
    position_action: PositionAction,
) -> tuple[PositionSide, PositionAction]:
    """
    market_type에 따라 position metadata를 정규화한다.

    SPOT:
      - position_action 기본값 UNKNOWN이면 NOT_APPLICABLE로 변환
      - position_action은 NOT_APPLICABLE만 허용
      - position_side는 BOTH만 허용

    PERP:
      - UNKNOWN 허용
      - NOT_APPLICABLE은 허용하지 않음

    이유:
      - SPOT은 일반적으로 futures position 개념이 없다.
      - PERP/FUTURES는 position_action 개념이 적용되므로,
        의도를 모르면 UNKNOWN으로 남겨둔다.
    """
    if market_type == MarketType.SPOT:
        if position_action == PositionAction.UNKNOWN:
            position_action = PositionAction.NOT_APPLICABLE

        if position_action != PositionAction.NOT_APPLICABLE:
            raise ValueError(
                "SPOT order cannot have position_action other than " "NOT_APPLICABLE"
            )

        if position_side != PositionSide.BOTH:
            raise ValueError("SPOT order must use position_side=BOTH")

    elif market_type in (MarketType.PERP, MarketType.FUTURES):
        if position_action == PositionAction.NOT_APPLICABLE:
            raise ValueError("PERP/FUTURES order cannot use position_action=NOT_APPLICABLE")

    return position_side, position_action

def validate_order_type_required_fields(
    *,
    order_type: OrderType,
    price: Optional[DecimalString],
    trigger_price: Optional[DecimalString],
    time_in_force: TimeInForce,
    reduce_only: bool,
    close_position: bool,
) -> None:
    """
    내부 표준 필드 기준 검증.

    MARKET:
      - price 없음
      - trigger_price 없음

    LIMIT:
      - price 필요
      - trigger_price 없음
      - time_in_force 필요

    STOP_MARKET:
      - trigger_price 필요
      - price 없음

    STOP_LIMIT:
      - trigger_price 필요
      - price 필요
      - time_in_force 필요
    """

    if reduce_only and close_position:
        raise ValueError("reduce_only and close_position cannot be used together")

    if order_type == OrderType.MARKET:
        if price is not None:
            raise ValueError("MARKET order cannot have price")
        if trigger_price is not None:
            raise ValueError("MARKET order cannot have trigger_price")

    elif order_type == OrderType.LIMIT:
        if price is None:
            raise ValueError("LIMIT order requires price")
        if trigger_price is not None:
            raise ValueError("LIMIT order cannot have trigger_price")
        if time_in_force is None:
            raise ValueError("LIMIT order requires time_in_force")

    elif order_type == OrderType.STOP_MARKET:
        if trigger_price is None:
            raise ValueError("STOP_MARKET order requires trigger_price")
        if price is not None:
            raise ValueError("STOP_MARKET order cannot have price")

    elif order_type == OrderType.STOP_LIMIT:
        if trigger_price is None:
            raise ValueError("STOP_LIMIT order requires trigger_price")
        if price is None:
            raise ValueError("STOP_LIMIT order requires price")
        if time_in_force is None:
            raise ValueError("STOP_LIMIT order requires time_in_force")
