"""
Order State Machine.

Order State Machine.

주문 생명주기를 추적하고 상태 전이 규칙을 강제합니다.

핵심 설계:
  - 상태 전이는 _VALID_TRANSITIONS에 정의된 경로만 허용
  - TERMINAL 상태(FILLED, CANCELLED, REJECTED, EXPIRED, RECONCILE_UNRESOLVED)에서는 전이 불가
  - UNKNOWN은 terminal이 아님
      - 503 Unknown
      - REST timeout
      - WS timeout
      - cancel 결과 불명
    이후 verify_unknown_order() 또는 User Data Stream 이벤트로 복구 가능
  - User Data Stream 이벤트는 실제 거래소 상태를 반영하는 강한 신호이므로,
    ACKNOWLEDGED를 관측하기 전에 FILLED/PARTIALLY_FILLED가 먼저 들어오는 경우도 허용
  - 상태 변경 시 updated_ts 기록
  - FILLED 상태 진입 시 filled_ts 기록

상태 다이어그램:
    PENDING_NEW → SUBMITTED → ACKNOWLEDGED → PARTIALLY_FILLED → FILLED
                                           ↘ CANCELLED
                                           ↘ EXPIRED
             ↘ REJECTED (제출 실패)
             ↘ UNKNOWN  (503 Unknown — 거래소 응답 불명)

규칙:
  - TERMINAL 상태(FILLED, CANCELLED, REJECTED, EXPIRED)에서는 전이 불가
  - UNKNOWN 상태는 non-terminal — verify_unknown_order()로 복구 후 재분류
  - 허용되지 않은 전이 시 InvalidTransitionError 발생
  - 상태 변경 시 타임스탬프 기록

"""

from __future__ import annotations

import time
from typing import Optional

from schemas.order import Order, OrderStatus, RejectReason, TERMINAL_STATUSES
from common.logging import setup_logger


logger = setup_logger(__name__)

# 유효한 전이 맵: {현재 상태} → {허용된 다음 상태들}
_VALID_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    # 내부 주문 생성 후,
    OrderStatus.PENDING_NEW: frozenset(
        {
            OrderStatus.SUBMITTED, # 거래소 요청 완료
            OrderStatus.REJECTED, # 주문이 취소됨(거래소 전송 또는 내부 검증 실패 가능)
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),

    # 
    # 거래소로 요청은 보냈지만 아직 REST/WS ACK를 관측하지 못한 상태.
    # 거래소 내부 논리상으로는 NEW/ACK 이후 체결되지만,
    # 내 서버 관측 순서상 User Data Stream의 FILLED/PARTIALLY_FILLED 이벤트가
    # REST/WS ACK보다 먼저 도착할 수 있으므로 직접 체결 상태 전이를 허용한다.
    OrderStatus.SUBMITTED: frozenset(
        {
            OrderStatus.ACKNOWLEDGED, # 거래소가 주문을 접수한 상태
            OrderStatus.PARTIALLY_FILLED, # 일부 주문들 체결된 상태
            OrderStatus.FILLED, # 모든 주문이 체결된 상태
            OrderStatus.REJECTED, # 주문이 취소됨
            OrderStatus.EXPIRED, # 주문이 만료됨
            OrderStatus.UNKNOWN, # 주문 결과 불명 상태
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),
    # 거래소가 주문을 접수한 상태. Binance의 NEW에 가까움.
    OrderStatus.ACKNOWLEDGED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.PENDING_CANCEL,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.UNKNOWN,
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),
    # 일부 체결된 상태.
    # 추가 부분 체결은 같은 PARTIALLY_FILLED로 반복 전이 허용.
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.PENDING_CANCEL,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.UNKNOWN,
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),
    # 취소 요청 중.
    #
    # 취소 요청 중에도 체결될 수 있고,
    # 취소 실패 시 ACKNOWLEDGED로 rollback할 수 있으며,
    # 취소 요청 자체가 503/timeout이면 UNKNOWN으로 갈 수 있다.
    OrderStatus.PENDING_CANCEL: frozenset(
        {
            OrderStatus.ACKNOWLEDGED,  # cancel 실패 / rollback
            OrderStatus.PARTIALLY_FILLED,  # cancel 중 일부 체결
            OrderStatus.FILLED,  # cancel 중 전량 체결
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.UNKNOWN,
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),
    # 결과 불명 상태.
    # verify_unknown_order() 또는 User Data Stream 이벤트로 실제 상태 복구.
    OrderStatus.UNKNOWN: frozenset(
        {
            OrderStatus.ACKNOWLEDGED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.RECONCILE_UNRESOLVED,
        }
    ),
    # Terminal states — 전이 없음
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
    OrderStatus.RECONCILE_UNRESOLVED: frozenset(),
}


class InvalidTransitionError(Exception):
    """허용되지 않은 상태 전이 시도."""

    def __init__(self, current: OrderStatus, target: OrderStatus):
        self.current = current
        self.target = target
        super().__init__(f"상태 전이 불가: {current.value} → {target.value}")


class OrderStateMachine:
    """
    단일 주문의 상태 전이를 관리.

    Args:
        order: 관리할 Order 인스턴스. transition() 호출 시 in-place 수정됨.

    사용 예:
        machine = OrderStateMachine(order)
        machine.transition(OrderStatus.SUBMITTED)
        machine.transition(OrderStatus.ACKNOWLEDGED, exchange_order_id="12345")
        machine.transition(OrderStatus.FILLED, filled_quantity="0.001", avg_fill_price="60000")
    """

    def __init__(self, order: Order):
        self._order = order

    @property
    def order(self) -> Order:
        return self._order

    @property
    def status(self) -> OrderStatus:
        return self._order.status

    @property
    def is_terminal(self) -> bool:
        return self._order.status in TERMINAL_STATUSES

    def can_transition(self, target: OrderStatus) -> bool:
        """전이 가능 여부 확인. 예외를 발생시키지 않는다."""
        allowed = _VALID_TRANSITIONS.get(self._order.status, frozenset())
        return target in allowed

    def assert_can_transition(self, target: OrderStatus) -> None:
        """전이 불가능하면 InvalidTransitionError 발생."""
        if not self.can_transition(target):
            raise InvalidTransitionError(self._order.status, target)

    def transition(
        self,
        target: OrderStatus,
        *,
        exchange_order_id: Optional[str] = None,
        reject_reason: Optional[RejectReason] = None,
        filled_quantity: Optional[str] = None,
        avg_fill_price: Optional[str] = None,
    ) -> "OrderStateMachine":
        """
        상태 전이 수행.

        Args:
            target: 목표 상태
            exchange_order_id: 거래소 주문 ID
            reject_reason: 거부 사유
            filled_quantity: 누적 체결 수량
            avg_fill_price: 평균 체결가

        Returns:
            self

        Raises:
            InvalidTransitionError: 허용되지 않은 전이
        """
        if not self.can_transition(target):
            raise InvalidTransitionError(self._order.status, target)

        now_ms = time.time_ns() // 1_000_000

        self._order.status = target
        self._order.updated_ts = now_ms

        if exchange_order_id is not None:
            self._order.exchange_order_id = exchange_order_id

        if reject_reason is not None:
            self._order.reject_reason = reject_reason

        if filled_quantity is not None:
            self._order.filled_quantity = filled_quantity

        if avg_fill_price is not None:
            self._order.avg_fill_price = avg_fill_price

        if target == OrderStatus.FILLED:
            self._order.filled_ts = now_ms

        return self

    def force_transition(
        self,
        target: OrderStatus,
        *,
        exchange_order_id: Optional[str] = None,
        reject_reason: Optional[RejectReason] = None,
        filled_quantity: Optional[str] = None,
        avg_fill_price: Optional[str] = None,
    ) -> "OrderStateMachine":
        """
        전이 규칙을 무시하고 강제 상태 변경.

        사용 권장 상황:
          - reconciliation
          - 관리자 수동 보정
          - verify_unknown_order() 결과 반영
          - terminal 보호를 Gateway/Repository에서 이미 처리한 경우

        일반 주문 흐름에서는 transition()을 사용해야 한다.
        """
        now_ms = time.time_ns() // 1_000_000

        previous = self._order.status
        self._order.status = target
        self._order.updated_ts = now_ms

        if exchange_order_id is not None:
            self._order.exchange_order_id = exchange_order_id

        if reject_reason is not None:
            self._order.reject_reason = reject_reason

        if filled_quantity is not None:
            self._order.filled_quantity = filled_quantity

        if avg_fill_price is not None:
            self._order.avg_fill_price = avg_fill_price

        if target == OrderStatus.FILLED:
            self._order.filled_ts = now_ms

        logger.warning(
            f"force_transition 수행: "
            f"order_id={getattr(self._order, 'order_id', None)}, "
            f"{previous.value} -> {target.value}"
        )

        return self

    # [claim] 멀티 거래소 가능하게 수정 필요 : order_event:dict => Normalized... -> [complate] : apply_order_update_event로 전환
    def apply_execution_event(self, order_event: dict) -> bool:
        """
        User Data Stream의 ORDER_TRADE_UPDATE 이벤트 중 Binance 'o' 필드를 적용.

        Args:
            order_event:
                Binance ORDER_TRADE_UPDATE 이벤트의 "o" 필드.

                주요 필드:
                  - X: order status
                    NEW / PARTIALLY_FILLED / FILLED / CANCELED / EXPIRED / EXPIRED_IN_MATCH
                  - x: execution type
                    NEW / TRADE / CANCELED / EXPIRED / AMENDMENT 등
                  - i: exchange order id
                  - z: 누적 체결 수량
                  - ap: 평균 체결가

        Returns:
            True:
                상태 전이가 적용됨.
            False:
                알 수 없는 이벤트이거나, 허용되지 않은/stale 가능성이 있는 이벤트라 무시됨.
        """
        """
        Deprecated
        """
        raise NotImplementedError("Deprecated")

        # 주석처리 함
        # exchange_status = order_event.get("X", "")

        # status_map = {
        #     "NEW": OrderStatus.ACKNOWLEDGED,
        #     "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
        #     "FILLED": OrderStatus.FILLED,
        #     "CANCELED": OrderStatus.CANCELLED,
        #     "CANCELLED": OrderStatus.CANCELLED,  # 혹시 모를 spelling 차이 방어
        #     "EXPIRED": OrderStatus.EXPIRED,
        #     "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
        #     "REJECTED": OrderStatus.REJECTED,
        # }

        # target = status_map.get(exchange_status)
        # if target is None:
        #     logger.debug(
        #         f"알 수 없는 exchange status 무시: "
        #         f"order_id={getattr(self._order, 'order_id', None)}, "
        #         f"exchange_status={exchange_status}, event={order_event}"
        #     )
        #     return False

        # if not self.can_transition(target):
        #     logger.warning(
        #         f"허용되지 않은 execution event transition 무시: "
        #         f"order_id={getattr(self._order, 'order_id', None)}, "
        #         f"{self._order.status.value} -> {target.value}, "
        #         f"exchange_status={exchange_status}, "
        #         f"execution_type={order_event.get('x')}, "
        #         f"event={order_event}"
        #     )
        #     return False

        # self.transition(
        #     target,
        #     exchange_order_id=str(order_event.get("i", "")),
        #     filled_quantity=order_event.get("z"),
        #     avg_fill_price=order_event.get("ap"),
        # )

        # return True
