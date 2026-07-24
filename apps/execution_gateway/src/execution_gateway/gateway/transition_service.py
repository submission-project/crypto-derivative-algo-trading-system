from typing import Any, Optional

from schemas.order import (
    Order,
    OrderRequest,
    OrderSource,
    OrderStatus,
    RejectReason,
    ConditionalStatus,
    TERMINAL_STATUSES,
    CONDITIONAL_TERMINAL_STATUSES,
)

from schemas.market import Exchange, MarketType

from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.state_machine.order_state_machine import OrderStateMachine
from execution_gateway.state_machine.conditional_order_state_machine import (
    ConditionalOrderStateMachine,
)

from storage.repositories.postgres.order_repo import StaleOrderVersionError

from common.time import epoch_ms

from common.logging import setup_logger

logger = setup_logger(__name__)

_DETAIL_MSG_MAX_LEN = 8192


def _sanitize_detail_msg(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return text[:_DETAIL_MSG_MAX_LEN]


class GatewayTransitionService:
    def __init__(
        self,
        *,
        state_service: OrderStateService,
    ):
        self.state_service = state_service

    async def _load_order_from_repo(self, order_id: str) -> Optional[Order]:
        """
        OrderStateService를 통해 주문 로드.
        """
        return await self.state_service.load_order(order_id=order_id)

    # [claim] 파라미터의 filed의 방식이 솔직히 안정서이나 무결성 면에서 좋은 접근법이라고 해석하진 않는다.
    async def _set_status(
        self,
        *,
        order: Order,
        status: OrderStatus,
        use_machine: bool = True,
        protect_terminal: bool = True,
        **fields: Any,
    ) -> Order:
        """
        상태 전이 공통 처리.

        역할:
          - terminal 보호
          - state machine 전이 검증
          - candidate Order 생성
          - PostgreSQL 원본 상태 갱신 위임
          - Redis projection 갱신은 OrderStateService가 담당
        """
        now = epoch_ms()

        if protect_terminal and order.status in TERMINAL_STATUSES:
            if order.status != status:
                logger.warning(
                    f"Terminal 상태 덮어쓰기 방지: "
                    f"{order.order_id} {order.status.value} -> {status.value}"
                )
                return order

        candidate = order.model_copy(deep=True)

        if use_machine:
            machine = OrderStateMachine(candidate)
            machine.assert_can_transition(status)

        # candidate에 상태/필드 반영
        candidate.status = status
        candidate.updated_ts = now

        valid_fields = Order.model_fields

        for key, value in fields.items():
            if key not in valid_fields:
                raise ValueError(
                    f"Invalid Order field for status update: "
                    f"order_id={candidate.order_id}, field={key}"
                )
            setattr(candidate, key, value)

        # FILLED 진입 시 filled_ts 공통 처리
        if status == OrderStatus.FILLED:
            if candidate.filled_ts is None:
                candidate.filled_ts = now

        try:
            return await self.state_service.transition_order(
                current_order=order,
                updated_order=candidate,
            )
        except StaleOrderVersionError as e:
            # 동시 업데이트(UDS/reconciliation)가 먼저 반영됨.
            # 현재 PG 원본 상태를 재조회해서 반환한다.
            logger.warning(
                f"Stale version conflict during transition: "
                f"order_id={order.order_id}, "
                f"{order.status.value} -> {status.value}, "
                f"expected_version={e.expected_version}, "
                f"actual_version={e.actual_version}, "
                f"actual_status={e.actual_status}. "
                f"현재 PG 원본 상태를 재조회합니다."
            )
            reloaded = await self._load_order_from_repo(order.order_id)
            if reloaded:
                return reloaded
            # PG에서도 못 찾으면 원래 order 반환
            return order

    # [claim] 파라미터의 filed의 방식이 솔직히 안정서이나 무결성 면에서 좋은 접근법이라고 해석하진 않는다.
    async def _set_conditional_status(
        self,
        *,
        order: Order,
        target: ConditionalStatus,
        use_machine: bool = True,
        protect_terminal: bool = True,
        **fields: Any,
    ) -> Order:
        """
        조건부 주문 상태 전이 공통 처리.

        Order.status가 아니라 Order.conditional_status만 갱신한다.
        """
        now = epoch_ms()

        current = order.conditional_status

        if protect_terminal and current in CONDITIONAL_TERMINAL_STATUSES:
            if current != target:
                logger.warning(
                    f"Conditional terminal 상태 덮어쓰기 방지: "
                    f"order_id={order.order_id}, "
                    f"{current.value} -> {target.value}"
                )
                return order

        if use_machine:
            machine = ConditionalOrderStateMachine(current)
            machine.assert_can_transition(target)

        candidate = order.model_copy(deep=True)
        candidate.conditional_status = target
        candidate.updated_ts = now

        for key, value in fields.items():
            if value is None:
                continue

            if not hasattr(candidate, key):
                raise ValueError(
                    f"Invalid Order field for conditional update: "
                    f"order_id={candidate.order_id}, field={key}"
                )

            setattr(candidate, key, value)

        if target == ConditionalStatus.TRIGGERED and candidate.triggered_ts is None:
            candidate.triggered_ts = now

        # [CLAIM] StaleOrderVersionError 처리 추가 필요?
        return await self.state_service.transition_order(
            current_order=order,
            updated_order=candidate,
        )

    async def _mark_rejected(
        self,
        *,
        order: Order,
        reason: RejectReason,
        exchange_error_code: Optional[int] = None,
        detail_msg: Optional[str] = None,
    ) -> Order:
        """REJECTED 상태로 전이."""
        return await self._set_status(
            order=order,
            status=OrderStatus.REJECTED,
            reject_reason=reason,
            exchange_error_code=exchange_error_code,
            detail_msg=_sanitize_detail_msg(detail_msg),
            protect_terminal=True,
        )

    async def _mark_unknown(self, order: Order) -> Order:
        """
        503 Unknown / timeout / 결과 불명 상태 저장.

        UNKNOWN은 terminal이 아니므로 이후 verify_unknown_order() 또는
        User Data Stream 이벤트로 복구해야 한다.
        """
        updated = await self._set_status(
            order=order,
            status=OrderStatus.UNKNOWN,
            reject_reason=RejectReason.UNKNOWN_EXECUTION,
            protect_terminal=True,
        )

        logger.warning(
            f"[UNKNOWN] 주문 실행 결과 불명 ({updated.order_id}). "
            f"get_order(client_order_id={updated.order_id})로 확인 필요."
        )

        return updated

    async def create_internal_order(
        self,
        req: OrderRequest,
        source: OrderSource,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        내부 주문 모델 생성 후:
          1. PostgreSQL 원본 저장
          2. Redis hot state 저장
        """
        now = epoch_ms()
        order = Order(
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
            exchange=req.exchange,
            market_type=req.market_type,
            symbol=req.symbol,
            side=req.side,
            order_type=req.order_type,
            order_route=req.order_route,
            time_in_force=req.time_in_force,
            quantity=req.quantity,
            price=req.price,
            trigger_price=req.trigger_price,
            reduce_only=req.reduce_only,
            close_position=req.close_position,
            created_ts=now,
            updated_ts=now,
            status=OrderStatus.PENDING_NEW,
            position_side=req.position_side,
            position_action=req.position_action,
        )
        # order.order_id는 자동 생성: Snowflake prefix
        order = await self.state_service.create_order(order)
        return order

    # [CLIAM] 현재 exchange, market_type, symbol를 받는 데, 막상 symbol 만 쓰는데, 수정 필요.
    async def _safe_get_local_open_orders_by_symbol(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
    ) -> list[Order]:
        """
        PostgreSQL 원본 기준 특정 심볼 open 주문 조회.

        Redis는 projection이므로, cancelAll 같은 중요한 경로에서는
        OrderStateService를 통해 PostgreSQL 기준으로 조회한다.
        """
        return await self.state_service.list_open_orders_by_symbol(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            refresh_projection=True,
        )


