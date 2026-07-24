"""
Execution Gateway Core.

상태 흐름:
  PENDING_NEW → SUBMITTED → ACKNOWLEDGED  (정상)
  PENDING_NEW → SUBMITTED → REJECTED      (거래소 거부)
  PENDING_NEW → SUBMITTED → UNKNOWN       (503 Unknown — 재주문 금지)
  ACKNOWLEDGED → PENDING_CANCEL → CANCELLED
"""

from typing import Any, Optional

from schemas.market import Exchange, MarketType

from common.logging import setup_logger
from common.time import epoch_ms
from schemas.order import (
    Order,
    OrderRequest,
    OrderSource,
    OrderStatus,
    RejectReason,
    TERMINAL_STATUSES,
)
from schemas.conditional_order_event import NormalizedConditionalOrderEvent

from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

from execution_gateway.services.order_state_service import OrderStateService

from execution_gateway.state_machine.order_state_machine import OrderStateMachine

from execution_gateway.state_machine.conditional_order_state_machine import (
    InvalidConditionalTransitionError,
)

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry

from execution_gateway.gateway.context import GatewayContext
from execution_gateway.gateway.transition_service import GatewayTransitionService
from execution_gateway.gateway.account_service import GatewayAccountService
from execution_gateway.gateway.submission_service import GatewaySubmissionService
from execution_gateway.gateway.cancellation_service import GatewayCancellationService

from execution_gateway.exchange import (
    ExchangeConditionalSnapshot,
    ExchangeOrderSnapshot,
    ExchangeCancelResult
)

from schemas.order_update_event import NormalizedOrderUpdateEvent

from .dto.cancel_service_resp import CancelBatchOrderResp


logger = setup_logger(__name__)

_DETAIL_MSG_MAX_LEN = 8192

# [deprecated]
# _BINANCE_ORDER_TYPE_MAP = {
#     OrderType.MARKET: "MARKET",
#     OrderType.LIMIT: "LIMIT",
#     OrderType.STOP_MARKET: "STOP_MARKET",
#     OrderType.STOP_LIMIT: "STOP",
# }


def _sanitize_detail_msg(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return text[:_DETAIL_MSG_MAX_LEN]

class ExecutionGateway:
    """
    전략이 만든 주문 의도를 실제 거래소 주문으로 바꿔서 보내고, 그 주문의 상태 생명주기를 관리하는 실행 게이트웨이

    - REST 경로: batch 주문/취소, 조회, 복구
    - WS 경로: 초저지연 단건 주문
    - Redis 상태 저장
    - UNKNOWN 복구 보조
    """

    def __init__(
        self,
        # adapter: BinanceRestAdapter,
        state_repo: OrderStateRedisRepository,
        state_service: OrderStateService,
        exchange_clients: ExchangeExecutionClientRegistry,
    ):
        # self.adapter = adapter
        self.state_repo = state_repo
        self.state_service = state_service
        self.exchange_clients = exchange_clients
        self.transitions = GatewayTransitionService(state_service=state_service)
        self.ctx = GatewayContext(exchange_clients=exchange_clients)
        self.account_service = GatewayAccountService(ctx=self.ctx)
        self.submission_service = GatewaySubmissionService(
            ctx=self.ctx,
            transitions=self.transitions,
            account=self.account_service,
        )
        self.cancellation_service = GatewayCancellationService(
            ctx=self.ctx,
            transitions=self.transitions,
            state_service=self.state_service,
        )

    # ──────────────────────────── Order API ────────────────────────────

    async def submit_order(
        self,
        req: OrderRequest,
        source: OrderSource = OrderSource.MANUAL,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        return await self.submission_service.submit_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    async def submit_batch_orders(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        requests: list[OrderRequest],
        source: OrderSource = OrderSource.MANUAL,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> list[Order]:
        return await self.submission_service.submit_batch_orders(
            exchange=exchange,
            market_type=market_type,
            requests=requests,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

    # ──────────────────────────── Cancel API ────────────────────────────

    async def cancel_order(self, order_id: str) -> ExchangeCancelResult:
        return await self.cancellation_service.cancel_order(
            order_id=order_id
        )

    async def cancel_batch_orders(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        order_ids: list[str],
    ) -> list[CancelBatchOrderResp]:
        return await self.cancellation_service.cancel_batch_orders(
            exchange=exchange, market_type=market_type, symbol=symbol, order_ids=order_ids
        )

    # [claim] 현재는 일반 주문 취소만 가능 -> 이후 conditional order도 지원하도록 확장 필요
    async def cancel_all_regular_open_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> ExchangeCancelResult:
        return await self.cancellation_service.cancel_all_regular_open_orders(
            exchange=exchange, market_type=market_type, symbol=symbol
        )

    async def cancel_all_open_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> dict:
        return await self.cancellation_service.cancel_all_open_orders(
            exchange=exchange, market_type=market_type, symbol=symbol
        )

    async def cancel_all_conditional_open_orders(
        self, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> ExchangeCancelResult:
        return await self.cancellation_service.cancel_all_conditional_open_orders(
            exchange=exchange, market_type=market_type, symbol=symbol
        )

    async def change_leverage(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        leverage: int,
    ):
        return await self.account_service.change_leverage(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            leverage=leverage,
        )

    async def get_symbol_price_ticker(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
    ) -> dict[str, Any]:
        client = self.exchange_clients.get(exchange=exchange, market_type=market_type)
        if not client:
            raise ValueError(f"Exchange client not found for {exchange}:{market_type}")
        return await client.get_symbol_price_ticker(symbol)

    async def apply_order_update_event(
        self,
        event: NormalizedOrderUpdateEvent,
    ) -> Order | None:
        """
        거래소별 listener/mapper가 정규화한 주문 이벤트를 로컬 주문 상태에 반영한다.
        """
        async with self.ctx.locks.lock(event.client_order_id):
            # order = await self.transitions._load_order_from_repo(
            #     event.client_order_id
            # )
            order = await self._resolve_order_for_order_update_event(event)

            if not order:
                logger.warning(
                    f"로컬 projection / PostgreSQL 원본에서 찾을 수 없는 주문 이벤트 수신: "
                    f"exchange={event.exchange.value}, "
                    f"market_type={event.market_type.value}, "
                    f"client_order_id={event.client_order_id}, "
                    f"raw={event.raw}"
                )
                return None

            target_status = event.target_status
            if target_status is None:
                return order

            # UDS/WS 이벤트는 중복 또는 역순으로 도착할 수 있으므로,
            # 불가능한 전이는 오류로 처리하지 않고 stale event로 간주해 무시한다.
            machine = OrderStateMachine(order)
            if not machine.can_transition(target_status):
                return order

            update_fields = {}

            if event.exchange_order_id is not None:
                update_fields["exchange_order_id"] = event.exchange_order_id

            if event.filled_quantity is not None:
                update_fields["filled_quantity"] = event.filled_quantity

            if event.avg_fill_price is not None:
                update_fields["avg_fill_price"] = event.avg_fill_price

            if target_status == OrderStatus.REJECTED:
                update_fields["reject_reason"] = RejectReason.EXCHANGE_REJECTED
                if event.reject_reason_text:
                    update_fields["detail_msg"] = event.reject_reason_text

            # 실제 저장 경로에서는 공통 transition guard를 유지한다.
            return await self.transitions._set_status(
                order=order,
                status=target_status,
                use_machine=True,
                protect_terminal=True,
                **update_fields,
            )

    # @staticmethod
    # def _map_exchange_status(exchange_status: str | None) -> Optional[OrderStatus]:
    #     """
    #     Binance REST / User Data Stream 상태 문자열을 내부 OrderStatus로 변환.
    #     """
    #     status_map = {
    #         "NEW": OrderStatus.ACKNOWLEDGED,
    #         "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    #         "FILLED": OrderStatus.FILLED,
    #         "CANCELED": OrderStatus.CANCELLED,
    #         "CANCELLED": OrderStatus.CANCELLED,
    #         "EXPIRED": OrderStatus.EXPIRED,
    #         "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
    #         "REJECTED": OrderStatus.REJECTED,
    #     }

    #     if not exchange_status:
    #         return None

    #     return status_map.get(exchange_status)

    async def apply_reconciliation_order_snapshot(
        self,
        *,
        order_id: str,
        snapshot: ExchangeOrderSnapshot,
    ) -> Order | None:
        """
        거래소 단건 조회 스냅샷을 로컬 주문 상태에 반영.
        """
        # return await self.apply_reconciliation_snapshot(
        #     order_id=order_id,
        #     snapshot=snapshot.raw,
        # )
        async with self.ctx.locks.lock(order_id):
            order = await self.transitions._load_order_from_repo(order_id)

            if not order:
                logger.error(f"reconciliation 대상 주문 없음: order_id={order_id}")
                return None

            # exchange_status = snapshot.get("status")
            # target_status = self._map_exchange_status(exchange_status)
            target_status = snapshot.status

            # if target_status is None:
            #     logger.error(
            #         f"reconciliation 알 수 없는 거래소 상태: "
            #         f"order_id={order_id}, exchange_status={exchange_status}, snapshot={snapshot}"
            #     )
            #     return order

            # Terminal 보호: 이미 terminal인 주문을 다른 상태로 되돌리지 않는다.
            # 같은 terminal 상태의 재관측(idempotent)은 허용한다.
            if order.status in TERMINAL_STATUSES and order.status != target_status:
                logger.info(
                    f"reconciliation terminal 보호: "
                    f"order_id={order_id}, current={order.status.value}, "
                    f"exchange={target_status.value}"
                )
                return order

            updated = await self.transitions._set_status(
                order=order,
                status=target_status,
                use_machine=False,
                protect_terminal=False,
                exchange_order_id=snapshot.exchange_order_id or order.exchange_order_id,
                filled_quantity=snapshot.filled_quantity,
                avg_fill_price=snapshot.avg_fill_price,
                raw_exchange_response=snapshot.raw,
            )

            logger.info(
                f"reconciliation 상태 반영 완료: "
                f"order_id={order_id}, "
                f"{order.status.value} -> {updated.status.value}"
            )

            return updated

    async def apply_conditional_order_snapshot(
        self,
        *,
        snapshot: ExchangeConditionalSnapshot,
    ) -> Order | None:
        event = NormalizedConditionalOrderEvent(
            exchange=snapshot.exchange,
            market_type=snapshot.market_type,
            symbol=snapshot.symbol,
            client_conditional_id=snapshot.client_conditional_id,
            exchange_conditional_id=snapshot.exchange_conditional_id,
            target_status=snapshot.conditional_status,
            exchange_conditional_status=snapshot.raw_status or snapshot.conditional_status.value,
            triggered_order_id=snapshot.triggered_order_id,
            triggered_client_order_id=snapshot.triggered_client_order_id,
            filled_quantity=snapshot.filled_quantity,
            avg_fill_price=snapshot.avg_fill_price,
            reject_reason_text=None,
            event_time=epoch_ms(),
            transaction_time=None,
            raw=snapshot.raw,
        )

        return await self.apply_conditional_order_event(event)

    async def mark_reconciliation_unresolved(
        self,
        *,
        order_id: str,
        exchange_error_code: int | None = None,
        detail_msg: str | None = None,
        raw_exchange_response: dict | None = None,
    ) -> Order | None:
        async with self.ctx.locks.lock(order_id):
            order = await self.transitions._load_order_from_repo(order_id)
            if order is None:
                return None

            if order.status in TERMINAL_STATUSES:
                return order

            # [claim] 처음에는 use_machine=False가 안전합니다. 이후 state machine에 정식 전이를 추가해도 됩
            return await self.transitions._set_status(
                order=order,
                status=OrderStatus.RECONCILE_UNRESOLVED,
                use_machine=False,
                protect_terminal=True,
                reject_reason=RejectReason.UNKNOWN_EXECUTION,
                exchange_error_code=exchange_error_code,
                detail_msg=detail_msg,
                raw_exchange_response=raw_exchange_response,
            )

    async def apply_reconciliation_snapshot(
        self,
        *,
        order_id: str,
        snapshot: dict[str, Any],
    ) -> Optional[Order]:
        """
        거래소 단건 조회 스냅샷을 로컬 주문 상태에 반영.

        사용처:
          - ReconciliationWorker
          - 향후 관리자 수동 보정
          - User Data Stream 유실 복구

        주의:
          - reconciliation은 거래소 스냅샷을 기준으로 강제 보정하는 경로이므로
            state machine 전이 검증을 우회한다.
          - 단, order_id별 lock은 반드시 사용한다.
        """
        """
        Deprecated: Binance raw snapshot 호환용.

        신규 recovery/reconciliation 경로에서는
        apply_reconciliation_order_snapshot()을 사용해야 한다.
        """
        raise NotImplementedError("Deprecated: Binance raw snapshot 호환용.")
        # async with self.ctx.locks.lock(order_id):
        #     order = await self.transitions._load_order_from_repo(order_id)

        #     if not order:
        #         logger.error(
        #             f"reconciliation 대상 주문을 projection / PostgreSQL 원본에서 찾지 못함: "
        #             f"order_id={order_id}"
        #         )
        #         return None

        #     exchange_status = snapshot.get("status")
        #     target_status = self._map_exchange_status(exchange_status)

        #     if target_status is None:
        #         logger.error(
        #             f"reconciliation 알 수 없는 거래소 상태: "
        #             f"order_id={order_id}, exchange_status={exchange_status}, snapshot={snapshot}"
        #         )
        #         return order

        #     # Terminal 보호: 이미 terminal인 주문을 다른 상태로 되돌리지 않는다.
        #     # 같은 terminal 상태의 재관측(idempotent)은 허용한다.
        #     if order.status in TERMINAL_STATUSES and order.status != target_status:
        #         logger.info(
        #             f"reconciliation terminal 보호: "
        #             f"order_id={order_id}, "
        #             f"current={order.status.value}, "
        #             f"exchange={target_status.value}. "
        #             f"이미 terminal 상태이므로 상태 변경을 건너뜁니다."
        #         )
        #         return order

        #     updated = await self.transitions._set_status(
        #         order=order,
        #         status=target_status,
        #         use_machine=False,
        #         protect_terminal=False,
        #         exchange_order_id=str(snapshot.get("orderId", "")),
        #         filled_quantity=str(snapshot.get("executedQty", order.filled_quantity)),
        #         avg_fill_price=str(snapshot.get("avgPrice", order.avg_fill_price)),
        #     )

        #     logger.info(
        #         f"reconciliation 상태 반영 완료: "
        #         f"order_id={order_id}, "
        #         f"{order.status.value} -> {updated.status.value}"
        #     )

        #     return updated

    async def apply_conditional_order_event(
        self,
        event: NormalizedConditionalOrderEvent,
    ) -> Order | None:
        """
        거래소별 조건부 주문 이벤트를 로컬 주문 상태에 반영한다.

        Binance ALGO_UPDATE, OKX algo event, Bitget plan event, Bybit conditional event는
        listener/adapter에서 NormalizedConditionalOrderEvent로 변환된 뒤
        이 메서드로 들어와야 한다.
        """
        order: Order | None = None

        if event.client_conditional_id:
            order = await self.state_service.load_order_by_client_conditional_id(
                exchange=event.exchange,
                market_type=event.market_type,
                client_conditional_id=event.client_conditional_id,
                refresh_projection=True,
            )

        if order is None and event.exchange_conditional_id:
            order = await self.state_service.load_order_by_exchange_conditional_id(
                exchange=event.exchange,
                market_type=event.market_type,
                exchange_conditional_id=event.exchange_conditional_id,
                refresh_projection=True,
            )

        if order is None:
            logger.warning(
                f"조건부 주문 이벤트 대상 로컬 주문 없음: "
                f"exchange={event.exchange.value}, "
                f"symbol={event.symbol}, "
                f"client_conditional_id={event.client_conditional_id}, "
                f"exchange_conditional_id={event.exchange_conditional_id}, "
                f"target_status={event.target_status.value}"
            )
            return None

        try:
            updated = await self.transitions._set_conditional_status(
                order=order,
                target=event.target_status,
                use_machine=True,
                protect_terminal=True,
                exchange_conditional_id=event.exchange_conditional_id,
                exchange_conditional_status=event.exchange_conditional_status,
                triggered_order_id=event.triggered_order_id,
                triggered_client_order_id=event.triggered_client_order_id,
                filled_quantity=event.filled_quantity,
                avg_fill_price=event.avg_fill_price,
                detail_msg=event.reject_reason_text,
                raw_exchange_response=event.raw,
            )

        except InvalidConditionalTransitionError as e:
            logger.warning(
                f"조건부 주문 상태 전이 무시: "
                f"order_id={order.order_id}, "
                f"{e.current} -> {e.target}, "
                f"raw_status={event.exchange_conditional_status}, "
                f"raw={event.raw}"
            )
            return order

        logger.info(
            f"조건부 주문 이벤트 반영 완료: "
            f"order_id={updated.order_id}, "
            f"conditional_status={updated.conditional_status.value if updated.conditional_status else None}, "
            f"exchange_conditional_status={updated.exchange_conditional_status}, "
            f"triggered_order_id={updated.triggered_order_id}"
        )

        return updated


    # ──────────────────────────── Helper ────────────────────────────
    async def _resolve_order_for_order_update_event(
        self,
        event: NormalizedOrderUpdateEvent,
    ) -> Order | None:
        # 1. 기존 경로: client_order_id가 내부 order_id인 경우
        order = await self.transitions._load_order_from_repo(event.client_order_id)
        if order:
            return order

        # 2. 거래소 실제 order id로 기존 regular order 조회
        if event.exchange_order_id:
            order = await self.state_service.load_order_by_exchange_order_id(
                exchange=event.exchange,
                market_type=event.market_type,
                exchange_order_id=event.exchange_order_id,
                refresh_projection=True,
            )
            if order:
                return order

        # 3. 조건부 주문이 trigger해서 생긴 actual order id로 parent conditional order 조회
        if event.exchange_order_id:
            order = await self.state_service.load_order_by_triggered_order_id(
                exchange=event.exchange,
                market_type=event.market_type,
                triggered_order_id=event.exchange_order_id,
                refresh_projection=True,
            )
            if order:
                return order

        return None

    



    # ──────────────────────────── Query / Recovery ────────────────────────────
    # 현재 -> reconciliation 및 recovery worker 에서 처리
    # async def verify_unknown_order(self, symbol: str, order_id: str) -> Order | None:
    #     """
    #     Deprecated:
    #         Binance adapter/raw response 기반 UNKNOWN 주문 검증 함수.

    #         다중 거래소 구조에서는 RecoveryWorker 또는
    #         verify_unknown_order_v2(order_id)를 사용해야 한다.

    #         TODO:
    #         - ExchangeExecutionClient.get_order(order)
    #         - ExchangeOrderSnapshot
    #         - apply_reconciliation_order_snapshot()
    #         기반으로 교체 후 제거
    #     """

    #     """
    #     UNKNOWN 상태 주문의 실제 상태를 거래소에서 조회.
    #     503 Unknown 발생 후 호출하여 주문 상태를 복구.
    #     """
    #     async with self.ctx.locks.lock(order_id):
    #         try:
    #             resp = await self.adapter.get_order(
    #                 symbol=symbol,
    #                 client_order_id=order_id,
    #             )

    #             order = await self.transitions._load_order_from_repo(order_id)
    #             if not order:
    #                 return None

    #             exchange_status = resp.get("status")

    #             status_map = {
    #                 "NEW": OrderStatus.ACKNOWLEDGED,
    #                 "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    #                 "FILLED": OrderStatus.FILLED,
    #                 "CANCELED": OrderStatus.CANCELLED,
    #                 "EXPIRED": OrderStatus.EXPIRED,
    #                 "REJECTED": OrderStatus.REJECTED,
    #             }

    #             new_status = status_map.get(exchange_status, OrderStatus.UNKNOWN)
    #             exchange_order_id = str(resp.get("orderId", ""))

    #             order = await self.transitions._set_status(
    #                 order=order,
    #                 status=new_status,
    #                 use_machine=False,
    #                 protect_terminal=False,
    #                 exchange_order_id=exchange_order_id,
    #                 filled_quantity=str(resp.get("executedQty", order.filled_quantity)),
    #                 avg_fill_price=str(resp.get("avgPrice", order.avg_fill_price)),
    #             )

    #             return order

    #         except BinanceApiError as e:
    #             logger.error(f"UNKNOWN 주문 조회 실패 ({order_id}): {e}")

    #             order = await self.transitions._load_order_from_repo(order_id)
    #             if not order:
    #                 return None

    #             machine = OrderStateMachine(order)
    #             if not machine.can_transition(OrderStatus.UNKNOWN):
    #                 logger.warning(
    #                     f"UNKNOWN 전이 불가로 상태 변경 생략: "
    #                     f"order_id={order_id}, "
    #                     f"current_status={order.status.value}, "
    #                     f"target_status={OrderStatus.UNKNOWN.value}"
    #                 )
    #                 return None

    #             await self.transitions._mark_unknown(order)
    #             return order

    #         except Exception as e:
    #             logger.error(
    #                 f"UNKNOWN 주문 상태 복구 내부 오류 ({order_id}): {e}",
    #                 exc_info=True,
    #             )
    #             return None


    # ──────────────────────────── Internal helpers ───────────────────────────

    # [deprecated]
    # def _map_to_binance_params(self, order: Order) -> dict:
    #     """
    #     MARKET / LIMIT 일반 주문을 Binance /fapi/v1/order params로 변환.
    #     """

    #     params: dict[str, Any] = {
    #         "symbol": order.symbol,
    #         "side": order.side.value,
    #         "type": _BINANCE_ORDER_TYPE_MAP[order.order_type],
    #         "quantity": order.quantity,
    #         "newClientOrderId": order.order_id,
    #     }

    #     if order.position_side is not None:
    #         # One-way BOTH는 보내도 보통 문제는 없지만,
    #         # Hedge Mode 호환성을 위해 내부 설정에 따라 제어할 수 있다.
    #         params["positionSide"] = order.position_side.value

    #     if order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
    #         params["price"] = order.price

    #     if order.order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT):
    #         params["stopPrice"] = order.stop_price

    #     if order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
    #         if order.time_in_force is not None:
    #             params["timeInForce"] = order.time_in_force.value

    #     # One-way mode reduceOnly만 허용
    #     if order.reduce_only and order.position_side == PositionSide.BOTH:
    #         params["reduceOnly"] = "true"

    #     return params




    # [deprecated]
    # async def submit_order_ws(
    #     self,
    #     req: OrderRequest,
    #     source: OrderSource = OrderSource.MANUAL,
    #     signal_id: Optional[str] = None,
    #     strategy_name: Optional[str] = None,
    #     allow_rest_fallback: bool = False,
    # ) -> Order:
    #     """
    #     초저지연 단건 주문 WebSocket Trade API 경로.

    #     기본적으로 WS adapter가 없으면 실패.
    #     REST fallback은 명시적으로 allow_rest_fallback=True일 때만 허용.
    #     """

    #     if req.order_route == OrderRoute.CONDITIONAL:
    #         raise RuntimeError(
    #             "submit_order_ws does not support CONDITIONAL orders. "
    #             "Use submit_order() for STOP_MARKET / STOP_LIMIT."
    #         )

    #     if not self.ws_adapter:
    #         if allow_rest_fallback:
    #             logger.warning("WsTradeAdapter 없음 — REST fallback")
    #             return await self.submit_order(req, source, signal_id, strategy_name)

    #         raise RuntimeError("WsTradeAdapter is not configured")

    #     await self._apply_order_request_leverage_if_present(req)
    #     order = await self._create_internal_order(
    #         req=req,
    #         source=source,
    #         signal_id=signal_id,
    #         strategy_name=strategy_name,
    #     )

    #     async with self._locks.lock(order.order_id):
    #         params = self._map_to_binance_params(order)

    #         order = await self._set_status(
    #             order=order,
    #             status=OrderStatus.SUBMITTED,
    #             submitted_ts=_NOW_MS(),
    #         )

    #         try:
    #             await self.rate_limiter.acquire_single_order()

    #             resp = await self.ws_adapter.place_order(params)

    #             exchange_order_id = str(resp.get("orderId", ""))

    #             order = await self._set_status(
    #                 order=order,
    #                 status=OrderStatus.ACKNOWLEDGED,
    #                 exchange_order_id=exchange_order_id,
    #             )

    #             logger.info(
    #                 f"[WS] 주문 접수 성공: "
    #                 f"{order.order_id} → exchange_id={exchange_order_id}"
    #             )

    #         except WsTradeError as e:
    #             logger.error(f"[WS] 주문 거부: code={e.code}, msg={e.msg}")
    #             api_err = BinanceApiError(e.code, e.msg)
    #             order = await self._mark_rejected(
    #                 order=order,
    #                 reason=_map_binance_error_to_reason(api_err),
    #                 exchange_error_code=api_err.code,
    #                 detail_msg=getattr(api_err, "msg", None),
    #             )

    #         except asyncio.TimeoutError:
    #             logger.error(f"[WS] 응답 타임아웃 ({order.order_id}) — UNKNOWN 전환")
    #             order = await self._mark_unknown(order)

    #         except Exception as e:
    #             logger.error(f"[WS] 내부 오류 ({order.order_id}): {e}", exc_info=True)
    #             order = await self._mark_rejected(
    #                 order=order,
    #                 reason=RejectReason.INTERNAL_ERROR,
    #                 exchange_error_code=None,
    #                 detail_msg=None,
    #             )

    #     return order



# [deprecated]
# def _map_binance_error_to_reason(e: BinanceApiError) -> RejectReason:
#     """Binance 에러코드 → 내부 RejectReason 매핑."""
#     """
#     Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code?utm_source=chatgpt.com
#     """
#     code = e.code

#     if isinstance(
#         e, (BinanceRateLimitError, BinanceIpBanError, BinanceSystemThrottleError)
#     ):
#         return RejectReason.RATE_LIMITED

#     if isinstance(e, (BinanceServiceUnavailableError, BinanceInternalRetryableError)):
#         return RejectReason.EXCHANGE_REJECTED

#     if code in (-2018, -2019):
#         return RejectReason.INSUFFICIENT_BALANCE

#     if code == -2010:
#         msg = getattr(e, "msg", "") or ""
#         msg_lower = msg.lower()
#         if "insufficient" in msg_lower or "margin" in msg_lower:
#             return RejectReason.INSUFFICIENT_BALANCE
#         return RejectReason.EXCHANGE_REJECTED

#     # 현재 RejectReason에 INVALID_PARAMETER / FILTER_VIOLATION이 없다면 임시로 INVALID_SYMBOL에 묶음
#     if code in (-1100, -1102, -1111, -1116, -1121):
#         return RejectReason.INVALID_SYMBOL

#     return RejectReason.EXCHANGE_REJECTED
