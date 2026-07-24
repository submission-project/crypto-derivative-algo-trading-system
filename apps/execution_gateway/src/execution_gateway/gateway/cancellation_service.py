from execution_gateway.gateway.context import GatewayContext
from execution_gateway.gateway.transition_service import GatewayTransitionService

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any
from common.time import epoch_ms
from schemas.order import (
    Order,
    OrderRoute,
    OrderStatus,
    ConditionalStatus,
    TERMINAL_STATUSES,
    CONDITIONAL_TERMINAL_STATUSES,
)

from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeErrorCategory,
)

from schemas.market import (
    Exchange,
    MarketType,
)

from execution_gateway.state_machine.conditional_order_state_machine import (
    ConditionalOrderStateMachine,
    InvalidConditionalTransitionError,
)

from common.logging import setup_logger

from execution_gateway.services.order_state_service import OrderStateService

from common.time import epoch_ms

from execution_gateway.exchange.types import ExchangeCancelResult

from .dto.cancel_service_resp import CancelSkipReason, CancelOrderSkipped, BatchCancelResultStatus, CancelBatchOrderResp

logger = setup_logger(__name__)

_DETAIL_MSG_MAX_LEN = 8192

def _sanitize_detail_msg(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return text[:_DETAIL_MSG_MAX_LEN]

def _terminal_cancel_skip_reason(status: OrderStatus) -> CancelSkipReason:
    if status == OrderStatus.CANCELLED:
        return CancelSkipReason.ALREADY_CANCELLED
    if status == OrderStatus.FILLED:
        return CancelSkipReason.ALREADY_FILLED
    if status == OrderStatus.REJECTED:
        return CancelSkipReason.ALREADY_REJECTED
    if status == OrderStatus.EXPIRED:
        return CancelSkipReason.ALREADY_EXPIRED

    return CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE

def _batch_cancel_result_payload(
    *,
    order_id: str,
    result: BatchCancelResultStatus,
    reason: CancelSkipReason | None = None,
    status: OrderStatus | None = None,
    conditional_status: ConditionalStatus | None = None,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    code: int | str | None = None,
    message: str | None = None,
    raw: dict[str, Any] | None = None,
) -> CancelBatchOrderResp:
    return CancelBatchOrderResp(
        order_id=order_id,
        result=result,
        reason=reason,
        status=status,
        conditional_status=conditional_status,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        code=code,
        message=message,
        raw=raw or {},
    )

class GatewayCancellationService:
    def __init__(
        self,
        *,
        ctx: GatewayContext,
        transitions: GatewayTransitionService,
        state_service: OrderStateService,
    ) -> None:
        self.ctx = ctx
        self.transitions = transitions
        self.state_service = state_service

    def _is_conditional_cancelable(self, order: Order) -> bool:
        """
        조건부 주문 자체를 cancel_algo_order로 취소할 수 있는 상태인지 판단.

        TRIGGERED / FINISHED 이후에는 조건부 주문 lifecycle이 이미 끝났을 수 있으므로
        actual order 취소 또는 reconciliation 대상이다.
        """
        if order.order_route != OrderRoute.CONDITIONAL:
            return False

        if order.conditional_status in CONDITIONAL_TERMINAL_STATUSES:
            return False

        if order.conditional_status is None:
            return True

        machine = ConditionalOrderStateMachine(order.conditional_status)
        try:
            machine.assert_can_transition(ConditionalStatus.CANCELLED)
            return True
        except InvalidConditionalTransitionError:
            return False

    async def _cancel_regular_order(
        self,
        *,
        order: Order,
        previous_status: OrderStatus,
    ) -> ExchangeCancelResult:
        """
        REGULAR 주문 취소.

        """
        order = await self.transitions._set_status(
            order=order,
            status=OrderStatus.PENDING_CANCEL,
        )

        try:
            # version: 0.1
            # await self.rate_limiter.acquire_request_weight(weight=1)

            # resp = await self.adapter.cancel_order(
            #     symbol=symbol,
            #     client_order_id=order.client_order_id or order.order_id,
            # )
            client = self.ctx.client_for_market(
                exchange=order.exchange, market_type=order.market_type
            )
            resp:ExchangeCancelResult = await client.cancel_order(order)

            order = await self.transitions._set_status(
                order=order,
                status=OrderStatus.CANCELLED,
                cancelled_ts=epoch_ms(),
                raw_exchange_response=resp.raw,
            )

            # return resp.raw
            return resp

        # version 0.1
        # except BinanceUnknownExecutionError:
        #     order = await self._mark_unknown(order)
        #     raise

        # except (BinanceApiError, Exception) as e:
        #     logger.error(f"주문 취소 내부 오류 ({order.order_id}): {e}", exc_info=True)

        #     latest = await self.state_service.load_order_from_postgres(order.order_id)

        #     if latest and latest.status not in TERMINAL_STATUSES:
        #         await self._set_status(
        #             order=latest,
        #             status=previous_status,
        #             use_machine=False,
        #             protect_terminal=False,
        #         )

        #     raise

        # version 0.2
        except ExchangeApiError as e:
            if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                order = await self.transitions._mark_unknown(order)
                raise

            logger.error(
                f"주문 취소 거래소 오류 ({order.order_id}): {e}", exc_info=True
            )

            latest = await self.state_service.load_order_from_postgres(order.order_id)
            if latest and latest.status not in TERMINAL_STATUSES:
                await self.transitions._set_status(
                    order=latest,
                    status=previous_status,
                    use_machine=False,
                    protect_terminal=False,
                )

            raise

    async def _cancel_conditional_order(
        self,
        *,
        order: Order,
        previous_status: OrderStatus,
    ) -> ExchangeCancelResult:
        """
        CONDITIONAL 주문 취소.

        성공 시:
        Order.status = CANCELLED
        Order.conditional_status = CANCELED
        """
        if not self._is_conditional_cancelable(order):
            # return {
            #     "skipped": True,
            #     "reason": "conditional_order_not_cancelable",
            #     "order_id": order.order_id,
            #     "status": order.status.value,
            #     "conditional_status": (
            #         order.conditional_status.value if order.conditional_status else None
            #     ),
            #     "triggered_order_id": order.triggered_order_id,
            # }
            raise CancelOrderSkipped(
                order_id=order.order_id,
                reason=CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE,
                status=order.status,
                conditional_status=order.conditional_status,
                triggered_order_id=order.triggered_order_id,
                message="Conditional order is not cancelable in its current state.",
            )

        order = await self.transitions._set_status(
            order=order,
            status=OrderStatus.PENDING_CANCEL,
        )

        try:
            # version: 0.1
            # await self.rate_limiter.acquire_request_weight(weight=1)
            # resp = await self.adapter.cancel_algo_order(
            #     symbol=symbol,
            #     client_algo_id=order.client_conditional_id,
            #     algo_id=order.exchange_conditional_id,
            # )

            # version: 0.2
            client = self.ctx.client_for_market(
                exchange=order.exchange, market_type=order.market_type
            )
            resp:ExchangeCancelResult = await client.cancel_order(order)

            # Order.status와 conditional_status를 같이 terminal로 보정.
            # _set_status()는 fields를 그대로 candidate에 반영하므로 같이 넘긴다.
            order = await self.transitions._set_status(
                order=order,
                status=OrderStatus.CANCELLED,
                cancelled_ts=epoch_ms(),
                conditional_status=ConditionalStatus.CANCELLED,
                exchange_conditional_status=resp.raw_status,
                raw_exchange_response=resp.raw,
            )

            return resp

        # version 0.1
        # except BinanceUnknownExecutionError:
        #     order = await self._mark_unknown(order)
        #     raise

        # except (BinanceApiError, Exception) as e:
        #     logger.error(f"주문 취소 내부 오류 ({order.order_id}): {e}", exc_info=True)

        #     latest = await self._load_order_from_repo(order.order_id)

        #     if latest and latest.status not in TERMINAL_STATUSES:
        #         await self._set_status(
        #             order=latest,
        #             status=previous_status,
        #             use_machine=False,
        #             protect_terminal=False,
        #         )

        #     raise

        # version 0.2
        except ExchangeApiError as e:
            if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                order = await self.transitions._mark_unknown(order)
                raise

            logger.error(
                f"조건부 주문 취소 거래소 오류 ({order.order_id}): {e}", exc_info=True
            )

            latest = await self.transitions._load_order_from_repo(order.order_id)
            if latest and latest.status not in TERMINAL_STATUSES:
                await self.transitions._set_status(
                    order=latest,
                    status=previous_status,
                    use_machine=False,
                    protect_terminal=False,
                )

            raise

    async def cancel_order(self, *, order_id: str) -> ExchangeCancelResult:
        """
        단건 취소.

        Args:
            order_id: 내부 client_order_id

        REGULAR:
          - DELETE /fapi/v1/order

        CONDITIONAL:
          - DELETE /fapi/v1/algoOrder

        주의:
            REST cancel 응답 성공 시 CANCELLED로 반영하지만,
            최종 상태는 User Data Stream 이벤트로 다시 보정하는 것이 좋음.
        """
        async with self.ctx.locks.lock(order_id):
            order = await self.transitions._load_order_from_repo(order_id)
            previous_status = order.status if order else None

            if not order:
                # logger.warning(
                #     f"로컬 주문 없이 cancel_order 요청: {order_id}. "
                #     f"거래소 취소 대상을 결정할 수 없어 중단."
                # )
                # return {
                #     "skipped": True,
                #     "reason": "local_order_not_found",
                #     "order_id": order_id,
                # }
                raise CancelOrderSkipped(
                    order_id=order_id,
                    reason=CancelSkipReason.LOCAL_ORDER_NOT_FOUND,
                    message="Local order was not found; exchange cancel target cannot be resolved.",
                )

            if order.status in TERMINAL_STATUSES:
                # logger.info(
                #     f"이미 terminal 상태라 취소 스킵: "
                #     f"{order_id}, status={order.status.value}"
                # )
                # return {
                #     "skipped": True,
                #     "reason": "terminal_status",
                #     "order_id": order_id,
                #     "status": order.status.value,
                #     "conditional_status": (
                #         order.conditional_status.value
                #         if order.conditional_status
                #         else None
                #     ),
                # }
                raise CancelOrderSkipped(
                    order_id=order.order_id,
                    reason=_terminal_cancel_skip_reason(order.status),
                    status=order.status,
                    conditional_status=order.conditional_status,
                    message=f"Order is already terminal: {order.status.value}",
                )

            if order.order_route == OrderRoute.REGULAR:
                return await self._cancel_regular_order(
                    order=order,
                    previous_status=previous_status,
                )

            if order.order_route == OrderRoute.CONDITIONAL:
                return await self._cancel_conditional_order(
                    order=order,
                    previous_status=previous_status,
                )

            raise RuntimeError(
                f"unsupported order_route for cancel: "
                f"order_id={order.order_id}, order_route={order.order_route}"
            )


    async def cancel_batch_orders(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        order_ids: list[str],
    ) -> list[CancelBatchOrderResp]:
        """
        일괄 취소. 특정 symbol 안에서 지정한 주문들만 취소

        - 10건씩 분할
        - terminal 상태 주문은 제외
        - 로컬에 존재하는 주문만 PENDING_CANCEL로 전이
        - 로컬에 없는 주문은 거래소 취소 요청 대상에는 포함하되, 로컬 상태 저장은 하지 않음
        - batch 응답 원소별 성공/실패 처리
        - 응답 누락 또는 결과 불명은 UNKNOWN으로 전환

        #
        """
        if not order_ids:
            return []

        symbol = symbol.strip().upper()

        client = self.ctx.client_for_market(
            exchange=exchange,
            market_type=market_type,
        )

        batch_size = client.capabilities.max_batch_cancel_size

        if not client.capabilities.supports_batch_cancel:
            raise RuntimeError(
                f"batch cancel is not supported: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}"
            )

        if batch_size <= 0:
            raise RuntimeError(
                f"invalid max_batch_cancel_size: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"max_batch_cancel_size={batch_size}"
            )

        results: list[CancelBatchOrderResp] = []

        for i in range(0, len(order_ids), batch_size):
            raw_batch = order_ids[i : i + batch_size]

            eligible_orders: list[Order] = []
            pending_orders: list[Order] = []
            previous_statuses: dict[str, OrderStatus] = {}
            results_by_order_id: dict[str, CancelBatchOrderResp] = {}

            # Validate the whole batch before mutating any local order state.
            for oid in raw_batch:
                async with self.ctx.locks.lock(oid):
                    order = await self.transitions._load_order_from_repo(oid)

                    if not order:
                        # logger.error(f"로컬 주문 없음. batch cancel 대상 제외: {oid}")
                        results_by_order_id[oid] = _batch_cancel_result_payload(
                            order_id=oid,
                            result=BatchCancelResultStatus.SKIPPED,
                            reason=CancelSkipReason.LOCAL_ORDER_NOT_FOUND,
                            message=(
                                "Local order was not found; exchange cancel target "
                                "cannot be resolved."
                            ),
                        )
                        continue

                    if order.status in TERMINAL_STATUSES:
                        # logger.info(
                        #     f"이미 terminal 상태라 batch cancel 대상 제외: "
                        #     f"{oid}, status={order.status.value}"
                        # )
                        results_by_order_id[oid] = _batch_cancel_result_payload(
                            order_id=order.order_id,
                            result=BatchCancelResultStatus.SKIPPED,
                            reason=_terminal_cancel_skip_reason(order.status),
                            status=order.status,
                            conditional_status=order.conditional_status,
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            message=f"Order is already terminal: {order.status.value}",
                        )
                        continue

                    if order.exchange != exchange or order.market_type != market_type:
                        raise ValueError(
                            f"order market mismatch: "
                            f"request={exchange.value}/{market_type.value}, "
                            f"order={order.exchange.value}/{order.market_type.value}, "
                            f"order_id={order.order_id}"
                        )

                    if order.symbol.upper() != symbol:
                        raise ValueError(
                            f"symbol mismatch for batch cancel: "
                            f"request_symbol={symbol}, "
                            f"order_id={order.order_id}, "
                            f"order_symbol={order.symbol}"
                        )

                    # [claim] 현재 conditional(조건부) 조건 배치 취소는 안됨 -> 수정 필용
                    if order.order_route != OrderRoute.REGULAR:
                        # raise RuntimeError(
                        #     "cancel_batch_orders currently supports only REGULAR orders. "
                        #     "Use cancel_order() for CONDITIONAL orders."
                        # )
                        results_by_order_id[oid] = _batch_cancel_result_payload(
                            order_id=order.order_id,
                            result=BatchCancelResultStatus.SKIPPED,
                            reason=CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE,
                            status=order.status,
                            conditional_status=order.conditional_status,
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            message="Batch cancel supports only regular orders.",
                        )
                        continue

                    eligible_orders.append(order)
                    previous_statuses[order.order_id] = order.status

            for order in eligible_orders:
                async with self.ctx.locks.lock(order.order_id):
                    latest = await self.transitions._load_order_from_repo(order.order_id)
                    # if not latest or latest.status in TERMINAL_STATUSES:
                    #     continue

                    if not latest:
                        results_by_order_id[order.order_id] = _batch_cancel_result_payload(
                            order_id=order.order_id,
                            result=BatchCancelResultStatus.SKIPPED,
                            reason=CancelSkipReason.LOCAL_ORDER_NOT_FOUND,
                            message="Local order disappeared before pending cancel.",
                        )
                        continue

                    if latest.status in TERMINAL_STATUSES:
                        results_by_order_id[order.order_id] = _batch_cancel_result_payload(
                            order_id=latest.order_id,
                            result=BatchCancelResultStatus.SKIPPED,
                            reason=_terminal_cancel_skip_reason(latest.status),
                            status=latest.status,
                            conditional_status=latest.conditional_status,
                            client_order_id=latest.client_order_id,
                            exchange_order_id=latest.exchange_order_id,
                            message=f"Order became terminal: {latest.status.value}",
                        )
                        continue

                    pending = await self.transitions._set_status(
                        order=latest,
                        status=OrderStatus.PENDING_CANCEL,
                    )
                    pending_orders.append(pending)

            if not pending_orders:
                # continue
                for oid in raw_batch:
                    item = results_by_order_id.get(oid)
                    if item:
                        results.append(item)
                continue

            try:
                resp_list = await client.cancel_batch_orders(pending_orders)

                # if len(resp_list) != len(pending_orders):
                #     logger.warning(
                #         f"cancel batch response length mismatch: "
                #         f"orders={len(pending_orders)}, responses={len(resp_list)}"
                #     )

                pending_by_client_id = {
                    (order.client_order_id or order.order_id): order
                    for order in pending_orders
                }
                pending_by_exchange_id = {
                    order.exchange_order_id: order
                    for order in pending_orders
                    if order.exchange_order_id
                }

                matched_order_ids: set[str] = set()

                for resp in resp_list:
                # for order, resp in zip(pending_orders, resp_list):
                    order = None

                    if resp.client_order_id:
                        order = pending_by_client_id.get(resp.client_order_id)

                    if not order and resp.exchange_order_id:
                        order = pending_by_exchange_id.get(resp.exchange_order_id)

                    if not order:
                        logger.error(
                            f"batch cancel 응답 매칭 실패: "
                            f"client_order_id={resp.client_order_id}, "
                            f"exchange_order_id={resp.exchange_order_id}, "
                            f"raw={resp.raw}"
                        )
                        continue

                    matched_order_ids.add(order.order_id)

                    async with self.ctx.locks.lock(order.order_id):
                        latest = await self.transitions._load_order_from_repo(
                            order.order_id
                        )

                        if resp.unknown_execution:
                            if latest and latest.status not in TERMINAL_STATUSES:
                                await self.transitions._mark_unknown(latest)
                            # results.append(resp.raw)
                            results_by_order_id[order.order_id] = (
                                _batch_cancel_result_payload(
                                    order_id=order.order_id,
                                    result=BatchCancelResultStatus.UNKNOWN,
                                    status=latest.status if latest else None,
                                    conditional_status=(
                                        latest.conditional_status if latest else None
                                    ),
                                    client_order_id=resp.client_order_id,
                                    exchange_order_id=resp.exchange_order_id,
                                    message="Exchange cancel execution is unknown.",
                                    raw=resp.raw,
                                )
                            )
                            continue

                        raw = resp.raw
                        item_code = raw.get("code") if isinstance(raw, dict) else None

                        if isinstance(item_code, int) and item_code < 0:
                            logger.error(
                                f"일괄 취소 일부 실패 ({order.order_id}): "
                                f"code={item_code}, msg={raw.get('msg')}"
                            )

                            # 이전 상태로 rollback
                            if latest and order.order_id in previous_statuses:
                                await self.transitions._set_status(
                                    order=latest,
                                    status=previous_statuses[order.order_id],
                                    use_machine=False,
                                    protect_terminal=False,
                                )

                            # results.append(raw)
                            results_by_order_id[order.order_id] = (
                                _batch_cancel_result_payload(
                                    order_id=order.order_id,
                                    result=BatchCancelResultStatus.FAILED,
                                    status=latest.status if latest else None,
                                    conditional_status=(
                                        latest.conditional_status if latest else None
                                    ),
                                    client_order_id=resp.client_order_id,
                                    exchange_order_id=resp.exchange_order_id,
                                    code=item_code,
                                    message=raw.get("msg"),
                                    raw=raw,
                                )
                            )
                            continue

                        if latest:
                            await self.transitions._set_status(
                                order=latest,
                                status=OrderStatus.CANCELLED,
                                cancelled_ts=epoch_ms(),
                                raw_exchange_response=resp.raw,
                            )
                        else:
                            logger.error(
                                f"로컬 주문 없음. 거래소 batch cancel 응답만 수신: {order.order_id}"
                            )

                        # results.append(resp.raw)
                        results_by_order_id[order.order_id] = (
                            _batch_cancel_result_payload(
                                order_id=order.order_id,
                                result=BatchCancelResultStatus.CANCELLED,
                                status=OrderStatus.CANCELLED,
                                client_order_id=resp.client_order_id,
                                exchange_order_id=resp.exchange_order_id,
                                raw=resp.raw,
                            )
                        )

                # 응답 누락 주문은 UNKNOWN
                # if len(resp_list) < len(pending_orders):
                #     missing = pending_orders[len(resp_list) :]
                #     for order in missing:
                #         async with self.ctx.locks.lock(order.order_id):
                #             latest = await self.transitions._load_order_from_repo(
                #                 order.order_id
                #             )
                #             if latest:
                #                 logger.error(
                #                     f"batch cancel response 누락 → UNKNOWN 처리: {order.order_id}"
                #                 )
                #                 await self.transitions._mark_unknown(latest)
                #             else:
                #                 logger.error(
                #                     f"batch cancel response 누락, 로컬 주문 없음: {order.order_id}"
                #                 )
                for order in pending_orders:
                    if order.order_id in matched_order_ids:
                        continue

                    async with self.ctx.locks.lock(order.order_id):
                        latest = await self.transitions._load_order_from_repo(
                            order.order_id
                        )

                        if latest and latest.status not in TERMINAL_STATUSES:
                            logger.error(
                                f"batch cancel response 누락 → UNKNOWN 처리: "
                                f"{order.order_id}"
                            )
                            await self.transitions._mark_unknown(latest)

                        results_by_order_id[order.order_id] = (
                            _batch_cancel_result_payload(
                                order_id=order.order_id,
                                result=BatchCancelResultStatus.UNKNOWN,
                                status=latest.status if latest else None,
                                conditional_status=(
                                    latest.conditional_status if latest else None
                                ),
                                client_order_id=order.client_order_id,
                                exchange_order_id=order.exchange_order_id,
                                message="Missing exchange cancel response.",
                            )
                        )

            except ExchangeApiError as e:
                if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                    logger.error(f"일괄 취소 결과 불명 (batch={raw_batch}): {e}")

                    for order in pending_orders:
                        async with self.ctx.locks.lock(order.order_id):
                            latest = await self.transitions._load_order_from_repo(
                                order.order_id
                            )
                            # if latest:
                            #     await self.transitions._mark_unknown(latest)
                            # else:
                            #     logger.warning(
                            #         f"batch cancel UNKNOWN, 로컬 주문 없음: "
                            #         f"{order.order_id}"
                            #     )
                            if latest and latest.status not in TERMINAL_STATUSES:
                                await self.transitions._mark_unknown(latest)
                    raise

                logger.error(
                    f"일괄 취소 실패 (batch={raw_batch}): {e}",
                    exc_info=True,
                )

                for order in pending_orders:
                    async with self.ctx.locks.lock(order.order_id):
                        latest = await self.transitions._load_order_from_repo(
                            order.order_id
                        )
                        if latest and order.order_id in previous_statuses:
                            await self.transitions._set_status(
                                order=latest,
                                status=previous_statuses[order.order_id],
                                use_machine=False,
                                protect_terminal=False,
                            )

                raise

            except Exception as e:
                logger.error(
                    f"일괄 취소 실패 (batch={raw_batch}): {e}",
                    exc_info=True,
                )

                # 일반 오류는 요청이 거래소에 도달하지 않았을 가능성도 있으므로
                # 이전 상태로 rollback한다.
                for order in pending_orders:
                    async with self.ctx.locks.lock(order.order_id):
                        latest = await self.transitions._load_order_from_repo(
                            order.order_id
                        )
                        if latest and order.order_id in previous_statuses:
                            await self.transitions._set_status(
                                order=latest,
                                status=previous_statuses[order.order_id],
                                use_machine=False,
                                protect_terminal=False,
                            )
                raise

            for oid in raw_batch:
                item = results_by_order_id.get(oid)
                if item:
                    results.append(item)

        return results

    # [claim] 현재는 일반 주문 취소만 가능 -> 이후 conditional order도 지원하도록 확장 필요
    async def cancel_all_regular_open_orders(
        self, *, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> ExchangeCancelResult:
        """
        특정 심볼 전체 미체결 취소.

        보수적 처리 방식:
            1. 로컬 open orders 조회
            2. terminal 제외 후 PENDING_CANCEL 선반영
            3. 거래소 cancelAll 호출
            4. 성공 시에도 로컬 주문을 바로 CANCELLED로 확정하지 않음
            5. User Data Stream CANCELED 이벤트 또는 reconciliation으로 최종 CANCELLED 확정
            6. 503/timeout 등 결과 불명 시 UNKNOWN
            7. 일반 오류 시 PENDING_CANCEL 이전 상태로 rollback

        이유:
            cancelAll REST 응답은 "취소 요청 접수/처리"에 대한 응답이고,
            개별 주문의 최종 상태는 User Data Stream 또는 get_order/openOrders
            reconciliation으로 확인하는 편이 더 안전하다.
        """
        local_open_orders = (
            await self.transitions._safe_get_local_open_orders_by_symbol(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            )
        )

        local_order_ids: list[str] = []
        previous_statuses: dict[str, OrderStatus] = {}

        # 1. 로컬 주문을 PENDING_CANCEL로 선반영
        for order in local_open_orders:
            if not order.order_id:
                continue

            async with self.ctx.locks.lock(order.order_id):
                latest = await self.transitions._load_order_from_repo(order.order_id)
                if not latest:
                    continue

                if latest.status in TERMINAL_STATUSES:
                    continue

                if latest.order_route != OrderRoute.REGULAR:
                    continue

                local_order_ids.append(latest.order_id)
                previous_statuses[latest.order_id] = latest.status

                latest = await self.transitions._set_status(
                    order=latest,
                    status=OrderStatus.PENDING_CANCEL,
                )

        try:
            # version: 0.2
            client = self.ctx.client_for_market(
                exchange=exchange,
                market_type=market_type,
            )
            resp = await client.cancel_all_regular_open_orders(symbol=symbol)

            # 여기서 local_order_ids를 CANCELLED로 확정하지 않는다.
            # 현재 상태는 PENDING_CANCEL 유지.
            #
            # 이후 처리:
            # - User Data Stream ORDER_TRADE_UPDATE: CANCELED 수신 시 CANCELLED
            # - reconciliation worker: openOrders/get_order 조회 후 CANCELLED/UNKNOWN/FILLED 보정

            return resp

        except ExchangeApiError as e:
            if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                logger.error(
                    f"전체 취소 결과 불명: "
                    f"exchange={exchange.value}, "
                    f"market_type={market_type.value}, "
                    f"symbol={symbol}: {e}"
                )

                for oid in local_order_ids:
                    async with self.ctx.locks.lock(oid):
                        order = await self.transitions._load_order_from_repo(oid)

                        if order and order.status not in TERMINAL_STATUSES:
                            await self.transitions._mark_unknown(order)

                raise

            logger.error(
                f"전체 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )

            for oid in local_order_ids:
                async with self.ctx.locks.lock(oid):
                    order = await self.transitions._load_order_from_repo(oid)

                    if order and oid in previous_statuses:
                        await self.transitions._set_status(
                            order=order,
                            status=previous_statuses[oid],
                            use_machine=False,
                            protect_terminal=False,
                        )
            raise

        except Exception as e:
            logger.error(
                f"전체 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )

            for oid in local_order_ids:
                async with self.ctx.locks.lock(oid):
                    order = await self.transitions._load_order_from_repo(oid)

                    if order and oid in previous_statuses:
                        await self.transitions._set_status(
                            order=order,
                            status=previous_statuses[oid],
                            use_machine=False,
                            protect_terminal=False,
                        )

            raise

    async def cancel_all_open_orders(
        self, *, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> dict:
        """
        특정 심볼의 일반 주문과 조건부 주문을 모두 취소한다.

        각 하위 취소 경로가 로컬 상태 선반영, UNKNOWN 처리, rollback을
        담당한다. 한쪽이 실패해도 다른 쪽 취소는 시도하고, 호출자에게
        부분 성공/실패 결과를 구조화해서 반환한다.
        """
        symbol = symbol.upper()
        result: dict[str, Any] = {
            "ok": True,
            "exchange": exchange.value,
            "market_type": market_type.value,
            "symbol": symbol,
            "regular": None,
            "conditional": None,
        }

        try:
            regular_resp = await self.cancel_all_regular_open_orders(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            )
            result["regular"] = {
                "ok": True,
                "response": regular_resp,
            }
        except Exception as e:
            logger.error(
                f"통합 전체 취소 중 일반 주문 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )
            result["ok"] = False
            result["regular"] = {
                "ok": False,
                "error_type": type(e).__name__,
                "error": str(e),
            }

        try:
            conditional_resp = await self.cancel_all_conditional_open_orders(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            )
            result["conditional"] = {
                "ok": True,
                "response": conditional_resp,
            }
        except Exception as e:
            logger.error(
                f"통합 전체 취소 중 조건부 주문 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )
            result["ok"] = False
            result["conditional"] = {
                "ok": False,
                "error_type": type(e).__name__,
                "error": str(e),
            }

        return result

    async def cancel_all_conditional_open_orders(
        self, *, exchange: Exchange, market_type: MarketType, symbol: str
    ) -> ExchangeCancelResult:
        """
        특정 심볼 전체 조건부 미체결 주문 취소.
        성공 응답만으로 개별 조건부 주문을 terminal로 확정하지 않고,
        User Data Stream 또는 reconciliation에서 최종 상태를 확정한다.
        """
        local_open_orders = (
            await self.transitions._safe_get_local_open_orders_by_symbol(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            )
        )

        local_order_ids: list[str] = []
        previous_statuses: dict[str, OrderStatus] = {}

        for order in local_open_orders:
            if not order.order_id:
                continue

            async with self.ctx.locks.lock(order.order_id):
                latest = await self.transitions._load_order_from_repo(order.order_id)
                if not latest:
                    continue

                if latest.status in TERMINAL_STATUSES:
                    continue

                if not self._is_conditional_cancelable(latest):
                    continue

                local_order_ids.append(latest.order_id)
                previous_statuses[latest.order_id] = latest.status

                await self.transitions._set_status(
                    order=latest,
                    status=OrderStatus.PENDING_CANCEL,
                )

        try:
            client = self.ctx.client_for_market(
                exchange=exchange,
                market_type=market_type,
            )
            resp:ExchangeCancelResult = await client.cancel_all_conditional_open_orders(symbol=symbol)

            logger.info(
                f"conditional cancelAll 요청 성공: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}, "
                f"local_pending_cancel_count={len(local_order_ids)}. "
                f"최종 CANCELLED 확정은 User Data Stream/reconciliation에서 처리."
            )

            return resp

        except ExchangeApiError as e:
            if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                logger.error(
                    f"조건부 전체 취소 결과 불명: "
                    f"exchange={exchange.value}, "
                    f"market_type={market_type.value}, "
                    f"symbol={symbol}: {e}"
                )

                for oid in local_order_ids:
                    async with self.ctx.locks.lock(oid):
                        order = await self.transitions._load_order_from_repo(oid)

                        if order and order.status not in TERMINAL_STATUSES:
                            await self.transitions._mark_unknown(order)

                raise

            logger.error(
                f"조건부 전체 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )

            for oid in local_order_ids:
                async with self.ctx.locks.lock(oid):
                    order = await self.transitions._load_order_from_repo(oid)

                    if order and oid in previous_statuses:
                        await self.transitions._set_status(
                            order=order,
                            status=previous_statuses[oid],
                            use_machine=False,
                            protect_terminal=False,
                        )
            raise

        except Exception as e:
            logger.error(
                f"조건부 전체 취소 실패: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"symbol={symbol}: {e}",
                exc_info=True,
            )

            for oid in local_order_ids:
                async with self.ctx.locks.lock(oid):
                    order = await self.transitions._load_order_from_repo(oid)

                    if order and oid in previous_statuses:
                        await self.transitions._set_status(
                            order=order,
                            status=previous_statuses[oid],
                            use_machine=False,
                            protect_terminal=False,
                        )

            raise
