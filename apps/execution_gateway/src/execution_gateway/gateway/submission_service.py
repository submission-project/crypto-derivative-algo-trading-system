from execution_gateway.gateway.context import GatewayContext
from execution_gateway.gateway.account_service import GatewayAccountService
from execution_gateway.gateway.transition_service import GatewayTransitionService

from execution_gateway.exchange import ExchangeExecutionClient

from typing import Optional
from common.time import epoch_ms
from schemas.order import (
    Order,
    OrderRoute,
    OrderRequest,
    OrderSource,
    OrderStatus,
    RejectReason,
)

from execution_gateway.state_machine.conditional_order_state_machine import (
    ConditionalOrderStateMachine,
)

from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeErrorCategory,
    ExchangeOrderAck,
    ExchangeConditionalAck,
    ExchangeOrderReject,
)

from schemas.market import (
    Exchange,
    MarketType,
)

from execution_gateway.gateway.errors import _map_exchange_error_to_reason

from typing import TypedDict

from common.logging import setup_logger

logger = setup_logger(__name__)

_DETAIL_MSG_MAX_LEN = 8192

class ExchangeMarketLeverageInfo(TypedDict):
    exchange: Exchange
    market_type: MarketType
    symbol: str
    leverage: int

def _sanitize_detail_msg(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return text[:_DETAIL_MSG_MAX_LEN]

class GatewaySubmissionService:
    def __init__(
        self,
        *,
        ctx: GatewayContext,
        transitions: GatewayTransitionService,
        account: GatewayAccountService,
    ) -> None:
        self.ctx = ctx
        self.transitions = transitions
        self.account = account

    async def submit_order(
        self,
        *,
        req: OrderRequest,
        source: OrderSource = OrderSource.MANUAL,
        signal_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
    ) -> Order:
        """
        단건 주문 생성 및 전송.

        ``req.leverage``가 설정되어 있으면 해당 심볼 레버리지를 거래소에 먼저 반영한 뒤 주문한다.

        반환값의 status를 반드시 확인:
          ACKNOWLEDGED → 정상 접수
          REJECTED     → 거래소/시스템 거부
          UNKNOWN      → 503 Unknown 또는 응답 불명
        """

        # 레버리지 값이 들어있으면, 먼저 거래소에 레버러지 변경 요청
        await self._apply_order_request_leverage_if_present(req)

        order = await self.transitions.create_internal_order(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )

        async with self.ctx.locks.lock(order.order_id):
            # params = self._map_to_binance_params(order)

            # [claim] 현재, submitted_ts 같이, **fields 로 받아서 처리하는 데, 일단 변수를 직접 입력하니깐 오타나 의존성 문제가 존재, 테이블 객체를 만들었으니 이걸로 의존성 해결하길 바람
            order = await self.transitions._set_status(
                order=order,
                status=OrderStatus.SUBMITTED,
                submitted_ts=epoch_ms(),
            )

            try:
                # version: 0.0.1
                # await self.rate_limiter.acquire_single_order()
                # resp = await self.adapter.place_order(params)
                # exchange_order_id = str(resp.get("orderId", ""))
                # order = await self._set_status(
                #     order=order,
                #     status=OrderStatus.ACKNOWLEDGED,
                #     exchange_order_id=exchange_order_id,
                # )

                client = self.ctx.client_for_market(
                    exchange=order.exchange,
                    market_type=order.market_type,
                )

                # version: 0.0.2
                # ack = await self._place_exchange_order(order)
                # acknowledged_ts = epoch_ms()
                # update_fields: dict[str, Any] = {
                #     "acknowledged_ts": acknowledged_ts,
                #     "raw_exchange_response": ack.raw,
                # }

                # if isinstance(ack, ExchangeOrderAck):
                #     update_fields["exchange_order_id"] = ack.exchange_order_id

                # elif isinstance(ack, ExchangeConditionalAck):
                #     update_fields["exchange_conditional_id"] = ack.exchange_conditional_id
                #     update_fields["conditional_status"] = ack.conditional_status

                #     exchange_conditional_status = client.get_mapper_exchange_conditional_order_status(ack.conditional_status)

                #     if exchange_conditional_status is None:
                #         raise ValueError(f"unsupported exchange conditional status={ack.conditional_status!r}")
                    
                #     update_fields["exchange_conditional_status"] = exchange_conditional_status
                #     # (
                #     #     ack.raw_status or "NEW"
                #     # )

                # else:
                #     raise ValueError(f"unsupported exchange ack type={type(ack)!r}")
                # order = await self.transitions._set_status(
                #     order=order,
                #     status=OrderStatus.ACKNOWLEDGED,
                #     **update_fields,
                # )
                ack = await self._place_exchange_order(order)
                order = await self._acknowledge_exchange_ack(
                    order=order,
                    ack=ack,
                    client=client,
                )

            # version: 0.1
            # except BinanceUnknownExecutionError as e:
            #     logger.error(f"503 Unknown ({order.order_id}): {e}")
            #     order = await self._mark_unknown(order)

            # except BinanceApiError as e:
            #     reason = _map_binance_error_to_reason(e)
            #     logger.warning(
            #         f"주문 제출 거부 ({order.order_id}): "
            #         f"code={e.code}, msg={e.msg}, reason={reason.value}"
            #     )
            #     order = await self._mark_rejected(
            #         order=order,
            #         reason=reason,
            #         exchange_error_code=e.code,
            #         detail_msg=getattr(e, "msg", None),
            #     )

            # version: 0.2
            except ExchangeApiError as e:
                if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                    logger.error(f"주문 제출 결과 불명 ({order.order_id}): {e}")
                    order = await self.transitions._mark_unknown(order)
                else:
                    reason = _map_exchange_error_to_reason(e)
                    logger.warning(
                        f"주문 제출 거부 ({order.order_id}): "
                        f"exchange={e.exchange.value}, "
                        f"category={e.category.value}, "
                        f"code={e.code}, "
                        f"reason={reason.value}, "
                        f"msg={e.message}"
                    )
                    order = await self.transitions._mark_rejected(
                        order=order,
                        reason=reason,
                        exchange_error_code=e.code,
                        detail_msg=e.message,
                    )

            except Exception as e:
                logger.error(f"내부 오류 ({order.order_id}): {e}", exc_info=True)
                order = await self.transitions._mark_rejected(
                    order=order,
                    reason=RejectReason.INTERNAL_ERROR,
                    exchange_error_code=None,
                    detail_msg=_sanitize_detail_msg(str(e)),
                )

        return order

    # [claim] 기능은 가능하지만, 현재는 submit_order만 사용하도록
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
        """
        일괄 주문 처리.

        - 요청 중 ``leverage``가 있는 경우, 심볼별로(목록 순서상 마지막 값 우선)
          거래소 레버리지를 반영한 뒤 배치 주문을 만든다.
        - 5건씩 분할 전송
        - 응답 순서 = 입력 순서
        - 체결/매칭 순서는 보장 안 됨
        - batch 내부 개별 실패 처리
        - 503 Unknown 발생 시 해당 batch 전체 UNKNOWN
        """
        if not requests:
            return []

        for exchange_market_info in _dedupe_symbol_leverage_updates(requests):
            await self.account.change_leverage(
                exchange=exchange_market_info.exchange,
                market_type=exchange_market_info.market_type,
                symbol=exchange_market_info.symbol,
                leverage=exchange_market_info.leverage,
            )

        orders: list[Order] = []

        # submit_batch_orders()는 일단 REGULAR만 허용
        for req in requests:
            if req.exchange != exchange or req.market_type != market_type:
                raise ValueError(
                    f"batch order market mismatch: "
                    f"request={exchange.value}/{market_type.value}, "
                    f"order_request={req.exchange.value}/{req.market_type.value}, "
                    f"symbol={req.symbol}"
                )

            if req.order_route == OrderRoute.CONDITIONAL:
                raise RuntimeError(
                    "submit_batch_orders currently supports only REGULAR orders. "
                    "Submit CONDITIONAL orders individually with submit_order()."
                )
            order = await self.transitions.create_internal_order(
                req=req,
                source=source,
                signal_id=signal_id,
                strategy_name=strategy_name,
            )
            orders.append(order)

        results: list[Order] = []
        client = self.ctx.client_for_market(exchange=exchange, market_type=market_type)

        if not client.capabilities.supports_batch_order:
            raise RuntimeError(
                f"batch order is not supported: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}"
            )

        batch_size = client.capabilities.max_batch_order_size
        if batch_size <= 0:
            raise RuntimeError(
                f"invalid max_batch_order_size: "
                f"exchange={exchange.value}, "
                f"market_type={market_type.value}, "
                f"max_batch_order_size={batch_size}"
            )

        for i in range(0, len(orders), batch_size):
            batch = orders[i : i + batch_size]

            # SUBMITTED 전이
            for idx, order in enumerate(batch):
                async with self.ctx.locks.lock(order.order_id):
                    batch[idx] = await self.transitions._set_status(
                        order=order,
                        status=OrderStatus.SUBMITTED,
                        submitted_ts=epoch_ms(),
                    )

            try:
                resp_list = await client.place_batch_orders(batch)

                if len(resp_list) != len(batch):
                    logger.warning(
                        f"batch response length mismatch: "
                        f"orders={len(batch)}, responses={len(resp_list)}"
                    )

                orders_by_client_id = {
                    (order.client_order_id or order.order_id): order
                    for order in batch
                }
                matched_order_ids: set[str] = set()

                for item in resp_list:
                    client_order_id = item.client_order_id
                    order = orders_by_client_id.get(client_order_id)
                    if order is None:
                        logger.warning(
                            f"알 수 없는 응답: client_order_id={client_order_id}"
                        )
                        continue

                    matched_order_ids.add(order.order_id)

                    async with self.ctx.locks.lock(order.order_id):
                        if isinstance(item, ExchangeOrderAck):
                            updated = await self.transitions._set_status(
                                order=order,
                                status=item.status,
                                exchange_order_id=item.exchange_order_id,
                                raw_exchange_response=item.raw,
                            )

                        elif isinstance(item, ExchangeOrderReject):
                            logger.warning(
                                f"일괄 주문 일부 실패 ({order.order_id}): "
                                f"code={item.code}, msg={item.message}"
                            )

                            updated = await self.transitions._mark_rejected(
                                order=order,
                                reason=item.reject_reason,
                                exchange_error_code=item.code,
                                detail_msg=item.message,
                            )

                        else:
                            raise RuntimeError(
                                f"Unexpected batch response item: {item!r}"
                            )

                        results.append(updated)

                # 응답 개수가 부족하거나 매칭되지 않은 주문들은 UNKNOWN 처리
                for order in batch:
                    if order.order_id in matched_order_ids:
                        continue

                    async with self.ctx.locks.lock(order.order_id):
                        logger.warning(
                            f"batch response 누락 → UNKNOWN 처리: {order.order_id}"
                        )
                        updated = await self.transitions._mark_unknown(order)
                        results.append(updated)

            except ExchangeApiError as e:
                if e.category == ExchangeErrorCategory.UNKNOWN_EXECUTION:
                    logger.warning(f"batch order result unknown: {e}")

                    for order in batch:
                        async with self.ctx.locks.lock(order.order_id):
                            updated = await self.transitions._mark_unknown(order)
                            results.append(updated)

                    continue

                reason = _map_exchange_error_to_reason(e)
                logger.error(f"일괄 주문 전송 실패: {e}", exc_info=True)

                for order in batch:
                    async with self.ctx.locks.lock(order.order_id):
                        updated = await self.transitions._mark_rejected(
                            order=order,
                            reason=reason,
                            exchange_error_code=e.code,
                            detail_msg=e.message,
                        )
                        results.append(updated)

            except Exception as e:
                logger.warning(f"일괄 주문 내부 오류: {e}", exc_info=True)

                for order in batch:
                    async with self.ctx.locks.lock(order.order_id):
                        updated = await self.transitions._mark_rejected(
                            order=order,
                            reason=RejectReason.INTERNAL_ERROR,
                            exchange_error_code=None,
                            detail_msg=None,
                        )
                        results.append(updated)

        return results

    async def _apply_order_request_leverage_if_present(self, req: OrderRequest) -> None:
        """요청에 ``leverage``가 있으면 레버리지를 맞춘 뒤 주문을 진행한다."""
        if req.leverage is None:
            return
        await self.account.change_leverage(
            exchange=req.exchange,
            market_type=req.market_type,
            symbol=req.symbol,
            leverage=int(req.leverage),
        )

    # Order Router
    async def _place_exchange_order(
        self, order: Order
    ) -> ExchangeOrderAck | ExchangeConditionalAck:
        """
        order_route에 따라 일반 주문 / conditional 주문을 제출한다.

        REGULAR:
        MARKET / LIMIT

        CONDITIONAL:
        STOP_MARKET / STOP_LIMIT
        """
        # version: 0.1
        # await self.rate_limiter.acquire_single_order()
        # return await self.order_router.place(order)

        # version: 0.2
        client = self.ctx.client_for_market(
            exchange=order.exchange,
            market_type=order.market_type,
        )
        if order.order_route == OrderRoute.REGULAR:
            return await client.place_order(order)
        if order.order_route == OrderRoute.CONDITIONAL:
            return await client.place_conditional_order(order)

        raise ValueError(f"unsupported order_route={order.order_route}")

    async def _acknowledge_exchange_ack(
        self,
        *,
        order: Order,
        ack: ExchangeOrderAck | ExchangeConditionalAck,
        client: ExchangeExecutionClient,
    ) -> Order:
        update_fields = {
            "acknowledged_ts": epoch_ms(),
            "raw_exchange_response": ack.raw,
        }

        if isinstance(ack, ExchangeOrderAck):
            update_fields["exchange_order_id"] = ack.exchange_order_id

        elif isinstance(ack, ExchangeConditionalAck):
            ConditionalOrderStateMachine(order.conditional_status).assert_can_transition(
                ack.conditional_status
            )

            update_fields["exchange_conditional_id"] = ack.exchange_conditional_id
            update_fields["conditional_status"] = ack.conditional_status
            exchange_conditional_status = client.get_mapper_exchange_conditional_order_status(ack.conditional_status)
            if exchange_conditional_status is None:
                raise ValueError(f"unsupported exchange conditional status={ack.conditional_status!r}")
            
            update_fields["exchange_conditional_status"] = exchange_conditional_status

        return await self.transitions._set_status(
            order=order,
            status=OrderStatus.ACKNOWLEDGED,
            **update_fields,
        )


def _dedupe_symbol_leverage_updates(
    requests: list[OrderRequest],
) -> list[ExchangeMarketLeverageInfo]:
    """
    배치 주문에서 심볼별 요청 레버리지를 정리한다.

    같은 심볼이 여러 번 나오면 목록상 **뒤쪽** 요청의 레버리지가 적용된다.
    """
    sym_to_lev: dict[str, ExchangeMarketLeverageInfo] = {}
    for r in requests:
        if r.leverage is None:
            continue
        sym_to_lev[
            f"{r.exchange.value}:{r.market_type.value}:{r.symbol.upper()}"
        ] = ExchangeMarketLeverageInfo(
            exchange=r.exchange,
            market_type=r.market_type,
            symbol=r.symbol,
            leverage=int(r.leverage),
        )
    return list(sym_to_lev.values())