"""
대상:
- 일반주문: SUBMITTED, PENDING_CANCEL, UNKNOWN, 
- 조건부 주문: NEW, ACTIVE, UNKNOWN, None, ""

방식:
Redis recovery index에 들어간 의심 주문만 단건 조회

목적:
주문 제출/취소 직후 UDS 이벤트 누락, 503 unknown, timeout 같은 상황을 빠르게 수습
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from common.logging import setup_logger

from schemas.conditional_order_event import NormalizedConditionalOrderEvent

from execution_gateway.gateway import (
    ExecutionGateway,
)

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
)

from execution_gateway.exchange import (
    ExchangeApiError,
)

from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.exchange.client import ExchangeExecutionClient

from common.time import epoch_ms

logger = setup_logger(__name__)
    
# def _algo_row_client_id(row: dict[str, Any]) -> str | None:
#     value = row.get("clientAlgoId")
#     if value in (None, ""):
#         return None
#     return str(value)


# def _algo_row_exchange_id(row: dict[str, Any]) -> str | None:
#     value = row.get("algoId")
#     if value in (None, ""):
#         return None
#     return str(value)

class RecoveryWorker:
    """
    Redis recovery index 기반 빠른 주문 복구 워커.

    대상:
      - SUBMITTED
      - PENDING_CANCEL
      - UNKNOWN
      - CONDITIONAL open/recovery 대상

    Redis repo는 위 상태들을 order:recovery ZSet에 넣는다.
    이 worker는 일정 시간 이상 해당 상태에 머문 주문을 execute_client get_order()
    로 확인하고, Gateway를 통해 PostgreSQL 원본 상태와 Redis projection을 보정한다.

    ReconciliationWorker와 차이:
      - RecoveryWorker:
          의심 주문만 빠르게 단건 조회
      - ReconciliationWorker:
          거래소 openOrders 전체와 로컬 openOrders 전체를 대사
    """

    def __init__(
        self,
        *,
        exchange_clients: ExchangeExecutionClientRegistry,
        gateway: ExecutionGateway,
        repo: OrderStateRedisRepository,
        markets: list[tuple[Exchange, MarketType]],
        interval_sec: float = 3.0,
        older_than_ms: int = 2_000,
        batch_size: int = 100,
        failure_backoff_ms: int = 10_000,
    ) -> None:
        self.exchange_clients = exchange_clients
        self.gateway = gateway
        self.redis_order_repo = repo
        self.markets = tuple(markets)

        self.interval_sec = interval_sec
        self.older_than_ms = older_than_ms
        self.batch_size = batch_size
        self.failure_backoff_ms = failure_backoff_ms

        self._running = False
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                for exchange, market_type in self.markets:
                    await self.recover_once(
                        exchange=exchange,
                        market_type=market_type,
                    )
                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(
                    f"RecoveryWorker loop error: {e}",
                    exc_info=True,
                )
                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

    async def start(self) -> None:
        if self._running:
            logger.warning("RecoveryWorker가 이미 실행 중입니다.")
            return

        self._running = True
        self._stop_event.clear()

        self._task = asyncio.create_task(
            self._run_loop(),
            name="recovery-worker",
        )

        logger.info(
            f"RecoveryWorker 시작 "
            f"(interval={self.interval_sec}s, "
            f"older_than_ms={self.older_than_ms}, "
            f"batch_size={self.batch_size}, "
            f"failure_backoff_ms={self.failure_backoff_ms})"
        )

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        logger.info("RecoveryWorker 종료 완료")

    async def _sleep_or_stop(self, delay_sec: float) -> bool:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=delay_sec,
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def recover_once(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> None:
        """
        오래된 recovery 대상 주문을 한 번 스캔하고 복구 시도.
        """
        older_than_ts = epoch_ms() - self.older_than_ms  

        orders = await self.redis_order_repo.list_recovery_orders(
            exchange=exchange.value,
            market_type=market_type.value,
            batch_size=self.batch_size,
            older_than_ts=older_than_ts,
        )

        if not orders:
            logger.debug("RecoveryWorker: 복구 대상 없음")
            return

        logger.info(
            f"RecoveryWorker: 복구 대상 {len(orders)}건 조회 "
            f"(older_than_ts={older_than_ts})"
        )

        seen_order_ids: set[str] = set()

        for row in orders:
            order_id = str(row.get("order_id") or "")

            if not order_id:
                logger.warning(f"RecoveryWorker: order_id 없는 row 무시: {row}")
                continue

            if order_id in seen_order_ids:
                continue

            seen_order_ids.add(order_id)
            recovered = await self._recover_one(
                local_order=row,
                exchange=exchange,
                market_type=market_type,
            )

            if not recovered:
                logger.error(f"RecoveryWorker: recovery 실패, 재시도 예정 order_id={order_id}")
                await self._postpone_recovery_order(
                    exchange=exchange,
                    market_type=market_type,
                    order_id=order_id,
                )

    async def _postpone_recovery_order(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        order_id: str,
    ) -> None:
        next_attempt_ts = epoch_ms() + self.failure_backoff_ms

        await self.redis_order_repo.postpone_recovery_order(
            exchange=exchange.value,
            market_type=market_type.value,
            order_id=order_id,
            next_attempt_ts=next_attempt_ts,
        )

        logger.warning(
            f"RecoveryWorker: recovery 재시도 지연 "
            f"order_id={order_id}, "
            f"exchange={exchange.value}, "
            f"market_type={market_type.value}, "
            f"next_attempt_ts={next_attempt_ts}"
        )

    async def _recover_one(
        self,
        *,
        local_order: dict[str, Any],
        exchange: Exchange,
        market_type: MarketType,
    ) -> bool:
        """
        Redis recovery row 하나를 복구.

        REGULAR / CONDITIONAL을 여기서 분기한다.
        """
        try:
            order = Order.model_validate(local_order)
        except Exception as e:
            logger.warning(
                f"RecoveryWorker: local_order 파싱 실패: "
                f"row={local_order}, err={e}",
                exc_info=True,
            )
            return False

        if order.exchange != exchange or order.market_type != market_type:
            return True


        if order.order_route == OrderRoute.CONDITIONAL:
            return await self._recover_conditional_order(order)
        
        return await self._recover_regular_order(order)

    async def _recover_regular_order(self, order: Order) -> bool:
        """
        REGULAR 주문 복구.
        """
        order_id = str(order.order_id)
        symbol = order.symbol
        current_status = order.status.value

        try:
            client = self._client_for_order(order)

            snapshot = await client.get_order(order)

            updated = await self.gateway.apply_reconciliation_order_snapshot(
                order_id=order_id,
                snapshot=snapshot,
            )

            if updated is None:
                logger.warning(
                    f"RecoveryWorker: Gateway 보정 결과 없음 "
                    f"(order_id={order_id}, symbol={symbol}, "
                    f"current_status={current_status})"
                )
                return False

            logger.info(
                f"RecoveryWorker: REGULAR 주문 복구 완료 "
                f"order_id={order_id}, "
                f"symbol={symbol}, "
                f"local_status={current_status}, "
                f"exchange_status={snapshot.raw_status}, "
                f"updated_status={updated.status.value}"
            )
            return True

        except ExchangeApiError as e:
            logger.warning(
                f"RecoveryWorker: exchange get_order 실패 "
                f"order_id={order_id}, symbol={symbol}, "
                f"status={current_status}, "
                f"exchange={e.exchange.value}, category={e.category.value}, "
                f"code={e.code}, msg={e.message}",
                exc_info=True,
            )
            return False

        except Exception as e:
            logger.error(
                f"RecoveryWorker: REGULAR 주문 복구 실패 "
                f"order_id={order_id}, symbol={symbol}, "
                f"status={current_status}, err={e}",
                exc_info=True,
            )
            return False

    async def _recover_conditional_order(self, order: Order) -> bool:
        """
        CONDITIONAL 주문 복구.

        순서:
          1. openAlgoOrders(symbol)에서 먼저 찾기
          2. 없으면 allAlgoOrders(symbol)에서 찾기
          3. 그래도 없으면 conditional_status=UNKNOWN 반영

        """
        try:
            # version: 0.2.0
            client = self._client_for_order(order)

            if not client.capabilities.supports_conditional_reconciliation:
                logger.debug(
                    f"conditional recovery not supported: "
                    f"exchange={order.exchange.value}, market_type={order.market_type.value}"
                )
                return False

            snapshot = await client.get_conditional_order(order)

            if snapshot is None:
                await self._mark_conditional_unknown(
                    order=order,
                    reason="conditional_order_not_found_on_exchange",
                    unknown_status_value=client.get_exchange_conditional_order_unknown_status_value()
                )
                return True

            updated = await self.gateway.apply_conditional_order_snapshot(
                snapshot=snapshot,
            )
            if updated is None:
                logger.warning(f"RecoveryWorker: conditional 복구 결과 없음 order_id={order.order_id}")
                return False

            logger.info(
                f"RecoveryWorker: CONDITIONAL 복구 완료 "
                f"order_id={order.order_id}, "
                f"exchange_status={snapshot.raw_status}, "
                f"updated_conditional_status={updated.conditional_status.value if updated.conditional_status else None}"
            )
            return True

            # version: 0.1.0
            # # 1. open algo 조회
            # await self.rate_limiter.acquire_request_weight(weight=1)

            # open_rows = await self.adapter.get_open_algo_orders(
            #     symbol=symbol,
            # )
            # matched = self._find_matching_algo_row(
            #     rows=open_rows,
            #     order=order,
            # )

            # Binance algo REST row를 Takora 표준 이벤트로 변환 후 Gateway에 반영.
            # if matched:
            #     await self._apply_algo_rest_row(
            #         order=order,
            #         row=matched,
            #         source="openAlgoOrders",
            #     )
            #     return

            # # 2. all algo 조회 fallback
            # await self.rate_limiter.acquire_request_weight(weight=5)

            # all_rows = await self.adapter.get_all_algo_orders(
            #     symbol=symbol,
            #     algo_id=order.exchange_conditional_id,
            #     limit=1000,
            # )

            # matched = self._find_matching_algo_row(
            #     rows=all_rows,
            #     order=order,
            # )

            # if matched:
            #     await self._apply_algo_rest_row(
            #         order=order,
            #         row=matched,
            #         source="allAlgoOrders",
            #     )
            #     return

            # # 3. 거래소에서도 못 찾음 -> UNKNOWN 유지/반영
            # await self._mark_conditional_unknown(
            #     order=order,
            #     reason="not_found_in_open_or_all_algo_orders",
            # )

        except ExchangeApiError as e:
            logger.warning(
                f"RecoveryWorker: conditional exchange 조회 실패 "
                f"order_id={order.order_id}, symbol={order.symbol}, "
                f"conditional_status={order.conditional_status.value if order.conditional_status else None}, "
                f"code={e.code}, msg={e.message}",
                exc_info=True,
            )
            return False

        except Exception as e:
            logger.error(
                f"RecoveryWorker: CONDITIONAL 주문 복구 실패 "
                f"order_id={order.order_id}, symbol={order.symbol}, "
                f"err={e}",
                exc_info=True,
            )
            return False

    async def _mark_conditional_unknown(
        self,
        *,
        order: Order,
        reason: str,
        unknown_status_value: str,
    ) -> None:
        """
        openAlgoOrders/allAlgoOrders 모두에서 찾지 못한 조건부 주문을 UNKNOWN으로 보정.

        여기서 직접 DB를 만지지 않고 Gateway의 표준 conditional event 경로를 사용한다.
        """
        event = NormalizedConditionalOrderEvent(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            client_conditional_id=order.client_conditional_id,
            exchange_conditional_id=order.exchange_conditional_id,
            target_status=ConditionalStatus.UNKNOWN,
            exchange_conditional_status=unknown_status_value,
            triggered_order_id=order.triggered_order_id,
            triggered_client_order_id=order.triggered_client_order_id,
            filled_quantity=order.filled_quantity,
            avg_fill_price=order.avg_fill_price,
            reject_reason_text=reason,
            event_time=epoch_ms(),
            transaction_time=None,
            raw={
                "source": "RecoveryWorker",
                "reason": reason,
                "order_id": order.order_id,
                "client_conditional_id": order.client_conditional_id,
                "exchange_conditional_id": order.exchange_conditional_id,
            },
        )

        updated = await self.gateway.apply_conditional_order_event(event)

        if updated is None:
            logger.warning(
                f"RecoveryWorker: conditional UNKNOWN 반영 실패 "
                f"order_id={order.order_id}, reason={reason}"
            )
            return

        logger.warning(
            f"RecoveryWorker: conditional UNKNOWN 반영 완료 "
            f"order_id={order.order_id}, "
            f"reason={reason}, "
            f"updated_version={updated.version}"
        )

    def _client_for_order(self, order: Order) -> ExchangeExecutionClient:
        return self.exchange_clients.get(
            exchange=order.exchange,
            market_type=order.market_type,
        )

    # version: 0.1.0
    # async def _apply_algo_rest_row(
    #     self,
    #     *,
    #     order: Order,
    #     row: dict[str, Any],
    #     source: str,
    # ) -> None:
    #     """
    #     Binance algo REST row를 Takora 표준 이벤트로 변환 후 Gateway에 반영.
    #     """
    #     event = normalize_binance_algo_rest_row(
    #         row,
    #         market_type=MarketType.PERP,
    #         event_time=_now_ms(),
    #     )

    #     updated = await self.gateway.apply_conditional_order_event(event)

    #     if updated is None:
    #         logger.warning(
    #             f"RecoveryWorker: conditional event 적용 결과 없음 "
    #             f"order_id={order.order_id}, source={source}, row={row}"
    #         )
    #         return

    #     logger.info(
    #         f"RecoveryWorker: CONDITIONAL 주문 복구 완료 "
    #         f"order_id={order.order_id}, "
    #         f"symbol={order.symbol}, "
    #         f"source={source}, "
    #         f"local_conditional_status="
    #         f"{order.conditional_status.value if order.conditional_status else None}, "
    #         f"exchange_conditional_status={event.exchange_conditional_status}, "
    #         f"updated_conditional_status="
    #         f"{updated.conditional_status.value if updated.conditional_status else None}"
    #     )

    # def _find_matching_algo_row(
    #     self,
    #     *,
    #     rows: list[dict[str, Any]],
    #     order: Order,
    # ) -> dict[str, Any] | None:
    #     """
    #     algo REST rows에서 local Order와 매칭되는 row 찾기.
    #     """
    #     for row in rows:
    #         client_id = _algo_row_client_id(row)
    #         exchange_id = _algo_row_exchange_id(row)

    #         if order.client_conditional_id and client_id == order.client_conditional_id:
    #             return row

    #         if order.exchange_conditional_id and exchange_id == order.exchange_conditional_id:
    #             return row

    #     return None
