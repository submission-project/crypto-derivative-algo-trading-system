from __future__ import annotations

import asyncio
from typing import Literal, Optional

from common.logging import setup_logger
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.services.order_state_service import OrderStateService
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    TERMINAL_STATUSES,
)
from schemas.conditional_order_event import NormalizedConditionalOrderEvent

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.exchange.client import ExchangeExecutionClient

from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeConditionalSnapshot,
    ExchangeOrderSnapshot,
    ExchangeExecutionClient,
    ExchangeErrorCategory,
)

from common.time import epoch_ms

from collections import defaultdict

# def _enum_value(value: Any) -> Any:
#     return enum_value(value)

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


# def _is_conditional_terminal(status: ConditionalStatus | None) -> bool:
#     return status in CONDITIONAL_TERMINAL_STATUSES

def _group_conditional_orders_by_symbol(
    orders: list[Order],
) -> dict[str, list[Order]]:
    grouped: dict[str, list[Order]] = {}

    for order in orders:
        grouped.setdefault(order.symbol, []).append(order)

    return grouped

logger = setup_logger(__name__)

ExternalOrphanPolicy = Literal["log", "cancel"]

# Binance USD-M Futures changelog에는 /fapi/v1/allOrders의 query time period가 7일보다 작아야 한다는 항목
# MAX_ALL_ORDERS_WINDOW_MS = 7 * 24 * 60 * 60 * 1000 - 1_000


class ReconciliationWorker:
    """
    PostgreSQL 원본 기준 주문 대사 워커.

    비교 대상:
      1. Binance openOrders
      2. PostgreSQL non-terminal orders = 원본
      3. Redis order:open = projection

    주요 탐지:
      - 거래소 O, PostgreSQL X
          외부 주문 또는 원본 누락
      - PostgreSQL O, 거래소 openOrders X
          UDS 유실 / 체결·취소·만료 이벤트 누락 가능성
          -> get_order 단건 조회 후 Gateway로 보정
      - PostgreSQL O, Redis X
          Redis projection 누락
          -> PostgreSQL 기준 projection 복구
      - Redis O, PostgreSQL X
          stale projection
          -> PostgreSQL 확인 후 terminal이면 projection 정리, 없으면 삭제
    """

    def __init__(
        self,
        *,
        # adapter: BinanceRestAdapter,
        exchange_clients: ExchangeExecutionClientRegistry,
        gateway: ExecutionGateway,
        order_state_service: OrderStateService,
        redis_order_repo: OrderStateRedisRepository,
        # rate_limiter: ExecutionRateLimiter,
        markets: list[tuple[Exchange, MarketType]],
        interval_sec: int = 60,
        recent_grace_ms: int = 3_000,
        external_orphan_policy: ExternalOrphanPolicy = "log",
        active_symbols: Optional[set[str]] = None,
        all_orders_threshold: int = 6,
        all_orders_lookback_ms: int = 60_000,
        all_orders_limit: int = 1000,
        conditional_batch_size: int = 1000,
        conditional_batch_delay_sec: float = 0.1,
        pg_batch_size: int = 1000,
        pg_batch_delay_sec: float = 0.05,
        reconcile_failure_ttl_sec: int = 3600,
        reconcile_not_found_threshold: int = 5,
    ) -> None:
        # self.adapter = adapter
        self.exchange_clients = exchange_clients
        self.gateway = gateway
        self.order_state_service = order_state_service
        self.redis_order_repo = redis_order_repo
        # self.rate_limiter = rate_limiter
        self.markets = tuple(markets)

        self.interval_sec = interval_sec
        self.recent_grace_ms = recent_grace_ms
        self.external_orphan_policy = external_orphan_policy
        self.active_symbols = tuple(active_symbols) if active_symbols else None

        self._running = False
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

        self.all_orders_threshold = all_orders_threshold
        self.all_orders_lookback_ms = all_orders_lookback_ms
        self.all_orders_limit = all_orders_limit
        self.conditional_batch_size = conditional_batch_size
        self.conditional_batch_delay_sec = conditional_batch_delay_sec
        self.pg_batch_size = pg_batch_size
        self.pg_batch_delay_sec = pg_batch_delay_sec

        self.reconcile_failure_ttl_sec = reconcile_failure_ttl_sec
        self.reconcile_not_found_threshold = reconcile_not_found_threshold

    @staticmethod
    def _now_ms() -> int:
        return epoch_ms()

    def _empty_conditional_reconcile_result(self) -> dict[str, int]:
        return {
            "checked": 0,
            "updated": 0,
            "orphan_exchange": 0,
            "local_missing_on_exchange": 0,
            "pg_repaired": 0,
        }


    def _merge_conditional_reconcile_result(
        self,
        total: dict[str, int],
        batch: dict[str, int],
    ) -> None:
        for key, value in batch.items():
            total[key] = total.get(key, 0) + value

    async def start(self) -> None:
        if self._running:
            logger.warning("Reconciliation Worker가 이미 실행 중입니다.")
            return

        self._running = True
        self._stop_event.clear()  # Stop event를 clear하여 running 상태임을 알림 [Reset the internal flag to false.]
        self._task = asyncio.create_task(
            self._run_loop(),
            name="reconciliation-worker",
        )

        logger.info(
            f"ReconciliationWorker 시작 "
            f"(interval={self.interval_sec}s, "
            f"recent_grace_ms={self.recent_grace_ms}, "
            f"external_orphan_policy={self.external_orphan_policy})"
        )

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()  # 대기 중이던 모든 코루틴이 즉시 깨어나 실행

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        logger.info("Reconciliation Worker 종료 완료")

    async def _run_loop(self) -> None:
        """
        시작 직후 1회 대사 후, interval마다 반복.
        """
        while self._running:
            try:
                await self.reconcile_once()
                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation Worker 에러 발생: {e}", exc_info=True)
                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

    async def _sleep_or_stop(self, delay_sec: float) -> bool:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=delay_sec,
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def reconcile_once(self) -> None:
        # logger.debug("주문 대사 시작")

        # regular_result: dict[str, int] | None = None
        # conditional_result: dict[str, int] | None = None

        # try:
        #     regular_result = await self.reconcile_regular_orders_once(exchange=Exchange.BINANCE)

        #     logger.info(
        #         f"일반 주문 대사 완료: "
        #         f"exchange_open={regular_result['exchange_open']}, "
        #         f"pg_open={regular_result['pg_open']}, "
        #         f"redis_open={regular_result['redis_open']}, "
        #         f"pg_missing_in_redis={regular_result['pg_missing_in_redis']}, "
        #         f"redis_extra_vs_pg={regular_result['redis_extra_vs_pg']}, "
        #         f"exchange_extra_vs_pg={regular_result['exchange_extra_vs_pg']}, "
        #         f"pg_missing_from_exchange_open="
        #         f"{regular_result['pg_missing_from_exchange_open']}"
        #     )

        # except Exception as e:
        #     logger.error(
        #         f"일반 주문 대사 실패: {e}",
        #         exc_info=True,
        #     )

        # # 조건부 주문 대사
        # try:
        #     conditional_result = await self.reconcile_conditional_orders_once(
        #         exchange=Exchange.BINANCE,
        #     )
        #     logger.info(
        #         f"조건부 주문 대사 완료: "
        #         f"checked={conditional_result['checked']}, "
        #         f"updated={conditional_result['updated']}, "
        #         f"orphan_exchange={conditional_result['orphan_exchange']}, "
        #         f"local_missing={conditional_result['local_missing_on_exchange']}"
        #     )
        # except Exception as cond_err:
        #     logger.error(
        #         f"조건부 주문 대사 실패 (REGULAR 대사는 정상 완료): {cond_err}",
        #         exc_info=True,
        #     )

        # logger.debug(
        #     f"주문 대사 완료: "
        #     f"regular_result={regular_result}, "
        #     f"conditional_result={conditional_result}"
        # )

        for exchange, market_type in self.markets:
            await self.reconcile_regular_orders_once(
                exchange=exchange,
                market_type=market_type,
            )

            client = self._client(exchange, market_type)

            if not client.capabilities.supports_conditional_reconciliation:
                continue

            await self.reconcile_conditional_orders_once(
                exchange=exchange,
                market_type=market_type,
            )

    def _client(
        self,
        exchange: Exchange,
        market_type: MarketType,
    ) -> ExchangeExecutionClient:
        return self.exchange_clients.get(
            exchange=exchange,
            market_type=market_type,
        )

    def _index_exchange_order_snapshots(
        self,
        snapshots: list[ExchangeOrderSnapshot],
    ) -> dict[str, ExchangeOrderSnapshot]:
        result: dict[str, ExchangeOrderSnapshot] = {}

        for snapshot in snapshots:
            if not snapshot.client_order_id:
                logger.warning(f"client_order_id 없는 exchange snapshot 무시: {snapshot}")
                continue

            result[str(snapshot.client_order_id)] = snapshot

        return result

    def _parse_matching_orders(
        self,
        *,
        rows: list[dict],
        exchange: Exchange,
        market_type: MarketType,
        order_route: OrderRoute,
    ) -> list[Order]:
        orders: list[Order] = []

        for row in rows:
            try:
                order = Order.model_validate(row)

            except Exception as e:
                logger.warning(
                    f"order projection parse failed: row={row}, err={e}",
                    exc_info=True,
                )
                continue

            if order.exchange != exchange:
                continue

            if order.market_type != market_type:
                continue

            if order.order_route != order_route:
                continue

            orders.append(order)

        return orders

    def _index_orders(
        self,
        orders: list[Order],
    ) -> dict[str, Order]:
        result: dict[str, Order] = {}

        for order in orders:
            if not order.order_id:
                logger.warning(f"order_id 없는 Order 무시: {order}")
                continue

            result[str(order.order_id)] = order

        return result

    async def _fetch_exchange_open_orders(
        self,
        *,
        client: ExchangeExecutionClient,
    ) -> list[ExchangeOrderSnapshot]:
        """
        거래소 openOrders 조회.

        active_symbols가 있으면 심볼별 조회(weight=1씩),
        없으면 전체 심볼 조회(weight=40)를 사용.
        """
        if self.active_symbols:
            snapshots: list[ExchangeOrderSnapshot] = []

            for symbol in self.active_symbols:
                # await self.rate_limiter.acquire_request_weight(weight=1)
                # rows = await self.adapter.get_open_orders(symbol=symbol)
                rows = await client.get_open_orders(symbol=symbol)
                snapshots.extend(rows)

            return snapshots

        # await self.rate_limiter.acquire_request_weight(weight=40)
        # return await self.adapter.get_open_orders()
        return await client.get_open_orders()

    async def _repair_pg_missing_in_redis(
        self,
        *,
        order_ids: set[str],
        pg_open_by_id: dict[str, Order],
    ) -> None:
        """
        PostgreSQL에는 open인데 Redis projection에 없는 주문 복구.

        이미 reconcile_once()에서 PostgreSQL open orders를 조회했으므로,
        여기서는 추가 SQL 없이 pg_open_by_id를 사용
        """
        if not order_ids:
            return

        logger.warning(
            f"PostgreSQL O, Redis X projection 누락 감지: "
            f"{len(order_ids)}건 -> {order_ids}"
        )

        for order_id in order_ids:
            order = pg_open_by_id.get(order_id)

            if not order:
                logger.error(f"pg_open_by_id에 없는 order_id: {order_id}")
                continue

            try:
                applied = await self.order_state_service.refresh_order_projection(order)

                logger.info(
                    f"Redis projection 복구 완료: "
                    f"order_id={order_id}, "
                    f"status={order.status.value}, "
                    f"version={order.version}, "
                    f"applied={applied}"
                )

            except Exception as e:
                logger.error(
                    f"Redis projection 복구 실패: " f"order_id={order_id}, err={e}",
                    exc_info=True,
                )

    async def _repair_redis_extra_vs_pg(
        self,
        order_ids: set[str],
    ) -> None:
        """
        Redis에는 open인데 PostgreSQL open 원본에는 없는 주문 처리.
        batch query로 PostgreSQL 원본을 한 번에 조

        가능한 경우:
          - PostgreSQL에는 terminal 상태인데 Redis projection이 stale
          - PostgreSQL에 아예 없음
          - Redis projection 오염
        """
        if not order_ids:
            return

        logger.warning(
            f"Redis O, PostgreSQL open X stale projection 감지: "
            f"{len(order_ids)}건 -> {order_ids}"
        )

        pg_orders_by_id = await self.order_state_service.load_orders_from_postgres(
            order_ids,
            refresh_projection=True,
        )

        for order_id in order_ids:
            order = pg_orders_by_id.get(order_id)

            if order is None:
                logger.error(
                    f"PostgreSQL 원본에 없는 Redis projection 삭제: "
                    f"order_id={order_id}"
                )
                await self.redis_order_repo.delete(order_id=order_id)
                continue

            if order.status in TERMINAL_STATUSES:
                logger.info(
                    f"terminal 원본 기준 Redis projection 정리 완료: "
                    f"order_id={order_id}, "
                    f"status={order.status.value}, "
                    f"version={order.version}"
                )
            else:
                logger.info(
                    f"PostgreSQL 원본 기준 Redis projection 재정렬 완료: "
                    f"order_id={order_id}, "
                    f"status={order.status.value}, "
                    f"version={order.version}"
                )

    async def _handle_exchange_extra_vs_pg(
        self,
        client: ExchangeExecutionClient,
        order_ids: set[str],
        exchange_open_by_client_id: dict[str, ExchangeOrderSnapshot],
    ) -> None:
        """
        거래소에는 open인데 PostgreSQL 원본에는 없는 주문 처리.

        시스템 전용 계정이면 external_orphan_policy='cancel' 사용 가능.
        """
        if not order_ids:
            return

        logger.warning(
            f"거래소 O, PostgreSQL X orphan 주문 감지: "
            f"{len(order_ids)}건 -> {order_ids}"
        )

        for order_id in order_ids:
            snapshot = exchange_open_by_client_id.get(order_id)
            if snapshot is None:
                continue

            if self.external_orphan_policy == "log":
                logger.error(
                    f"외부/미등록 open 주문 감지: "
                    f"client_order_id={order_id}, "
                    f"symbol={snapshot.symbol}, status={snapshot.status}. "
                    f"현재 정책=log, 자동 취소하지 않음."
                )
                continue

            elif self.external_orphan_policy == "cancel":
                try:
                    # await self.rate_limiter.acquire_request_weight(weight=1)
                    # await self.adapter.cancel_order(
                    #     symbol=symbol,
                    #     client_order_id=order_id,
                    # )
                    await client.cancel_regular_order_by_client_id(
                        symbol=snapshot.symbol,
                        client_order_id=order_id,
                    )

                    logger.warning(
                        f"외부/미등록 open 주문 자동 취소 요청 완료: "
                        f"client_order_id={order_id}, symbol={snapshot.symbol}"
                    )

                    continue

                except Exception as e:
                    logger.error(
                        f"외부/미등록 open 주문 자동 취소 실패: "
                        f"client_order_id={order_id}, "
                        f"symbol={snapshot.symbol}, err={e}",
                        exc_info=True,
                    )
                    continue

            else:
                raise NotImplementedError(
                    f"unsupported external_orphan_policy={self.external_orphan_policy}"
                )

    async def _repair_pg_missing_from_exchange_open(
        self,
        client: ExchangeExecutionClient,
        order_ids: set[str],
        pg_open_by_id: dict[str, Order],
    ) -> None:
        """
        PostgreSQL에는 open인데 Binance openOrders에는 없는 주문 처리.
        - symbol별로 묶음
        - 같은 symbol 내 대상 주문 수가 많으면 allOrders 사용
        - 적으면 get_order 단건 사용
        - allOrders에서 못 찾은 주문은 get_order fallback
        """
        if not order_ids:
            return

        logger.warning(
            f"PostgreSQL order:open O, 거래소 openOrders X 주문 감지: "
            f"{len(order_ids)}건 -> {order_ids}"
        )

        now_ms = epoch_ms()
        by_symbol: dict[str, list[Order]] = defaultdict(list)

        for order_id in order_ids:
            order = pg_open_by_id.get(order_id)

            if not order:
                logger.warning(
                    f"pg_open_by_id에 없는 reconciliation 대상: order_id={order_id}"
                )
                continue

            # if not order.order_id:
            #     logger.warning(f"order_id 없는 주문 skip: {order}")
            #     continue

            # 너무 최근 주문은 Binance openOrders 반영 지연 가능성을 고려해 보류
            age_ms = now_ms - order.updated_ts
            if age_ms < self.recent_grace_ms:
                logger.debug(
                    f"최근 갱신 주문이라 reconciliation 보류: "
                    f"order_id={order.order_id}, "
                    f"status={order.status.value}, "
                    f"age_ms={age_ms}"
                )
                continue

            by_symbol[order.symbol.upper()].append(order)

        for symbol, orders in by_symbol.items():
            threshold = self._bulk_order_lookup_threshold(client)

            if len(orders) >= threshold:
                repaired = await self._try_repair_missing_open_by_bulk_lookup(
                    client=client,
                    symbol=symbol,
                    orders=orders,
                )

                if repaired:
                    continue

            await self._repair_missing_open_by_single_get_order(
                client=client,
                symbol=symbol,
                orders=orders,
            )

    async def _try_repair_missing_open_by_bulk_lookup(
        self,
        *,
        client: ExchangeExecutionClient,
        symbol: str,
        orders: list[Order],
    ) -> bool:
        """
        특정 symbol에서 여러 주문을 allOrders로 한 번에 조회해 보정.

        allOrders 조회 범위:
        - 대상 주문 중 가장 오래된 created_ts 기준
        - lookback을 조금 더 빼서 안전하게 조회

        allOrders에서 못 찾은 주문:
        - get_order 단건 fallback
        """

        # valid_orders = [order for order in orders if order.order_id]

        # if not valid_orders:
        #     return

        # target_order_ids = {str(order.order_id) for order in valid_orders}

        # oldest_created_ts = min(order.created_ts for order in valid_orders)
        # end_time = epoch_ms()
        # start_time = max(
        #     0,
        #     max(
        #         oldest_created_ts - self.all_orders_lookback_ms,
        #         end_time - MAX_ALL_ORDERS_WINDOW_MS,
        #     ),
        # )

        # logger.info(
        #     f"allOrders reconciliation 시작: "
        #     f"symbol={symbol}, "
        #     f"target_count={len(valid_orders)}, "
        #     f"start_time={start_time}, "
        #     f"end_time={end_time}, "
        #     f"limit={self.all_orders_limit}"
        # )

        # try:
        #     rows = await self._fetch_all_orders_until_targets_found(
        #         symbol=symbol,
        #         start_time=start_time,
        #         end_time=end_time,
        #         target_order_ids=target_order_ids,
        #     )

        # except Exception as e:
        #     logger.error(
        #         f"allOrders 조회 실패. 단건 get_order fallback 수행: "
        #         f"symbol={symbol}, "
        #         f"target_count={len(valid_orders)}, "
        #         f"err={e}",
        #         exc_info=True,
        #     )

        #     await self._repair_missing_open_by_single_get_order(
        #         symbol=symbol,
        #         orders=valid_orders,
        #     )
        #     return

        # by_client_id: dict[str, dict[str, Any]] = {
        #     str(row.get("clientOrderId")): row
        #     for row in rows
        #     if row.get("clientOrderId")
        # }

        # missing_after_all_orders: list[Order] = []

        # for order in valid_orders:
        #     if not order.order_id:
        #         continue

        #     snapshot = by_client_id.get(order.order_id)

        #     if snapshot is None:
        #         missing_after_all_orders.append(order)
        #         continue

        #     await self._apply_reconciliation_snapshot(
        #         order=order,
        #         snapshot=snapshot,
        #         source="allOrders",
        #     )

        # if missing_after_all_orders:
        #     logger.warning(
        #         f"allOrders에서 일부 주문을 찾지 못함. get_order fallback 수행: "
        #         f"symbol={symbol}, "
        #         f"missing_count={len(missing_after_all_orders)}, "
        #         f"missing_ids={[order.order_id for order in missing_after_all_orders]}"
        #     )

        #     await self._repair_missing_open_by_single_get_order(
        #         client=client
        #         symbol=symbol,
        #         orders=missing_after_all_orders,
        #     )

        try:
            if not client.capabilities.supports_bulk_order_lookup:
                return False
            snapshots_by_order_id:dict[str, ExchangeOrderSnapshot] = await client.find_order_snapshots(
                symbol=symbol,
                orders=orders,
                lookback_ms=self.all_orders_lookback_ms,
                limit=self.all_orders_limit,
            )

        except Exception as e:
            logger.error(
                f"bulk order snapshot 조회 실패. 단건 조회 fallback 수행: "
                f"symbol={symbol}, target_count={len(orders)}, err={e}",
                exc_info=True,
            )
            return False

        missing_after_bulk: list[Order] = []

        for order in orders:
            if not order.order_id:
                continue

            snapshot = snapshots_by_order_id.get(order.order_id)

            if snapshot is None:
                missing_after_bulk.append(order)
                continue

            await self._apply_reconciliation_snapshot(
                order=order,
                snapshot=snapshot,
                source="bulk_order_snapshot",
            )

        # 주로 7일 넘어간 주문 또는 3일 이상 지난 취소 또는 연기된 주문들 처리
        if missing_after_bulk:
            await self._repair_missing_open_by_single_get_order(
                client=client,
                symbol=symbol,
                orders=missing_after_bulk,
            )

        return True

        

    async def _repair_missing_open_by_single_get_order(
        self,
        *,
        client: ExchangeExecutionClient,
        symbol: str,
        orders: list[Order],
    ) -> None:
        """
        PG/Redis에는 open인데 거래소 open 목록에 없으면 단건 조회 후 로컬에 반영

        사용처:
        - 대상 주문 수가 적을 때
        - allOrders 실패 시 fallback
        - allOrders에서 특정 주문을 찾지 못했을 때 fallback
        """
        for order in orders:
            if not order.order_id:
                continue

            try:
                snapshot = await client.get_order(order)
                applied = await self._apply_reconciliation_snapshot(
                    order=order,
                    snapshot=snapshot,
                    source="get_order",
                )

                if applied:
                    await self._clear_reconcile_failure(order)

            except ExchangeApiError as e:
                if e.category == ExchangeErrorCategory.ORDER_NOT_FOUND:
                    count = await self._increment_reconcile_failure(order)

                    if count < self.reconcile_not_found_threshold:
                        logger.warning(
                            "거래소 주문 조회 실패: 아직 임계치 전 "
                            f"order_id={order.order_id}, count={count}"
                        )
                        continue

                    elif count == self.reconcile_not_found_threshold:
                        logger.error(
                            "거래소에서 주문을 반복적으로 찾지 못해 reconciliation unresolved 처리: "
                            f"order_id={order.order_id}, count={count}, err={e}"
                        )
                    else:
                        logger.warning(
                            "거래소 주문 조회 실패가 임계치를 초과해 unresolved 처리를 재시도합니다: "
                            f"order_id={order.order_id}, count={count}, err={e}"
                        )

                    await self.gateway.mark_reconciliation_unresolved(
                        order_id=order.order_id,
                        exchange_error_code=self._int_error_code(e.code),
                        detail_msg=e.message,
                        raw_exchange_response=e.raw,
                    )

                    continue

                logger.error(
                    f"reconciliation get_order 실패: "
                    f"order_id={order.order_id}, symbol={symbol}, "
                    f"exchange={e.exchange.value}, category={e.category.value}, "
                    f"code={e.code}, msg={e.message}",
                    exc_info=True,
                )
                continue

            except Exception as e:
                logger.error(
                    f"reconciliation get_order 예외: "
                    f"order_id={order.order_id}, symbol={symbol}, err={e}",
                    exc_info=True,
                )
                continue
                

    async def _apply_reconciliation_snapshot(
        self,
        *,
        order: Order,
        snapshot: ExchangeOrderSnapshot,
        source: str,
    ) -> bool:
        """
        거래소 snapshot을 Gateway를 통해 로컬 원본 상태에 반영.

        모든 상태 변경은 반드시:
        Gateway.apply_reconciliation_snapshot()
            -> OrderStateService.transition_order()
            -> PostgreSQL + Redis projection
        경로를 탄다.
        """
        if not order.order_id:
            logger.warning(f"snapshot 적용 불가: order_id 없음, order={order}")
            return True

        updated = await self.gateway.apply_reconciliation_order_snapshot(
            order_id=order.order_id,
            snapshot=snapshot,
        )

        if updated is None:
            logger.warning(
                f"reconciliation snapshot 적용 결과 없음: "
                f"order_id={order.order_id}, "
                f"symbol={order.symbol}, "
                f"source={source}"
            )
            return False

        logger.info(
            f"reconciliation snapshot 보정 완료: "
            f"source={source}, "
            f"order_id={order.order_id}, "
            f"symbol={order.symbol}, "
            f"local_before={order.status.value}, "
            f"exchange_status={snapshot.raw_status}, "
            f"local_after={updated.status.value}, "
            f"version={updated.version}"
        )

        return True

    # async def _fetch_all_orders_until_targets_found(
    #     self,
    #     *,
    #     symbol: str,
    #     start_time: int,
    #     end_time: int,
    #     target_order_ids: set[str],
    # ) -> list[dict[str, Any]]:
    #     rows: list[dict[str, Any]] = []
    #     found: set[str] = set()
    #     next_order_id: int | None = None

    #     while True:
    #         await self.rate_limiter.acquire_request_weight(weight=5)

    #         new_rows = await self.adapter.get_all_orders(
    #             symbol=symbol,
    #             order_id=next_order_id,
    #             start_time=start_time,
    #             end_time=end_time,
    #             limit=self.all_orders_limit,
    #         )

    #         if not new_rows:
    #             break

    #         rows.extend(new_rows)

    #         for row in new_rows:
    #             cid = row.get("clientOrderId")
    #             if cid and str(cid) in target_order_ids:
    #                 found.add(str(cid))

    #         if target_order_ids <= found:
    #             break

    #         if len(new_rows) < self.all_orders_limit:
    #             break

    #         last_order_id_raw = new_rows[-1].get("orderId")
    #         if last_order_id_raw is None:
    #             logger.warning(
    #                 f"allOrders pagination 중 orderId 누락. 중단: "
    #                 f"symbol={symbol}, last_row={new_rows[-1]}"
    #             )
    #             break

    #         next_order_id = int(last_order_id_raw) + 1

    #     return rows

    async def reconcile_conditional_orders_once(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> dict[str, int]:
        """
        조건부 주문 reconciliation 1회 수행.

        기준:
        - Redis orders:conditional:open:{exchange}
        - Binance openAlgoOrders(symbol)
        - Binance allAlgoOrders(symbol) fallback

        처리:
        1. Redis에는 있는데 Binance openAlgoOrders에는 없음
            -> allAlgoOrders로 최종 상태 조회 후 local 반영
        2. Binance openAlgoOrders에는 있는데 local에 없음
            -> orphan conditional order 감지
        """
        client = self._client(exchange=exchange, market_type=market_type)

        total = self._empty_conditional_reconcile_result()
        
        async for raw_items in self.redis_order_repo.iter_open_conditional_order_batches(
            exchange=exchange.value,
            market_type=market_type.value,
            batch_size=self.conditional_batch_size,
        ):

            batch_result = await self._reconcile_conditional_batch(
                client=client,
                exchange=exchange,
                market_type=market_type,
                raw_items=raw_items,
            )

            self._merge_conditional_reconcile_result(total, batch_result)

            if self.conditional_batch_delay_sec > 0:
                await asyncio.sleep(self.conditional_batch_delay_sec)

        return total


    async def _reconcile_conditional_batch(
        self,
        *,
        client: ExchangeExecutionClient,
        exchange: Exchange,
        market_type: MarketType,
        raw_items: list[dict[str, Any]],
    ) -> dict[str, int]:

        result = self._empty_conditional_reconcile_result()

        local_orders = self._parse_matching_orders(
            rows=raw_items,
            exchange=exchange,
            market_type=market_type,
            order_route=OrderRoute.CONDITIONAL,
        )

        grouped = _group_conditional_orders_by_symbol(local_orders)

        for symbol, symbol_orders in grouped.items():
            exchange_open_snapshots = await client.get_open_conditional_orders(symbol=symbol)

            open_by_client_id = {
                snapshot.client_conditional_id: snapshot
                for snapshot in exchange_open_snapshots
                if snapshot.client_conditional_id
            }
            open_by_exchange_id = {
                snapshot.exchange_conditional_id: snapshot
                for snapshot in exchange_open_snapshots
                if snapshot.exchange_conditional_id
            }

            local_client_ids = {
                order.client_conditional_id
                for order in symbol_orders
                if order.client_conditional_id
            }
            local_exchange_ids = {
                order.exchange_conditional_id
                for order in symbol_orders
                if order.exchange_conditional_id
            }

            # A. local open conditional이 실제 거래소 open conditional에 있는지 확인
            for order in symbol_orders:
                result["checked"] += 1

                found_open = (
                    order.client_conditional_id in open_by_client_id
                    if order.client_conditional_id
                    else False
                ) or (
                    order.exchange_conditional_id in open_by_exchange_id
                    if order.exchange_conditional_id
                    else False
                )

                if found_open:
                    continue

                result["local_missing_on_exchange"] += 1

                # Redis에는 conditional open으로 남아 있는데
                # Binance openAlgoOrders에는 없는 경우.
                # allAlgoOrders에서 최종 상태를 찾아 local 상태를 갱신한다.
                changed = await self._repair_conditional_missing_from_open(
                    client=client,
                    order=order,
                )

                if changed:
                    result["updated"] += 1

            # B. exchange에는 open인데 local에는 없는 조건부 주문 감지
            for snapshot in exchange_open_snapshots:
                client_id = snapshot.client_conditional_id
                exchange_id = snapshot.exchange_conditional_id

                if client_id in local_client_ids or exchange_id in local_exchange_ids:
                    continue

                result["orphan_exchange"] += 1

                repaired = await self._repair_exchange_conditional_orphan(
                    client=client,
                    snapshot=snapshot,
                    exchange=exchange,
                    market_type=market_type,
                )

                if repaired:
                    result["pg_repaired"] += 1

        return result



    # [claim]
    # PG 조회는 DB 부하를 줄이기 위해 batch로 수행하지만,
    # reconciliation diff는 PG/Redis/Exchange 전체 open set 비교가 필요하므로
    # 현재 단계에서는 PG 결과를 누적한 뒤 한 번에 처리한다.
    # 주문 수가 크게 증가하면 seen_pg_ids 기반의 streaming repair로 분리할 수 있다.
    async def reconcile_regular_orders_once(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> dict[str, int]:
        # 거래소 API로 REGULAR open orders 조회
        client = self._client(exchange=exchange, market_type=market_type)

        exchange_open_snapshots = await self._fetch_exchange_open_orders(client=client)

        exchange_open_by_client_id:dict[str, ExchangeOrderSnapshot] = self._index_exchange_order_snapshots(
            snapshots=exchange_open_snapshots
        )

        # Redis(Local)의 Open Orders 목록 조회
        redis_open_rows = await self.redis_order_repo.list_open_regular_orders(
            exchange=exchange.value,
            market_type=market_type.value
        )
        redis_open_orders = self._parse_matching_orders(
            rows=redis_open_rows,
            exchange=exchange,
            market_type=market_type,
            order_route=OrderRoute.REGULAR,
        )
        redis_open_by_id = self._index_orders(redis_open_orders)

        # PostgreSQL의 Open Orders 목록 조회.
        # 전체 non-terminal을 가져온 뒤 Python에서 필터링하지 않고,
        # exchange/market_type/route 조건을 DB에 내려 batch 단위로 가져온다.
        pg_open_by_id: dict[str, Order] = {}


        # version: 1
        # pg_open_rows = await self.order_state_service.list_open_orders(refresh_projection=False)
        # pg_regular_open_rows: list[Order] = [
        #     order
        #     for order in pg_open_rows
        #     if order.exchange == exchange
        #     and order.market_type == market_type
        #     and order.order_route == OrderRoute.REGULAR
        # ]
        
        # pg_open_by_id = self._index_orders(pg_regular_open_rows)

        # version: 2
        async for pg_batch in self.order_state_service.iter_open_order_batches(
            exchange=exchange,
            market_type=market_type,
            order_route=OrderRoute.REGULAR,
            batch_size=self.pg_batch_size,
            refresh_projection=False,
        ):
            pg_open_by_id.update(self._index_orders(pg_batch))

            if (
                self.pg_batch_delay_sec > 0
                and len(pg_batch) >= self.pg_batch_size
            ):
                await asyncio.sleep(self.pg_batch_delay_sec)


        exchange_open_ids = set(exchange_open_by_client_id)
        redis_open_ids = set(redis_open_by_id)
        pg_open_ids = set(pg_open_by_id)

        # 1. PostgreSQL O, Redis X
        # 원본에는 open인데 projection에는 없는 경우.
        pg_missing_in_redis = pg_open_ids - redis_open_ids

        # 2. Redis O, PostgreSQL X
        # projection에만 남은 stale row.
        redis_extra_vs_pg = redis_open_ids - pg_open_ids

        # 3. 거래소 O, PostgreSQL X
        # 외부 주문 또는 원본 누락.
        exchange_extra_vs_pg = exchange_open_ids - pg_open_ids

        # 4. PostgreSQL O, 거래소 openOrders X
        # 로컬은 open인데 거래소 openOrders에는 없음.
        pg_missing_from_exchange_open = pg_open_ids - exchange_open_ids

        logger.info(
            f"주문 대사 diff: "
            f"exchange_open={len(exchange_open_ids)}, "
            f"pg_open={len(pg_open_ids)}, "
            f"redis_open={len(redis_open_ids)}, "
            f"pg_missing_in_redis={len(pg_missing_in_redis)}, "
            f"redis_extra_vs_pg={len(redis_extra_vs_pg)}, "
            f"exchange_extra_vs_pg={len(exchange_extra_vs_pg)}, "
            f"pg_missing_from_exchange_open={len(pg_missing_from_exchange_open)}"
        )

        await self._repair_pg_missing_in_redis(
            order_ids=pg_missing_in_redis,
            pg_open_by_id=pg_open_by_id,
        )

        await self._repair_redis_extra_vs_pg(redis_extra_vs_pg)

        await self._handle_exchange_extra_vs_pg(
            client=client,
            order_ids=exchange_extra_vs_pg,
            exchange_open_by_client_id=exchange_open_by_client_id,
        )
        await self._repair_pg_missing_from_exchange_open(
            client=client,
            order_ids=pg_missing_from_exchange_open,
            pg_open_by_id=pg_open_by_id,
        )

        return {
            "exchange_open": len(exchange_open_ids),
            "pg_open": len(pg_open_ids),
            "redis_open": len(redis_open_ids),
            "pg_missing_in_redis": len(pg_missing_in_redis),
            "redis_extra_vs_pg": len(redis_extra_vs_pg),
            "exchange_extra_vs_pg": len(exchange_extra_vs_pg),
            "pg_missing_from_exchange_open": len(pg_missing_from_exchange_open),
        }


    async def _repair_conditional_missing_from_open(
        self,
        *,
        client: ExchangeExecutionClient,
        order: Order,
    ) -> bool:
        """
        Redis에는 conditional open으로 남아 있는데
        Binance openAlgoOrders에는 없는 경우.

        allAlgoOrders에서 최종 상태를 찾아 local 상태를 갱신한다.
        """

        try:
            # await self.rate_limiter.acquire_request_weight(weight=5)

            # # [CLAIM] 왜 algo_id를 인자로 전달해야만 하는 지? 
            # rows = await self.adapter.get_all_algo_orders(
            #     symbol=order.symbol,
            #     algo_id=order.exchange_conditional_id,
            #     limit=1000,
            # )

            snapshot = await client.get_conditional_order(order)
        except ExchangeApiError as e:
            logger.error(
                f"conditional reconciliation 조회 실패: "
                f"order_id={order.order_id}, symbol={order.symbol}, "
                f"exchange={e.exchange.value}, category={e.category.value}, "
                f"code={e.code}, msg={e.message}",
                exc_info=True,
            )
            return False

        except Exception as e:
            logger.error(
                f"conditional reconciliation 조회 예외: "
                f"order_id={order.order_id}, symbol={order.symbol}, err={e}",
                exc_info=True,
            )
            return False

        # matched = self._find_matching_algo_row(
        #     rows=rows,
        #     order=order,
        # )

        # if not matched:
        #     # allAlgoOrders에서도 못 찾으면 UNKNOWN으로 보정.
        #     event = NormalizedConditionalOrderEvent(
        #         exchange=order.exchange,
        #         market_type=order.market_type,
        #         symbol=order.symbol,
        #         client_conditional_id=order.client_conditional_id,
        #         exchange_conditional_id=order.exchange_conditional_id,
        #         target_status=ConditionalStatus.UNKNOWN,
        #         exchange_conditional_status="UNKNOWN",
        #         triggered_order_id=order.triggered_order_id,
        #         triggered_client_order_id=order.triggered_client_order_id,
        #         filled_quantity=order.filled_quantity,
        #         avg_fill_price=order.avg_fill_price,
        #         reject_reason_text="not_found_in_openAlgoOrders_or_allAlgoOrders",
        #         event_time=_now_ms(),
        #         transaction_time=None,
        #         raw={
        #             "source": "ReconciliationWorker",
        #             "reason": "not_found_in_openAlgoOrders_or_allAlgoOrders",
        #             "order_id": order.order_id,
        #             "client_conditional_id": order.client_conditional_id,
        #             "exchange_conditional_id": order.exchange_conditional_id,
        #         },
        #     )

        #     updated = await self.gateway.apply_conditional_order_event(event)

        #     if updated is None:
        #         logger.warning(
        #             f"conditional UNKNOWN repair 결과 없음: order_id={order.order_id}"
        #         )
        #         return False

        #     logger.warning(
        #         f"conditional order marked UNKNOWN by reconciliation: "
        #         f"order_id={order.order_id}, "
        #         f"client_conditional_id={order.client_conditional_id}, "
        #         f"exchange_conditional_id={order.exchange_conditional_id}"
        #     )

        #     return updated.version != order.version

        # event = normalize_binance_algo_rest_row(
        #     matched,
        #     market_type=MarketType.PERP,
        #     event_time=self._now_ms(),
        # )

        # updated = await self.gateway.apply_conditional_order_event(event)
        # if updated is None:
        #     logger.warning(
        #         f"conditional repair 결과 없음: "
        #         f"order_id={order.order_id}, row={matched}"
        #     )
        #     return False

        # logger.info(
        #     f"conditional local missing repaired: "
        #     f"order_id={order.order_id}, "
        #     f"old_conditional_status="
        #     f"{order.conditional_status.value if order.conditional_status else None}, "
        #     f"new_conditional_status="
        #     f"{updated.conditional_status.value if updated.conditional_status else None}, "
        #     f"exchange_status={event.exchange_conditional_status}"
        # )

        if snapshot is None:
            return await self._mark_conditional_unknown(
                order=order,
                reason="not_found_in_exchange_conditional_reconciliation",
                unknown_status_value=client.get_exchange_conditional_order_unknown_status_value,
            )

        updated = await self.gateway.apply_conditional_order_snapshot(
            snapshot=snapshot,
        )

        if updated is None:
            logger.warning(
                f"conditional repair 결과 없음: "
                f"order_id={order.order_id}, snapshot={snapshot}"
            )
            return False

        logger.info(
            f"conditional local missing repaired: "
            f"order_id={order.order_id}, "
            f"old_conditional_status="
            f"{order.conditional_status.value if order.conditional_status else None}, "
            f"new_conditional_status="
            f"{updated.conditional_status.value if updated.conditional_status else None}, "
            f"exchange_status={snapshot.raw_status}"
        )

        return updated.version != order.version

    # def _find_matching_algo_row(
    #     self,
    #     *,
    #     rows: list[dict[str, Any]],
    #     order: Order,
    # ) -> dict[str, Any] | None:
    #     for row in rows:
    #         client_id = _algo_row_client_id(row)
    #         exchange_id = _algo_row_exchange_id(row)

    #         if (order.client_conditional_id and client_id == order.client_conditional_id) or (
    #             order.exchange_conditional_id and exchange_id == order.exchange_conditional_id
    #         ):
    #             return row

    #     return None

    async def _repair_exchange_conditional_orphan(
        self,
        *,
        client: ExchangeExecutionClient,
        snapshot: ExchangeConditionalSnapshot,
        exchange: Exchange,
        market_type: MarketType,
    ) -> bool:
        """
        Exchange에는 open conditional order가 있는데
        Redis local projection에는 없는 경우.

        초기 정책:
        - 자동 취소하지 않음
        - warning log
        - 나중에 PG client_conditional_id / exchange_conditional_id 조회로 복구 가능
        """
        order: Order | None = None

        if snapshot.client_conditional_id:
            order = await self.order_state_service.load_order_by_client_conditional_id(
                exchange=exchange,
                market_type=market_type,
                client_conditional_id=snapshot.client_conditional_id,
                refresh_projection=True,
            )

        if order is None and snapshot.exchange_conditional_id:
            order = await self.order_state_service.load_order_by_exchange_conditional_id(
                exchange=exchange,
                market_type=market_type,
                exchange_conditional_id=snapshot.exchange_conditional_id,
                refresh_projection=True,
            )

        if order:
            logger.info(
                f"exchange conditional orphan repaired from PostgreSQL: "
                f"order_id={order.order_id}, symbol={snapshot.symbol}, "
                f"client_conditional_id={snapshot.client_conditional_id}, "
                f"exchange_conditional_id={snapshot.exchange_conditional_id}"
            )
            return True

        # [claim] 주문 취소 넣어야 됨
        if self.external_orphan_policy == "log":
            logger.warning(
                f"true orphan exchange conditional order detected; auto-cancel skipped: "
                f"symbol={snapshot.symbol}, "
                f"client_conditional_id={snapshot.client_conditional_id}, "
                f"exchange_conditional_id={snapshot.exchange_conditional_id}"
            )
            return False

        elif self.external_orphan_policy == "cancel":
            try:
                await client.cancel_conditional_order_by_id(
                    symbol=snapshot.symbol,
                    client_conditional_id=snapshot.client_conditional_id,
                    exchange_conditional_id=snapshot.exchange_conditional_id,
                )

                logger.warning(
                    f"true orphan exchange conditional order cancel requested: "
                    f"symbol={snapshot.symbol}, "
                    f"client_conditional_id={snapshot.client_conditional_id}, "
                    f"exchange_conditional_id={snapshot.exchange_conditional_id}"
                )
                return False

            except Exception as e:
                logger.error(
                    f"true orphan exchange conditional order cancel failed: "
                    f"symbol={snapshot.symbol}, "
                    f"client_conditional_id={snapshot.client_conditional_id}, "
                    f"exchange_conditional_id={snapshot.exchange_conditional_id}, "
                    f"err={e}",
                    exc_info=True,
                )
                return False

        raise NotImplementedError(
            f"unsupported external_orphan_policy={self.external_orphan_policy}"
        )

    async def _mark_conditional_unknown(
        self,
        *,
        order: Order,
        reason: str,
        unknown_status_value: str,
    ) -> bool:
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
            event_time=self._now_ms(),
            transaction_time=None,
            raw={
                "source": "ReconciliationWorker",
                "reason": reason,
                "order_id": order.order_id,
                "client_conditional_id": order.client_conditional_id,
                "exchange_conditional_id": order.exchange_conditional_id,
            },
        )

        updated = await self.gateway.apply_conditional_order_event(event)

        if updated is None:
            logger.warning(
                f"conditional UNKNOWN repair 결과 없음: "
                f"order_id={order.order_id}, reason={reason}"
            )
            return False

        logger.warning(
            f"conditional order marked UNKNOWN by reconciliation: "
            f"order_id={order.order_id}, "
            f"client_conditional_id={order.client_conditional_id}, "
            f"exchange_conditional_id={order.exchange_conditional_id}, "
            f"reason={reason}"
        )

        return updated.version != order.version

    def _bulk_order_lookup_threshold(
        self,
        client: ExchangeExecutionClient,
    ) -> int:
        threshold = client.capabilities.bulk_order_lookup_threshold

        if threshold is None:
            return self.all_orders_threshold

        if threshold <= 0:
            return self.all_orders_threshold

        return threshold

    @staticmethod
    def _int_error_code(code: int | str | None) -> int | None:
        if isinstance(code, int):
            return code
        if isinstance(code, str):
            stripped = code.strip()
            if stripped.lstrip("-").isdigit():
                return int(stripped)
        return None

    async def _clear_reconcile_failure(
        self,
        order: Order,
    ) -> None:
        if not order.order_id:
            return

        await self.redis_order_repo.clear_reconcile_failure(
            exchange=order.exchange.value,
            market_type=order.market_type.value,
            order_id=order.order_id,
        )

    async def _increment_reconcile_failure(
        self,
        order: Order,
    ) -> int:
        if not order.order_id:
            return 0

        return await self.redis_order_repo.increment_reconcile_failure(
            exchange=order.exchange.value,
            market_type=order.market_type.value,
            order_id=order.order_id,
            ttl_sec=self.reconcile_failure_ttl_sec,
        )
