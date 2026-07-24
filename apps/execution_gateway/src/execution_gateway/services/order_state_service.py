from __future__ import annotations

from typing import Optional

from storage.postgres_client import PostgresClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import (
    OrderPostgresRepository,
)
from storage.repositories.postgres.outbox_repo import (
    OutboxPostgresRepository,
)
from common.logging import setup_logger

logger = setup_logger(__name__)

from schemas.order import Order, TERMINAL_STATUSES, OrderRoute

from schemas.market import Exchange, MarketType
from collections.abc import AsyncIterator

# from storage.identifiers import (
#     RedisKey,
#     redis_order_live_key,
# )

from common.converters import enum_value


class OrderStateService:
    """
    주문 상태 저장 오케스트레이션 서비스.

    PostgreSQL:
      - 원본 상태(Source of Truth)
      - order_intents
      - orders
      - outbox_events

    Redis:
      - 빠른 조회용 projection
      - order:live:{id}
      - order:open
    """

    def __init__(
        self,
        *,
        postgres: PostgresClient,
        intent_repo: OrderIntentPostgresRepository,
        postgres_order_repo: OrderPostgresRepository,
        outbox_repo: OutboxPostgresRepository,
        redis_order_repo: OrderStateRedisRepository,
    ) -> None:
        self.postgres = postgres
        self.order_intent_repo = intent_repo
        self.postgres_order_repo = postgres_order_repo
        self.outbox_repo = outbox_repo
        self.redis_order_repo = redis_order_repo

    async def create_order(self, order: Order) -> Order:
        """
        최초 주문 생성.

        순서:
          1. PostgreSQL transaction
             - order_intents insert
             - orders insert
             - outbox insert
          2. commit 성공 후 Redis 저장

        Redis 저장이 실패하면 예외를 올린다.
        이 경우 Binance 주문 전송까지는 가지 않으므로 안전하다.
        PostgreSQL에는 PENDING_NEW 원본이 남고,
        나중에 repair/rebuild 대상으로 복구할 수 있다.
        """
        payload = order.model_dump(mode="json", exclude_none=True)
        pool = self.postgres.require_pool()

        # Postgres 트랜잭션안에서 처리
        async with pool.acquire() as conn:
            async with conn.transaction():
                await self.order_intent_repo.insert(conn=conn, order=order)
                await self.postgres_order_repo.insert_initial(conn=conn, order=order)

                # [claim], event_type를 문자열로 직접 입력하는 데, 이 방식이 맞는 지 랑 잘 동작하는 지 파악 바람
                await self.outbox_repo.insert(
                    conn=conn,
                    aggregate_id=order.order_id,
                    event_type="ORDER_CREATED",
                    payload=payload,
                    created_ts=order.created_ts,
                )

        # PostgreSQL commit 이후 기존 Redis hot state 저장
        await self.redis_order_repo.save(order)

        logger.info(
            f"주문 원본 생성 완료: "
            f"order_id={order.order_id}, "
            f"status={order.status.value}, "
            f"version={order.version}"
        )

        return order

    async def transition_order(
        self,
        *,
        current_order: Order,
        updated_order: Order,
    ) -> Order:
        """
        상태 전이 저장.

        current_order:
          - 전이 전 상태
          - expected_version의 기준

        updated_order:
          - 전이 후 상태가 반영된 Order 객체

        상태 반영
          - Postgres
          - Redis

        순서:
          1. PostgreSQL transaction
             - orders versioned update
             - outbox insert
          2. commit
          3. Redis projection 전체 재저장

        """
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                updated_row = await self.postgres_order_repo.transition(
                    conn=conn,
                    order=updated_order,
                    expected_version=current_order.version,
                )

                persisted_order = updated_order.model_copy(deep=True)
                persisted_order.version = int(updated_row["version"])

                await self.outbox_repo.insert(
                    conn=conn,
                    aggregate_id=persisted_order.order_id,
                    event_type="ORDER_STATUS_CHANGED",
                    payload={
                        "previous_status": current_order.status.value,
                        "order": persisted_order.model_dump(
                            mode="json",
                            exclude_none=True,
                        ),
                    },
                    created_ts=persisted_order.updated_ts,
                )

        redis_projection_applied: bool | None = None
        try:
            redis_projection_applied = await self.refresh_order_projection(
                persisted_order
            )
        except Exception as e:
            logger.error(
                f"Redis projection 갱신 실패: "
                f"order_id={persisted_order.order_id}, "
                f"status={persisted_order.status.value}, "
                f"version={persisted_order.version}, "
                f"err={e}",
                exc_info=True,
            )

        logger.info(
            f"주문 상태 원본 갱신 완료: "
            f"order_id={persisted_order.order_id}, "
            f"{current_order.status.value} -> {persisted_order.status.value}, "
            f"version={current_order.version} -> {persisted_order.version}, "
            f"redis_projection_applied={redis_projection_applied!r}"
        )

        return persisted_order

    async def load_order_from_postgres(
        self,
        *,
        order_id: str,
        refresh_projection: bool = True,
    ) -> Optional[Order]:
        """
        PostgreSQL 원본에서 주문을 로드한다.

        동작:
          1. PostgreSQL order_intents + orders 조인 조회
          2. Order 모델로 복원
          3. refresh_projection=True면 Redis projection 재생성 시도
        """
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            row = await self.postgres_order_repo.get_joined_order(
                conn=conn,
                order_id=order_id,
            )

        if not row:
            return None

        order = Order.model_validate(row)

        if refresh_projection:
            try:
                await self.refresh_order_projection(order)
            except Exception as e:
                logger.error(
                    f"Redis projection 재생성 실패: "
                    f"order_id={order_id}, "
                    f"status={order.status.value}, "
                    f"version={order.version}, "
                    f"err={e}",
                    exc_info=True,
                )

        return order

    async def load_orders_from_postgres(
        self,
        order_ids: set[str] | list[str],
        *,
        refresh_projection: bool = True,
    ) -> dict[str, Order]:
        """
        PostgreSQL 원본에서 여러 주문을 한 번에 로드.

        Returns:
            dict[order_id, Order]
        """
        ids = list(order_ids)

        if not ids:
            return {}

        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            rows = await self.postgres_order_repo.get_joined_orders_by_ids(
                conn,
                ids,
            )

        result: dict[str, Order] = {}

        for row in rows:
            try:
                order = Order.model_validate(row)

                if not order.order_id:
                    continue

                result[order.order_id] = order

                if refresh_projection:
                    try:
                        await self.refresh_order_projection(order)
                    except Exception as e:
                        logger.error(
                            f"Redis projection batch refresh 실패: "
                            f"order_id={order.order_id}, "
                            f"status={order.status.value}, "
                            f"version={order.version}, "
                            f"err={e}",
                            exc_info=True,
                        )

            except Exception as e:
                logger.warning(
                    f"PostgreSQL batch order row 파싱 실패: " f"row={row}, err={e}",
                    exc_info=True,
                )

        missing_ids = set(ids) - set(result)

        if missing_ids:
            logger.warning(
                f"PostgreSQL batch load 결과 누락: "
                f"requested={len(ids)}, "
                f"found={len(result)}, "
                f"missing={missing_ids}"
            )

        return result

    async def load_order(
        self,
        *,
        order_id: str,
        refresh_projection: bool = True,
    ) -> Optional[Order]:
        """
        주문 로드.

        우선순위:
          1. Redis projection
          2. PostgreSQL source of truth fallback
          3. PostgreSQL에서 찾으면 Redis projection 재생성

        Redis는 projection이므로 miss가 곧 주문 없음은 아니다.
        """
        data = await self.redis_order_repo.get(order_id)

        if data:
            try:
                return Order.model_validate(data)
            except Exception as e:
                logger.warning(
                    f"Redis projection 주문 파싱 실패. PostgreSQL fallback 시도: "
                    f"order_id={order_id}, data={data}, err={e}",
                    exc_info=True,
                )

        order = await self.load_order_from_postgres(
            order_id=order_id,
            refresh_projection=refresh_projection,
        )

        if order:
            logger.info(
                f"Redis projection miss 복구 완료: "
                f"order_id={order_id}, "
                f"status={order.status.value}, "
                f"version={order.version}"
            )
            return order

        logger.warning(
            f"주문을 Redis projection / PostgreSQL 원본 어디에서도 찾지 못함: "
            f"order_id={order_id}"
        )
        return None

    # [claim] 아직 실제로 쓰이지 않는 메소드, 다른 메소드들도 이러한 케이스가 있는 지 파악
    async def list_open_orders_by_symbol_from_redis(self, exchange, market_type, symbol: str) -> list[Order]:
        """
        Redis projection 기준 특정 심볼 open 주문 조회.

        빠르지만 Redis projection 누락 가능성이 있다.
        실행 경로의 빠른 조회나 모니터링용으로 적합.
        """
        items = await self.redis_order_repo.list_open_by_symbol(
            exchange=exchange, market_type=market_type, symbol=symbol
        )
        orders: list[Order] = []

        for item in items:
            try:
                order = Order.model_validate(item)
                if order.status not in TERMINAL_STATUSES:
                    orders.append(order)
            except Exception as e:
                logger.warning(
                    f"Redis projection 주문 파싱 실패: "
                    f"symbol={symbol}, item={item}, err={e}",
                    exc_info=True,
                )

        return orders

    async def list_open_orders_by_symbol(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        refresh_projection: bool = True,
    ) -> list[Order]:
        """
        PostgreSQL 원본 기준 특정 심볼 open 주문 조회.

        cancel_all_open_orders 같은 중요한 실행 경로에서는
        Redis projection보다 이 메서드를 사용하는 것이 안전하다.
        """
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            orders = await self.postgres_order_repo.list_open_joined_by_symbol(
                conn=conn,
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
            )

        if refresh_projection:
            for order in orders:
                try:
                    await self.refresh_order_projection(order=order)
                except Exception as e:
                    logger.error(
                        f"Redis projection refresh 실패: "
                        f"order_id={order.order_id}, "
                        f"symbol={symbol}, "
                        f"status={order.status.value}, "
                        f"version={order.version}, "
                        f"err={e}",
                        exc_info=True,
                    )

        return orders

    async def list_open_orders(
        self,
        *,
        refresh_projection: bool = True,
    ) -> list[Order]:
        """
        PostgreSQL 원본 기준 전체 open 주문 조회.

        여기서 open은 Binance NEW만 뜻하지 않고,
        terminal이 아닌 로컬 활성 주문 전체를 의미한다.

        사용처:
        - ReconciliationWorker
        - startup 이후 projection 검증
        - 관리자 점검
        """
        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            # terminal이 아닌 모든 주문 조회
            orders = await self.postgres_order_repo.list_non_terminal_joined_orders(conn)

        if refresh_projection:
            for order in orders:
                try:
                    await self.refresh_order_projection(order)

                except Exception as e:
                    logger.error(
                        f"Redis projection refresh 실패: "
                        f"order_id={order.order_id}, "
                        f"status={order.status.value}, "
                        f"version={order.version}, "
                        f"err={e}",
                        exc_info=True,
                    )

        return orders

    async def iter_open_order_batches(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        order_route: OrderRoute | None = None,
        batch_size: int = 1000,
        refresh_projection: bool = False,
    ) -> AsyncIterator[list[Order]]:
        cursor_updated_ts: int | None = None
        cursor_order_id: str | None = None
        pool = self.postgres.require_pool()

        while True:
            async with pool.acquire() as conn:
                orders = await self.postgres_order_repo.list_non_terminal_joined_orders_page(
                    conn,
                    exchange=exchange,
                    market_type=market_type,
                    order_route=order_route,
                    cursor_updated_ts=cursor_updated_ts,
                    cursor_order_id=cursor_order_id,
                    limit=batch_size,
                )

            if not orders:
                break

            if refresh_projection:
                for order in orders:
                    await self.refresh_order_projection(order)

            yield orders

            last = orders[-1]
            cursor_updated_ts = last.updated_ts
            cursor_order_id = last.order_id

    async def refresh_order_projection(self, order: Order) -> bool:
        """
        이미 가지고 있는 Order 객체를 Redis projection에 반영.
        """
        applied = await self.redis_order_repo.save(
            order
        )
        logger.info(
            "[Redis] projection upsert: "
            f"order_id={order.order_id} "
            # f"live_key={redis_order_live_key(order.order_id)} "
            # f"open_set={RedisKey.ORDER_OPEN_SET} "
            f"applied={applied} "
            f"status={order.status.value} "
            f"version={order.version}"
        )
        return applied

    async def load_order_by_exchange_order_id(
        self,
        *,
        exchange: Exchange | str,
        market_type: MarketType | str,
        exchange_order_id: str,
        refresh_projection: bool,
    ) -> Order | None:
        pool = self.postgres.require_pool()

        exchange_value = enum_value(exchange, str)
        market_type_value = enum_value(market_type, str)

        async with pool.acquire() as conn:
            row = await self.postgres_order_repo.get_by_exchange_order_id(
                conn=conn,
                exchange=exchange_value,
                market_type=market_type_value,
                exchange_order_id=exchange_order_id,
            )

        if not row:
            return None

        order = Order.model_validate(row)

        if refresh_projection:
            await self.refresh_order_projection(order)

        return order


    async def load_order_by_triggered_order_id(
        self,
        *,
        exchange: Exchange | str,
        market_type: MarketType | str,
        triggered_order_id: str,
        refresh_projection: bool,
    ) -> Order | None:
        pool = self.postgres.require_pool()

        exchange_value = enum_value(exchange, str)
        market_type_value = enum_value(market_type, str)

        async with pool.acquire() as conn:
            row = await self.postgres_order_repo.get_by_triggered_order_id(
                conn=conn,
                exchange=exchange_value,
                market_type=market_type_value,
                triggered_order_id=triggered_order_id,
            )

        if not row:
            return None

        order = Order.model_validate(row)

        if refresh_projection:
            await self.refresh_order_projection(order)

        return order


    async def load_order_by_client_conditional_id(
        self,
        *,
        exchange: Exchange | str,
        market_type: MarketType | str,
        client_conditional_id: str,
        refresh_projection: bool,
    ) -> Order | None:
        pool = self.postgres.require_pool()

        # [CLAIM]enum_value 로 변환
        exchange_value = exchange.value if hasattr(exchange, "value") else str(exchange)
        market_type_value = (
            market_type.value if hasattr(market_type, "value") else str(market_type)
        )

        async with pool.acquire() as conn:
            row = await self.postgres_order_repo.get_by_client_conditional_id(
                conn=conn,
                exchange=exchange_value,
                market_type=market_type_value,
                client_conditional_id=client_conditional_id,
            )

        if not row:
            return None

        order = Order.model_validate(row)

        if refresh_projection:
            try:
                await self.refresh_order_projection(order)
            except Exception as e:
                logger.error(
                    f"Redis projection refresh failed after conditional lookup: "
                    f"order_id={order.order_id}, err={e}",
                    exc_info=True,
                )

        return order


    async def load_order_by_exchange_conditional_id(
        self,
        *,
        exchange: Exchange | str,
        market_type: MarketType | str,
        exchange_conditional_id: str,
        refresh_projection: bool,
    ) -> Order | None:
        """
        Postgres[Source] 에서 주문 데이터를 로드한다.

        refresh_projection이 True이면 Redis projection을 갱신한다.
        """
        pool = self.postgres.require_pool()

        exchange_value = exchange.value if hasattr(exchange, "value") else str(exchange)
        market_type_value = (
            market_type.value if hasattr(market_type, "value") else str(market_type)
        )

        async with pool.acquire() as conn:
            row = await self.postgres_order_repo.get_by_exchange_conditional_id(
                conn=conn,
                exchange=exchange_value,
                market_type=market_type_value,
                exchange_conditional_id=exchange_conditional_id,
            )

        if not row:
            return None

        order = Order.model_validate(row)

        if refresh_projection:
            try:
                await self.refresh_order_projection(order)
            except Exception as e:
                logger.error(
                    f"Redis projection refresh failed after exchange conditional lookup: "
                    f"order_id={order.order_id}, err={e}",
                    exc_info=True,
                )

        return order
