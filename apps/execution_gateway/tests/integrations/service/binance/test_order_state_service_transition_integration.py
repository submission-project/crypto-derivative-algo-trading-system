from __future__ import annotations

import os

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from execution_gateway.services.order_state_service import OrderStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
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

pytestmark = pytest.mark.integration


def make_order() -> Order:
    return Order(
        order_id="ORD-IT-TRANSITION-001",
        source=OrderSource.MANUAL,
        signal_id=None,
        strategy_name=None,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        stop_price=None,
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
        version=1,
    )


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN이 설정되지 않았습니다.")

    client = PostgresClient(
        dsn=dsn,
        min_size=1,
        max_size=2,
    )
    await client.connect()

    pool = client.require_pool()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox_events,
                orders,
                order_intents
            RESTART IDENTITY CASCADE
            """
        )

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox_events,
                orders,
                order_intents
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis_stream_client() -> RedisStreamClient:
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=15,
    )

    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()
    yield client
    await client.client.flushdb()
    await client.close()


@pytest.fixture
def order_state_service(
    postgres_client: PostgresClient,
    redis_stream_client: RedisStreamClient,
) -> OrderStateService:
    return OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=OrderStateRedisRepository(redis_stream_client),
    )


@pytest.mark.asyncio
async def test_transition_order_updates_pg_outbox_and_redis(
    order_state_service: OrderStateService,
    postgres_client: PostgresClient,
    redis_stream_client: RedisStreamClient,
) -> None:
    current_order = make_order()
    await order_state_service.create_order(current_order)

    updated_order = current_order.model_copy(deep=True)
    updated_order.status = OrderStatus.SUBMITTED
    updated_order.submitted_ts = 1_700_000_000_100
    updated_order.updated_ts = 1_700_000_000_100

    result = await order_state_service.transition_order(
        current_order=current_order,
        updated_order=updated_order,
    )

    assert result.status == OrderStatus.SUBMITTED
    assert result.version == 2

    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        order_row = await conn.fetchrow(
            """
            SELECT *
            FROM orders
            WHERE order_id = $1
            """,
            current_order.order_id,
        )

        outbox_rows = await conn.fetch(
            """
            SELECT *
            FROM outbox_events
            WHERE aggregate_id = $1
            ORDER BY event_id
            """,
            current_order.order_id,
        )

    assert order_row is not None
    assert order_row["status"] == OrderStatus.SUBMITTED.value
    assert order_row["version"] == 2
    assert order_row["submitted_ts"] == 1_700_000_000_100

    assert len(outbox_rows) == 2
    assert outbox_rows[0]["event_type"] == "ORDER_CREATED"
    assert outbox_rows[1]["event_type"] == "ORDER_STATUS_CHANGED"

    redis_repo = OrderStateRedisRepository(redis_stream_client)

    assert current_order.order_id
    redis_row = await redis_repo.get(current_order.order_id)

    assert redis_row is not None
    assert redis_row["status"] == OrderStatus.SUBMITTED.value
    assert redis_row["version"] == 2

    open_orders = await redis_repo.list_open_regular_orders(
        exchange=current_order.exchange.value,
        market_type=current_order.market_type.value,
    )
    open_order_ids = {row["order_id"] for row in open_orders}
    assert current_order.order_id in open_order_ids