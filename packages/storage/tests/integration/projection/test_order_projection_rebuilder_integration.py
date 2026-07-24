from __future__ import annotations

import os

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from storage.projection.order_projection_rebuilder import (
    OrderProjectionRebuilder,
)
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


def make_order(
    *,
    order_id: str,
) -> Order:
    return Order(
        order_id=order_id,
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
        submitted_ts=None,
        filled_ts=None,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
        version=1,
    )


async def transition(
    *,
    service: OrderStateService,
    current: Order,
    status: OrderStatus,
    updated_ts: int,
) -> Order:
    updated = current.model_copy(deep=True)
    updated.status = status
    updated.updated_ts = updated_ts
    return await service.transition_order(
        current_order=current,
        updated_order=updated,
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

@pytest.mark.stable
@pytest.mark.asyncio
async def test_rebuilder_rebuilds_only_non_terminal_orders(
    postgres_client: PostgresClient,
    redis_stream_client: RedisStreamClient,
    order_state_service: OrderStateService,
) -> None:
    redis_repo = OrderStateRedisRepository(redis_stream_client)
    pg_order_repo = OrderPostgresRepository()

    order_open = make_order(order_id="ORD-OPEN-001")
    order_unknown = make_order(order_id="ORD-UNKNOWN-001")
    order_terminal = make_order(order_id="ORD-FILLED-001")

    await order_state_service.create_order(order_open)
    await order_state_service.create_order(order_unknown)
    await order_state_service.create_order(order_terminal)

    order_unknown = await transition(
        service=order_state_service,
        current=order_unknown,
        status=OrderStatus.UNKNOWN,
        updated_ts=1_700_000_000_100,
    )

    order_terminal = await transition(
        service=order_state_service,
        current=order_terminal,
        status=OrderStatus.FILLED,
        updated_ts=1_700_000_000_200,
    )

    rebuilder = OrderProjectionRebuilder(
        postgres=postgres_client,
        postgres_order_repo=pg_order_repo,
        redis_order_repo=redis_repo,
    )

    result = await rebuilder.rebuild_active_projection(
        reset_existing=True,
    )

    assert result.total_rows == 2
    assert result.rebuilt == 2
    assert result.failed == 0

    open_orders = await redis_repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    open_ids = {row["order_id"] for row in open_orders}

    assert open_ids == {
        "ORD-OPEN-001",
        "ORD-UNKNOWN-001",
    }

    unknown_orders = await redis_repo.list_unknown_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    unknown_ids = {row["order_id"] for row in unknown_orders}

    assert unknown_ids == {"ORD-UNKNOWN-001"}

    recovery_orders = await redis_repo.list_recovery_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    recovery_ids = {row["order_id"] for row in recovery_orders}

    assert recovery_ids == {"ORD-UNKNOWN-001"}

    assert await redis_repo.get("ORD-FILLED-001") is None