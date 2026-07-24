from __future__ import annotations

import os

import pytest
import pytest_asyncio

from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)
from schemas.position import PositionSide
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)

pytestmark = pytest.mark.integration


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN is not set")

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
                order_intents,
                positions
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
                order_intents,
                positions
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


def make_limit_order() -> Order:
    return Order(
        order_id="ORD-INTENT-LIMIT-001",
        source=OrderSource.MANUAL,
        signal_id="SIG-001",
        strategy_name="test-strategy",
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        order_route=OrderRoute.REGULAR,
        time_in_force=TimeInForce.GTC,
        quantity="0.01",
        price="60000",
        trigger_price=None,
        reduce_only=False,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=OrderStatus.PENDING_NEW,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=1,
    )


def make_conditional_order() -> Order:
    return Order(
        order_id="ORD-INTENT-STOP-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        price=None,
        trigger_price="59000",
        reduce_only=True,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.PENDING_NEW,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=1,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_order_intent_repo_insert_and_get(
    postgres_client: PostgresClient,
) -> None:
    """
    OrderIntentPostgresRepository.insert()가 order_intents row를 저장하고 get()으로 조회되는지 검증한다.
    """
    repo = OrderIntentPostgresRepository()
    pool = postgres_client.require_pool()
    order = make_limit_order()

    async with pool.acquire() as conn:
        await repo.insert(conn=conn, order=order)
        assert order.order_id
        row = await repo.get(conn, order.order_id)

    assert row is not None
    assert row["order_id"] == order.order_id
    assert row["source"] == OrderSource.MANUAL.value
    assert row["exchange"] == Exchange.BINANCE.value
    assert row["market_type"] == MarketType.PERP.value
    assert row["symbol"] == "BTCUSDT"
    assert row["order_type"] == OrderType.LIMIT.value
    assert row["order_route"] == OrderRoute.REGULAR.value
    assert row["time_in_force"] == TimeInForce.GTC.value
    assert row["price"] == "60000"
    assert row["trigger_price"] is None
    assert row["client_order_id"] == order.order_id

@pytest.mark.stable
@pytest.mark.asyncio
async def test_order_intent_repo_insert_returning_returns_order(
    postgres_client: PostgresClient,
) -> None:
    """
    insert_returning()이 DB RETURNING row와 raw_request를 합쳐 Order 모델을 복원하는지 검증한다.
    """
    repo = OrderIntentPostgresRepository()
    pool = postgres_client.require_pool()
    order = make_conditional_order()

    async with pool.acquire() as conn:
        inserted = await repo.insert_returning(conn=conn, order=order)

        assert order.order_id
        row = await repo.get(conn, order.order_id)

    assert row is not None
    assert inserted.order_id == order.order_id
    assert inserted.symbol == "BTCUSDT"
    assert inserted.order_route == OrderRoute.CONDITIONAL
    assert inserted.client_order_id is None
    assert inserted.client_conditional_id == order.order_id
    assert inserted.trigger_price == "59000"
    assert inserted.position_action == PositionAction.CLOSE


@pytest.mark.stable
@pytest.mark.asyncio
async def test_order_intent_rejects_market_with_trigger_price(
    postgres_client: PostgresClient,
) -> None:
    """
    MARKET 주문에 trigger_price가 들어오면 order_intents 제약조건이 거부하는지 검증한다.
    """
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                """
                INSERT INTO order_intents (
                    order_id,
                    source,
                    exchange,
                    market_type,
                    symbol,
                    side,
                    order_type,
                    order_route,
                    quantity,
                    trigger_price,
                    reduce_only,
                    close_position,
                    position_side,
                    position_action,
                    client_order_id,
                    raw_request,
                    created_ts
                )
                VALUES (
                    'BAD-001',
                    'MANUAL',
                    'BINANCE',
                    'PERP',
                    'BTCUSDT',
                    'BUY',
                    'MARKET',
                    'REGULAR',
                    '0.01',
                    '59000',
                    false,
                    false,
                    'BOTH',
                    'OPEN',
                    'BAD-001',
                    '{}'::jsonb,
                    1700000000000
                )
                """
            )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_order_intent_rejects_stop_market_without_trigger_price(
    postgres_client: PostgresClient,
) -> None:
    """
    STOP_MARKET 조건부 주문에 trigger_price가 없으면 order_intents 제약조건이 거부하는지 검증한다.
    """
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        with pytest.raises(Exception) as e:
            await conn.execute(
                """
                INSERT INTO order_intents (
                    order_id,
                    source,
                    exchange,
                    market_type,
                    symbol,
                    side,
                    order_type,
                    order_route,
                    quantity,
                    reduce_only,
                    close_position,
                    position_side,
                    position_action,
                    client_conditional_id,
                    raw_request,
                    created_ts
                )
                VALUES (
                    'BAD-002',
                    'MANUAL',
                    'BINANCE',
                    'PERP',
                    'BTCUSDT',
                    'SELL',
                    'STOP_MARKET',
                    'CONDITIONAL',
                    '0.01',
                    true,
                    false,
                    'BOTH',
                    'CLOSE',
                    'BAD-002',
                    '{}'::jsonb,
                    1700000000000
                )
                """
            )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_order_intent_accepts_close_position_quantity_zero(
    postgres_client: PostgresClient,
) -> None:
    """
    close_position=True 조건부 주문은 quantity=0이어도 order_intents에 저장 가능한지 검증한다.
    이 경우 수량을 직접 지정하지 않고, 거래소가 “현재 열린 포지션 전체”를 대상으로 처리합니다. 그래서 로컬 의도 테이블에서도 close_position=True이면 quantity=0을 허용해야 함.
    """
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO order_intents (
                order_id,
                source,
                exchange,
                market_type,
                symbol,
                side,
                order_type,
                order_route,
                quantity,
                trigger_price,
                reduce_only,
                close_position,
                position_side,
                position_action,
                client_conditional_id,
                raw_request,
                created_ts
            )
            VALUES (
                'GOOD-001',
                'MANUAL',
                'BINANCE',
                'PERP',
                'BTCUSDT',
                'SELL',
                'STOP_MARKET',
                'CONDITIONAL',
                '0',
                '59000',
                false,
                true,
                'BOTH',
                'CLOSE',
                'GOOD-001',
                '{}'::jsonb,
                1700000000000
            )
            """
        )

        row = await conn.fetchrow(
            """
            SELECT order_id, close_position, quantity
            FROM order_intents
            WHERE order_id = 'GOOD-001'
            """
        )

    assert row is not None
    assert row["close_position"] is True
    assert row["quantity"] == "0"
