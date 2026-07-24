from __future__ import annotations

import os

import pytest
import pytest_asyncio

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
)
from schemas.position import PositionSide
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import (
    OrderPostgresRepository,
    StaleOrderVersionError,
)

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState


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


def make_market_order() -> Order:
    return Order(
        order_id="ORD-MARKET-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity="0.01",
        price=None,
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


def make_stop_market_order() -> Order:
    return Order(
        order_id="ORD-STOP-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
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


def make_stop_market_close_position_order() -> Order:
    return Order(
        order_id="ORD-STOP-CLOSEPOS-001",
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        quantity="0",
        price=None,
        trigger_price="59000",
        reduce_only=False,
        close_position=True,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=OrderStatus.PENDING_NEW,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        version=1,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_insert_and_load_regular_market_order(
    postgres_client: PostgresClient,
) -> None:
    """
    일반 MARKET 주문을 order_intents/orders에 저장하고 joined 조회로 완전한 Order를 복원하는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_market_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            inserted_intent = await intent_repo.insert_returning(
                conn=conn,
                order=order,
            )
            inserted_order = await order_repo.insert_initial_returning(
                conn=conn,
                order=order,
            )

        assert order.order_id
        assert inserted_intent.order_id == order.order_id
        assert inserted_order.order_id == order.order_id
        assert inserted_order.status == OrderStatus.PENDING_NEW
        row = await order_repo.get_joined_order(
            conn=conn,
            order_id=order.order_id,
        )

    assert row is not None

    loaded = Order.model_validate(row)

    assert loaded.order_id == order.order_id
    assert loaded.order_route == OrderRoute.REGULAR
    assert loaded.client_order_id == order.order_id
    assert loaded.client_conditional_id is None
    assert loaded.order_type == OrderType.MARKET
    assert loaded.quantity == "0.01"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_insert_and_load_conditional_stop_market_order(
    postgres_client: PostgresClient,
) -> None:
    """
    STOP_MARKET 조건부 주문이 CONDITIONAL route와 client_conditional_id 기준으로 저장/복원되는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_stop_market_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await intent_repo.insert(conn=conn, order=order)
            await order_repo.insert_initial(conn=conn, order=order)

        assert order.order_id
        row = await order_repo.get_joined_order(
            conn=conn,
            order_id=order.order_id,
        )

    assert row is not None

    loaded = Order.model_validate(row)

    assert loaded.order_id == order.order_id
    assert loaded.order_route == OrderRoute.CONDITIONAL
    assert loaded.client_order_id is None
    assert loaded.client_conditional_id == order.order_id
    assert loaded.trigger_price == "59000"
    assert loaded.position_action == PositionAction.CLOSE

@pytest.mark.stable
@pytest.mark.asyncio
async def test_insert_close_position_stop_market_order(
    postgres_client: PostgresClient,
) -> None:
    """
    close_position=True 조건부 주문이 quantity=0, reduce_only=False 상태로 저장/복원되는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_stop_market_close_position_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await intent_repo.insert(conn=conn, order=order)
            await order_repo.insert_initial(conn=conn, order=order)

        assert order.order_id
        row = await order_repo.get_joined_order(
            conn=conn,
            order_id=order.order_id,
        )

    assert row is not None

    loaded = Order.model_validate(row)

    assert loaded.close_position is True
    assert loaded.reduce_only is False
    assert loaded.quantity == "0"
    # assert loaded.conditional_status == ConditionalStatus.NEW
    assert loaded.order_route == OrderRoute.CONDITIONAL
    assert loaded.order_type == OrderType.STOP_MARKET

@pytest.mark.stable
@pytest.mark.asyncio
async def test_transition_conditional_order_acknowledged(
    postgres_client: PostgresClient,
) -> None:
    """
    조건부 주문 ACK 전이가 version 증가와 조건부 상태/거래소 응답 필드까지 반영하는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_stop_market_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await intent_repo.insert(conn=conn, order=order)
            await order_repo.insert_initial(conn=conn, order=order)

        updated = order.model_copy(deep=True)
        updated.status = OrderStatus.ACKNOWLEDGED
        updated.conditional_status = ConditionalStatus.NEW
        updated.exchange_conditional_id = "123456"
        updated.exchange_conditional_status = BinanceConditionalOrderState.new
        updated.acknowledged_ts = 1_700_000_001_000
        updated.updated_ts = 1_700_000_001_000
        updated.raw_exchange_response = {
            "algoId": "123456",
            "algoStatus": BinanceConditionalOrderState.new,
        }

        row = await order_repo.transition(
            conn=conn,
            order=updated,
            expected_version=1,
        )

        assert int(row["version"]) == 2

        assert order.order_id
        loaded_row = await order_repo.get_joined_order(
            conn=conn,
            order_id=order.order_id,
        )

    loaded = Order.model_validate(loaded_row)

    assert loaded.version == 2
    assert loaded.status == OrderStatus.ACKNOWLEDGED
    assert loaded.conditional_status == ConditionalStatus.NEW
    assert loaded.exchange_conditional_id == "123456"
    assert loaded.raw_exchange_response == {
        "algoId": "123456",
        "algoStatus": BinanceConditionalOrderState.new,
    }

@pytest.mark.stable
@pytest.mark.asyncio
async def test_transition_stale_version_raises(
    postgres_client: PostgresClient,
) -> None:
    """
    낡은 expected_version으로 상태 전이를 시도하면 optimistic lock 충돌이 발생하는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_market_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await intent_repo.insert(conn=conn, order=order)
            await order_repo.insert_initial(conn=conn, order=order)

        updated = order.model_copy(deep=True)
        updated.status = OrderStatus.SUBMITTED
        updated.updated_ts = 1_700_000_001_000

        await order_repo.transition(
            conn=conn,
            order=updated,
            expected_version=1,
        )

        updated_again = updated.model_copy(deep=True)
        updated_again.status = OrderStatus.ACKNOWLEDGED
        updated_again.updated_ts = 1_700_000_002_000

        with pytest.raises(StaleOrderVersionError):
            await order_repo.transition(
                conn=conn,
                order=updated_again,
                expected_version=1,
            )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_get_by_client_conditional_id(
    postgres_client: PostgresClient,
) -> None:
    """
    client_conditional_id로 조건부 주문을 joined 조회할 수 있는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    order = make_stop_market_order()

    async with pool.acquire() as conn:
        async with conn.transaction():
            await intent_repo.insert(conn=conn, order=order)
            await order_repo.insert_initial(conn=conn, order=order)

        assert order.client_conditional_id
        row = await order_repo.get_by_client_conditional_id(
            conn=conn,
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP.value,
            client_conditional_id=order.client_conditional_id,
        )

    assert row is not None
    assert row["order_id"] == order.order_id


@pytest.mark.stable
@pytest.mark.asyncio
async def test_list_non_terminal_joined_orders_page_filters_and_paginates(
    postgres_client: PostgresClient,
) -> None:
    """
    reconciliation worker용 PG page 조회가 exchange/market/route를 DB에서 필터링하고
    updated_ts + order_id cursor로 다음 페이지를 이어서 조회하는지 검증한다.
    """
    intent_repo = OrderIntentPostgresRepository()
    order_repo = OrderPostgresRepository()
    pool = postgres_client.require_pool()

    regular_1 = make_market_order().model_copy(
        update={
            "order_id": "ORD-PAGE-001",
            "status": OrderStatus.ACKNOWLEDGED,
            "updated_ts": 1_700_000_001_000,
        }
    )
    regular_2 = make_market_order().model_copy(
        update={
            "order_id": "ORD-PAGE-002",
            "status": OrderStatus.SUBMITTED,
            "updated_ts": 1_700_000_002_000,
        }
    )
    regular_3 = make_market_order().model_copy(
        update={
            "order_id": "ORD-PAGE-003",
            "status": OrderStatus.PENDING_CANCEL,
            "updated_ts": 1_700_000_003_000,
        }
    )
    conditional = make_stop_market_order().model_copy(
        update={
            "order_id": "ORD-PAGE-COND-001",
            "status": OrderStatus.ACKNOWLEDGED,
            "updated_ts": 1_700_000_004_000,
        }
    )
    terminal = make_market_order().model_copy(
        update={
            "order_id": "ORD-PAGE-FILLED-001",
            "status": OrderStatus.FILLED,
            "updated_ts": 1_700_000_005_000,
        }
    )

    orders = [regular_1, regular_2, regular_3, conditional, terminal]

    async with pool.acquire() as conn:
        async with conn.transaction():
            for order in orders:
                await intent_repo.insert(conn=conn, order=order)
                await order_repo.insert_initial(conn=conn, order=order)

        first_page = await order_repo.list_non_terminal_joined_orders_page(
            conn,
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            order_route=OrderRoute.REGULAR,
            limit=2,
        )

        second_page = await order_repo.list_non_terminal_joined_orders_page(
            conn,
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            order_route=OrderRoute.REGULAR,
            cursor_updated_ts=first_page[-1].updated_ts,
            cursor_order_id=first_page[-1].order_id,
            limit=2,
        )

    assert [order.order_id for order in first_page] == [
        "ORD-PAGE-001",
        "ORD-PAGE-002",
    ]
    assert [order.order_id for order in second_page] == ["ORD-PAGE-003"]
