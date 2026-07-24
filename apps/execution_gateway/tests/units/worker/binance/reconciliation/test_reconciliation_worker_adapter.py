from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from execution_gateway.exchange import ExchangeCapabilities, ExchangeOrderSnapshot
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.workers.reconciliation_worker import ReconciliationWorker, ExternalOrphanPolicy
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
    PositionAction,
)
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

pytestmark = pytest.mark.integration


def make_order(
    order_id: str,
    *,
    symbol: str = "BTCUSDT",
    status: OrderStatus = OrderStatus.PENDING_NEW,
    version: int = 1,
    created_ts: int = 1_700_000_000_000,
    updated_ts: int = 1_700_000_000_000,
) -> Order:
    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        signal_id=None,
        strategy_name=None,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        stop_price=None,
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=created_ts,
        submitted_ts=None,
        filled_ts=None,
        updated_ts=updated_ts,
        status=status,
        version=version,
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

    if status == OrderStatus.SUBMITTED:
        updated.submitted_ts = updated_ts

    if status == OrderStatus.FILLED:
        updated.filled_ts = updated_ts

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
    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"PostgreSQL 연결 불가: {e}")

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
def redis_repo(
    redis_stream_client: RedisStreamClient,
) -> OrderStateRedisRepository:
    return OrderStateRedisRepository(redis_stream_client)


@pytest.fixture
def order_state_service(
    postgres_client: PostgresClient,
    redis_repo: OrderStateRedisRepository,
) -> OrderStateService:
    return OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=redis_repo,
    )


def make_worker(
    *,
    adapter,
    gateway,
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
    external_orphan_policy:ExternalOrphanPolicy = "log",
) -> ReconciliationWorker:
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities(
        supports_conditional_reconciliation=True,
        supports_bulk_order_lookup=True,
    )
    client.get_open_conditional_orders = AsyncMock(return_value=[])
    client.get_conditional_order = AsyncMock(return_value=None)
    client.cancel_conditional_order_by_id = AsyncMock()

    def snapshot_from_row(row: dict) -> ExchangeOrderSnapshot:
        raw_status = row.get("status")
        return ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=str(row.get("symbol", "BTCUSDT")).upper(),
            client_order_id=str(row.get("clientOrderId")) if row.get("clientOrderId") else None,
            exchange_order_id=str(row.get("orderId")) if row.get("orderId") else None,
            status=OrderStatus.FILLED if raw_status == "FILLED" else OrderStatus.ACKNOWLEDGED,
            filled_quantity=str(row.get("executedQty", "0")),
            avg_fill_price=str(row.get("avgPrice", "0")),
            raw_status=str(raw_status) if raw_status else None,
            raw=row,
        )

    async def get_open_orders(*, symbol: str | None = None):
        rows = await adapter.get_open_orders(symbol=symbol)
        return [snapshot_from_row(row) for row in rows]

    async def get_order(order: Order):
        row = await adapter.get_order(
            symbol=order.symbol,
            client_order_id=order.order_id,
        )
        return snapshot_from_row(row)

    async def find_order_snapshots(
        *,
        symbol: str,
        orders: list[Order],
        lookback_ms: int,
        limit: int,
    ):
        rows = await adapter.get_all_orders(
            symbol=symbol,
            order_id=None,
            start_time=None,
            end_time=None,
            limit=limit,
        )
        rows_by_client_id = {
            str(row.get("clientOrderId")): row
            for row in rows
            if row.get("clientOrderId")
        }
        return {
            order.order_id: snapshot_from_row(rows_by_client_id[order.order_id])
            for order in orders
            if order.order_id in rows_by_client_id
        }

    async def cancel_regular_order_by_client_id(
        *,
        symbol: str,
        client_order_id: str,
    ) -> None:
        await adapter.cancel_order(
            symbol=symbol,
            client_order_id=client_order_id,
        )

    client.get_open_orders = AsyncMock(side_effect=get_open_orders)
    client.get_order = AsyncMock(side_effect=get_order)
    client.find_order_snapshots = AsyncMock(side_effect=find_order_snapshots)
    client.cancel_regular_order_by_client_id = AsyncMock(
        side_effect=cancel_regular_order_by_client_id,
    )
    client.capabilities = ExchangeCapabilities(
        supports_bulk_order_lookup=True,
        bulk_order_lookup_threshold=3,
    )

    registry = ExchangeExecutionClientRegistry()
    registry.register(client)

    async def apply_reconciliation_order_snapshot(
        *,
        order_id: str,
        snapshot: ExchangeOrderSnapshot,
    ):
        return await gateway.apply_reconciliation_snapshot(
            order_id=order_id,
            snapshot=snapshot.raw,
        )

    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        side_effect=apply_reconciliation_order_snapshot,
    )

    return ReconciliationWorker(
        exchange_clients=registry,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_order_repo=redis_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=60,
        recent_grace_ms=0,
        external_orphan_policy=external_orphan_policy,
        active_symbols=None,
        all_orders_threshold=6,
        all_orders_lookback_ms=60_000,
        all_orders_limit=1000,
    )


@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_repairs_pg_open_missing_in_redis_projection(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """실제 PG에는 open 주문이 있고 Redis projection만 없을 때 projection 복구를 검증한다."""
    order = make_order("ORD-PG-REDIS-MISS-001")

    order = await order_state_service.create_order(order)

    order = await transition(
        service=order_state_service,
        current=order,
        status=OrderStatus.SUBMITTED,
        updated_ts=1_700_000_000_100,
    )

    order = await transition(
        service=order_state_service,
        current=order,
        status=OrderStatus.ACKNOWLEDGED,
        updated_ts=1_700_000_000_200,
    )

    # Redis projection만 삭제해서 PG O, Redis X 상태를 만든다.
    # pyrefly: ignore [bad-argument-type]
    await redis_repo.delete(order.order_id)

    # pyrefly: ignore [bad-argument-type]
    assert await redis_repo.get(order.order_id) is None

    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": order.order_id,
                "symbol": order.symbol,
                "status": "NEW",
                "orderId": 123,
                "executedQty": "0",
                "avgPrice": "0",
            }
        ]
    )

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
    )

    with patch.object(
        worker,
        "_repair_pg_missing_in_redis",
        wraps=worker._repair_pg_missing_in_redis,
    ) as repair_pg_missing_in_redis_spy, patch.object(
        worker,
        "_repair_missing_open_by_single_get_order",
        wraps=worker._repair_missing_open_by_single_get_order,
    ) as single_get_order_spy:
        await worker.reconcile_once()

        repair_pg_missing_in_redis_spy.assert_awaited_once()
        single_get_order_spy.assert_not_awaited()

    # pyrefly: ignore [bad-argument-type]
    redis_row = await redis_repo.get(order.order_id)

    assert redis_row is not None
    assert redis_row["order_id"] == order.order_id
    assert redis_row["status"] == OrderStatus.ACKNOWLEDGED.value
    assert redis_row["version"] == order.version

    open_orders = await redis_repo.list_open_regular_orders(
        exchange=order.exchange.value,
        market_type=order.market_type.value,
    )
    open_ids = {row["order_id"] for row in open_orders}

    assert order.order_id in open_ids

    # 거래소 openOrders에도 있었으므로 get_order 보정은 필요 없어야 한다.
    gateway.apply_reconciliation_snapshot.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_deletes_redis_projection_when_missing_in_postgres(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """실제 Redis projection만 있고 PG 원본이 없을 때 stale projection 삭제를 검증한다."""
    fake_order = make_order(
        "ORD-REDIS-ONLY-001",
        status=OrderStatus.ACKNOWLEDGED,
        version=3,
    )

    # PostgreSQL에는 넣지 않고 Redis에만 projection 생성.
    await redis_repo.save(fake_order)

    # pyrefly: ignore [bad-argument-type]
    assert await redis_repo.get(fake_order.order_id) is not None

    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(return_value=[])

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    # pyrefly: ignore [bad-argument-type]
    redis_row = await redis_repo.get(fake_order.order_id)

    assert redis_row is None

    open_orders = await redis_repo.list_open_regular_orders(
        exchange=fake_order.exchange.value,
        market_type=fake_order.market_type.value,
    )
    open_ids = {row["order_id"] for row in open_orders}

    assert fake_order.order_id not in open_ids

    gateway.apply_reconciliation_snapshot.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_detects_exchange_orphan_and_skips_gateway_with_log_policy(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """거래소 regular orphan 주문을 발견해도 log 정책에서는 자동 취소하지 않음을 검증한다."""
    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": "EXTERNAL-ORDER-001",
                "symbol": "BTCUSDT",
                "status": "NEW",
                "orderId": 999,
            }
        ]
    )
    adapter.cancel_order = AsyncMock()

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    adapter.cancel_order.assert_not_awaited()
    gateway.apply_reconciliation_snapshot.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_detects_exchange_orphan_gateway_with_cancel_policy(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """거래소 regular orphan 주문의 경우 cancel 정책에서는 자동 취소 되는 지 검증한다."""
    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": "EXTERNAL-ORDER-001",
                "symbol": "BTCUSDT",
                "status": "NEW",
                "orderId": 999,
            }
        ]
    )
    adapter.cancel_order = AsyncMock()

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
        external_orphan_policy="cancel"
    )

    await worker.reconcile_once()

    adapter.cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        client_order_id="EXTERNAL-ORDER-001",
    )
    gateway.apply_reconciliation_snapshot.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_detects_exchange_orphan_and_does_not_cancel_with_log_policy(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """거래소 regular orphan 주문을 발견해도 log 정책에서는 Gateway 보정도 수행하지 않음을 검증한다."""
    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": "EXTERNAL-ORDER-001",
                "symbol": "BTCUSDT",
                "status": "NEW",
                "orderId": 999,
            }
        ]
    )
    adapter.cancel_order = AsyncMock()

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    adapter.cancel_order.assert_not_awaited()
    gateway.apply_reconciliation_snapshot.assert_not_awaited()


@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_detects_exchange_orphan_cancel_with_log_policy(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """거래소 regular orphan 주문을 발견해도 log 정책에서는 Gateway 보정도 수행하지 않음을 검증한다."""
    adapter = MagicMock()
    adapter.get_open_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": "EXTERNAL-ORDER-001",
                "symbol": "BTCUSDT",
                "status": "NEW",
                "orderId": 999,
            }
        ]
    )
    adapter.cancel_order = AsyncMock()

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock()

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
        external_orphan_policy="cancel"
    )

    await worker.reconcile_once()

    adapter.cancel_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        client_order_id="EXTERNAL-ORDER-001",
    )
    gateway.apply_reconciliation_snapshot.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_repairs_pg_open_missing_from_exchange_open_with_all_orders(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """PG open 주문 다수가 거래소 open 목록에 없으면 allOrders bulk 조회로 보정한다."""
    orders: list[Order] = []

    all_orders_threshold = 6

    for i in range(all_orders_threshold):
        order = make_order(f"ORD-MISSING-EXCHANGE-{i}")
        order = await order_state_service.create_order(order)

        order = await transition(
            service=order_state_service,
            current=order,
            status=OrderStatus.SUBMITTED,
            updated_ts=1_700_000_000_100 + i,
        )

        order = await transition(
            service=order_state_service,
            current=order,
            status=OrderStatus.ACKNOWLEDGED,
            updated_ts=1_700_000_000_200 + i,
        )

        orders.append(order)

    adapter = MagicMock()

    # Binance openOrders에는 아무것도 없다고 가정.
    # PG에는 open인데 거래소 openOrders에는 없음.
    adapter.get_open_orders = AsyncMock(return_value=[])

    adapter.get_all_orders = AsyncMock(
        return_value=[
            {
                "clientOrderId": order.order_id,
                "symbol": order.symbol,
                "status": "FILLED",
                "orderId": i,
                "executedQty": "0.1",
                "avgPrice": "60000",
            }
            for i, order in enumerate(orders)
        ]
    )

    adapter.get_order = AsyncMock()

    updated_order = orders[0].model_copy(deep=True)
    updated_order.status = OrderStatus.FILLED
    updated_order.version = 99

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock(return_value=updated_order)

    worker = make_worker(
        adapter=adapter,
        gateway=gateway,
        order_state_service=order_state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    adapter.get_open_orders.assert_awaited_once()
    adapter.get_all_orders.assert_awaited_once()
    adapter.get_order.assert_not_awaited()

    assert gateway.apply_reconciliation_snapshot.await_count == 6
