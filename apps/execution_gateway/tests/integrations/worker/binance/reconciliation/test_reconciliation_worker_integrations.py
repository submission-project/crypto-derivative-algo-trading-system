from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.dto.resp.OrderResponseDto import OrderRespDto
from common.config import settings as common_settings
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.workers.reconciliation_worker import ReconciliationWorker
from execution_gateway.exchange.capabilities import ExchangeCapabilities
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


def make_exchange_clients(
    *,
    adapter: MagicMock,
    # rate_limiter: LocalBinanceRateLimiter,
) -> ExchangeExecutionClientRegistry:
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=MagicMock(),
    )

    client.capabilities = ExchangeCapabilities(
        supports_bulk_order_lookup=True,
        bulk_order_lookup_threshold=3,
    )

    # pyrefly: ignore [bad-assignment]
    # client.rate_limiter = rate_limiter
    registry = ExchangeExecutionClientRegistry()
    registry.register(client)
    return registry


def make_order(
    order_id: str,
    *,
    symbol: str = "BTCUSDT",
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
        created_ts=1_700_000_000_000,
        submitted_ts=None,
        filled_ts=None,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
        version=1,
    )


def order_resp(row: dict) -> OrderRespDto:
    return OrderRespDto.from_response(row)


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

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_once_uses_all_orders_for_many_missing_exchange_open_orders(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """여러 PG open 주문이 거래소 openOrders에서 빠졌을 때 allOrders bulk 경로를 사용한다."""
    orders: list[Order] = []

    for i in range(6):
        order = make_order(f"ORD-IT-ALL-{i}")
        order = await order_state_service.create_order(order)

        # PENDING_NEW -> SUBMITTED -> ACKNOWLEDGED
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
    # 즉 PG에는 open인데 거래소 openOrders에는 없는 상태.
    adapter.get_open_orders = AsyncMock(return_value=[])

    adapter.get_all_orders = AsyncMock(
        return_value=[
            order_resp({
                "clientOrderId": order.order_id,
                "symbol": order.symbol,
                "status": "FILLED",
                "orderId": i,
                "executedQty": "0.1",
                "avgPrice": "60000",
            })
            for i, order in enumerate(orders)
        ]
    )
    adapter.get_order = AsyncMock()

    updated_order = orders[0].model_copy(deep=True)
    updated_order.status = OrderStatus.FILLED
    updated_order.version = 99

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock(return_value=updated_order)
    async def apply_reconciliation_order_snapshot(*, order_id, snapshot):
        return await gateway.apply_reconciliation_snapshot(
            order_id=order_id,
            snapshot=snapshot.raw,
        )

    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        side_effect=apply_reconciliation_order_snapshot,
    )

    worker = ReconciliationWorker(
        exchange_clients=make_exchange_clients(
            adapter=adapter,
        ),
        gateway=gateway,
        order_state_service=order_state_service,
        redis_order_repo=redis_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=60,
        recent_grace_ms=0,
        external_orphan_policy="log",
        active_symbols=None,
        all_orders_threshold=6,
        all_orders_lookback_ms=60_000,
        all_orders_limit=1000,
    )

    await worker.reconcile_once()
    
    adapter.get_open_orders.assert_awaited_once()
    adapter.get_all_orders.assert_awaited_once()
    adapter.get_order.assert_not_awaited()

    assert gateway.apply_reconciliation_snapshot.await_count == 6


@pytest.mark.asyncio
async def test_reconcile_once_fallbacks_to_get_order_when_all_orders_misses_some(
    order_state_service: OrderStateService,
    redis_repo: OrderStateRedisRepository,
) -> None:
    """allOrders bulk 조회에서 일부 주문을 못 찾으면 해당 주문만 get_order fallback으로 보정한다."""
    orders: list[Order] = []

    for i in range(6):
        order = make_order(f"ORD-IT-FALLBACK-{i}")
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
    adapter.get_open_orders = AsyncMock(return_value=[])

    # allOrders는 첫 5개만 반환
    adapter.get_all_orders = AsyncMock(
        return_value=[
            order_resp({
                "clientOrderId": order.order_id,
                "symbol": order.symbol,
                "status": "FILLED",
                "orderId": i,
                "executedQty": "0.1",
                "avgPrice": "60000",
            })
            for i, order in enumerate(orders[:5])
        ]
    )

    # 마지막 1개는 단건 fallback
    adapter.get_order = AsyncMock(
        return_value=order_resp({
            "clientOrderId": orders[5].order_id,
            "symbol": orders[5].symbol,
            "status": "FILLED",
            "orderId": 999,
            "executedQty": "0.1",
            "avgPrice": "60000",
        })
    )

    updated_order = orders[0].model_copy(deep=True)
    updated_order.status = OrderStatus.FILLED
    updated_order.version = 99

    gateway = MagicMock()
    gateway.apply_reconciliation_snapshot = AsyncMock(return_value=updated_order)
    async def apply_reconciliation_order_snapshot(*, order_id, snapshot):
        return await gateway.apply_reconciliation_snapshot(
            order_id=order_id,
            snapshot=snapshot.raw,
        )

    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        side_effect=apply_reconciliation_order_snapshot,
    )

    # limiter = LocalBinanceRateLimiter()

    worker = ReconciliationWorker(
        exchange_clients=make_exchange_clients(
            adapter=adapter,
            # rate_limiter=limiter,
        ),
        gateway=gateway,
        order_state_service=order_state_service,
        redis_order_repo=redis_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=60,
        recent_grace_ms=0,
        external_orphan_policy="log",
        active_symbols=None,
        all_orders_threshold=6,
        all_orders_lookback_ms=60_000,
        all_orders_limit=1000,
    )

    await worker.reconcile_once()

    adapter.get_open_orders.assert_awaited_once()
    adapter.get_all_orders.assert_awaited_once()

    adapter.get_order.assert_awaited_once_with(
        symbol="BTCUSDT",
        client_order_id=orders[5].order_id,
    )

    assert gateway.apply_reconciliation_snapshot.await_count == 6
