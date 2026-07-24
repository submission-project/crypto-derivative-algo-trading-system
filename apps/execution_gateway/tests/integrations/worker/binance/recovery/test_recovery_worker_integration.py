from __future__ import annotations

from schemas.position import PositionSide
from schemas import OrderRequest

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter, BinanceKeyType
from execution_gateway.exchange import ExchangeCapabilities, ExchangeOrderSnapshot
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.workers.recovery_worker import RecoveryWorker
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    OrderRoute,
    PositionAction,
    TimeInForce,
    ConditionalStatus,
)
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository

from execution_gateway.config import settings as gw_settings

pytestmark = pytest.mark.integration

import logging
logger = logging.getLogger(__name__)

from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from common.time import epoch_ms

def _load_pem() -> str:
    pem_path = gw_settings.active_ed25519_key_pem
    if not pem_path or not os.path.exists(pem_path):
        pytest.skip(f"PEM 파일이 없습니다: {pem_path}")
    with open(pem_path, "r") as f:
        return f.read()


def make_order(
    *,
    order_id: str = "ORD-RECOVERY-NOMOCK-001",
    status: OrderStatus = OrderStatus.PENDING_NEW,
    version: int = 1,
    updated_ts: int = 1_700_000_000_000,
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
        created_ts=updated_ts,
        submitted_ts=None,
        filled_ts=None,
        updated_ts=updated_ts,
        status=status,
        version=version,
    )


class FakeExecutionClient:
    """
    RecoveryWorker가 사용하는 get_order(order)만 구현하는 테스트용 client.
    """

    exchange = Exchange.BINANCE
    market_type = MarketType.PERP
    capabilities = ExchangeCapabilities(
        supports_conditional_reconciliation=True,
    )

    def __init__(self, snapshots: dict[str, dict[str, Any]]) -> None:
        self.snapshots = snapshots
        self.calls: list[Order] = []

    async def get_order(self, order: Order) -> ExchangeOrderSnapshot:
        self.calls.append(order)

        # pyrefly: ignore [bad-argument-type]
        raw = self.snapshots.get(order.order_id)
        if raw is None:
            raise RuntimeError(f"snapshot not found: {order.order_id}")

        return ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=order.symbol,
            client_order_id=order.order_id,
            exchange_order_id=str(raw.get("orderId")),
            status=OrderStatus.ACKNOWLEDGED,
            raw_status=str(raw.get("status")),
            raw=raw,
        )

    async def close(self) -> None:
        return None


def _make_worker(
    *,
    gateway: ExecutionGateway | MagicMock,
    repo: OrderStateRedisRepository,
    client: FakeExecutionClient | MagicMock,
    failure_backoff_ms: int = 10_000,
) -> RecoveryWorker:
    registry = ExchangeExecutionClientRegistry()
    # pyrefly: ignore [bad-argument-type]
    registry.register(client)

    return RecoveryWorker(
        exchange_clients=registry,
        gateway=gateway,
        repo=repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=1,
        older_than_ms=1,
        batch_size=100,
        failure_backoff_ms=failure_backoff_ms,
    )


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

@pytest.mark.stable
@pytest.mark.asyncio
async def test_recovery_worker_reads_recovery_index_and_calls_gateway(
    redis_stream_client: RedisStreamClient,
) -> None:
    repo = OrderStateRedisRepository(redis_stream_client)

    order_id = "ORD-RECOVERY-IT-001"

    await repo.save(
        make_order(
            order_id=order_id,
            status=OrderStatus.SUBMITTED,
            updated_ts=1_700_000_000_000,
            version=1,
        )
    )

    raw_snapshot = {
        "clientOrderId": order_id,
        "symbol": "BTCUSDT",
        "status": "NEW",
        "orderId": 123,
        "executedQty": "0",
        "avgPrice": "0",
    }

    client = FakeExecutionClient(
        snapshots={order_id: raw_snapshot},
    )

    updated_order = make_order(
        order_id=order_id,
        status=OrderStatus.ACKNOWLEDGED,
        version=2,
    )

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        return_value=updated_order,
    )

    worker: RecoveryWorker = _make_worker(gateway=gateway, repo=repo, client=client)

    await worker.recover_once(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
    )

    assert len(client.calls) == 1
    assert client.calls[0].order_id == order_id

    gateway.apply_reconciliation_order_snapshot.assert_awaited_once()
    # pyrefly: ignore [missing-attribute]
    call_kwargs = gateway.apply_reconciliation_order_snapshot.await_args.kwargs
    assert call_kwargs["order_id"] == order_id
    assert call_kwargs["snapshot"].raw == raw_snapshot

@pytest.mark.stable
@pytest.mark.asyncio
async def test_recovery_worker_postpones_failed_order_in_redis(
    redis_stream_client: RedisStreamClient,
) -> None:
    repo = OrderStateRedisRepository(redis_stream_client)
    order = make_order(
        order_id="ORD-RECOVERY-POSTPONE-001",
        status=OrderStatus.SUBMITTED,
        updated_ts=1_700_000_000_000,
        version=1,
    )

    await repo.save(order)

    client = FakeExecutionClient(snapshots={})
    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock()

    worker: RecoveryWorker = _make_worker(
        gateway=gateway,
        repo=repo,
        client=client,
        failure_backoff_ms=5_000,
    )

    with patch(
        "execution_gateway.workers.recovery_worker.epoch_ms",
        return_value=1_700_000_010_000,
    ):
        await worker.recover_once(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
        )

    assert len(client.calls) == 1
    assert client.calls[0].order_id == order.order_id
    gateway.apply_reconciliation_order_snapshot.assert_not_awaited()

    deferred_orders = await repo.list_recovery_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_009_999,
    )
    assert order.order_id not in {row["order_id"] for row in deferred_orders}

    retryable_orders = await repo.list_recovery_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_015_000,
    )
    assert order.order_id in {row["order_id"] for row in retryable_orders}


# -------- 실제 PostgreSQL + Redis 테스트 --------


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


@pytest.fixture
def order_state_service(
    postgres_client: PostgresClient,
    redis_stream_client: RedisStreamClient,
) -> OrderStateService:
    redis_repo = OrderStateRedisRepository(redis_stream_client)

    return OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=redis_repo,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_recovery_worker_without_mock_updates_pg_and_redis(
    postgres_client: PostgresClient,
    redis_stream_client: RedisStreamClient,
    order_state_service: OrderStateService,
) -> None:
    redis_repo = OrderStateRedisRepository(redis_stream_client)

    # 1. 고유한 client_order_id 생성
    order_id = f"TKRECOV{epoch_ms()}"

    # 2. 최초 주문 객체 생성 (기본 status=PENDING_NEW, version=1)
    # STOP_MARKET 조건부 주문으로 설정 (잔고 $0 검증 우회)

    order = make_order(order_id=order_id)
    order = order.model_copy(update={
        "order_type": OrderType.STOP_MARKET,
        "order_route": OrderRoute.CONDITIONAL,
        "trigger_price": "1200000",
        "price": None,
        "client_conditional_id": order_id,
        "position_action": PositionAction.OPEN,
    })
    
    pem = _load_pem()

    adapter = BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem,
    )

    order_router = BinanceOrderRouter(adapter=adapter)
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=order_router,
    )

    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        # adapter=adapter,
        state_repo=redis_repo,
        state_service=order_state_service,
        exchange_clients=exchange_clients,
    )

    worker:RecoveryWorker = RecoveryWorker(
        exchange_clients=gateway.exchange_clients,
        gateway=gateway,
        repo=redis_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=1,
        older_than_ms=1,
        batch_size=100,
    )

    actual_exchange_order_id = None

    try:
        # 3. Gateway 표준 경로로 실제 바이낸스 테스트넷 조건부 주문 등록
        req = OrderRequest(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_MARKET,
            order_route=OrderRoute.CONDITIONAL,
            quantity="0.1",
            trigger_price="120000",
            position_side=PositionSide.BOTH,
            position_action=PositionAction.OPEN,
        )

        order = await gateway.submit_order(req)

        assert order.status == OrderStatus.ACKNOWLEDGED
        assert order.exchange_conditional_id

        actual_exchange_order_id = order.exchange_conditional_id

        # 4. 테스트를 위해 로컬 상태를 의도적으로 오염
        # 실제 거래소 상태는 NEW인데, 로컬 conditional_status만 ACTIVE로 틀어둔다.
        submitted = order.model_copy(deep=True)
        submitted.status = OrderStatus.SUBMITTED
        submitted.conditional_status = ConditionalStatus.UNKNOWN
        submitted.submitted_ts = 1_700_000_000_100
        submitted.updated_ts = 1_700_000_000_100

        submitted = await order_state_service.transition_order(
            current_order=order,
            updated_order=submitted,
        )

        assert submitted.status == OrderStatus.SUBMITTED
        assert submitted.version == order.version + 1

        # 6. 복구 한 번 실행 -> UNKNOWN -> NEW
        await worker.recover_once(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
        )

        # 7. PostgreSQL 원본 상태 확인
        pool = postgres_client.require_pool()

        async with pool.acquire() as conn:
            order_row = await conn.fetchrow(
                """
                SELECT *
                FROM orders
                WHERE order_id = $1
                """,
                order.order_id,
            )

            outbox_rows = await conn.fetch(
                """
                SELECT event_type
                FROM outbox_events
                WHERE aggregate_id = $1
                ORDER BY event_id
                """,
                order.order_id,
            )

        assert order_row is not None
        assert order_row["status"] == OrderStatus.SUBMITTED.value
        assert order_row["conditional_status"] == ConditionalStatus.NEW.value
        assert order_row["exchange_conditional_id"] == actual_exchange_order_id
        assert order_row["version"] == order.version + 2

        assert [row["event_type"] for row in outbox_rows] == [
            "ORDER_CREATED",
            "ORDER_STATUS_CHANGED",
            "ORDER_STATUS_CHANGED",
            "ORDER_STATUS_CHANGED",
            "ORDER_STATUS_CHANGED",
        ]

        # 8. Redis projection 확인
        assert order.order_id
        redis_row = await redis_repo.get(order.order_id)

        assert redis_row is not None
        assert redis_row["status"] == OrderStatus.SUBMITTED.value
        assert redis_row["conditional_status"] == ConditionalStatus.NEW.value
        assert redis_row["exchange_conditional_id"] == actual_exchange_order_id
        assert redis_row["version"] == order.version + 2

        # 9. CONDITIONAL open 주문은 계속 감시 대상이므로 order:recovery에 남아있어야 함
        recovery_orders = await redis_repo.list_recovery_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP.value,
        )
        assert order.order_id in {row["order_id"] for row in recovery_orders}

        # 10. terminal은 아니므로 conditional open 목록에 남아야 함
        open_orders = await redis_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP.value,
        )
        assert order.order_id in {row["order_id"] for row in open_orders}

    finally:
        # 11. 테스트 종료 후 클린업: 거래소 조건부 주문 취소
        if actual_exchange_order_id:
            try:
                cancel_target = order.model_copy(deep=True)
                cancel_target.exchange_conditional_id = actual_exchange_order_id
                cancel_target.client_conditional_id = order.order_id
                await client.cancel_order(cancel_target)
            except Exception as e:
                logger.warning(f"테스트 주문 취소 클린업 실패: {e}")
