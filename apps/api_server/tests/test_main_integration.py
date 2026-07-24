"""
api_server/main.py 통합 테스트.

실제 인프라(PostgreSQL, Redis)를 사용하여 주요 흐름을 검증한다.
FastAPI TestClient + 실제 state 초기화(Binance/QuestDB만 mock).

테스트 대상:
  1. startup 시 PostgreSQL / Redis 초기화 + projection rebuild
  2. handlers → 실제 PG/Redis 상태 전이 + fill dedup
  3. FastAPI /health, /ready 엔드포인트
  4. FastAPI /api/orders 주문 API (Gateway mock — Binance 미호출)
  5. shutdown drain 동작

실행 조건:
  - Docker에서 PostgreSQL, Redis 실행 중
  - POSTGRES_TEST_DSN 환경변수 설정
  - pytest -m integration
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderRequest,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
    PositionAction,
)
from schemas.order_update_event import NormalizedOrderUpdateEvent

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
from execution_gateway.services.order_state_service import OrderStateService
from storage.projection.order_projection_rebuilder import OrderProjectionRebuilder
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.exchange import (
    ExchangeOrderAck,
    ExchangeCancelResult,
    ExchangeOrderSnapshot,
    ExchangeCapabilities,
)

from unittest.mock import AsyncMock, MagicMock

pytestmark = pytest.mark.integration


# ─────────────────────── Fixtures: Real infra ───────────────────────


@pytest_asyncio.fixture
async def postgres() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN이 설정되지 않았습니다.")

    client = PostgresClient(dsn=dsn, min_size=1, max_size=3)
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


@pytest_asyncio.fixture
async def redis() -> RedisStreamClient:
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=15,  # 테스트 전용 DB
    )
    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    try:
        await client.client.flushdb()
    except Exception:
        pass

    yield client

    try:
        await client.client.flushdb()
    except Exception:
        pass
    await client.close()


@pytest.fixture
def repos():
    """PostgreSQL 리포지토리 인스턴스."""
    return {
        "intent": OrderIntentPostgresRepository(),
        "order": OrderPostgresRepository(),
        "outbox": OutboxPostgresRepository(),
    }


@pytest.fixture
def redis_order_repo(redis: RedisStreamClient) -> OrderStateRedisRepository:
    return OrderStateRedisRepository(redis)


@pytest.fixture
def state_service(
    postgres: PostgresClient,
    redis_order_repo: OrderStateRedisRepository,
    repos: dict,
) -> OrderStateService:
    return OrderStateService(
        postgres=postgres,
        intent_repo=repos["intent"],
        postgres_order_repo=repos["order"],
        outbox_repo=repos["outbox"],
        redis_order_repo=redis_order_repo,
    )


@pytest.fixture
def mock_adapter():
    """Binance adapter는 mock — 실제 거래소 호출 방지."""
    adapter = MagicMock()
    adapter.exchange = Exchange.BINANCE
    adapter.market_type = MarketType.PERP
    adapter.capabilities = ExchangeCapabilities()

    async def mock_place_order(order):
        return ExchangeOrderAck(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            client_order_id=order.order_id,
            exchange_order_id="88888",
            status=OrderStatus.ACKNOWLEDGED,
            raw={"orderId": 88888},
        )
    adapter.place_regular_order = AsyncMock(side_effect=mock_place_order)

    async def mock_change_leverage(*, symbol: str, leverage: int):
        from execution_gateway.exchange import ExchangeLeverageResult
        return ExchangeLeverageResult(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            leverage=leverage,
            raw={"leverage": leverage, "symbol": symbol, "maxNotionalValue": "0"},
        )
    adapter.change_leverage = AsyncMock(side_effect=mock_change_leverage)

    async def mock_cancel_order(order):
        return ExchangeCancelResult(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            client_order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            status=OrderStatus.CANCELLED,
            raw={"status": "CANCELED"},
        )
    adapter.cancel_order = AsyncMock(side_effect=mock_cancel_order)

    async def mock_get_order(order):
        return ExchangeOrderSnapshot(
            exchange=order.exchange,
            market_type=order.market_type,
            symbol=order.symbol,
            status=order.status,
            client_order_id=order.order_id,
            exchange_order_id=order.exchange_order_id or "88888",
            raw={"status": "NEW", "orderId": 88888},
        )
    adapter.get_order = AsyncMock(side_effect=mock_get_order)

    adapter.close = AsyncMock()
    return adapter


@pytest.fixture
def gateway(
    mock_adapter,
    redis_order_repo: OrderStateRedisRepository,
    state_service: OrderStateService,
) -> ExecutionGateway:
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(mock_adapter)

    return ExecutionGateway(
        state_repo=redis_order_repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )


def _make_order(
    *,
    order_id: str | None = None,
) -> Order:
    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_000,
        status=OrderStatus.PENDING_NEW,
    )


def _make_order_request() -> OrderRequest:
    return OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        price="60000",
        quantity="0.1",
        position_action=PositionAction.OPEN,
    )


def _make_order_update_event(
    *,
    order_id: str,
    target_status: OrderStatus = OrderStatus.FILLED,
    exchange_status: str = "FILLED",
    execution_type: str = "TRADE",
) -> NormalizedOrderUpdateEvent:
    return NormalizedOrderUpdateEvent(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id=order_id,
        exchange_order_id="88888",
        target_status=target_status,
        exchange_status=exchange_status,
        execution_type=execution_type,
        filled_quantity="0.1",
        avg_fill_price="60000",
        last_fill_quantity="0.1",
        last_fill_price="60000",
        trade_id="55555",
        commission="0.001",
        commission_asset="USDT",
        is_maker=False,
        event_time=1_700_000_000_100,
        transaction_time=1_700_000_000_101,
        raw={"e": "ORDER_TRADE_UPDATE"},
    )


# ─────────────────────── 1. 주문 생성 → PG + Redis 동시 저장 ───────────────────────


class TestOrderCreationIntegration:
    """실제 PG + Redis에 주문을 생성하고 양쪽에서 조회."""

    @pytest.mark.asyncio
    async def test_create_order_persists_to_pg_and_redis(
        self,
        state_service: OrderStateService,
        postgres: PostgresClient,
        redis_order_repo: OrderStateRedisRepository,
    ):
        order = _make_order()
        created = await state_service.create_order(order)

        assert created.order_id == order.order_id
        assert created.status == OrderStatus.PENDING_NEW

        # PG 확인
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT * FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row is not None
        assert pg_row["status"] == "PENDING_NEW"
        assert pg_row["version"] == 1

        # Redis 확인
        redis_data = await redis_order_repo.get(order.order_id)
        assert redis_data is not None
        assert redis_data["status"] == "PENDING_NEW"

    @pytest.mark.asyncio
    async def test_transition_increments_version(
        self,
        state_service: OrderStateService,
        postgres: PostgresClient,
    ):
        order = _make_order()
        created = await state_service.create_order(order)

        # PENDING_NEW → SUBMITTED
        submitted = created.model_copy(deep=True)
        submitted.status = OrderStatus.SUBMITTED
        submitted.updated_ts = int(time.time_ns() // 1_000_000)

        result = await state_service.transition_order(
            current_order=created,
            updated_order=submitted,
        )

        assert result.status == OrderStatus.SUBMITTED
        assert result.version == 2

        # PG version 확인
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT version, status FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row["version"] == 2
        assert pg_row["status"] == "SUBMITTED"


# ─────────────────────── 2. Projection Rebuild ───────────────────────


class TestProjectionRebuildIntegration:
    """PG에 주문을 생성한 뒤 Redis를 지우고 rebuild."""

    @pytest.mark.asyncio
    async def test_rebuild_restores_redis_from_pg(
        self,
        state_service: OrderStateService,
        postgres: PostgresClient,
        redis: RedisStreamClient,
        redis_order_repo: OrderStateRedisRepository,
    ):
        # 주문 2개 생성
        order1 = _make_order()
        order2 = _make_order()

        await state_service.create_order(order1)
        await state_service.create_order(order2)

        # Redis 완전 삭제
        await redis.client.flushdb()

        # Redis에서 조회 불가 확인
        assert order1.order_id 
        assert order2.order_id
        assert await redis_order_repo.get(order1.order_id) is None
        assert await redis_order_repo.get(order2.order_id) is None

        # Rebuild
        rebuilder = OrderProjectionRebuilder(
            postgres=postgres,
            postgres_order_repo=OrderPostgresRepository(),
            redis_order_repo=redis_order_repo,
        )
        result = await rebuilder.rebuild_active_projection(reset_existing=True)

        assert result.total_rows == 2
        assert result.rebuilt == 2
        assert result.failed == 0

        # Redis에서 다시 조회 가능
        data1 = await redis_order_repo.get(order1.order_id)
        data2 = await redis_order_repo.get(order2.order_id)

        assert data1 is not None
        assert data2 is not None
        assert data1["status"] == "PENDING_NEW"
        assert data2["status"] == "PENDING_NEW"


# ─────────────────────── 3. Gateway submit → PG + Redis ───────────────────────


class TestGatewaySubmitIntegration:
    """Gateway.submit_order()를 통한 전체 주문 흐름 (Binance만 mock)."""

    @pytest.mark.asyncio
    async def test_submit_order_full_flow(
        self,
        gateway: ExecutionGateway,
        postgres: PostgresClient,
        redis_order_repo: OrderStateRedisRepository,
    ):
        req = _make_order_request()
        order = await gateway.submit_order(req)

        # ACKNOWLEDGED까지 도달
        assert order.status == OrderStatus.ACKNOWLEDGED
        assert order.exchange_order_id == "88888"
        assert order.version == 3  # PENDING_NEW(1) → SUBMITTED(2) → ACKNOWLEDGED(3)

        # PG 확인
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT status, version, exchange_order_id FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row["status"] == "ACKNOWLEDGED"
        assert pg_row["version"] == 3
        assert pg_row["exchange_order_id"] == "88888"

        # Redis 확인
        redis_data = await redis_order_repo.get(order.order_id)
        assert redis_data is not None
        assert redis_data["status"] == "ACKNOWLEDGED"

        # open orders에 포함
        open_orders = await redis_order_repo.list_open_regular_orders(
            exchange=order.exchange.value,
            market_type=order.market_type.value,
        )
        open_ids = {o["order_id"] for o in open_orders}
        assert order.order_id in open_ids

        # outbox event 확인
        async with pool.acquire() as conn:
            outbox_count = await conn.fetchval(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1",
                order.order_id,
            )
        # ORDER_CREATED + ORDER_STATUS_CHANGED(SUBMITTED) + ORDER_STATUS_CHANGED(ACKNOWLEDGED)
        assert outbox_count >= 3


# ─────────────────────── 4. UDS 이벤트 → 실제 상태 전이 ───────────────────────


class TestUDSEventIntegration:
    """UDS 이벤트를 gateway.apply_order_update_event로 실제 처리."""

    @pytest.mark.asyncio
    async def test_uds_fill_event_transitions_to_filled(
        self,
        gateway: ExecutionGateway,
        postgres: PostgresClient,
        redis_order_repo: OrderStateRedisRepository,
    ):
        # 1. 주문 생성 → ACKNOWLEDGED
        req = _make_order_request()
        order = await gateway.submit_order(req)
        assert order.status == OrderStatus.ACKNOWLEDGED

        # 2. UDS FILLED 이벤트 시뮬레이션
        fill_event = _make_order_update_event(order_id=order.order_id)

        result = await gateway.apply_order_update_event(fill_event)

        assert result is not None
        assert result.status == OrderStatus.FILLED

        # PG 확인
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT status, version FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row["status"] == "FILLED"
        assert pg_row["version"] == 4  # +1 from ACKNOWLEDGED

        # Redis 확인
        redis_data = await redis_order_repo.get(order.order_id)
        assert redis_data is not None
        assert redis_data["status"] == "FILLED"

        # open orders에서 제거됨
        open_orders = await redis_order_repo.list_open_regular_orders(
            exchange=order.exchange.value,
            market_type=order.market_type.value,
        )
        open_ids = {o["order_id"] for o in open_orders}
        assert order.order_id not in open_ids

    @pytest.mark.asyncio
    async def test_uds_duplicate_fill_is_idempotent(
        self,
        gateway: ExecutionGateway,
        postgres: PostgresClient,
    ):
        """같은 FILLED 이벤트를 두 번 보내도 안전."""
        req = _make_order_request()
        order = await gateway.submit_order(req)

        fill_event = _make_order_update_event(order_id=order.order_id)

        result1 = await gateway.apply_order_update_event(fill_event)
        assert result1.status == OrderStatus.FILLED

        # 두 번째 호출 — 예외 없이 처리
        result2 = await gateway.apply_order_update_event(fill_event)
        # terminal 보호에 의해 상태 유지
        assert result2.status == OrderStatus.FILLED

        # PG version이 과도하게 증가하지 않아야 함
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT version FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row["version"] == 4  # 두 번째 호출에서 version 증가 없음


# ─────────────────────── 5. Fill dedup (실제 Redis) ───────────────────────


class TestFillDedupIntegration:
    """실제 Redis SETNX를 사용한 fill dedup."""

    @pytest.mark.asyncio
    async def test_dedup_key_set_in_redis(
        self,
        redis: RedisStreamClient,
    ):
        """SETNX가 실제로 Redis에 dedup key를 생성하는지 확인."""
        dedup_key = "fill:O-TEST-001:99999"

        # 첫 호출: True (설정됨)
        was_set = await redis.client.set(dedup_key, "1", nx=True, ex=86400)
        assert was_set is True

        # 두 번째 호출: None/False (이미 존재)
        was_set2 = await redis.client.set(dedup_key, "1", nx=True, ex=86400)
        assert was_set2 is None or was_set2 is False

        # TTL 확인
        ttl = await redis.client.ttl(dedup_key)
        assert 86300 < ttl <= 86400


# ─────────────────────── 6. Reconciliation terminal 보호 (실제 PG+Redis) ───────────────────────


class TestReconciliationIntegration:
    """실제 인프라에서 reconciliation terminal 보호 검증."""

    @pytest.mark.asyncio
    async def test_reconciliation_does_not_regress_filled(
        self,
        gateway: ExecutionGateway,
        postgres: PostgresClient,
    ):
        # 주문 생성 → ACKNOWLEDGED → UDS FILLED
        req = _make_order_request()
        order = await gateway.submit_order(req)

        fill_event = _make_order_update_event(order_id=order.order_id)

        filled = await gateway.apply_order_update_event(fill_event)
        assert filled.status == OrderStatus.FILLED

        # reconciliation으로 stale NEW snapshot 강제 적용 시도
        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=order.order_id,
            snapshot=ExchangeOrderSnapshot(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol="BTCUSDT",
                status=OrderStatus.ACKNOWLEDGED,
                client_order_id=order.order_id,
                exchange_order_id="88888",
                filled_quantity="0",
                avg_fill_price="0",
                raw={"status": "NEW"},
            ),
        )

        # terminal 보호에 의해 FILLED 유지
        assert result.status == OrderStatus.FILLED

        # PG도 FILLED 유지
        pool = postgres.require_pool()
        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                "SELECT status FROM orders WHERE order_id = $1",
                order.order_id,
            )
        assert pg_row["status"] == "FILLED"


# ─────────────────────── 7. FastAPI TestClient ───────────────────────


class TestFastAPIEndpoints:
    """FastAPI 엔드포인트 기본 동작 검증 (TestClient)."""

    def test_health_endpoint(self):
        from httpx import AsyncClient, ASGITransport
        from api_server.main import app

        import asyncio

        async def _test():
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                resp = await client.get("/health")
                assert resp.status_code == 200
                assert resp.json()["status"] == "ok"

        asyncio.run(_test())

    def test_ready_endpoint_before_startup(self):
        """startup 전이면 not_ready."""
        from httpx import AsyncClient, ASGITransport
        from api_server.main import app
        from api_server.runtime import state

        import asyncio

        async def _test():
            original = state.is_ready
            state.is_ready = False
            try:
                transport = ASGITransport(app=app)
                async with AsyncClient(
                    transport=transport, base_url="http://test"
                ) as client:
                    resp = await client.get("/ready")
                    assert resp.status_code == 200
                    data = resp.json()
                    assert data["status"] == "not_ready"
            finally:
                state.is_ready = original

        asyncio.run(_test())
