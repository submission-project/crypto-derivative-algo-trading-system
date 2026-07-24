from __future__ import annotations

import os

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from storage.projection.position_projection_rebuilder import (
    PositionProjectionRebuilder,
)
from schemas.market import Exchange, MarketType
from schemas.position import Position, PositionSide, PositionStatus
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository

pytestmark = pytest.mark.integration


def _make_open_position(
    *,
    symbol: str = "BTCUSDT",
    position_side: PositionSide = PositionSide.BOTH,
    position_amt: str = "0.01",
    updated_ts: int = 1_700_000_000_000,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=position_side,
        position_amt=position_amt,
        entry_price="60000",
        updated_ts=updated_ts,
    )


def _make_flat_position(
    *,
    symbol: str = "ETHUSDT",
    position_side: PositionSide = PositionSide.BOTH,
    updated_ts: int = 1_700_000_000_000,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=position_side,
        position_amt="0",
        entry_price="0",
        updated_ts=updated_ts,
    )


@pytest_asyncio.fixture
async def postgres_client():
    dsn = os.getenv("POSTGRES_TEST_DSN") or common_settings.postgres_dsn

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
                positions,
                outbox_events
            RESTART IDENTITY CASCADE
            """
        )

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                positions,
                outbox_events
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


@pytest_asyncio.fixture
async def redis_client():
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db or 15,
    )

    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()

    yield client

    await client.client.flushdb()
    await client.close()


@pytest.mark.asyncio
async def test_position_projection_rebuilder_rebuilds_only_open_positions(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
) -> None:
    """
    PostgreSQL에 OPEN과 FLAT position을 seed한 뒤,
    Redis를 flush하고 rebuilder를 실행하면
    OPEN position만 Redis projection에 복구된다.
    """
    pg_repo = PositionPostgresRepository()
    redis_repo = PositionRedisRepository(redis_client)

    open_btc = _make_open_position(symbol="BTCUSDT", position_amt="0.01")
    open_eth = _make_open_position(symbol="ETHUSDT", position_amt="-0.5")
    flat_sol = _make_flat_position(symbol="SOLUSDT")

    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        open_btc = await pg_repo.upsert(conn, position=open_btc)
        open_eth = await pg_repo.upsert(conn, position=open_eth)
        flat_sol = await pg_repo.upsert(conn, position=flat_sol)

    # Redis가 비어 있는 상태에서 rebuild
    rebuilder = PositionProjectionRebuilder(
        postgres=postgres_client,
        position_repo=pg_repo,
        redis_position_repo=redis_repo,
    )

    result = await rebuilder.rebuild_active_projection(reset_existing=True)

    # OPEN position만 rebuild됨
    assert result.total_rows == 2
    assert result.rebuilt == 2
    assert result.failed == 0

    # Redis open index 검증
    open_positions = await redis_repo.list_open_positions(
        exchange=Exchange.BINANCE.value,
    )
    open_ids = {p["position_id"] for p in open_positions}

    assert open_btc.position_id in open_ids
    assert open_eth.position_id in open_ids
    assert flat_sol.position_id not in open_ids

    # Redis live hash 검증
    assert open_btc.position_id
    btc_data = await redis_repo.get(open_btc.position_id)
    assert btc_data is not None
    assert btc_data["status"] == PositionStatus.OPEN.value
    assert btc_data["position_amt"] == "0.01"

    assert open_eth.position_id
    eth_data = await redis_repo.get(open_eth.position_id)
    assert eth_data is not None
    assert eth_data["status"] == PositionStatus.OPEN.value

    assert flat_sol.position_id
    # FLAT position은 Redis live에 없음 (rebuild 대상 아님)
    sol_data = await redis_repo.get(flat_sol.position_id)
    assert sol_data is None


@pytest.mark.asyncio
async def test_position_projection_rebuilder_clears_stale_projection(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
) -> None:
    """
    Redis에 stale position projection이 남아 있어도
    rebuild(reset_existing=True)하면 올바른 상태로 복구된다.
    """
    pg_repo = PositionPostgresRepository()
    redis_repo = PositionRedisRepository(redis_client)

    # Redis에 stale OPEN position 저장
    stale_pos = _make_open_position(symbol="XRPUSDT", position_amt="100")
    await redis_repo.save(stale_pos)

    assert stale_pos.position_id
    stale_check = await redis_repo.get(stale_pos.position_id)
    assert stale_check is not None  # stale projection 존재

    # PostgreSQL에는 다른 position만 OPEN
    real_open = _make_open_position(symbol="BTCUSDT", position_amt="0.01")

    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        real_open = await pg_repo.upsert(conn, position=real_open)

    # Rebuild
    rebuilder = PositionProjectionRebuilder(
        postgres=postgres_client,
        position_repo=pg_repo,
        redis_position_repo=redis_repo,
    )

    result = await rebuilder.rebuild_active_projection(reset_existing=True)

    assert result.total_rows == 1
    assert result.rebuilt == 1
    assert result.deleted_keys > 0

    # Stale projection이 사라졌는지 확인
    assert stale_pos.position_id
    stale_after = await redis_repo.get(stale_pos.position_id)
    assert stale_after is None

    # 진짜 OPEN position만 남아 있는지 확인
    assert real_open.position_id
    btc_data = await redis_repo.get(real_open.position_id)
    assert btc_data is not None
    assert btc_data["status"] == PositionStatus.OPEN.value

    open_positions = await redis_repo.list_open_positions(
        exchange=Exchange.BINANCE.value,
    )
    assert len(open_positions) == 1
    assert open_positions[0]["position_id"] == real_open.position_id
