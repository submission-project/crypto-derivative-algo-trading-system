from __future__ import annotations

import os

import pytest
import pytest_asyncio

from schemas.market import Exchange, MarketType
from schemas.position import Position, PositionSide, PositionStatus, make_position_id
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.position_repo import PositionPostgresRepository

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
            TRUNCATE TABLE positions
            RESTART IDENTITY CASCADE
            """
        )

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE positions
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


def make_position(
    *,
    amt: str,
    event_time: int,
    updated_ts: int,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        position_side=PositionSide.BOTH,
        position_amt=amt,
        entry_price="60000" if amt != "0" else "0",
        break_even_price="60010" if amt != "0" else "0",
        mark_price="60020" if amt != "0" else "0",
        unrealized_pnl="1.23" if amt != "0" else "0",
        isolated_margin="2.34" if amt != "0" else "0",
        isolated_wallet="3.45" if amt != "0" else "0",
        margin_type="cross",
        leverage=10,
        liquidation_price="50000" if amt != "0" else None,
        notional="600.2" if amt != "0" else "0",
        update_reason="ORDER",
        last_event_time=event_time,
        last_transaction_time=event_time + 10,
        updated_ts=updated_ts,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_position_repo_upsert_open_and_projection(
    postgres_client: PostgresClient,
) -> None:
    """
    non-zero position_amt upsert가 OPEN 포지션을 만들고 projection 조회 대상에 포함되는지 검증한다.
    """
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )

    async with pool.acquire() as conn:
        persisted = await repo.upsert(
            conn,
            position=position,
        )
        projection_rows = await repo.list_open_for_projection(conn)

    assert persisted.position_id == make_position_id(exchange=position.exchange,market_type=position.market_type,symbol=position.symbol,position_side=position.position_side)
    assert persisted.symbol == position.symbol
    assert persisted.status == PositionStatus.OPEN  # position_amt가 0이 아니면 OPEN(포지션이 열려 있음)으로 추론된다.
    assert persisted.position_amt == "0.01"
    assert persisted.version == 1
    assert persisted.opened_ts == 1_700_000_000_100
    assert persisted.closed_ts is None

    assert len(projection_rows) == 1
    assert projection_rows[0].position_id == persisted.position_id
    assert projection_rows[0].position_amt == "0.01"
    assert projection_rows[0].entry_price == "60000"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_position_repo_upsert_flat_closes_existing_position(
    postgres_client: PostgresClient,
) -> None:
    """
    기존 OPEN 포지션에 position_amt=0 이벤트가 오면 FLAT으로 전환되고 open 조회에서 제외되는지 검증한다.
    """
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    open_position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )
    flat_position = make_position(
        amt="0",
        event_time=1_700_000_000_500,
        updated_ts=1_700_000_000_600,
    )

    async with pool.acquire() as conn:
        persisted_open = await repo.upsert(conn, position=open_position)
        persisted_flat = await repo.upsert(conn, position=flat_position)
        open_rows = await repo.list_open(conn)

    assert persisted_open.status == PositionStatus.OPEN
    assert persisted_flat.status == PositionStatus.FLAT
    assert persisted_flat.position_amt == "0"
    assert persisted_flat.version == 2
    assert persisted_flat.opened_ts == 1_700_000_000_100
    assert persisted_flat.closed_ts == 1_700_000_000_600
    assert open_rows == []

@pytest.mark.stable
@pytest.mark.asyncio
async def test_position_repo_ignores_stale_event(
    postgres_client: PostgresClient,
) -> None:
    """
    last_event_time이 더 오래된 position 이벤트는 현재 row를 덮어쓰지 않는지 검증한다.
    """
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    newer_position = make_position(
        amt="0.02",
        event_time=1_700_000_001_000,
        updated_ts=1_700_000_001_100,
    )
    stale_position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )

    async with pool.acquire() as conn:
        persisted_newer = await repo.upsert(conn, position=newer_position) # BINANCE:PERP:BTCUSDT:BOTH
        persisted_after_stale = await repo.upsert(conn, position=stale_position)

    assert persisted_newer.position_amt == "0.02"
    assert persisted_after_stale.position_amt == "0.02"
    assert persisted_after_stale.last_event_time == 1_700_000_001_000
    assert persisted_after_stale.version == 1

@pytest.mark.stable
@pytest.mark.asyncio
async def test_position_repo_applies_newer_event(
    postgres_client: PostgresClient,
) -> None:
    """
    last_event_time이 더 최신인 position 이벤트는 현재 row를 갱신하고 version을 증가시키는지 검증한다.
    """
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    initial_position = make_position(
        amt="0.02",
        event_time=1_700_000_001_000,
        updated_ts=1_700_000_001_100,
    )
    newer_position = make_position(
        amt="0.03",
        event_time=1_700_000_002_000,
        updated_ts=1_700_000_002_100,
    )

    async with pool.acquire() as conn:
        persisted_initial = await repo.upsert(conn, position=initial_position)
        persisted_after_newer = await repo.upsert(conn, position=newer_position)

    assert persisted_initial.position_amt == "0.02"
    assert persisted_initial.last_event_time == 1_700_000_001_000
    assert persisted_initial.version == 1

    assert persisted_after_newer.position_amt == "0.03"
    assert persisted_after_newer.status == PositionStatus.OPEN
    assert persisted_after_newer.last_event_time == 1_700_000_002_000
    assert persisted_after_newer.version == 2
