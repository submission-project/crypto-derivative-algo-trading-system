from __future__ import annotations

import os

import pytest
import pytest_asyncio

from schemas.market import Exchange, MarketType
from schemas.position import Position, PositionSide, PositionStatus
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.position_repo import PositionPostgresRepository

pytestmark = pytest.mark.integration


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
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
        position_amt=amt,
        entry_price="60000" if amt != "0" else "0",
        break_even_price="60010" if amt != "0" else "0",
        unrealized_pnl="1.23" if amt != "0" else "0",
        margin_type="cross",
        update_reason="ORDER",
        last_event_time=event_time,
        last_transaction_time=event_time + 10,
        updated_ts=updated_ts,
    )


@pytest.mark.asyncio
async def test_upsert_open_position_inserts_row(
    postgres_client: PostgresClient,
) -> None:
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

    assert persisted.position_id == "BINANCE:PERP:BTCUSDT:BOTH"
    assert persisted.status == PositionStatus.OPEN
    assert persisted.position_amt == "0.01"
    assert persisted.version == 1
    assert persisted.opened_ts == 1_700_000_000_100
    assert persisted.closed_ts is None


@pytest.mark.asyncio
async def test_upsert_flat_position_closes_position(
    postgres_client: PostgresClient,
) -> None:
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    open_position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )

    async with pool.acquire() as conn:
        persisted_open = await repo.upsert(
            conn,
            position=open_position,
        )

    flat_position = make_position(
        amt="0",
        event_time=1_700_000_000_500,
        updated_ts=1_700_000_000_600,
    )

    async with pool.acquire() as conn:
        persisted_flat = await repo.upsert(
            conn,
            position=flat_position,
        )

    assert persisted_open.status == PositionStatus.OPEN
    assert persisted_flat.status == PositionStatus.FLAT
    assert persisted_flat.position_amt == "0"
    assert persisted_flat.version == 2
    assert persisted_flat.opened_ts == 1_700_000_000_100
    assert persisted_flat.closed_ts == 1_700_000_000_600


@pytest.mark.asyncio
async def test_stale_account_update_does_not_overwrite_newer_position(
    postgres_client: PostgresClient,
) -> None:
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    newer_position = make_position(
        amt="0.02",
        event_time=1_700_000_001_000,
        updated_ts=1_700_000_001_100,
    )

    async with pool.acquire() as conn:
        persisted_newer = await repo.upsert(
            conn,
            position=newer_position,
        )

    stale_position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )

    async with pool.acquire() as conn:
        persisted_after_stale = await repo.upsert(
            conn,
            position=stale_position,
        )

    assert persisted_newer.position_amt == "0.02"
    assert persisted_after_stale.position_amt == "0.02"
    assert persisted_after_stale.last_event_time == 1_700_000_001_000
    assert persisted_after_stale.version == 1


@pytest.mark.asyncio
async def test_list_open_returns_only_open_positions(
    postgres_client: PostgresClient,
) -> None:
    repo = PositionPostgresRepository()
    pool = postgres_client.require_pool()

    open_position = make_position(
        amt="0.01",
        event_time=1_700_000_000_000,
        updated_ts=1_700_000_000_100,
    )

    async with pool.acquire() as conn:
        await repo.upsert(conn, position=open_position)

    rows = None
    async with pool.acquire() as conn:
        rows = await repo.list_open(conn)

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["status"] == PositionStatus.OPEN.value