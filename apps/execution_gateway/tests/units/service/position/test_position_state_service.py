from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from execution_gateway.adapters.binance.mapper.binance_position_event_mapper import (
    normalize_binance_account_update_positions,
)
from execution_gateway.exchange import ExchangePositionSnapshot
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.position import PositionSide, PositionStatus
from schemas.position_update_event import NormalizedPositionSnapshot
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.position_repo import PositionPostgresRepository

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
                positions
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


def create_fake_position_snapshots(
    price_list: list[dict] | None = None,
    event_time: int = 1_700_000_000_100,
    transaction_time: int = 1_700_000_000_200,
) -> list[NormalizedPositionSnapshot]:
    raw = {
        "e": "ACCOUNT_UPDATE",
        "E": event_time,
        "T": transaction_time,
        "a": {
            "m": "ORDER",
            "B": [],
            "P": price_list or []
        },
    }

    return normalize_binance_account_update_positions(raw)


async def load_position_row(
    postgres_client: PostgresClient,
    position_id: str,
) -> dict | None:
    pool = postgres_client.require_pool()
    position_repo = PositionPostgresRepository()

    async with pool.acquire() as conn:
        return await position_repo.get(conn, position_id)


async def load_outbox_rows(
    postgres_client: PostgresClient,
    aggregate_id: str,
) -> list[dict]:
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT aggregate_type, aggregate_id, event_type, payload, created_ts
            FROM outbox_events
            WHERE aggregate_id = $1
            ORDER BY event_id ASC
            """,
            aggregate_id,
        )

    return [dict(row) for row in rows]


def make_service(
    *,
    postgres_client: PostgresClient,
    position_repo: PositionPostgresRepository,
    outbox_repo: OutboxPostgresRepository,
    redis_position_repo,
) -> PositionStateService:
    return PositionStateService(
        postgres=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_apply_position_snapshots_persists_open_position(postgres_client: PostgresClient) -> None:
    position_repo = PositionPostgresRepository()
    outbox_repo = OutboxPostgresRepository()
    redis_position_repo = AsyncMock()

    redis_position_repo.save = AsyncMock()

    postion_sate_service = make_service(
        postgres_client=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

    event_time = 1_700_000_000_100
    transaction_time = 1_700_000_000_200
    symbol = "BTCUSDT"
    position_amt= "0.01"
    entry_price = "60000"
    break_even_price = "60010"
    unrealized_pnl = "12.34"
    margin_type = "cross"

    result = await postion_sate_service.apply_position_snapshots(snapshots=create_fake_position_snapshots(
        event_time=event_time,
        transaction_time=transaction_time,
        price_list = [
                {
                    "s": symbol,
                    "pa": position_amt,
                    "ep": entry_price,
                    "bep": break_even_price,
                    "cr": "0",
                    "up": unrealized_pnl,
                    "mt": margin_type,
                    "iw": "0",
                    "ps": "BOTH",
                }
            ], 
    ))

    assert len(result) == 1

    position = result[0]

    assert position.symbol == symbol
    assert position.position_side == PositionSide.BOTH
    assert position.position_amt == position_amt
    assert position.entry_price == entry_price
    assert position.break_even_price == break_even_price
    assert position.unrealized_pnl == unrealized_pnl
    assert position.margin_type == margin_type
    assert position.status == PositionStatus.OPEN
    assert position.update_reason == "ORDER"
    assert position.last_event_time == event_time
    assert position.last_transaction_time == transaction_time
    assert position.version == 1
    assert position.opened_ts is not None
    assert position.closed_ts is None

    assert position.position_id is not None
    row = await load_position_row(postgres_client, position.position_id)

    assert row is not None
    assert row["position_id"] == position.position_id
    assert row["exchange"] == Exchange.BINANCE.value
    assert row["market_type"] == MarketType.PERP.value
    assert row["symbol"] == symbol
    assert row["position_side"] == PositionSide.BOTH.value
    assert row["status"] == PositionStatus.OPEN.value
    assert row["position_amt"] == position_amt
    assert row["entry_price"] == entry_price
    assert row["break_even_price"] == break_even_price
    assert row["unrealized_pnl"] == unrealized_pnl
    assert row["margin_type"] == margin_type
    assert row["last_event_time"] == event_time
    assert row["last_transaction_time"] == transaction_time
    assert row["opened_ts"] is not None
    assert row["closed_ts"] is None
    assert row["version"] == 1

    outbox_rows = await load_outbox_rows(postgres_client, position.position_id)

    assert len(outbox_rows) == 1
    assert outbox_rows[0]["aggregate_type"] == "POSITION"
    assert outbox_rows[0]["aggregate_id"] == position.position_id
    assert outbox_rows[0]["event_type"] == "POSITION_UPDATED"

    redis_position_repo.save.assert_awaited_once()

    assert redis_position_repo.save.await_args is not None
    saved_position = redis_position_repo.save.await_args.args[0]
    assert saved_position.position_id == position.position_id
    assert saved_position.status == PositionStatus.OPEN

@pytest.mark.stable
@pytest.mark.asyncio
async def test_apply_position_snapshots_persists_flat_position(postgres_client: PostgresClient) -> None:
    position_repo = PositionPostgresRepository()
    outbox_repo = OutboxPostgresRepository()
    redis_position_repo = AsyncMock()

    redis_position_repo.save = AsyncMock()

    service = make_service(
        postgres_client=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

    opened = await service.apply_position_snapshots(snapshots=create_fake_position_snapshots(
        event_time=1_700_000_000_100,
        transaction_time=1_700_000_000_200,
        price_list = [
                {
                    "s": "BTCUSDT",
                    "pa": "0.01",
                    "ep": "60000",
                    "bep": "60010",
                    "cr": "0",
                    "up": "1.23",
                    "mt": "cross",
                    "iw": "0",
                    "ps": "BOTH",
                }
            ],
    ))

    result = await service.apply_position_snapshots(snapshots=create_fake_position_snapshots(
        event_time=1_700_000_001_100,
        transaction_time=1_700_000_001_200,
        price_list = [
                {
                    "s": "BTCUSDT",
                    "pa": "0",
                    "ep": "0",
                    "bep": "0",
                    "cr": "0",
                    "up": "0",
                    "mt": "cross",
                    "iw": "0",
                    "ps": "BOTH",
                }
            ],
    ))

    assert len(opened) == 1
    assert opened[0].status == PositionStatus.OPEN
    assert len(result) == 1
    assert result[0].position_amt == "0"
    assert result[0].status == PositionStatus.FLAT
    assert result[0].opened_ts is not None
    assert result[0].closed_ts is not None
    assert result[0].version == 2

    assert result[0].position_id is not None
    row = await load_position_row(postgres_client, result[0].position_id)

    assert row is not None
    assert row["status"] == PositionStatus.FLAT.value
    assert row["position_amt"] == "0"
    assert row["opened_ts"] is not None
    assert row["closed_ts"] is not None
    assert row["last_event_time"] == 1_700_000_001_100
    assert row["last_transaction_time"] == 1_700_000_001_200
    assert row["version"] == 2

    outbox_rows = await load_outbox_rows(postgres_client, result[0].position_id)

    assert [row["event_type"] for row in outbox_rows] == [
        "POSITION_UPDATED",
        "POSITION_UPDATED",
    ]
    assert redis_position_repo.save.await_count == 2

@pytest.mark.stable
@pytest.mark.asyncio
async def test_apply_position_snapshots_returns_empty_when_no_positions(postgres_client: PostgresClient) -> None:
    position_repo = AsyncMock(spec=PositionPostgresRepository)
    outbox_repo = AsyncMock(spec=OutboxPostgresRepository)
    redis_position_repo = AsyncMock()

    service = make_service(
        postgres_client=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

    result = await service.apply_position_snapshots(
        snapshots=create_fake_position_snapshots(price_list=[]),
    )

    assert result == []

    position_repo.upsert.assert_not_awaited()
    outbox_repo.insert.assert_not_awaited()
    redis_position_repo.save.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_refresh_position_snapshots_persists_exchange_position_snapshots(postgres_client: PostgresClient) -> None:
    position_repo = PositionPostgresRepository()
    outbox_repo = OutboxPostgresRepository()
    redis_position_repo = AsyncMock()

    redis_position_repo.save = AsyncMock()

    service = make_service(
        postgres_client=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

    snapshots = [
        ExchangePositionSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            position_amt="0.01",
            entry_price="60000",
            break_even_price="60010",
            mark_price="60100",
            unrealized_pnl="1.23",
            isolated_margin="0",
            isolated_wallet="0",
            margin_type="cross",
            leverage=10,
            liquidation_price="50000",
            notional="601",
            updated_ts=1_700_000_000_500,
            raw={
                "source": "unit-test",
            },
        )
    ]

    result = await service.refresh_position_snapshots(snapshots)

    assert len(result) == 1

    position = result[0]

    assert position.symbol == "BTCUSDT"
    assert position.position_amt == "0.01"
    assert position.mark_price == "60100"
    assert position.liquidation_price == "50000"
    assert position.leverage == 10
    assert position.status == PositionStatus.OPEN
    assert position.break_even_price == "60010"
    assert position.isolated_margin == "0"
    assert position.isolated_wallet == "0"
    assert position.margin_type == "cross"
    assert position.notional == "601"
    assert position.updated_ts == 1_700_000_000_500
    assert position.update_reason == "POSITION_SNAPSHOT_REFRESH"
    assert position.version == 1
    assert position.opened_ts is not None

    assert position.position_id is not None
    row = await load_position_row(postgres_client, position.position_id)

    assert row is not None
    assert row["status"] == PositionStatus.OPEN.value
    assert row["position_amt"] == "0.01"
    assert row["entry_price"] == "60000"
    assert row["break_even_price"] == "60010"
    assert row["mark_price"] == "60100"
    assert row["unrealized_pnl"] == "1.23"
    assert row["isolated_margin"] == "0"
    assert row["isolated_wallet"] == "0"
    assert row["margin_type"] == "cross"
    assert row["leverage"] == 10
    assert row["liquidation_price"] == "50000"
    assert row["notional"] == "601"
    assert row["updated_ts"] == 1_700_000_000_500
    assert row["version"] == 1

    outbox_rows = await load_outbox_rows(postgres_client, position.position_id)

    assert len(outbox_rows) == 1
    assert outbox_rows[0]["aggregate_type"] == "POSITION"
    assert outbox_rows[0]["event_type"] == "POSITION_SNAPSHOT_REFRESHED"

    redis_position_repo.save.assert_awaited_once()
