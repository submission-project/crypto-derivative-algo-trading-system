from __future__ import annotations

import os

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from execution_gateway.adapters.binance.mapper.binance_position_event_mapper import (
    normalize_binance_account_update_positions,
)
from execution_gateway.exchange import ExchangePositionSnapshot
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.position import (
    PositionSide,
    PositionStatus,
    make_position_id,
)
from schemas.position_update_event import NormalizedPositionSnapshot
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository


pytestmark = pytest.mark.integration


def _make_account_update_snapshots(
    *,
    event_time: int,
    transaction_time: int,
    reason: str,
    positions: list[dict],
    balances: list[dict] | None = None,
) -> list[NormalizedPositionSnapshot]:
    raw = {
        "e": "ACCOUNT_UPDATE",
        "E": event_time,
        "T": transaction_time,
        "a": {
            "m": reason,
            "B": balances or [],
            "P": positions,
        },
    }

    return normalize_binance_account_update_positions(raw)


def _btc_position_id(
    *,
    position_side: PositionSide = PositionSide.BOTH,
) -> str:
    return make_position_id(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=position_side,
    )


def _make_normalized_position_snapshot(
    *,
    symbol: str = "BTCUSDT",
    position_side: PositionSide = PositionSide.BOTH,
    status: PositionStatus = PositionStatus.OPEN,
    position_amt: str = "0.01",
    entry_price: str | None = "50000",
    break_even_price: str | None = "50000",
    mark_price: str | None = None,
    unrealized_pnl: str | None = "12.3",
    isolated_margin: str | None = None,
    isolated_wallet: str | None = "0",
    margin_type: str | None = "cross",
    leverage: int | None = None,
    liquidation_price: str | None = None,
    notional: str | None = None,
    update_reason: str = "ORDER",
    event_time: int = 1_700_000_000_000,
    transaction_time: int = 1_700_000_000_100,
) -> NormalizedPositionSnapshot:
    return NormalizedPositionSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=position_side,
        status=status,
        position_amt=position_amt,
        entry_price=entry_price,
        break_even_price=break_even_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        isolated_margin=isolated_margin,
        isolated_wallet=isolated_wallet,
        margin_type=margin_type,
        leverage=leverage,
        liquidation_price=liquidation_price,
        notional=notional,
        update_reason=update_reason,
        event_time=event_time,
        transaction_time=transaction_time,
        raw={"source": "integration-test"},
    )


def _make_exchange_position_snapshot(
    *,
    symbol: str = "BTCUSDT",
    position_side: PositionSide = PositionSide.BOTH,
    position_amt: str = "0.02",
    entry_price: str | None = "51000",
    break_even_price: str | None = "51010",
    mark_price: str | None = "51200",
    unrealized_pnl: str | None = "4",
    isolated_margin: str | None = "0",
    isolated_wallet: str | None = "0",
    margin_type: str | None = "cross",
    leverage: int | None = 10,
    liquidation_price: str | None = "45000",
    notional: str | None = "1024",
    updated_ts: int | None = 1_700_000_001_000,
) -> ExchangePositionSnapshot:
    return ExchangePositionSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=position_side,
        position_amt=position_amt,
        entry_price=entry_price,
        break_even_price=break_even_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        isolated_margin=isolated_margin,
        isolated_wallet=isolated_wallet,
        margin_type=margin_type,
        leverage=leverage,
        liquidation_price=liquidation_price,
        notional=notional,
        updated_ts=updated_ts,
        raw={"source": "integration-test-position-risk"},
    )


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")

    assert dsn
    client = PostgresClient(
        dsn=dsn,
        min_size=1,
        max_size=3,
    )
    await client.connect()

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


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis_client() -> RedisStreamClient:
    db = int(os.getenv("REDIS_TEST_DB", "15"))

    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=db,
    )

    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()

    yield client

    await client.client.flushdb()
    await client.close()


@pytest_asyncio.fixture
async def position_bundle(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
):
    position_repo = PositionPostgresRepository()
    outbox_repo = OutboxPostgresRepository()
    redis_position_repo = PositionRedisRepository(redis_client)

    service = PositionStateService(
        postgres=postgres_client,
        position_repo=position_repo,
        outbox_repo=outbox_repo,
        redis_position_repo=redis_position_repo,
    )

    return {
        "postgres": postgres_client,
        "position_repo": position_repo,
        "outbox_repo": outbox_repo,
        "redis_position_repo": redis_position_repo,
        "service": service,
    }


@pytest.mark.asyncio
async def test_apply_position_snapshots_open_position_persists_postgres_and_redis(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    postgres: PostgresClient = position_bundle["postgres"]
    position_repo: PositionPostgresRepository = position_bundle["position_repo"]
    redis_position_repo: PositionRedisRepository = position_bundle[
        "redis_position_repo"
    ]

    snapshots = _make_account_update_snapshots(
        event_time=1_700_000_000_000,
        transaction_time=1_700_000_000_100,
        reason="ORDER",
        positions=[
            {
                "s": "BTCUSDT",
                "pa": "0.01",
                "ep": "50000",
                "bep": "50000",
                "cr": "0",
                "up": "12.3",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    )

    updated = await service.apply_position_snapshots(snapshots=snapshots)

    assert len(updated) == 1

    position = updated[0]

    assert position.position_id == _btc_position_id()
    assert position.exchange == Exchange.BINANCE
    assert position.market_type == MarketType.PERP
    assert position.symbol == "BTCUSDT"
    assert position.position_side == PositionSide.BOTH
    assert position.status == PositionStatus.OPEN
    assert position.position_amt == "0.01"
    assert position.entry_price == "50000"
    assert position.break_even_price == "50000"
    assert position.unrealized_pnl == "12.3"
    assert position.isolated_margin is None
    assert position.isolated_wallet == "0"
    assert position.margin_type == "cross"
    assert position.opened_ts is not None
    assert position.closed_ts is None
    assert position.version == 1

    pool = postgres.require_pool()

    assert position.position_id

    async with pool.acquire() as conn:
        row = await position_repo.get(
            conn,
            position.position_id,
        )

    assert row is not None
    assert row["position_id"] == position.position_id
    assert str(row["position_amt"]) == "0.01"
    assert row["status"] == "OPEN"

    redis_row = await redis_position_repo.get(position.position_id)

    assert redis_row is not None
    assert redis_row["position_id"] == position.position_id
    assert redis_row["status"] == "OPEN"
    assert redis_row["position_amt"] == "0.01"

    open_positions = await redis_position_repo.list_open_positions(
        exchange=Exchange.BINANCE.value,
    )

    assert any(
        row["position_id"] == position.position_id
        for row in open_positions
    )

    by_symbol = await redis_position_repo.list_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="BTCUSDT",
    )

    assert any(
        row["position_id"] == position.position_id
        for row in by_symbol
    )


@pytest.mark.asyncio
async def test_apply_position_snapshots_open_position_persists_postgres_and_redis(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    postgres: PostgresClient = position_bundle["postgres"]
    position_repo: PositionPostgresRepository = position_bundle["position_repo"]
    redis_position_repo: PositionRedisRepository = position_bundle[
        "redis_position_repo"
    ]

    snapshot = _make_normalized_position_snapshot()

    updated = await service.apply_position_snapshots(snapshots=[snapshot])

    assert len(updated) == 1

    position = updated[0]

    assert position.position_id == _btc_position_id()
    assert position.exchange == Exchange.BINANCE
    assert position.market_type == MarketType.PERP
    assert position.symbol == "BTCUSDT"
    assert position.position_side == PositionSide.BOTH
    assert position.status == PositionStatus.OPEN
    assert position.position_amt == "0.01"
    assert position.entry_price == "50000"
    assert position.break_even_price == "50000"
    assert position.unrealized_pnl == "12.3"
    assert position.isolated_wallet == "0"
    assert position.margin_type == "cross"
    assert position.update_reason == "ORDER"
    assert position.opened_ts is not None
    assert position.closed_ts is None
    assert position.version == 1

    pool = postgres.require_pool()

    assert position.position_id

    async with pool.acquire() as conn:
        row = await position_repo.get(
            conn,
            position.position_id,
        )

    assert row is not None
    assert row["position_id"] == position.position_id
    assert str(row["position_amt"]) == "0.01"
    assert row["status"] == "OPEN"

    redis_row = await redis_position_repo.get(position.position_id)

    assert redis_row is not None
    assert redis_row["position_id"] == position.position_id
    assert redis_row["status"] == "OPEN"
    assert redis_row["position_amt"] == "0.01"

    open_positions = await redis_position_repo.list_open_positions(
        exchange=Exchange.BINANCE.value,
    )

    assert any(
        row["position_id"] == position.position_id
        for row in open_positions
    )


@pytest.mark.asyncio
async def test_refresh_position_snapshots_persists_postgres_and_redis(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    postgres: PostgresClient = position_bundle["postgres"]
    position_repo: PositionPostgresRepository = position_bundle["position_repo"]
    redis_position_repo: PositionRedisRepository = position_bundle[
        "redis_position_repo"
    ]

    snapshot = _make_exchange_position_snapshot()

    updated = await service.refresh_position_snapshots(snapshots=[snapshot])

    assert len(updated) == 1

    position = updated[0]

    assert position.position_id == _btc_position_id()
    assert position.status == PositionStatus.OPEN
    assert position.position_amt == "0.02"
    assert position.entry_price == "51000"
    assert position.break_even_price == "51010"
    assert position.mark_price == "51200"
    assert position.unrealized_pnl == "4"
    assert position.isolated_margin == "0"
    assert position.isolated_wallet == "0"
    assert position.margin_type == "cross"
    assert position.leverage == 10
    assert position.liquidation_price == "45000"
    assert position.notional == "1024"
    assert position.update_reason == "POSITION_SNAPSHOT_REFRESH"
    assert position.last_event_time == 1_700_000_001_000
    assert position.last_transaction_time == 1_700_000_001_000
    assert position.updated_ts == 1_700_000_001_000
    assert position.opened_ts is not None
    assert position.version == 1

    pool = postgres.require_pool()

    assert position.position_id

    async with pool.acquire() as conn:
        row = await position_repo.get(
            conn,
            position.position_id,
        )

    assert row is not None
    assert row["status"] == "OPEN"
    assert str(row["position_amt"]) == "0.02"
    assert str(row["entry_price"]) == "51000"
    assert str(row["mark_price"]) == "51200"
    assert row["leverage"] == 10

    redis_row = await redis_position_repo.get(position.position_id)

    assert redis_row is not None
    assert redis_row["status"] == "OPEN"
    assert redis_row["position_amt"] == "0.02"
    assert redis_row["leverage"] == "10"


@pytest.mark.asyncio
async def test_apply_position_snapshots_flat_position_removes_open_indexes(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    redis_position_repo: PositionRedisRepository = position_bundle[
        "redis_position_repo"
    ]

    open_snapshots = _make_account_update_snapshots(
        event_time=1_700_000_000_000,
        transaction_time=1_700_000_000_100,
        reason="ORDER",
        positions=[
            {
                "s": "BTCUSDT",
                "pa": "0.01",
                "ep": "50000",
                "bep": "50000",
                "up": "12.3",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    )

    opened = await service.apply_position_snapshots(snapshots=open_snapshots)

    assert len(opened) == 1
    assert opened[0].status == PositionStatus.OPEN

    flat_snapshots = _make_account_update_snapshots(
        event_time=1_700_000_001_000,
        transaction_time=1_700_000_001_100,
        reason="ORDER",
        positions=[
            {
                "s": "BTCUSDT",
                "pa": "0",
                "ep": "0",
                "bep": "0",
                "up": "0",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    )

    flattened = await service.apply_position_snapshots(snapshots=flat_snapshots)

    assert len(flattened) == 1

    position = flattened[0]

    assert position.position_id == _btc_position_id()
    assert position.status == PositionStatus.FLAT
    assert position.position_amt == "0"
    assert position.opened_ts is not None
    assert position.closed_ts is not None
    assert position.version == 2

    assert position.position_id

    redis_row = await redis_position_repo.get(position.position_id)

    assert redis_row is not None
    assert redis_row["status"] == "FLAT"
    assert redis_row["position_amt"] == "0"

    open_positions = await redis_position_repo.list_open_positions(
        exchange=Exchange.BINANCE.value,
    )

    assert all(
        row["position_id"] != position.position_id
        for row in open_positions
    )

    by_symbol = await redis_position_repo.list_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="BTCUSDT",
    )

    assert all(
        row["position_id"] != position.position_id
        for row in by_symbol
    )


@pytest.mark.asyncio
async def test_apply_position_snapshots_stale_event_does_not_overwrite_position(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    postgres: PostgresClient = position_bundle["postgres"]
    position_repo: PositionPostgresRepository = position_bundle["position_repo"]

    newer_snapshots = _make_account_update_snapshots(
        event_time=2_000,
        transaction_time=2_100,
        reason="ORDER",
        positions=[
            {
                "s": "BTCUSDT",
                "pa": "0.01",
                "ep": "50000",
                "bep": "50000",
                "up": "1",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    )

    await service.apply_position_snapshots(snapshots=newer_snapshots)

    stale_snapshots = _make_account_update_snapshots(
        event_time=1_000,
        transaction_time=1_100,
        reason="ORDER",
        positions=[
            {
                "s": "BTCUSDT",
                "pa": "0.99",
                "ep": "1",
                "bep": "1",
                "up": "999",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    )

    updated = await service.apply_position_snapshots(snapshots=stale_snapshots)

    # repository.upsert()가 stale update를 무시하고 current row를 반환해야 한다.
    assert len(updated) == 1
    assert updated[0].position_amt == "0.01"

    pool = postgres.require_pool()

    async with pool.acquire() as conn:
        row = await position_repo.get(
            conn,
            _btc_position_id(),
        )

    assert row is not None
    assert str(row["position_amt"]) == "0.01"
    assert str(row["entry_price"]) == "50000"
    assert row["last_event_time"] == 2_000


@pytest.mark.asyncio
async def test_apply_position_snapshots_ignores_event_without_positions(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]

    snapshots = _make_account_update_snapshots(
        event_time=1_700_000_000_000,
        transaction_time=1_700_000_000_100,
        reason="FUNDING_FEE",
        positions=[],
        balances=[
            {
                "a": "USDT",
                "wb": "1000",
                "cw": "1000",
                "bc": "0",
            }
        ],
    )

    updated = await service.apply_position_snapshots(snapshots=snapshots)

    assert updated == []


@pytest.mark.asyncio
async def test_apply_position_snapshots_handles_hedge_long_position(
    position_bundle,
) -> None:
    service: PositionStateService = position_bundle["service"]
    redis_position_repo: PositionRedisRepository = position_bundle[
        "redis_position_repo"
    ]

    snapshots = _make_account_update_snapshots(
        event_time=1_700_000_000_000,
        transaction_time=1_700_000_000_100,
        reason="ORDER",
        positions=[
            {
                "s": "ETHUSDT",
                "pa": "0.5",
                "ep": "3000",
                "bep": "3000",
                "up": "10",
                "mt": "isolated",
                "iw": "100",
                "ps": "LONG",
            }
        ],
    )

    updated = await service.apply_position_snapshots(snapshots=snapshots)

    assert len(updated) == 1

    position = updated[0]

    assert position.symbol == "ETHUSDT"
    assert position.position_side == PositionSide.LONG
    assert position.status == PositionStatus.OPEN
    assert position.position_amt == "0.5"

    assert position.position_id

    redis_row = await redis_position_repo.get(position.position_id)

    assert redis_row is not None
    assert redis_row["symbol"] == "ETHUSDT"
    assert redis_row["position_side"] == "LONG"
    assert redis_row["status"] == "OPEN"

    by_symbol = await redis_position_repo.list_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="ETHUSDT",
    )

    assert any(
        row["position_id"] == position.position_id
        for row in by_symbol
    )
