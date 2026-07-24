from __future__ import annotations

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from schemas.market import Exchange, MarketType
from schemas.position import Position, PositionSide, PositionStatus
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.position_state_repo import PositionRedisRepository

pytestmark = pytest.mark.integration


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
def position_repo(
    redis_stream_client: RedisStreamClient,
) -> PositionRedisRepository:
    return PositionRedisRepository(redis_stream_client)


def make_position(
    *,
    amt: str,
    updated_ts: int = 1_700_000_000_000,
) -> Position:
    return Position(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
        position_amt=amt,
        entry_price="60000",
        updated_ts=updated_ts,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_save_open_position_adds_open_index(
    position_repo: PositionRedisRepository,
) -> None:
    position = make_position(amt="0.01")

    await position_repo.save(position)

    assert position.position_id

    row = await position_repo.get(position.position_id)

    assert row is not None
    assert row["position_id"] == position.position_id
    assert row["symbol"] == "BTCUSDT"
    assert row["status"] == PositionStatus.OPEN.value

    open_positions = await position_repo.list_open_positions(
        exchange=position.exchange.value,
        market_type=position.market_type.value,
    )
    open_ids = {row["position_id"] for row in open_positions}

    assert position.position_id in open_ids

    by_symbol = await position_repo.list_by_symbol(
        exchange=position.exchange.value,
        market_type=position.market_type.value,
        symbol=position.symbol,
    )
    symbol_ids = {row["position_id"] for row in by_symbol}

    assert position.position_id in symbol_ids


@pytest.mark.stable
@pytest.mark.asyncio
async def test_save_flat_position_removes_open_index(
    position_repo: PositionRedisRepository,
) -> None:
    open_position = make_position(amt="0.01")
    await position_repo.save(open_position)

    flat_position = make_position(
        amt="0",
        updated_ts=1_700_000_000_100,
    )
    await position_repo.save(flat_position)
    
    assert flat_position.position_id

    row = await position_repo.get(flat_position.position_id)

    assert row is not None
    assert row["status"] == PositionStatus.FLAT.value
    assert row["position_amt"] == "0"

    open_positions = await position_repo.list_open_positions(
        exchange=flat_position.exchange.value,
        market_type=flat_position.market_type.value,
    )
    open_ids = {row["position_id"] for row in open_positions}

    assert flat_position.position_id not in open_ids

    by_symbol = await position_repo.list_by_symbol(
        exchange=flat_position.exchange.value,
        market_type=flat_position.market_type.value,
        symbol=flat_position.symbol,
    )
    symbol_ids = {row["position_id"] for row in by_symbol}

    assert flat_position.position_id not in symbol_ids

@pytest.mark.stable
@pytest.mark.asyncio
async def test_delete_position_removes_hash_and_indexes(
    position_repo: PositionRedisRepository,
) -> None:
    position = make_position(amt="0.01")
    await position_repo.save(position)

    assert position.position_id

    assert await position_repo.get(position.position_id) is not None

    await position_repo.delete(position.position_id)

    assert await position_repo.get(position.position_id) is None

    open_positions = await position_repo.list_open_positions(
        exchange=position.exchange.value,
        market_type=position.market_type.value,
    )
    open_ids = {row["position_id"] for row in open_positions}

    assert position.position_id not in open_ids