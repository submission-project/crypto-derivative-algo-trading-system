from __future__ import annotations

import os

import pytest
import pytest_asyncio

from storage.postgres_client import PostgresClient
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository

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
            TRUNCATE TABLE outbox_events
            RESTART IDENTITY CASCADE
            """
        )

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE outbox_events
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_outbox_claim_and_mark_published(
    postgres_client: PostgresClient,
) -> None:
    """
    outbox 이벤트 insert 후 claim하고, 발행 완료 처리하면 다시 claim되지 않는지 검증한다.
    """
    repo = OutboxPostgresRepository()
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        inserted = await repo.insert(
            conn=conn,
            aggregate_id="ORD-001",
            event_type="ORDER_CREATED",
            payload={"order_id": "ORD-001"},
            created_ts=1_700_000_000_000,
        )

    assert inserted.event_id == 1
    assert inserted.aggregate_type == "ORDER"
    assert inserted.aggregate_id == "ORD-001"
    assert inserted.event_type == "ORDER_CREATED"
    assert inserted.payload == {"order_id": "ORD-001"}
    assert inserted.created_ts == 1_700_000_000_000
    assert inserted.retry_count == 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            events = await repo.claim_unpublished(
                conn=conn,
                publisher_id="test-publisher",
                now_ms=1_700_000_000_100,
                batch_size=10,
                lock_ttl_ms=30_000,
                max_retry_count=20,
            )

    assert len(events) == 1
    assert events[0].aggregate_id == "ORD-001"

    async with pool.acquire() as conn:
        ok = await repo.mark_published(
            conn=conn,
            event_id=events[0].event_id,
            publisher_id="test-publisher",
            published_ts=1_700_000_000_200,
        )

    assert ok is True

    async with pool.acquire() as conn:
        async with conn.transaction():
            events_after = await repo.claim_unpublished(
                conn=conn,
                publisher_id="test-publisher",
                now_ms=1_700_000_000_300,
                batch_size=10,
                lock_ttl_ms=30_000,
                max_retry_count=20,
            )

    assert events_after == []


@pytest.mark.asyncio
async def test_outbox_claim_skips_locked_until_expired(
    postgres_client: PostgresClient,
) -> None:
    """
    다른 publisher가 lock 중인 이벤트는 건너뛰고, lock 만료 후에만 다시 claim되는지 검증한다.
    """
    repo = OutboxPostgresRepository()
    pool = postgres_client.require_pool()

    async with pool.acquire() as conn:
        inserted = await repo.insert(
            conn=conn,
            aggregate_id="ORD-LOCKED",
            event_type="ORDER_CREATED",
            payload={"order_id": "ORD-LOCKED"},
            created_ts=1_700_000_000_000,
        )

    assert inserted.aggregate_id == "ORD-LOCKED"

    async with pool.acquire() as conn:
        async with conn.transaction():
            first = await repo.claim_unpublished(
                conn=conn,
                publisher_id="publisher-1",
                now_ms=1_700_000_000_100,
                batch_size=10,
                lock_ttl_ms=30_000,
                max_retry_count=20,
            )

    assert len(first) == 1

    async with pool.acquire() as conn:
        async with conn.transaction():
            second = await repo.claim_unpublished(
                conn=conn,
                publisher_id="publisher-2",
                now_ms=1_700_000_000_200,
                batch_size=10,
                lock_ttl_ms=30_000,
                max_retry_count=20,
            )

    assert second == []

    async with pool.acquire() as conn:
        async with conn.transaction():
            third = await repo.claim_unpublished(
                conn=conn,
                publisher_id="publisher-2",
                now_ms=1_700_000_040_200,
                batch_size=10,
                lock_ttl_ms=30_000,
                max_retry_count=20,
            )

    assert len(third) == 1
