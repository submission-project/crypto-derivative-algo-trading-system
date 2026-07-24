from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any
from unittest.mock import AsyncMock

import pytest

from execution_gateway.publishers.outbox_publisher import OutboxPublisher
from schemas.outbox import OutboxEvent


class FakeTransaction(AbstractAsyncContextManager):
    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type,
        exc,
        tb,
    ):
        return None


class FakeConnection:
    def transaction(self):
        return FakeTransaction()


class FakeAcquire(AbstractAsyncContextManager):
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return None


class FakePool:
    def __init__(self):
        self.conn = FakeConnection()

    def acquire(self):
        return FakeAcquire(self.conn)


class FakePostgres:
    def __init__(self):
        self.pool = FakePool()

    def require_pool(self):
        return self.pool


def make_event(event_id: int = 1) -> OutboxEvent:
    return OutboxEvent(
        event_id=event_id,
        aggregate_type="ORDER",
        aggregate_id="ORD-001",
        event_type="ORDER_CREATED",
        payload={"order_id": "ORD-001"},
        created_ts=1_700_000_000_000,
        retry_count=0,
    )


@pytest.mark.asyncio
async def test_publish_once_publishes_and_marks_published() -> None:
    postgres = FakePostgres()

    outbox_repo = AsyncMock()
    outbox_repo.claim_unpublished = AsyncMock(return_value=[make_event()])
    outbox_repo.mark_published = AsyncMock(return_value=True)
    outbox_repo.mark_failed = AsyncMock()

    event_publisher = AsyncMock()
    event_publisher.start = AsyncMock()
    event_publisher.stop = AsyncMock()
    event_publisher.publish = AsyncMock()

    publisher = OutboxPublisher(
        postgres=postgres,
        outbox_repo=outbox_repo,
        event_publisher=event_publisher,
        topic="takora.order.events",
        publisher_id="test-publisher",
    )

    stats = await publisher.publish_once()

    assert stats.claimed == 1
    assert stats.published == 1
    assert stats.failed == 0

    event_publisher.publish.assert_awaited_once()
    outbox_repo.mark_published.assert_awaited_once()
    outbox_repo.mark_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_once_marks_failed_when_publish_fails() -> None:
    postgres = FakePostgres()

    outbox_repo = AsyncMock()
    outbox_repo.claim_unpublished = AsyncMock(return_value=[make_event()])
    outbox_repo.mark_published = AsyncMock()
    outbox_repo.mark_failed = AsyncMock(return_value=True)

    event_publisher = AsyncMock()
    event_publisher.start = AsyncMock()
    event_publisher.stop = AsyncMock()
    event_publisher.publish = AsyncMock(side_effect=RuntimeError("redpanda down"))

    publisher = OutboxPublisher(
        postgres=postgres,
        outbox_repo=outbox_repo,
        event_publisher=event_publisher,
        publisher_id="test-publisher",
        retry_delay_ms=1000,
        topic="takora.order.events",
    )

    stats = await publisher.publish_once()

    assert stats.claimed == 1
    assert stats.published == 0
    assert stats.failed == 1

    outbox_repo.mark_published.assert_not_awaited()
    outbox_repo.mark_failed.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_once_no_events() -> None:
    postgres = FakePostgres()

    outbox_repo = AsyncMock()
    outbox_repo.claim_unpublished = AsyncMock(return_value=[])

    event_publisher = AsyncMock()
    event_publisher.start = AsyncMock()
    event_publisher.stop = AsyncMock()
    event_publisher.publish = AsyncMock()

    publisher = OutboxPublisher(
        postgres=postgres,
        outbox_repo=outbox_repo,
        event_publisher=event_publisher,
        publisher_id="test-publisher",
        topic="takora.order.events",
    )

    stats = await publisher.publish_once()

    assert stats.claimed == 0
    assert stats.published == 0
    assert stats.failed == 0

    event_publisher.publish.assert_not_awaited()
