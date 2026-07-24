from __future__ import annotations

import asyncio
import socket
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from common.logging import setup_logger
from execution_gateway.publishers.event_publisher import EventPublisher
from schemas.outbox import OutboxEvent
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from common.time import epoch_ms

logger = setup_logger(__name__)


def _now_ms() -> int:
    return epoch_ms()


@dataclass(slots=True)
class OutboxPublisherStats:
    claimed: int = 0
    published: int = 0
    failed: int = 0


class OutboxPublisher:
    """
    PostgreSQL transactional outbox -> Redpanda publisher.

    보장:
      - at-least-once delivery
      - 중복 발행 가능성 있음
      - consumer는 event_id 기준 idempotent 처리 필요

    흐름:
      1. outbox_events에서 unpublished 이벤트 claim
      2. Redpanda topic으로 publish
      3. 성공 시 published_ts 기록
      4. 실패 시 retry_count 증가 및 next_attempt_ts 설정
    """

    def __init__(
        self,
        *,
        postgres: PostgresClient,
        outbox_repo: OutboxPostgresRepository,
        event_publisher: EventPublisher,
        topic: str,
        interval_sec: float = 1.0,
        batch_size: int = 100,
        lock_ttl_ms: int = 30_000,
        retry_delay_ms: int = 5_000,
        max_retry_count: int = 20,
        publisher_id: Optional[str] = None,
    ) -> None:
        self.postgres = postgres
        self.outbox_repo = outbox_repo
        self.event_publisher = event_publisher

        self.topic = topic
        self.interval_sec = interval_sec
        self.batch_size = batch_size
        self.lock_ttl_ms = lock_ttl_ms
        self.retry_delay_ms = retry_delay_ms
        self.max_retry_count = max_retry_count

        self.publisher_id = publisher_id or (
            f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        )

        self._running = False
        self._stop_event = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if self._running:
            logger.warning("OutboxPublisher가 이미 실행 중입니다.")
            return

        await self.event_publisher.start()

        self._running = True
        self._stop_event.clear()

        self._task = asyncio.create_task(
            self._run_loop(),
            name="outbox-publisher",
        )

        logger.info(
            f"OutboxPublisher 시작: "
            f"publisher_id={self.publisher_id}, "
            f"topic={self.topic}, "
            f"interval={self.interval_sec}s, "
            f"batch_size={self.batch_size}"
        )

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

        await self.event_publisher.stop()
        logger.info("OutboxPublisher 종료 완료")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                stats = await self.publish_once()

                if stats.claimed == 0:
                    stopped = await self._sleep_or_stop(self.interval_sec)
                    if stopped:
                        break
                else:
                    # backlog이 있으면 바로 다음 batch 처리
                    await asyncio.sleep(0)

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(f"OutboxPublisher loop error: {e}", exc_info=True)
                stopped = await self._sleep_or_stop(self.interval_sec)
                if stopped:
                    break

    async def _sleep_or_stop(self, delay_sec: float) -> bool:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=delay_sec,
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def publish_once(self) -> OutboxPublisherStats:
        """
        outbox 이벤트 batch 1회를 claim 후 publish.
        """
        now_ms = _now_ms()
        stats = OutboxPublisherStats()

        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            async with conn.transaction():
                events = await self.outbox_repo.claim_unpublished(
                    conn=conn,
                    publisher_id=self.publisher_id,
                    now_ms=now_ms,
                    batch_size=self.batch_size,
                    lock_ttl_ms=self.lock_ttl_ms,
                    max_retry_count=self.max_retry_count,
                )

        stats.claimed = len(events)

        if not events:
            return stats

        for event in events:
            try:
                await self._publish_event(event)

                async with pool.acquire() as conn:
                    await self.outbox_repo.mark_published(
                        conn=conn,
                        event_id=event.event_id,
                        publisher_id=self.publisher_id,
                        published_ts=_now_ms(),
                    )

                stats.published += 1

            except Exception as e:
                stats.failed += 1

                logger.error(
                    f"outbox event publish 실패: "
                    f"event_id={event.event_id}, "
                    f"event_type={event.event_type}, "
                    f"aggregate_id={event.aggregate_id}, "
                    f"err={e}",
                    exc_info=True,
                )

                async with pool.acquire() as conn:
                    await self.outbox_repo.mark_failed(
                        conn,
                        event_id=event.event_id,
                        publisher_id=self.publisher_id,
                        now_ms=_now_ms(),
                        error=str(e),
                        retry_delay_ms=self._compute_retry_delay_ms(event.retry_count),
                    )

        logger.info(
            f"Outbox publish batch 완료: "
            f"claimed={stats.claimed}, "
            f"published={stats.published}, "
            f"failed={stats.failed}"
        )

        return stats

    async def _publish_event(self, event: OutboxEvent) -> None:
        """
        단일 outbox event를 Redpanda topic으로 발행.
        """
        value = {
            "event_id": event.event_id,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": event.aggregate_id,
            "event_type": event.event_type,
            "payload": event.payload,
            "created_ts": event.created_ts,
        }

        headers = {
            "event_type": event.event_type,
            "aggregate_type": event.aggregate_type,
        }

        await self.event_publisher.publish(
            topic=self.topic,
            key=event.aggregate_id,
            value=value,
            headers=headers,
        )

    def _compute_retry_delay_ms(self, retry_count: int) -> int:
        """
        간단한 exponential backoff.

        retry_count는 실패 전 기존 count이므로,
        첫 실패에서 retry_count=0으로 들어온다.
        """
        multiplier = min(2**retry_count, 12)
        return self.retry_delay_ms * multiplier
