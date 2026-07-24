from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from common.market_naming import csv_values, default_market_topics
from common.logging import setup_logger
from messaging.consumer import KafkaConsumer
from storage.identifiers import QuestDBTable
from storage.questdb_client import QuestDBClient
from storage.redis_client import RedisStreamClient
from storage.repositories.market_repo import (
    MarketEventRedisBufferRepository,
    MarketTradeQuestDBRepository,
    OpenInterestQuestDBRepository,
    OrderBookQuestDBRepository,
)

logger = setup_logger(__name__)


DEFAULT_MARKET_TOPICS: tuple[str, ...] = default_market_topics()


def parse_market_topics(raw: str | Iterable[str] | None) -> tuple[str, ...]:
    """Parse a comma-separated topic list while preserving deterministic order."""
    if raw is None:
        return default_market_topics()
    return csv_values(raw) or default_market_topics()


def normalize_market_data_type(data_type: Any) -> str:
    normalized = str(data_type or "").strip().lower()
    if normalized in {"depth", "book", "order_book"}:
        return "orderbook"
    if normalized in {"trades", "trade"}:
        return "trade"
    return normalized


@dataclass(slots=True)
class MarketEventRouter:
    """Route mixed market events to data-type specific storage repositories."""

    trade_repo: MarketTradeQuestDBRepository
    orderbook_repo: OrderBookQuestDBRepository
    open_interest_repo: OpenInterestQuestDBRepository
    redis_repo: MarketEventRedisBufferRepository

    async def publish_batch(self, batch: list[dict]) -> dict[str, int]:
        trades: list[dict] = []
        orderbooks: list[dict] = []
        open_interests: list[dict] = []
        unknown = 0

        for item in batch:
            data_type = normalize_market_data_type(item.get("data_type"))
            item["data_type"] = data_type
            if data_type == "trade":
                trades.append(item)
            elif data_type == "orderbook":
                orderbooks.append(item)
            elif data_type == "open_interest":
                open_interests.append(item)
            else:
                unknown += 1
                logger.warning("Skipping unknown market event data_type=%r item=%r", item.get("data_type"), item)

        tasks = []
        if trades:
            tasks.append(self.trade_repo.publish_batch(trades))
        if orderbooks:
            tasks.append(self.orderbook_repo.publish_batch(orderbooks))
        if open_interests:
            tasks.append(self.open_interest_repo.publish_batch(open_interests))
        if trades or orderbooks or open_interests:
            tasks.append(self.redis_repo.publish_batch(trades + orderbooks + open_interests))
        if tasks:
            await asyncio.gather(*tasks)

        return {
            "trade": len(trades),
            "orderbook": len(orderbooks),
            "open_interest": len(open_interests),
            "unknown": unknown,
        }


def build_market_event_router(
    *,
    questdb_client: QuestDBClient,
    redis_client: RedisStreamClient,
    redis_maxlen: int,
) -> MarketEventRouter:
    return MarketEventRouter(
        trade_repo=MarketTradeQuestDBRepository(
            questdb=questdb_client,
            table_name=QuestDBTable.MARKET_TRADES,
        ),
        orderbook_repo=OrderBookQuestDBRepository(
            questdb=questdb_client,
            table_name=QuestDBTable.MARKET_ORDERBOOKS,
        ),
        open_interest_repo=OpenInterestQuestDBRepository(
            questdb=questdb_client,
            table_name=QuestDBTable.MARKET_OPEN_INTEREST,
        ),
        redis_repo=MarketEventRedisBufferRepository(
            redis=redis_client,
            maxlen=redis_maxlen,
        ),
    )


async def run_market_event_pipeline(
    *,
    topics: Iterable[str] | None = None,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
) -> None:
    from common.config import settings as common_settings
    from stream_processor.config import settings

    topic_list = parse_market_topics(topics or settings.market_topics)
    logger.info("Starting Market Event Pipeline topics=%s", topic_list)

    consumer = consumer_factory(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=topic_list,
        group_id="market-event-pipeline-group",
    )
    questdb_client = QuestDBClient(
        host=common_settings.questdb_host,
        ilp_port=common_settings.questdb_ilp_port,
    )
    redis_client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db,
    )

    await questdb_client.connect()
    await redis_client.connect()
    await consumer.connect()

    router = build_market_event_router(
        questdb_client=questdb_client,
        redis_client=redis_client,
        redis_maxlen=settings.market_event_maxlen,
    )
    buffer: list[dict] = []
    total = 0

    async def do_flush() -> None:
        if not buffer:
            return
        batch = list(buffer)
        buffer.clear()
        counts = await router.publish_batch(batch)
        logger.debug("Flushed market events counts=%s", counts)

    async def flush_timer() -> None:
        while True:
            await asyncio.sleep(settings.flush_interval_sec)
            await do_flush()

    timer_task = asyncio.create_task(flush_timer())
    try:
        async for data in consumer.consume_stream():
            if isinstance(data, dict):
                buffer.append(data)
                total += 1
            else:
                logger.warning("Skipping non-dict market event: %r", data)
            if len(buffer) >= settings.batch_size:
                await do_flush()
    except asyncio.CancelledError:
        logger.info("Market event pipeline cancelled.")
    finally:
        timer_task.cancel()
        if buffer:
            await do_flush()
        await consumer.stop()
        await questdb_client.close()
        await redis_client.close()
        logger.info("Market Event Pipeline stopped. Total=%s", total)


async def main() -> None:
    await run_market_event_pipeline()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
