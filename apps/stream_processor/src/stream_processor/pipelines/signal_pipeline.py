from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable

from common.logging import setup_logger
from common.market_naming import default_market_topics
from messaging.consumer import KafkaConsumer
from messaging.producer import KafkaProducer
from schemas.signal import Signal
from schemas.topics import TopicNames
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.strategy_control_repo import StrategyControlRedisRepository
from storage.repositories.signal_repo import SignalRedisRepository
from strategies.registry import StrategyRegistry, build_default_strategy_registry

logger = setup_logger(__name__)


def signal_to_payload(signal: Signal) -> dict:
    return signal.model_dump(mode="json")


async def publish_signals(
    *,
    event: dict,
    registry: StrategyRegistry,
    producer: KafkaProducer,
    signal_repo: SignalRedisRepository | None = None,
    strategy_control_repo: StrategyControlRedisRepository | None = None,
) -> list[dict]:
    signals = registry.on_market_event(event)
    payloads = []
    for signal in signals:
        if strategy_control_repo is not None:
            enabled = await strategy_control_repo.is_enabled(signal.strategy_name, default=True)
            if not enabled:
                logger.info("Signal suppressed because strategy is disabled strategy=%s", signal.strategy_name)
                continue
        payloads.append(signal_to_payload(signal))
    for payload in payloads:
        if signal_repo is not None:
            await signal_repo.save_pending(payload.copy())
        await producer.produce(str(payload["signal_id"]), payload)
    return payloads


async def run_signal_pipeline(
    *,
    topics: Iterable[str] | None = None,
    registry: StrategyRegistry | None = None,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
    producer_factory: Callable[..., KafkaProducer] = KafkaProducer,
) -> None:
    from common.config import settings as common_settings

    topic_list = tuple(topics or default_market_topics())
    registry = registry or build_default_strategy_registry()
    consumer = consumer_factory(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=topic_list,
        group_id="signal-pipeline-group",
    )
    producer = producer_factory(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=TopicNames.SIGNALS,
    )
    redis_client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db,
    )
    signal_repo = SignalRedisRepository(redis_client)
    strategy_control_repo = StrategyControlRedisRepository(redis_client)

    await consumer.connect()
    await producer.connect()
    await redis_client.connect()

    total_events = 0
    total_signals = 0
    try:
        async for event in consumer.consume_stream():
            if not isinstance(event, dict):
                continue
            total_events += 1
            payloads = await publish_signals(
                event=event,
                registry=registry,
                producer=producer,
                signal_repo=signal_repo,
                strategy_control_repo=strategy_control_repo,
            )
            total_signals += len(payloads)
            if payloads:
                logger.info("Published %s signals total=%s", len(payloads), total_signals)
    except asyncio.CancelledError:
        logger.info("Signal pipeline cancelled.")
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()
        logger.info("Signal pipeline stopped events=%s signals=%s", total_events, total_signals)


async def main() -> None:
    from stream_processor.config import settings

    await run_signal_pipeline(topics=settings.market_topics.split(",") if settings.market_topics else None)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
