"""
Legacy Binance canonical trade storage pipeline.

Not used by run-api-server-dev.
Use market_pipeline.py for the current operational market data flow.
"""

"""
canonical_pipeline.py — canonical_trades 토픽을 구독하여 QuestDB + Redis에 동시 저장
실행:
  ENV_FILE=.env.dev uv run python -m stream_processor.canonical_pipeline
"""

import asyncio

from messaging.consumer import KafkaConsumer
from storage.identifiers import QuestDBTable
from storage.questdb_client import QuestDBClient
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.trade_redis_buffer import (
    TradeRedisBufferRepository,
)
from storage.repositories.trade_questdb import TradeQuestDBRepository
from common.logging import setup_logger
from ..config import settings
from common.config import settings as common_settings

logger = setup_logger(__name__)

BATCH_SIZE = settings.batch_size
FLUSH_INTERVAL_SEC = settings.flush_interval_sec
TRADE_MAXLEN = settings.trade_maxlen


async def main():
    logger.info("Starting Canonical Trade Pipeline...")

    # ── 컴포넌트 초기화 ──
    consumer = KafkaConsumer(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=common_settings.binance_perp_topic_canonical,
        group_id="canonical-pipeline-group",
    )
    questdb_client = QuestDBClient(
        host=common_settings.questdb_host,
        ilp_port=common_settings.questdb_ilp_port,
    )
    await questdb_client.connect()

    redis_client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db,
    )

    binance_perp_trade_redis_buffer = TradeRedisBufferRepository(
        redis=redis_client,
        maxlen=TRADE_MAXLEN,
    )
    binance_perp_trade_questdb_storage = TradeQuestDBRepository(
        questdb=questdb_client,
        table_name=QuestDBTable.CANONICAL_TRADES,
    )

    await consumer.connect()
    await redis_client.connect()

    buffer = []
    total = 0

    async def do_flush():
        if not buffer:
            return
        batch = list(buffer)
        buffer.clear()
        # QuestDB와 Redis에 동시 전송
        await asyncio.gather(
            binance_perp_trade_questdb_storage.publish_batch(batch),
            binance_perp_trade_redis_buffer.publish_batch(batch),
        )
        logger.debug(f"Flushed {len(batch)} canonical trades to QuestDB + Redis")

    async def flush_timer():
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SEC)
            await do_flush()

    timer_task = asyncio.create_task(flush_timer())

    try:
        logger.info(f"🎧 Listening on '{settings.binance_perp_topic_canonical}'...")
        async for data in consumer.consume_stream():
            if "is_buyer_maker" in data:
                data["is_buyer_maker"] = str(data["is_buyer_maker"]).lower()

            buffer.append(data)
            total += 1

            if len(buffer) >= BATCH_SIZE:
                await do_flush()

    except asyncio.CancelledError:
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        timer_task.cancel()
        if buffer:
            await do_flush()
        await consumer.stop()
        await questdb_client.close()
        await redis_client.close()
        logger.info(f"Canonical Pipeline stopped. Total: {total}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
