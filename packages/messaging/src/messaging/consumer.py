import asyncio
import logging
from collections.abc import Sequence
from typing import AsyncGenerator

from aiokafka import AIOKafkaConsumer
import orjson

logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str | Sequence[str],
        group_id: str = "stream-processor-group",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topics = self._normalize_topics(topic)
        self.group_id = group_id
        self.consumer = None

    async def connect(self):
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: orjson.loads(m) if m else None,
        )
        await self.consumer.start()
        logger.info("Connected to Redpanda Consumer (%s) - Topics: %s", self.bootstrap_servers, self.topics)

    async def consume_stream(self) -> AsyncGenerator[dict, None]:
        if not self.consumer:
            raise RuntimeError("Consumer not connected. Call connect() first.")
            
        try:
            async for msg in self.consumer:
                if msg.value:
                    yield msg.value
        except asyncio.CancelledError:
            logger.info("Consumer stream cancelled.")
        except Exception as e:
            logger.error(f"Error while consuming: {e}")
        finally:
            await self.stop()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None
            logger.info("Redpanda consumer stopped.")

    @staticmethod
    def _normalize_topics(topic: str | Sequence[str]) -> tuple[str, ...]:
        topics = (topic,) if isinstance(topic, str) else tuple(topic)
        cleaned = tuple(str(value).strip() for value in topics if str(value).strip())
        if not cleaned:
            raise ValueError("KafkaConsumer requires at least one topic.")
        return cleaned
