from __future__ import annotations

from typing import Protocol

import orjson

from .operational_models import MarketEvent

from messaging.producer import KafkaProducer


class EventSink(Protocol):
    async def emit(self, topic: str, key: str, event: MarketEvent) -> None: ...
    async def close(self) -> None: ...


class StdoutSink:
    async def emit(self, topic: str, key: str, event: MarketEvent) -> None:
        payload = {"topic": topic, "key": key, "value": event}
        print(orjson.dumps(payload).decode())

    async def close(self) -> None:
        return None


class RedpandaSink:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producers = {}

    async def emit(self, topic: str, key: str, event: MarketEvent) -> None:
        producer = await self._producer(topic)
        await producer.produce(key, event)

    async def close(self) -> None:
        for producer in self._producers.values():
            await producer.stop()
        self._producers.clear()

    async def _producer(self, topic: str):
        if topic not in self._producers:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=topic,
            )
            await producer.connect()
            self._producers[topic] = producer
        return self._producers[topic]
