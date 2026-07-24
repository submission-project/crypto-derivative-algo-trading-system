from __future__ import annotations

import json
from typing import Any, Optional

from aiokafka import AIOKafkaProducer

from common.logging import setup_logger
from execution_gateway.publishers.event_publisher import EventPublisher

logger = setup_logger(__name__)


class RedpandaEventPublisher(EventPublisher):
    """
    Redpanda/Kafka topic 발행 adapter.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        acks: str = "all",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            acks=self.acks,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8"),
        )

        await self._producer.start()
        logger.info(
            f"RedpandaEventPublisher 시작: "
            f"bootstrap_servers={self.bootstrap_servers}, client_id={self.client_id}"
        )

    async def stop(self) -> None:
        if self._producer is None:
            return

        await self._producer.stop()
        self._producer = None
        logger.info("RedpandaEventPublisher 종료 완료")

    async def publish(
        self,
        *,
        topic: str,
        key: str,
        value: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("RedpandaEventPublisher is not started")

        kafka_headers = None
        if headers:
            kafka_headers = [
                (k, v.encode("utf-8"))
                for k, v in headers.items()
            ]

        await self._producer.send_and_wait(
            topic,
            key=key,
            value=value,
            headers=kafka_headers,
        )