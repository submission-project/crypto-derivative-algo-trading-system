from __future__ import annotations

import asyncio
from collections.abc import Callable

from common.logging import setup_logger
from messaging.consumer import KafkaConsumer
from schemas.topics import TopicNames

from api_server.websocket_manager import WebSocketManager

logger = setup_logger(__name__)


def signal_to_websocket_payload(signal: dict) -> dict:
    value = dict(signal)
    value["data_type"] = "signal"
    return {
        "topic": TopicNames.SIGNALS,
        "key": str(value.get("signal_id") or ""),
        "value": value,
    }


async def run_signal_websocket_broadcaster(
    *,
    manager: WebSocketManager,
    bootstrap_servers: str,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
) -> None:
    consumer = consumer_factory(
        bootstrap_servers=bootstrap_servers,
        topic=TopicNames.SIGNALS,
        group_id="api-signal-websocket-broadcaster",
    )
    await consumer.connect()
    total = 0
    try:
        async for signal in consumer.consume_stream():
            if not isinstance(signal, dict):
                continue
            total += 1
            await manager.broadcast(signal_to_websocket_payload(signal))
    except asyncio.CancelledError:
        logger.info("Signal websocket broadcaster cancelled.")
    finally:
        await consumer.stop()
        logger.info("Signal websocket broadcaster stopped total=%s", total)
