from __future__ import annotations

import asyncio
from collections.abc import Callable

from common.logging import setup_logger
from messaging.consumer import KafkaConsumer
from messaging.producer import KafkaProducer
from schemas.topics import TopicNames

from execution_gateway.handlers.order_submit_handler import OrderSubmitHandler

logger = setup_logger(__name__)


def process_result_to_payload(result) -> dict:
    order = result.order
    return {
        "accepted": result.accepted,
        "stage": result.stage,
        "reason": result.reason,
        "detail": result.detail,
        "dedup_key": result.dedup_key,
        "risk_metadata": result.risk_metadata or {},
        "order_id": getattr(order, "order_id", None),
        "order_status": getattr(getattr(order, "status", None), "value", None),
        "exchange_order_id": getattr(order, "exchange_order_id", None),
    }


async def handle_order_intent(
    *,
    intent: dict,
    handler: OrderSubmitHandler,
    audit_producer: KafkaProducer | None = None,
) -> dict:
    result = await handler.process(intent)
    payload = process_result_to_payload(result)
    if audit_producer is not None:
        key = str(intent.get("signal_id") or payload.get("dedup_key") or "")
        await audit_producer.produce(key, payload)
    return payload


async def run_order_intent_consumer(
    *,
    handler: OrderSubmitHandler,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
    producer_factory: Callable[..., KafkaProducer] = KafkaProducer,
    publish_audit: bool = True,
) -> None:
    from execution_gateway.config import settings

    consumer = consumer_factory(
        bootstrap_servers=settings.redpanda_brokers,
        topic=TopicNames.ORDER_INTENTS,
        group_id="execution-gateway-order-intents",
    )
    audit_producer = (
        producer_factory(
            bootstrap_servers=settings.redpanda_brokers,
            topic=TopicNames.ORDER_STATE_UPDATES,
        )
        if publish_audit
        else None
    )

    await consumer.connect()
    if audit_producer is not None:
        await audit_producer.connect()

    processed = 0
    accepted = 0
    try:
        async for intent in consumer.consume_stream():
            if not isinstance(intent, dict):
                continue
            processed += 1
            payload = await handle_order_intent(intent=intent, handler=handler, audit_producer=audit_producer)
            if payload["accepted"]:
                accepted += 1
                logger.info("Submitted order intent signal_id=%s total=%s", intent.get("signal_id"), accepted)
            else:
                logger.warning(
                    "Rejected order intent signal_id=%s stage=%s reason=%s",
                    intent.get("signal_id"),
                    payload.get("stage"),
                    payload.get("reason"),
                )
    except asyncio.CancelledError:
        logger.info("Order intent consumer cancelled.")
    finally:
        await consumer.stop()
        if audit_producer is not None:
            await audit_producer.stop()
        logger.info("Order intent consumer stopped processed=%s accepted=%s", processed, accepted)


async def main() -> None:
    raise RuntimeError(
        "Build an OrderSubmitHandler with a configured ExecutionGateway, "
        "PreTradeRiskHandler, and OrderIntentDedupHandler, then call run_order_intent_consumer()."
    )


if __name__ == "__main__":
    asyncio.run(main())
