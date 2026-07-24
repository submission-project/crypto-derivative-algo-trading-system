from __future__ import annotations

import asyncio
from collections.abc import Callable

from common.logging import setup_logger
from messaging.consumer import KafkaConsumer
from messaging.producer import KafkaProducer
from schemas.order import OrderRequest, OrderSide, OrderSource, OrderType, PositionAction
from schemas.position import PositionSide
from schemas.signal import Signal, SignalDirection, SignalStatus
from schemas.topics import TopicNames

logger = setup_logger(__name__)


def signal_to_order_intent(signal: Signal) -> dict | None:
    if signal.status not in {SignalStatus.PENDING, SignalStatus.APPROVED}:
        return None
    if signal.direction == SignalDirection.FLAT:
        return None

    side = signal.suggested_side or ("BUY" if signal.direction == SignalDirection.LONG else "SELL")
    quantity = signal.suggested_quantity
    if quantity is None:
        logger.warning("Signal has no suggested_quantity signal_id=%s", signal.signal_id)
        return None

    order_type = OrderType(signal.suggested_order_type or "MARKET")
    position_side = PositionSide.LONG if signal.direction == SignalDirection.LONG else PositionSide.SHORT
    req = OrderRequest(
        exchange=signal.exchange,
        market_type=signal.market_type,
        symbol=signal.symbol,
        side=OrderSide(side),
        order_type=order_type,
        quantity=quantity,
        price=signal.suggested_price,
        position_side=position_side,
        position_action=PositionAction.OPEN,
    )
    payload = req.model_dump(mode="json")
    payload.update(
        {
            "signal_id": signal.signal_id,
            "strategy_name": signal.strategy_name,
            "order_source": OrderSource.STRATEGY.value,
            "generated_ts": signal.generated_ts,
            "confidence": signal.confidence,
            "entry_price": signal.suggested_entry_price,
            "stop_loss_price": signal.suggested_stop_loss,
            "take_profit_price": signal.suggested_take_profit,
        }
    )
    return payload


async def publish_order_intent(
    *,
    signal_payload: dict,
    producer: KafkaProducer,
) -> dict | None:
    signal = Signal.model_validate(signal_payload)
    intent = signal_to_order_intent(signal)
    if intent is None:
        return None
    key = str(intent.get("signal_id") or f"{intent['exchange']}:{intent['symbol']}")
    await producer.produce(key, intent)
    return intent


async def run_order_intent_pipeline(
    *,
    consumer_factory: Callable[..., KafkaConsumer] = KafkaConsumer,
    producer_factory: Callable[..., KafkaProducer] = KafkaProducer,
) -> None:
    from common.config import settings as common_settings

    consumer = consumer_factory(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=TopicNames.SIGNALS,
        group_id="order-intent-pipeline-group",
    )
    producer = producer_factory(
        bootstrap_servers=common_settings.redpanda_brokers,
        topic=TopicNames.ORDER_INTENTS,
    )
    await consumer.connect()
    await producer.connect()

    total_signals = 0
    total_intents = 0
    try:
        async for signal_payload in consumer.consume_stream():
            if not isinstance(signal_payload, dict):
                continue
            total_signals += 1
            intent = await publish_order_intent(signal_payload=signal_payload, producer=producer)
            if intent is not None:
                total_intents += 1
                logger.info("Published order intent signal_id=%s total=%s", intent.get("signal_id"), total_intents)
    except asyncio.CancelledError:
        logger.info("Order intent pipeline cancelled.")
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Order intent pipeline stopped signals=%s intents=%s", total_signals, total_intents)


async def main() -> None:
    await run_order_intent_pipeline()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
