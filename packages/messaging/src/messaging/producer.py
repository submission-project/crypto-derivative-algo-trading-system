from aiokafka import AIOKafkaProducer
import orjson
from common.logging import setup_logger

logger = setup_logger(__name__)

class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def connect(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda m: orjson.dumps(m),
        )
        await self.producer.start()
        logger.info(f"Connected to Redpanda Producer ({self.bootstrap_servers}) - Topic: {self.topic}")

    async def produce(self, key: str, value: dict):
        if not self.producer:
            raise RuntimeError("Producer not started. Call start() first.")
        try:
            await self.producer.send(self.topic, key=key.encode(), value=value)
        except Exception as e:
            logger.error(f"Error while producing to {self.topic}: {e}")

    async def send_messages(self, messages: list[dict]):
        if not self.producer:
            raise RuntimeError("Producer not started. Call start() first.")
        
        try:
            for msg in messages:
                await self.producer.send(self.topic, value=msg)
        except Exception as e:
            logger.error(f"Error while producing: {e}")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            self.producer = None
            logger.info("Redpanda producer stopped.")
