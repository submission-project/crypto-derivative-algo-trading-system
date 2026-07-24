from abc import ABC, abstractmethod
from typing import List

from storage.redis_client import RedisStreamClient


class BaseHotBufferRepository(ABC):
    """
    Redis Stream을 이용하는 Hot Buffer용 기본 리포지토리
    공통 I/O(publish, publish_batch)를 구현하고, 하위 클래스에서 key 생성 및 인코딩 방식을 정의합니다.
    """

    def __init__(self, redis: RedisStreamClient, maxlen: int):
        self.redis = redis
        self.maxlen = maxlen

    @abstractmethod
    def get_stream_key(self, data: dict) -> str:
        """단일 데이터 딕셔너리에서 Redis Stream Key를 추출하거나 생성합니다."""
        pass

    @abstractmethod
    def encode(self, data: dict) -> dict:
        """데이터를 Redis Stream 저장용 문자열 딕셔너리로 변환합니다."""
        pass

    async def publish(self, data: dict):
        """단건 데이터를 Redis Stream에 발행합니다."""
        key = self.get_stream_key(data)
        fields = self.encode(data)
        
        await self.redis.xadd(
            stream=key,
            fields=fields,
            maxlen=self.maxlen
        )

    async def publish_batch(self, items: List[dict]):
        """다수의 데이터를 Pipeline을 이용해 한 번에 발행합니다."""
        if not items:
            return

        async with self.redis.client.pipeline(transaction=False) as pipe:
            for item in items:
                key = self.get_stream_key(item)
                fields = self.encode(item)
                pipe.xadd(key, fields, maxlen=self.maxlen, approximate=True)
            
            await pipe.execute()


class BaseStateRedisRepository(ABC):
    """
    Redis Hash를 이용하는 State 관리용 기본 리포지토리.
    공통 I/O(get)를 구현하고, 하위 클래스에서 key 생성 및 직렬화/역직렬화 방식을 정의합니다.
    """

    def __init__(self, redis: RedisStreamClient):
        self.redis = redis
