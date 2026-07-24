"""
Redis Stream Client — Redis와의 통신(저장/조회)만 담당하는 순수 인프라 계층
도메인 지식(trade, symbol 등)을 포함하지 않습니다.
"""
import logging
from typing import List, Optional

import redis.asyncio as aioredis
from common.logging import setup_logger

logger = setup_logger(__name__)


class RedisStreamClient:
    """
    Redis Stream 조작을 위한 래퍼 클라이언트
    XADD, XREVRANGE 등의 명령어와 Pipeline을 제공합니다.
    """

    def __init__(self, host: str, port: int, db: int = 0):
        self.host = host
        self.port = port
        self.db = db
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self):
        self._redis = await aioredis.from_url(
            f"redis://{self.host}:{self.port}/{self.db}",
            encoding="utf-8",
            decode_responses=True,
        )
        await self._redis.ping()
        logger.info(f"RedisStreamClient connected to {self.host}:{self.port}/{self.db}")

    async def close(self):
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            logger.info("RedisStreamClient disconnected.")

    @property
    def client(self) -> aioredis.Redis:
        if not self._redis:
            raise RuntimeError("RedisStreamClient not connected.")
        return self._redis

    async def xadd(self, stream: str, fields: dict, maxlen: int | None = None) -> str:
        """단건 데이터를 Stream에 발행합니다."""
        if maxlen:
            # approximate=True를 사용하여 성능 최적화 (MAXLEN ~ maxlen)
            return await self.client.xadd(stream, fields, maxlen=maxlen, approximate=True)
        else:
            return await self.client.xadd(stream, fields)

    async def xrevrange(self, stream: str, count: int = 100) -> List[tuple]:
        """최신 N건을 역순으로 읽어옵니다."""
        return await self.client.xrevrange(stream, count=count)