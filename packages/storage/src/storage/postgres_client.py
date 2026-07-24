from __future__ import annotations

from typing import Optional

import asyncpg
from asyncpg import Pool

from common.logging import setup_logger

logger = setup_logger(__name__)


class PostgresClient:
    """
    asyncpg connection pool 래퍼.

    역할:
      - 앱 시작 시 pool 생성
      - 앱 종료 시 pool 종료
      - repository가 pool을 공유해서 사용
    """

    def __init__(
        self,
        *,
        dsn: str,
        min_size: int = 1,
        max_size: int = 10,
    ) -> None:
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Optional[Pool] = None

    async def connect(self) -> None:
        if self.pool is not None:
            return

        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
        )
        logger.info(
            f"PostgreSQL pool 연결 완료 "
            f"(min_size={self.min_size}, max_size={self.max_size})"
        )

    async def close(self) -> None:
        if self.pool is None:
            return

        await self.pool.close()
        self.pool = None
        logger.info("PostgreSQL pool 종료 완료")

    def require_pool(self) -> Pool:
        if self.pool is None:
            raise RuntimeError("PostgreSQL pool이 아직 초기화되지 않았습니다.")
        return self.pool