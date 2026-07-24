from __future__ import annotations

from dataclasses import dataclass

from common.logging import setup_logger
from schemas.position import Position
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository, PositionClearProjection

logger = setup_logger(__name__)


@dataclass(slots=True)
class PositionProjectionRebuildResult:
    total_rows: int = 0
    rebuilt: int = 0
    skipped: int = 0
    failed: int = 0
    deleted_keys: int = 0


class PositionProjectionRebuilder:
    """
    PostgreSQL 원본 기준 Redis 포지션 projection 재생성.

    startup 사용 시:
      - 기존 Redis position projection을 비운 뒤
      - PostgreSQL의 OPEN positions만 다시 올린다.

    복구되는 Redis 구조:
      - position:live:{position_id}
      - position:open:{exchange}
      - position:by:symbol:{exchange}:{market_type}:{symbol}
    """

    def __init__(
        self,
        *,
        postgres: PostgresClient,
        position_repo: PositionPostgresRepository,
        redis_position_repo: PositionRedisRepository,
    ) -> None:
        self.postgres = postgres
        self.position_repo = position_repo
        self.redis_position_repo = redis_position_repo

    async def rebuild_active_projection(
        self,
        *,
        reset_existing: bool = True,
    ) -> PositionProjectionRebuildResult:
        """
        PostgreSQL의 OPEN positions를 기준으로 Redis projection 재생성.

        reset_existing=True:
          - startup 용도
          - 기존 live hash / open set / symbol index를 모두 지우고 새로 만든다.

        reset_existing=False:
          - 실행 중 repair 용도
          - 기존 projection을 지우지 않고 OPEN position만 save한다.
        """
        result = PositionProjectionRebuildResult()

        if reset_existing:
            cleared:PositionClearProjection = await self.redis_position_repo.clear_projection(
                include_live_hashes=True,
            )
            result.deleted_keys = cleared.deleted_keys

        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            positions:list[Position] = await self.position_repo.list_open_for_projection(conn)

        result.total_rows = len(positions)

        for position in positions:
            try:
                await self.redis_position_repo.save(position)

                result.rebuilt += 1

            except Exception as e:
                result.failed += 1
                logger.error(
                    f"Redis position projection rebuild 실패: "
                    f"row={row}, err={e}",
                    exc_info=True,
                )

        logger.info(
            f"Redis position projection rebuild 완료: "
            f"total={result.total_rows}, "
            f"rebuilt={result.rebuilt}, "
            f"skipped={result.skipped}, "
            f"failed={result.failed}, "
            f"deleted_keys={result.deleted_keys}"
        )

        return result
