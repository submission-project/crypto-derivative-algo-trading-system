from __future__ import annotations

from dataclasses import dataclass

from common.logging import setup_logger
from storage.postgres_client import PostgresClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository, OrderClearProjectionResult
from storage.repositories.postgres.order_repo import OrderPostgresRepository

logger = setup_logger(__name__)


@dataclass(slots=True)
class OrderProjectionRebuildResult:
    total_rows: int = 0
    rebuilt: int = 0
    skipped: int = 0
    failed: int = 0
    cleared_live_hashes: int = 0
    cleared_indexes: int = 0

class OrderProjectionRebuilder:
    """
    PostgreSQL 원본 기준 Redis 주문 projection 재생성.

    startup 사용 시:
      - 기존 Redis projection을 비운 뒤
      - PostgreSQL의 non-terminal 주문만 다시 올린다.
    """

    def __init__(
        self,
        *,
        postgres: PostgresClient,
        postgres_order_repo: OrderPostgresRepository,
        redis_order_repo: OrderStateRedisRepository,
    ) -> None:
        self.postgres = postgres
        self.postgres_order_repo = postgres_order_repo
        self.redis_order_repo = redis_order_repo

    # 서비스 운영 전 Order 상태[redis] 초기화 
    async def rebuild_active_projection(
        self,
        *,
        reset_existing: bool = True,
    ) -> OrderProjectionRebuildResult:
        """
        PostgreSQL의 non-terminal 주문을 기준으로 Order State Projection[Redis projection] 재생성.

        reset_existing=True:
          - startup 용도
          - 기존 live hash / index를 지우고 새로 만든다.

        reset_existing=False:
          - 실행 중 repair 용도
          - 기존 projection을 지우지 않고 version-aware upsert만 수행한다.
        """
        result = OrderProjectionRebuildResult()

        if reset_existing:
            cleared:OrderClearProjectionResult = await self.redis_order_repo.clear_projection(
                include_live_hashes=True,
            )
            result.cleared_live_hashes = cleared.cleared_live_hashes
            result.cleared_indexes = cleared.cleared_indexes

        pool = self.postgres.require_pool()

        async with pool.acquire() as conn:
            orders = await self.postgres_order_repo.list_non_terminal_joined_orders(conn)

        result.total_rows = len(orders)

        for order in orders:
            try:

                applied = await self.redis_order_repo.save(
                    order
                )

                if applied:
                    result.rebuilt += 1
                else:
                    result.skipped += 1

            except Exception as e:
                result.failed += 1
                logger.error(
                    f"Redis order projection rebuild 실패: "
                    f"row={row}, err={e}",
                    exc_info=True,
                )

        logger.info(
            f"Redis order projection rebuild 완료: "
            f"total={result.total_rows}, "
            f"rebuilt={result.rebuilt}, "
            f"skipped={result.skipped}, "
            f"failed={result.failed}, "
            f"cleared_live_hashes={result.cleared_live_hashes}, "
            f"cleared_indexes={result.cleared_indexes}"
        )

        return result