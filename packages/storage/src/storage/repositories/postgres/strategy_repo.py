"""
StrategyPostgresRepository — 전략 메타데이터 및 활성화 상태 영속 관리.

strategies 테이블을 사용하여:
- 전략 목록 조회 (list_all)
- 전략 상태 조회 (get_status)
- 활성화/비활성화 토글 (set_enabled)
- 레지스트리 동기화 시 upsert (upsert)
"""
from __future__ import annotations

import time
from typing import Any

from common.logging import setup_logger

logger = setup_logger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class StrategyPostgresRepository:
    TABLE = "strategies"

    async def list_all(self, conn) -> list[dict[str, Any]]:
        """모든 전략 목록 조회."""
        rows = await conn.fetch(
            f"SELECT * FROM {self.TABLE} ORDER BY created_ts"
        )
        return [dict(row) for row in rows]

    async def list_enabled(self, conn) -> list[dict[str, Any]]:
        """활성화된 전략만 조회."""
        rows = await conn.fetch(
            f"SELECT * FROM {self.TABLE} WHERE enabled = true ORDER BY created_ts"
        )
        return [dict(row) for row in rows]

    async def get_by_name(self, conn, strategy_name: str) -> dict[str, Any] | None:
        """전략명으로 단건 조회."""
        row = await conn.fetchrow(
            f"SELECT * FROM {self.TABLE} WHERE strategy_name = $1",
            strategy_name,
        )
        return dict(row) if row else None

    async def is_enabled(self, conn, strategy_name: str, default: bool = True) -> bool:
        """전략 활성화 여부 조회. 미등록 시 default 반환."""
        row = await conn.fetchrow(
            f"SELECT enabled FROM {self.TABLE} WHERE strategy_name = $1",
            strategy_name,
        )
        if row is None:
            return default
        return bool(row["enabled"])

    async def set_enabled(self, conn, strategy_name: str, enabled: bool) -> dict[str, Any]:
        """전략 활성화/비활성화 토글. 미등록 시 새로 생성."""
        now = _now_ms()
        row = await conn.fetchrow(
            f"""
            INSERT INTO {self.TABLE} (strategy_name, enabled, updated_ts)
            VALUES ($1, $2, $3)
            ON CONFLICT (strategy_name)
            DO UPDATE SET enabled = $2, updated_ts = $3
            RETURNING *
            """,
            strategy_name,
            enabled,
            now,
        )
        return dict(row) if row else {"strategy_name": strategy_name, "enabled": enabled}

    async def upsert(
        self,
        conn,
        *,
        strategy_name: str,
        display_name: str = "",
        description: str = "",
        target_exchange: str = "",
        target_symbol: str = "",
        quantity: str = "0",
    ) -> dict[str, Any]:
        """
        전략 메타데이터 upsert.
        레지스트리 동기화 시 호출 — enabled 상태는 건드리지 않음.
        """
        now = _now_ms()
        row = await conn.fetchrow(
            f"""
            INSERT INTO {self.TABLE} (
                strategy_name, display_name, description,
                target_exchange, target_symbol, quantity,
                created_ts, updated_ts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            ON CONFLICT (strategy_name)
            DO UPDATE SET
                display_name    = EXCLUDED.display_name,
                description     = EXCLUDED.description,
                target_exchange = EXCLUDED.target_exchange,
                target_symbol   = EXCLUDED.target_symbol,
                quantity        = EXCLUDED.quantity,
                updated_ts      = EXCLUDED.updated_ts
            RETURNING *
            """,
            strategy_name,
            display_name,
            description,
            target_exchange,
            target_symbol,
            quantity,
            now,
        )
        return dict(row) if row else {}
