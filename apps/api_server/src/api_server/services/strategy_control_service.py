from __future__ import annotations

import re
from typing import Any

from common.logging import setup_logger
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.strategy_repo import StrategyPostgresRepository
from storage.repositories.postgres.strategy_risk_config_repo import StrategyRiskConfigPostgresRepository
from storage.repositories.redis.strategy_control_repo import StrategyControlRedisRepository
from strategies.registry import build_default_strategy_registry

logger = setup_logger(__name__)


# ──────────────────────────────────────────────────────────────────────
# 전략 레지스트리에서 동적으로 메타데이터를 추출하는 유틸리티
# ──────────────────────────────────────────────────────────────────────

def _build_strategy_catalog() -> list[dict[str, Any]]:
    """
    stream_processor의 전략 레지스트리를 import 하여
    서버에 실제 탑재된 전략 메타데이터를 동적으로 수집한다.
    """
    catalog: list[dict[str, Any]] = []
    try:
        registry = build_default_strategy_registry()
        for strategy in registry.strategies:
            name = getattr(strategy, "name", "")
            cls_name = type(strategy).__name__
            catalog.append({
                "strategy_name": name,
                "display_name": _humanize_strategy_name(cls_name),
                "description": (strategy.__doc__ or "").strip().split("\n")[0] if strategy.__doc__ else "",
                "target_symbol": getattr(strategy, "target_symbol", ""),
                "target_exchange": getattr(strategy, "target_exchange", ""),
                "quantity": str(getattr(strategy, "quantity", "0")),
            })
    except Exception as e:
        logger.warning("전략 레지스트리 로드 실패: %s", e)
    return catalog


def _humanize_strategy_name(cls_name: str) -> str:
    """CamelCase 클래스명을 사람이 읽기 좋은 형태로 변환."""
    spaced = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", cls_name)
    spaced = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", spaced)
    return spaced.replace("Strategy", "").strip()


# ──────────────────────────────────────────────────────────────────────
# PostgreSQL 기반 실제 성과 집계 쿼리
# ──────────────────────────────────────────────────────────────────────

_STRATEGY_PERFORMANCE_SQL = """
SELECT
    i.strategy_name,
    COUNT(*)                                            AS total_orders,
    COUNT(*) FILTER (WHERE o.status = 'FILLED')         AS filled_orders,
    SUM(o.filled_quantity)::text                         AS total_filled_qty,
    AVG(o.avg_fill_price)::text                         AS avg_fill_price,
    MIN(o.created_ts)                                   AS first_order_ts,
    MAX(o.created_ts)                                   AS last_order_ts
FROM order_intents i
JOIN orders o ON o.order_id = i.order_id
WHERE i.strategy_name IS NOT NULL
GROUP BY i.strategy_name
"""


class StrategyControlService:
    def __init__(
        self,
        *,
        repo: StrategyControlRedisRepository,
        postgres: PostgresClient | None = None,
        strategy_pg_repo: StrategyPostgresRepository | None = None,
        strategy_risk_config_pg_repo: StrategyRiskConfigPostgresRepository | None = None,
    ) -> None:
        self._repo = repo  # Redis (실시간 캐시, signal_pipeline 호환)
        self._postgres = postgres
        self._strategy_pg_repo = strategy_pg_repo
        self._strategy_risk_config_pg_repo = strategy_risk_config_pg_repo

    # ── 개별 전략 상태 조회 (Redis 캐시 + PG fallback) ──

    async def get_status(self, strategy_name: str) -> dict[str, object]:
        """Redis에서 상태를 먼저 확인하고, PG에도 조회."""
        # Redis에서 빠르게 조회 (signal_pipeline 호환)
        redis_enabled = await self._repo.is_enabled(strategy_name, default=True)
        return {"strategy_name": strategy_name, "enabled": redis_enabled}

    async def set_enabled(self, strategy_name: str, enabled: bool) -> dict[str, object]:
        """전략 활성화/비활성화 — Redis + PostgreSQL 양쪽에 영속."""
        # 1) Redis 업데이트 (signal_pipeline 즉시 반영)
        await self._repo.set_enabled(strategy_name, enabled)

        # 2) PostgreSQL 영속 (서버 재시작 시 복원용)
        if self._postgres and self._strategy_pg_repo:
            try:
                async with self._postgres.pool.acquire() as conn:
                    await self._strategy_pg_repo.set_enabled(conn, strategy_name, enabled)
            except Exception as e:
                logger.warning("PostgreSQL 전략 상태 업데이트 실패: %s", e)

        return {"strategy_name": strategy_name, "enabled": enabled}

    # ── 전략 목록 + 실시간 성과 ──

    async def list_strategies(self) -> list[dict[str, Any]]:
        """
        1) 전략 레지스트리에서 메타데이터 수집
        2) PostgreSQL strategies 테이블에 upsert (메타 동기화)
        3) PG strategies 테이블에서 최종 목록 + enabled 상태 조회
        4) PG orders에서 실제 성과 지표 집계
        5) Redis에 enabled 상태 동기화 (signal_pipeline 호환)
        """
        catalog = _build_strategy_catalog()

        if self._postgres and self._strategy_pg_repo:
            try:
                async with self._postgres.pool.acquire() as conn:
                    # 레지스트리 메타데이터를 PG에 upsert (enabled 상태는 보존)
                    for entry in catalog:
                        name = entry["strategy_name"]
                        await self._strategy_pg_repo.upsert(
                            conn,
                            strategy_name=name,
                            display_name=entry.get("display_name", ""),
                            description=entry.get("description", ""),
                            target_exchange=entry.get("target_exchange", ""),
                            target_symbol=entry.get("target_symbol", ""),
                            quantity=entry.get("quantity", "0"),
                        )
                        # 전략별 리스크 설정이 존재하지 않으면 기본값으로 등록
                        # [주석 처리]: 삭제하지 않고 클라이언트가 직접 설정하도록 유도하기 위해 비활성화
                        # if self._strategy_risk_config_pg_repo:
                        #     existing = await self._strategy_risk_config_pg_repo.get_by_strategy(conn, name)
                        #     if not existing:
                        #         from execution_gateway.handlers.risk_handler import RiskConfig
                        #         default_config = RiskConfig()
                        #         default_dict = {
                        #             "account_equity": float(default_config.account_equity),
                        #             "risk_per_trade": float(default_config.risk_per_trade),
                        #             "max_leverage": float(default_config.max_leverage),
                        #             "max_position_notional": float(default_config.max_position_notional),
                        #             "min_notional": float(default_config.min_notional),
                        #             "min_stop_bps": float(default_config.min_stop_bps),
                        #             "min_reward_risk": float(default_config.min_reward_risk),
                        #             "quantity_step": float(default_config.quantity_step),
                        #             "fee_bps": float(default_config.fee_bps),
                        #             "slippage_bps": float(default_config.slippage_bps),
                        #             "spread_bps": float(default_config.spread_bps),
                        #         }
                        #         await self._strategy_risk_config_pg_repo.upsert(conn, name, default_dict)

                    # PG에서 최종 목록 조회 (enabled 상태 포함)
                    pg_rows = await self._strategy_pg_repo.list_all(conn)

                    # 리스크 설정이 기등록되었는지 확인
                    configured_strategies = set()
                    if self._strategy_risk_config_pg_repo:
                        configs = await conn.fetch("SELECT strategy_name FROM strategy_risk_configs")
                        configured_strategies = {r["strategy_name"] for r in configs}

                    # 성과 지표 집계
                    perf_map = await self._fetch_performance(conn)

                # PG rows를 프론트엔드 응답 형태로 변환
                result: list[dict[str, Any]] = []
                for idx, row in enumerate(pg_rows, start=1):
                    key = row["strategy_name"]
                    perf = perf_map.get(key, {})

                    # Redis에 PG의 enabled 상태 동기화
                    await self._repo.set_enabled(key, row["enabled"])

                    result.append({
                        "id": f"strat-{idx}",
                        "apiKeyName": key,
                        "name": row.get("display_name") or key,
                        "description": row.get("description", ""),
                        "symbol": row.get("target_symbol", ""),
                        "exchange": row.get("target_exchange", ""),
                        "quantity": row.get("quantity", "0"),
                        "status": "ACTIVE" if row["enabled"] else "PAUSED",
                        "needsRiskConfig": key not in configured_strategies,
                        "totalOrders": perf.get("total_orders", 0),
                        "filledOrders": perf.get("filled_orders", 0),
                        "totalFilledQty": perf.get("total_filled_qty", "0"),
                        "avgFillPrice": perf.get("avg_fill_price"),
                        "firstOrderTs": perf.get("first_order_ts"),
                        "lastOrderTs": perf.get("last_order_ts"),
                        "createdTs": row.get("created_ts"),
                        "updatedTs": row.get("updated_ts"),
                    })
                return result

            except Exception as e:
                logger.warning("PostgreSQL 전략 목록 조회 실패 (fallback): %s", e)

        # PostgreSQL 미사용 시 fallback: 레지스트리 + Redis
        result = []
        for idx, entry in enumerate(catalog, start=1):
            key = entry["strategy_name"]
            status = await self._repo.get_status(key, default=True)
            result.append({
                "id": f"strat-{idx}",
                "apiKeyName": key,
                "name": entry.get("display_name", key),
                "description": entry.get("description", ""),
                "symbol": entry.get("target_symbol", ""),
                "exchange": entry.get("target_exchange", ""),
                "quantity": entry.get("quantity", "0"),
                "status": "ACTIVE" if status.get("enabled") else "PAUSED",
                "totalOrders": 0,
                "filledOrders": 0,
                "totalFilledQty": "0",
                "avgFillPrice": None,
                "firstOrderTs": None,
                "lastOrderTs": None,
            })
        return result

    async def _fetch_performance(self, conn) -> dict[str, dict[str, Any]]:
        """PostgreSQL에서 strategy_name별 주문 실적을 집계."""
        try:
            rows = await conn.fetch(_STRATEGY_PERFORMANCE_SQL)
            result: dict[str, dict[str, Any]] = {}
            for row in rows:
                d = dict(row)
                name = d.pop("strategy_name", None)
                if name:
                    result[name] = d
            return result
        except Exception as e:
            logger.warning("전략 성과 쿼리 실패: %s", e)
            return {}

    # ── 서버 시작 시 PG → Redis 동기화 ──

    async def sync_pg_to_redis(self) -> None:
        """
        서버 시작 시 PostgreSQL의 strategies.enabled 상태를
        Redis에 동기화하여 signal_pipeline이 올바른 상태를 참조하도록 보장.
        """
        if not self._postgres or not self._strategy_pg_repo:
            return
        try:
            async with self._postgres.pool.acquire() as conn:
                rows = await self._strategy_pg_repo.list_all(conn)
                for row in rows:
                    await self._repo.set_enabled(row["strategy_name"], row["enabled"])
                logger.info("PG → Redis 전략 상태 동기화 완료 (%d건)", len(rows))
        except Exception as e:
            logger.warning("PG → Redis 전략 상태 동기화 실패: %s", e)

    # ── 전략별 리스크 설정 조회 및 수정 ──

    async def get_risk_config(self, strategy_name: str) -> dict[str, Any] | None:
        """PostgreSQL에서 해당 전략의 리스크 설정을 조회."""
        if not self._postgres or not self._strategy_risk_config_pg_repo:
            return None
        try:
            async with self._postgres.pool.acquire() as conn:
                row = await self._strategy_risk_config_pg_repo.get_by_strategy(conn, strategy_name)
                if row:
                    # JSON 직렬화를 위해 Decimal/타임스탬프 등 정제해서 반환
                    return {
                        "strategy_name": row["strategy_name"],
                        "account_equity": float(row["account_equity"]),
                        "risk_per_trade": float(row["risk_per_trade"]),
                        "max_leverage": float(row["max_leverage"]),
                        "max_position_notional": float(row["max_position_notional"]),
                        "min_notional": float(row["min_notional"]),
                        "min_stop_bps": float(row["min_stop_bps"]),
                        "min_reward_risk": float(row["min_reward_risk"]),
                        "quantity_step": float(row["quantity_step"]),
                        "fee_bps": float(row["fee_bps"]),
                        "slippage_bps": float(row["slippage_bps"]),
                        "spread_bps": float(row["spread_bps"]),
                        "created_ts": row["created_ts"],
                        "updated_ts": row["updated_ts"],
                    }
        except Exception as e:
            logger.warning("리스크 설정 조회 실패: %s", e)
        return None

    async def update_risk_config(self, strategy_name: str, config_data: dict[str, Any]) -> dict[str, Any]:
        """PostgreSQL에서 해당 전략의 리스크 설정을 업데이트."""
        if not self._postgres or not self._strategy_risk_config_pg_repo:
            raise RuntimeError("PostgreSQL 또는 리스크 레포가 준비되지 않았습니다.")
        async with self._postgres.pool.acquire() as conn:
            row = await self._strategy_risk_config_pg_repo.upsert(conn, strategy_name, config_data)
            return {
                "strategy_name": row["strategy_name"],
                "account_equity": float(row["account_equity"]),
                "risk_per_trade": float(row["risk_per_trade"]),
                "max_leverage": float(row["max_leverage"]),
                "max_position_notional": float(row["max_position_notional"]),
                "min_notional": float(row["min_notional"]),
                "min_stop_bps": float(row["min_stop_bps"]),
                "min_reward_risk": float(row["min_reward_risk"]),
                "quantity_step": float(row["quantity_step"]),
                "fee_bps": float(row["fee_bps"]),
                "slippage_bps": float(row["slippage_bps"]),
                "spread_bps": float(row["spread_bps"]),
                "created_ts": row["created_ts"],
                "updated_ts": row["updated_ts"],
            }
