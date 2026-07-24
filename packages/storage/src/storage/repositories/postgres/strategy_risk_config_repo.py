"""
StrategyRiskConfigPostgresRepository — 전략별 리스크 설정 매개변수 영속 관리.
"""
from __future__ import annotations

import time
from typing import Any

from common.logging import setup_logger

logger = setup_logger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class StrategyRiskConfigPostgresRepository:
    TABLE = "strategy_risk_configs"

    async def get_by_strategy(self, conn, strategy_name: str) -> dict[str, Any] | None:
        """전략명으로 리스크 설정 단건 조회."""
        row = await conn.fetchrow(
            f"SELECT * FROM {self.TABLE} WHERE strategy_name = $1",
            strategy_name,
        )
        return dict(row) if row else None

    async def upsert(
        self,
        conn,
        strategy_name: str,
        config_data: dict[str, Any],
    ) -> dict[str, Any]:
        """전략별 리스크 설정 upsert."""
        now = _now_ms()
        
        # 1) 기존 설정 조회
        existing = await self.get_by_strategy(conn, strategy_name)

        # 2) 우선순위: config_data > existing DB record. 둘 다 없으면 ValueError 발생.
        def get_val(key: str) -> Any:
            if key in config_data:
                return config_data[key]
            if existing and key in existing:
                return existing[key]
            raise ValueError(f"Missing required risk config parameter: {key}")

        account_equity = get_val("account_equity")
        risk_per_trade = get_val("risk_per_trade")
        max_leverage = get_val("max_leverage")
        max_position_notional = get_val("max_position_notional")
        min_notional = get_val("min_notional")
        min_stop_bps = get_val("min_stop_bps")
        min_reward_risk = get_val("min_reward_risk")
        quantity_step = get_val("quantity_step")
        fee_bps = get_val("fee_bps")
        slippage_bps = get_val("slippage_bps")
        spread_bps = get_val("spread_bps")

        row = await conn.fetchrow(
            f"""
            INSERT INTO {self.TABLE} (
                strategy_name, account_equity, risk_per_trade, max_leverage,
                max_position_notional, min_notional, min_stop_bps, min_reward_risk,
                quantity_step, fee_bps, slippage_bps, spread_bps,
                created_ts, updated_ts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)
            ON CONFLICT (strategy_name)
            DO UPDATE SET
                account_equity          = EXCLUDED.account_equity,
                risk_per_trade          = EXCLUDED.risk_per_trade,
                max_leverage            = EXCLUDED.max_leverage,
                max_position_notional   = EXCLUDED.max_position_notional,
                min_notional            = EXCLUDED.min_notional,
                min_stop_bps            = EXCLUDED.min_stop_bps,
                min_reward_risk         = EXCLUDED.min_reward_risk,
                quantity_step           = EXCLUDED.quantity_step,
                fee_bps                 = EXCLUDED.fee_bps,
                slippage_bps            = EXCLUDED.slippage_bps,
                spread_bps              = EXCLUDED.spread_bps,
                updated_ts              = EXCLUDED.updated_ts
            RETURNING *
            """,
            strategy_name,
            account_equity,
            risk_per_trade,
            max_leverage,
            max_position_notional,
            min_notional,
            min_stop_bps,
            min_reward_risk,
            quantity_step,
            fee_bps,
            slippage_bps,
            spread_bps,
            now,
        )
        return dict(row) if row else {}
