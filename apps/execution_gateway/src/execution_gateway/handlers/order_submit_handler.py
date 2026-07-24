from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from schemas.order import Order, OrderSource

from execution_gateway.handlers.dedup_handler import OrderIntentDedupHandler
from execution_gateway.handlers.risk_handler import PreTradeRiskHandler, RiskDecision


from common.logging import setup_logger

logger = setup_logger(__name__)


class GatewaySubmitter(Protocol):
    async def submit_order(
        self,
        req,
        source: OrderSource = OrderSource.MANUAL,
        signal_id: str | None = None,
        strategy_name: str | None = None,
    ) -> Order:
        ...


@dataclass(frozen=True, slots=True)
class OrderIntentProcessResult:
    accepted: bool
    order: Order | None = None
    stage: str | None = None
    reason: str | None = None
    detail: str | None = None
    risk_metadata: dict[str, str] | None = None
    dedup_key: str | None = None


class OrderSubmitHandler:
    def __init__(
        self,
        *,
        gateway: GatewaySubmitter,
        risk_handler: PreTradeRiskHandler,
        dedup_handler: OrderIntentDedupHandler | None = None,
        postgres: Any | None = None,
        strategy_risk_config_repo: Any | None = None,
    ) -> None:
        self.gateway = gateway
        self.risk_handler = risk_handler
        self.dedup_handler = dedup_handler
        self.postgres = postgres
        self.strategy_risk_config_repo = strategy_risk_config_repo

    async def process(self, intent: dict) -> OrderIntentProcessResult:
        dedup_key: str | None = None
        if self.dedup_handler is not None:
            dedup_decision = await self.dedup_handler.evaluate(intent)
            dedup_key = dedup_decision.key
            if not dedup_decision.accepted:
                return OrderIntentProcessResult(
                    accepted=False,
                    stage="dedup",
                    reason="DUPLICATE",
                    detail=f"duplicate order intent key={dedup_decision.key}",
                    dedup_key=dedup_decision.key,
                )

        strategy_name = intent.get("strategy_name")
        risk_config = None
        if self.postgres and self.strategy_risk_config_repo and strategy_name:
            try:
                async with self.postgres.pool.acquire() as conn:
                    row = await self.strategy_risk_config_repo.get_by_strategy(conn, strategy_name)
                    if row:
                        from decimal import Decimal
                        from execution_gateway.handlers.risk_handler import RiskConfig
                        risk_keys = {
                            "account_equity", "risk_per_trade", "max_leverage",
                            "max_position_notional", "min_notional", "min_stop_bps",
                            "min_reward_risk", "quantity_step", "fee_bps",
                            "slippage_bps", "spread_bps"
                        }
                        filtered = {k: Decimal(str(v)) for k, v in row.items() if k in risk_keys}
                        risk_config = RiskConfig(**filtered)
            except Exception as exc:
                logger.warning("Failed to fetch Postgres strategy risk config: %s", exc)

        risk_decision: RiskDecision = self.risk_handler.evaluate(intent, config=risk_config)
        if not risk_decision.accepted or risk_decision.order_request is None:
            return OrderIntentProcessResult(
                accepted=False,
                stage="risk",
                reason=risk_decision.reason.value if risk_decision.reason else "RISK_REJECTED",
                detail=risk_decision.detail,
                risk_metadata=risk_decision.metadata,
                dedup_key=dedup_key,
            )

        order = await self.gateway.submit_order(
            risk_decision.order_request,
            source=OrderSource.STRATEGY,
            signal_id=intent.get("signal_id"),
            strategy_name=intent.get("strategy_name"),
        )
        return OrderIntentProcessResult(
            accepted=True,
            order=order,
            stage="submitted",
            risk_metadata=risk_decision.metadata,
            dedup_key=dedup_key,
        )
