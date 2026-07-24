from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from enum import Enum
from typing import Any

from schemas.order import OrderRequest, OrderSide
from schemas.position import PositionSide


class RiskRejectReason(str, Enum):
    INVALID_ORDER_INTENT = "INVALID_ORDER_INTENT" # 주문 의도가 잘못됨
    MISSING_RISK_LEVELS = "MISSING_RISK_LEVELS" # 리스크 레벨이 설정되지 않음
    INVALID_PRICE_STRUCTURE = "INVALID_PRICE_STRUCTURE" # 가격 구조가 잘못됨
    STOP_TOO_TIGHT = "STOP_TOO_TIGHT" # 스탑 거리가 너무 짧음
    REWARD_RISK_TOO_LOW = "REWARD_RISK_TOO_LOW" # 리워드/리스크가 너무 낮음
    NOTIONAL_TOO_SMALL = "NOTIONAL_TOO_SMALL" # 노션이 너무 작음
    EXPOSURE_LIMIT_EXCEEDED = "EXPOSURE_LIMIT_EXCEEDED" # 익스포저 한도가 초과됨


@dataclass(frozen=True, slots=True)
class RiskConfig:
    account_equity: Decimal
    risk_per_trade: Decimal
    max_leverage: Decimal
    max_position_notional: Decimal
    min_notional: Decimal
    min_stop_bps: Decimal
    min_reward_risk: Decimal
    quantity_step: Decimal
    fee_bps: Decimal
    slippage_bps: Decimal
    spread_bps: Decimal


@dataclass(frozen=True, slots=True)
class RiskDecision:
    accepted: bool
    order_request: OrderRequest | None = None
    reason: RiskRejectReason | None = None
    detail: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


class PreTradeRiskHandler:
    """
    전략이 제안한 수량을 그대로 따르기보다는, 주문을 스탑 거리에서 주문 수량을 리스크 관리에 맞게 수정.

    Long은 SL < Entry < TP 일때, 거래 성립
    Short는 TP < Entry < SL 일때, 거래 성립

    수수료·슬리피지·스프레드를 반영하고,
    최대 손실이 계좌 위험 한도를 넘지 않는 경우에만 포지션을 생성
    """

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config

    def evaluate(self, intent: dict[str, Any], config: RiskConfig | None = None) -> RiskDecision:
        active_config = config or self.config
        if active_config is None:
            return self._reject(
                RiskRejectReason.MISSING_RISK_LEVELS,
                "Risk configuration is missing for this strategy.",
            )
        try:
            req = OrderRequest.model_validate(intent)
        except Exception as exc:
            return self._reject(RiskRejectReason.INVALID_ORDER_INTENT, str(exc))

        entry = self._decimal(intent.get("entry_price") or intent.get("price") or intent.get("suggested_entry_price"))
        stop = self._decimal(intent.get("stop_loss_price") or intent.get("stop_loss") or intent.get("suggested_stop_loss"))
        target = self._decimal(
            intent.get("take_profit_price") or intent.get("take_profit") or intent.get("suggested_take_profit")
        )
        if entry is None or stop is None or target is None:
            return self._reject(
                RiskRejectReason.MISSING_RISK_LEVELS,
                "entry_price, stop_loss_price, and take_profit_price are required",
            )
        if entry <= 0 or stop <= 0 or target <= 0:
            return self._reject(RiskRejectReason.INVALID_PRICE_STRUCTURE, "risk prices must be positive")

        side = self._position_side(req)
        if side == PositionSide.LONG and not (stop < entry < target):
            return self._reject(RiskRejectReason.INVALID_PRICE_STRUCTURE, "LONG requires SL < Entry < TP")
        if side == PositionSide.SHORT and not (target < entry < stop):
            return self._reject(RiskRejectReason.INVALID_PRICE_STRUCTURE, "SHORT requires TP < Entry < SL")

        stop_bps = abs(entry - stop) / entry * Decimal("10000")
        if stop_bps < active_config.min_stop_bps:
            return self._reject(
                RiskRejectReason.STOP_TOO_TIGHT,
                f"stop distance {stop_bps:.4f} bps is below minimum {active_config.min_stop_bps} bps",
            )

        reward_bps = abs(target - entry) / entry * Decimal("10000")
        reward_risk = reward_bps / stop_bps
        if reward_risk < active_config.min_reward_risk:
            return self._reject(
                RiskRejectReason.REWARD_RISK_TOO_LOW,
                f"reward/risk {reward_risk:.4f} is below minimum {active_config.min_reward_risk}",
            )

        risk_budget = active_config.account_equity * active_config.risk_per_trade
        stop_fraction = stop_bps / Decimal("10000")
        risk_sized_notional = risk_budget / stop_fraction
        leverage_cap = active_config.account_equity * active_config.max_leverage
        notional = min(risk_sized_notional, leverage_cap, active_config.max_position_notional)
        if notional < active_config.min_notional:
            return self._reject(
                RiskRejectReason.NOTIONAL_TOO_SMALL,
                f"notional {notional:.4f} is below minimum {active_config.min_notional}",
            )

        quantity = self._round_quantity_with_config(notional / entry, active_config)
        if quantity <= 0:
            return self._reject(RiskRejectReason.NOTIONAL_TOO_SMALL, "rounded quantity is zero")

        adjusted = req.model_copy(update={"quantity": self._decimal_str(quantity)})
        cost_bps = active_config.fee_bps + active_config.slippage_bps + active_config.spread_bps / Decimal("2")
        metadata = {
            "entry_price": self._decimal_str(entry),
            "stop_loss_price": self._decimal_str(stop),
            "take_profit_price": self._decimal_str(target),
            "stop_bps": self._decimal_str(stop_bps),
            "reward_bps": self._decimal_str(reward_bps),
            "reward_risk": self._decimal_str(reward_risk),
            "risk_budget": self._decimal_str(risk_budget),
            "notional": self._decimal_str(quantity * entry),
            "quantity": self._decimal_str(quantity),
            "cost_bps": self._decimal_str(cost_bps),
        }
        return RiskDecision(accepted=True, order_request=adjusted, metadata=metadata)

    @property
    def cost_bps(self) -> Decimal:
        return self.config.fee_bps + self.config.slippage_bps + self.config.spread_bps / Decimal("2")

    @staticmethod
    def _position_side(req: OrderRequest) -> PositionSide:
        if req.position_side in {PositionSide.LONG, PositionSide.SHORT}:
            return req.position_side
        return PositionSide.LONG if req.side == OrderSide.BUY else PositionSide.SHORT

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None

    def _round_quantity_with_config(self, quantity: Decimal, config: RiskConfig) -> Decimal:
        step = config.quantity_step
        if step <= 0:
            return quantity
        return (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step

    def _round_quantity(self, quantity: Decimal) -> Decimal:
        return self._round_quantity_with_config(quantity, self.config)

    @staticmethod
    def _decimal_str(value: Decimal) -> str:
        text = format(value.normalize(), "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    @staticmethod
    def _reject(reason: RiskRejectReason, detail: str) -> RiskDecision:
        return RiskDecision(accepted=False, reason=reason, detail=detail)
