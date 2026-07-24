from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass(frozen=True, slots=True)
class CostModel:
    taker_fee_bps: float = 4.0
    maker_fee_bps: float = 2.0
    slippage_bps: float = 1.0
    market_impact_coeff: float = 0.0

    # Square-root market impact parameters (Almgren-Chriss style)
    sqrt_impact_coeff: float = 0.0  # σ * γ coefficient
    daily_volume_usd: float = 1e9  # average daily volume for participation rate

    def estimate_bps(
        self,
        *,
        order_type: str = "market",
        participation_rate: float = 0.0,
        notional: float = 0.0,
    ) -> float:
        if order_type not in {"market", "limit"}:
            raise ValueError("order_type must be 'market' or 'limit'")
        fee = self.taker_fee_bps if order_type == "market" else self.maker_fee_bps
        impact = self.market_impact_coeff * max(0.0, participation_rate)

        # Square-root market impact: cost ∝ σ * sqrt(Q / V)
        sqrt_impact = 0.0
        if self.sqrt_impact_coeff > 0 and self.daily_volume_usd > 0 and notional > 0:
            sqrt_impact = self.sqrt_impact_coeff * sqrt(notional / self.daily_volume_usd) * 10_000.0

        # Dynamic slippage: increases with participation rate
        dynamic_slip = self.slippage_bps * (1.0 + participation_rate)

        return fee + dynamic_slip + impact + sqrt_impact

    def cost_amount(
        self,
        notional: float,
        *,
        order_type: str = "market",
        participation_rate: float = 0.0,
    ) -> float:
        return abs(notional) * self.estimate_bps(
            order_type=order_type,
            participation_rate=participation_rate,
            notional=abs(notional),
        ) / 10_000.0
