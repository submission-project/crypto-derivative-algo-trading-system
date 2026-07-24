from typing import Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api_server.services.strategy_control_service import StrategyControlService

router = APIRouter(prefix="/api/strategies", tags=["Strategies"])


def get_strategy_control_service() -> StrategyControlService:
    raise NotImplementedError()


class StrategyToggleRequest(BaseModel):
    enabled: bool


class StrategyRiskConfigRequest(BaseModel):
    account_equity: float | None = None
    risk_per_trade: float | None = None
    max_leverage: float | None = None
    max_position_notional: float | None = None
    min_notional: float | None = None
    min_stop_bps: float | None = None
    min_reward_risk: float | None = None
    quantity_step: float | None = None
    fee_bps: float | None = None
    slippage_bps: float | None = None
    spread_bps: float | None = None


@router.get("")
async def list_strategies(
    service: StrategyControlService = Depends(get_strategy_control_service),
) -> list[dict[str, Any]]:
    """서버에 탑재된 전략 목록 + Redis 활성화 상태 + PostgreSQL 실적 지표 반환"""
    return await service.list_strategies()


@router.get("/{strategy_name}/status")
async def get_strategy_status(
    strategy_name: str,
    service: StrategyControlService = Depends(get_strategy_control_service),
) -> dict[str, object]:
    return await service.get_status(strategy_name)


@router.put("/{strategy_name}/status")
async def set_strategy_status(
    strategy_name: str,
    req: StrategyToggleRequest,
    service: StrategyControlService = Depends(get_strategy_control_service),
) -> dict[str, object]:
    return await service.set_enabled(strategy_name, req.enabled)


@router.get("/{strategy_name}/risk-config")
async def get_strategy_risk_config(
    strategy_name: str,
    service: StrategyControlService = Depends(get_strategy_control_service),
) -> dict[str, object]:
    config = await service.get_risk_config(strategy_name)
    if config is None:
        raise HTTPException(status_code=404, detail="Strategy risk config not found")
    return config


@router.put("/{strategy_name}/risk-config")
async def update_strategy_risk_config(
    strategy_name: str,
    req: StrategyRiskConfigRequest,
    service: StrategyControlService = Depends(get_strategy_control_service),
) -> dict[str, object]:
    # Exclude unset fields so we only pass specified parameters
    config_dict = req.model_dump(exclude_unset=True)
    return await service.update_risk_config(strategy_name, config_dict)
