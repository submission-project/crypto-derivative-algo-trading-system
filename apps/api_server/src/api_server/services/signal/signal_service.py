from __future__ import annotations

import time
from typing import Any
from fastapi import HTTPException

from execution_gateway.gateway import ExecutionGateway
from execution_gateway.exchange import ExchangeApiError
from schemas.order import OrderRequest, OrderSource, Order
from schemas.signal import Signal, SignalStatus
from storage.repositories.signal_repo import SignalRedisRepository
from api_server.helper import exchange_error_to_http

class SignalService:
    """시그널 관리 및 승인/거부 비즈니스 로직을 처리하는 서비스."""

    def __init__(self, repo: SignalRedisRepository, gateway: ExecutionGateway) -> None:
        self._repo = repo
        self._gateway = gateway

    async def get_pending_signals(self) -> list[Signal]:
        """승인 대기 중인 시그널 목록 조회"""
        signals = await self._repo.list_pending(limit=50)
        return [Signal.model_validate(s) for s in signals]

    async def approve_signal(self, signal_id: str, order_request: OrderRequest) -> Order:
        """시그널 승인 및 주문 전송"""

        sig_dict = await self._repo.get_pending(signal_id)
        if not sig_dict:
            raise HTTPException(status_code=404, detail="Signal not found")

        if sig_dict.get("status") != SignalStatus.PENDING.value:
            raise HTTPException(status_code=400, detail="Signal is not pending")

        # 주문 생성
        try:
            order = await self._gateway.submit_order(
                order_request,
                source=OrderSource.SIGNAL_APPROVED,
                signal_id=signal_id,
                strategy_name=sig_dict.get("strategy_name"),
            )
        except ExchangeApiError as e:
            raise exchange_error_to_http(e) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        # 시그널 상태 업데이트 (APPROVED)
        now = time.time_ns() // 1_000_000
        await self._repo.approve(
            signal_id,
            order_id=order.order_id,
            approved_ts=now,
        )

        return order

    async def dismiss_signal(self, signal_id: str) -> dict[str, str]:
        """시그널 거부 (무시)"""
        sig_dict = await self._repo.get_pending(signal_id)
        if not sig_dict:
            raise HTTPException(status_code=404, detail="Signal not found")

        await self._repo.dismiss(signal_id)
        return {"status": "success"}

