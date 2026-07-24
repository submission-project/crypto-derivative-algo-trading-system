from fastapi import APIRouter, Depends
from pydantic import BaseModel

from schemas.order import OrderRequest, Order
from schemas.signal import Signal
from api_server.services.signal.signal_service import SignalService

router = APIRouter(prefix="/api/signals", tags=["Signals"])


def get_signal_service() -> SignalService:
    raise NotImplementedError()


class ApproveSignalRequest(BaseModel):
    # 사용자가 승인 시 파라미터를 수정해서 보낼 수 있음
    order_request: OrderRequest


@router.get("/pending", response_model=list[Signal])
async def get_pending_signals(service: SignalService = Depends(get_signal_service)):
    """승인 대기 중인 시그널 목록 조회"""
    return await service.get_pending_signals()


@router.post("/{signal_id}/approve", response_model=Order)
async def approve_signal(
    signal_id: str,
    req: ApproveSignalRequest,
    service: SignalService = Depends(get_signal_service),
):
    """시그널 승인 및 주문 전송"""
    return await service.approve_signal(signal_id, req.order_request)


@router.post("/{signal_id}/dismiss")
async def dismiss_signal(
    signal_id: str, service: SignalService = Depends(get_signal_service)
):
    """시그널 거부 (무시)"""
    return await service.dismiss_signal(signal_id)
