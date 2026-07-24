from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from enum import Enum

from schemas.order import ConditionalStatus, OrderStatus

class CancelSkipReason(str, Enum):
    LOCAL_ORDER_NOT_FOUND = "local_order_not_found"
    ALREADY_CANCELLED = "already_cancelled"
    ALREADY_FILLED = "already_filled"
    ALREADY_REJECTED = "already_rejected"
    ALREADY_EXPIRED = "already_expired"
    CONDITIONAL_ORDER_NOT_CANCELABLE = "conditional_order_not_cancelable"

class BatchCancelResultStatus(str, Enum):
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    FAILED = "failed"
    UNKNOWN = "unknown"
    
@dataclass(frozen=True, slots=True)
class CancelBatchOrderResp:
    """Normalized order cancel response."""

    order_id: str
    result: BatchCancelResultStatus
    reason: CancelSkipReason | None = None
    status: OrderStatus | None = None
    conditional_status: ConditionalStatus | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    code: int | str | None = None
    message: str | None = None
    raw: dict[str, Any] | None = None

@dataclass(frozen=True)
class CancelOrderSkipped(Exception):
    order_id: str
    reason: CancelSkipReason
    status: OrderStatus | None = None
    conditional_status: ConditionalStatus | None = None
    triggered_order_id: str | None = None
    message: str | None = None

    def __post_init__(self) -> None:
        Exception.__init__(
            self,
            self.message
            or f"cancel order skipped: order_id={self.order_id}, reason={self.reason.value}",
        )

    @property
    def http_status(self) -> int:
        match self.reason:
            case CancelSkipReason.LOCAL_ORDER_NOT_FOUND:
                return 404

            case CancelSkipReason.ALREADY_CANCELLED:
                # 멱등 성공으로 보고 싶으면 이건 예외로 던지지 않고 result로 반환하는 쪽이 더 좋음.
                return 200

            case (
                CancelSkipReason.ALREADY_FILLED
                | CancelSkipReason.ALREADY_REJECTED
                | CancelSkipReason.ALREADY_EXPIRED
                | CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE
            ):
                return 409

        return 409

    def to_payload(self) -> dict:
        return {
            "skipped": True,
            "reason": self.reason.value,
            "order_id": self.order_id,
            "status": self.status.value if self.status else None,
            "conditional_status": (
                self.conditional_status.value if self.conditional_status else None
            ),
            "triggered_order_id": self.triggered_order_id,
            "message": self.message,
        }