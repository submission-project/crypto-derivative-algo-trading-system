from __future__ import annotations

from enum import Enum
from typing import Any

from schemas.market import Exchange


class ExchangeErrorCategory(str, Enum):
    RATE_LIMITED = "RATE_LIMITED"
    IP_BANNED = "IP_BANNED"
    WAF_BLOCKED = "WAF_BLOCKED"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    INVALID_PARAMETER = "INVALID_PARAMETER"
    UNKNOWN_EXECUTION = "UNKNOWN_EXECUTION"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    INTERNAL_RETRYABLE = "INTERNAL_RETRYABLE"
    SYSTEM_THROTTLE = "SYSTEM_THROTTLE"
    NETWORK = "NETWORK"
    EXCHANGE_REJECTED = "EXCHANGE_REJECTED"
    ORDER_NOT_FOUND = "ORDER_NOT_FOUND"


class ExchangeApiError(Exception):
    """
    Exchange-neutral API error.

    Adapters should catch their native errors, map them to this shape, and keep
    native details in ``raw`` so the core gateway can make decisions without
    importing exchange-specific exception classes.
    """

    def __init__(
        self,
        *,
        exchange: Exchange,
        category: ExchangeErrorCategory,
        message: str,
        code: int | str | None = None,
        status_code: int | None = None,
        raw: dict[str, Any] | None = None,
    ) -> None:
        self.exchange = exchange
        self.category = category
        self.message = message
        self.code = code
        self.status_code = status_code
        self.raw = raw or {}

        super().__init__(
            f"ExchangeApiError("
            f"exchange={exchange.value}, "
            f"category={category.value}, "
            f"code={code}, "
            f"status_code={status_code}, "
            f"message={message})"
        )
