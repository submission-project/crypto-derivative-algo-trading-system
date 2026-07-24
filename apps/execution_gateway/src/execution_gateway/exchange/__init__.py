"""
Exchange execution abstractions.

This package defines the exchange-neutral contract used by ExecutionGateway.
Concrete adapters such as Binance, OKX, and Bitget should translate their
native REST/WS payloads into these shared types before returning to the core
gateway.
"""

from abc import ABC, abstractmethod

from .capabilities import ExchangeCapabilities
from .client import ExchangeExecutionClient
from .errors import ExchangeApiError, ExchangeErrorCategory
from .types import (
    ExchangeBatchOrderResult,
    ExchangeCancelResult,
    ExchangeConditionalAck,
    ExchangeConditionalSnapshot,
    ExchangeLeverageResult,
    ExchangeOrderAck,
    ExchangeOrderReject,
    ExchangeOrderSnapshot,
    ExchangePositionSnapshot,
)

class AsyncClosable(ABC):
    @abstractmethod
    async def close(self) -> None:
        pass

__all__ = [
    "AsyncClosable",
    "ExchangeApiError",
    "ExchangeBatchOrderResult",
    "ExchangeCancelResult",
    "ExchangeCapabilities",
    "ExchangeConditionalAck",
    "ExchangeConditionalSnapshot",
    "ExchangeErrorCategory",
    "ExchangeExecutionClient",
    "ExchangeLeverageResult",
    "ExchangeOrderAck",
    "ExchangeOrderReject",
    "ExchangeOrderSnapshot",
    "ExchangePositionSnapshot",
]
