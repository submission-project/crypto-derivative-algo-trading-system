from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from schemas.market import Exchange, MarketType
from schemas.order import Order

from .capabilities import ExchangeCapabilities
from .types import (
    ExchangeBatchOrderResult,
    ExchangeCancelResult,
    ExchangeConditionalAck,
    ExchangeConditionalSnapshot,
    ExchangeLeverageResult,
    ExchangeOrderAck,
    ExchangeOrderSnapshot,
    ExchangePositionSnapshot,
)

from schemas.order import ConditionalStatus, OrderStatus

# [claim] 현재 ExchangeCancelResult 를 쓰고 있는 메소드는 (cancel_order, cancel_regular_order_by_client_id, cancel_batch_orders, cancel_all_regular_open_orders, cancel_conditional_order_by_id, cancel_all_conditional_open_orders, cancel_conditional_order)
# 인데, 단건 주문 처리는 크게 문제가 되지 않을 거 같지만, batch, all 주문 취소 메소드 경우, 응답을 list로 못받는 거래소가 있다면, 변경이 필요할 수 있다고 생각함. 
# but 현재 지금 바뀌지 않더라도 크게 동작하는 데에는 문제가 발생하지 않는 다 생각하여, 이후 시간이 될 떄 바뀌도록 해야됨 그리고 관련 테스트 로직도 같이 바뀌야 됨

class ExchangeExecutionClient(ABC):
    """
    Exchange-neutral execution client contract.

    ExecutionGateway should eventually depend on this protocol instead of
    concrete exchange adapters such as BinanceRestAdapter.
    """

    @property
    @abstractmethod
    def exchange(self) -> Exchange:
        pass

    @property
    @abstractmethod
    def market_type(self) -> MarketType:
        pass

    @property
    @abstractmethod
    def capabilities(self) -> ExchangeCapabilities:
        pass

    @abstractmethod
    async def place_order(self, order: Order) -> ExchangeOrderAck:
        pass

    @abstractmethod
    async def place_batch_orders(
        self,
        orders: list[Order],
    ) -> list[ExchangeBatchOrderResult]:
        pass

    @abstractmethod
    async def cancel_order(self, order: Order) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def cancel_regular_order_by_client_id(
        self,
        *,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def cancel_batch_orders(
        self,
        orders: list[Order],
    ) -> list[ExchangeCancelResult]:
        pass

    @abstractmethod
    async def cancel_all_regular_open_orders(
        self,
        *,
        symbol: str,
    ) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def cancel_conditional_order_by_id(
        self,
        *,
        symbol: str,
        client_conditional_id: str | None = None,
        exchange_conditional_id: str | None = None,
    ) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def cancel_all_conditional_open_orders(
        self,
        *,
        symbol: str,
    ) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def get_order(self, order: Order) -> ExchangeOrderSnapshot:
        pass

    @abstractmethod
    async def get_open_orders(
        self,
        *,
        symbol: str | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        pass

    @abstractmethod
    async def place_conditional_order(
        self,
        order: Order,
    ) -> ExchangeConditionalAck:
        pass

    @abstractmethod
    async def cancel_conditional_order(
        self,
        order: Order,
    ) -> ExchangeCancelResult:
        pass

    @abstractmethod
    async def get_conditional_order(
        self,
        order: Order,
    ) -> ExchangeConditionalSnapshot | None:
        pass

    @abstractmethod
    async def get_open_conditional_orders(
        self,
        symbol: str,
    ) -> list[ExchangeConditionalSnapshot]:
        pass

    @abstractmethod
    async def change_leverage(
        self,
        *,
        symbol: str,
        leverage: int,
    ) -> ExchangeLeverageResult:
        pass

    @abstractmethod
    async def get_positions(
        self,
        *,
        symbol: str | None = None,
    ) -> list[ExchangePositionSnapshot]:
        pass

    @abstractmethod
    async def get_symbol_price_ticker(self, symbol: str) -> dict[str, Any]:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    async def find_order_snapshots(
        self,
        *,
        symbol: str,
        orders: list[Order],
        lookback_ms: int = 60_000,
        limit: int = 1000,
    ) -> dict[str, ExchangeOrderSnapshot]:
        pass

    @abstractmethod
    def get_mapper_internal_conditional_order_status(
        self,
        exchange_conditional_status: str,
    ) -> ConditionalStatus | None:
        pass

    @abstractmethod
    def get_mapper_internal_order_status(
        self,
        exchange_order_status: str ,
    ) -> OrderStatus | None:
        pass


    @abstractmethod
    def get_mapper_exchange_conditional_order_status(
        self,
        internal_conditional_status: ConditionalStatus,
    ) -> str | None:
        pass

    @abstractmethod
    def get_mapper_exchange_order_status(
        self,
        internal_order_status: OrderStatus,
    ) -> str | None:
        pass

    @abstractmethod
    def get_exchange_conditional_order_unknown_status_value(
        self,
    ) -> str | None:
        pass
