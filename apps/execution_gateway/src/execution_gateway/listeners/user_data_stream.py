from typing import Awaitable, Callable

from abc import ABC, abstractmethod

from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.position_update_event import NormalizedPositionSnapshot
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.market import Exchange, MarketType


class UserDataStreamListener(ABC):
    @property
    @abstractmethod
    def exchange(self) -> Exchange:
        pass

    @property
    @abstractmethod
    def market_type(self) -> MarketType:
        pass

    @abstractmethod
    def on_order_update(
        self,
        callback: Callable[[NormalizedOrderUpdateEvent], Awaitable[None]],
    ) -> Callable[[NormalizedOrderUpdateEvent], Awaitable[None]]:
        pass

    # def on_account_update(
    #     self,
    #     callback: Callable[[AccountUpdateEnvelope], Awaitable[None]],
    # ) -> Callable[[AccountUpdateEnvelope], Awaitable[None]]:
    #     ...

    @abstractmethod
    def on_algo_update(
        self,
        callback: Callable[[NormalizedConditionalOrderEvent], Awaitable[None]],
    ) -> None:
        pass

    @abstractmethod
    def on_position_update(
        self,
        callback: Callable[[list[NormalizedPositionSnapshot]], Awaitable[None]],
    ) -> Callable[[list[NormalizedPositionSnapshot]], Awaitable[None]]:
        pass

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass
        ...
