from __future__ import annotations

from abc import ABC, abstractmethod

from execution_gateway.listeners.user_data_stream import UserDataStreamListener
from schemas.market import Exchange, MarketType


class UserDataStreamListenerFactory(ABC):
    @property
    @abstractmethod
    def exchange(self) -> Exchange:
        pass

    @property
    @abstractmethod
    def market_type(self) -> MarketType:
        pass

    @abstractmethod
    def create(self) -> UserDataStreamListener:
        pass