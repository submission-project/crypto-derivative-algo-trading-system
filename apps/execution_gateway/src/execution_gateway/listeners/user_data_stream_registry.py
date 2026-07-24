from __future__ import annotations

from execution_gateway.listeners.user_data_stream import UserDataStreamListener
from execution_gateway.listeners.user_data_stream_factory import (
    UserDataStreamListenerFactory,
)
from schemas.market import Exchange, MarketType


class UserDataStreamListenerRegistry:
    def __init__(self) -> None:
        self._factories: dict[
            tuple[Exchange, MarketType],
            UserDataStreamListenerFactory,
        ] = {}

    def register(self, factory: UserDataStreamListenerFactory) -> None:
        key = (factory.exchange, factory.market_type)
        self._factories[key] = factory

    def create(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> UserDataStreamListener:
        key = (exchange, market_type)
        factory = self._factories.get(key)

        if factory is None:
            raise RuntimeError(
                f"UserDataStreamListenerFactory not registered: "
                f"{exchange.value}/{market_type.value}"
            )

        return factory.create()