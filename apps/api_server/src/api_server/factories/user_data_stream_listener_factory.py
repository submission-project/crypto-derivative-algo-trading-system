from __future__ import annotations

from execution_gateway.listeners.user_data_stream import UserDataStreamListener
from execution_gateway.listeners.user_data_stream_registry import (
    UserDataStreamListenerRegistry,
)
from schemas.market import Exchange, MarketType


def create_user_data_stream_listeners(
    *,
    markets: list[tuple[Exchange, MarketType]],
    registry: UserDataStreamListenerRegistry,
) -> dict[tuple[Exchange, MarketType], UserDataStreamListener]:
    listeners: dict[tuple[Exchange, MarketType], UserDataStreamListener] = {}

    for exchange, market_type in markets:
        listeners[(exchange, market_type)] = registry.create(
            exchange=exchange,
            market_type=market_type,
        )

    return listeners