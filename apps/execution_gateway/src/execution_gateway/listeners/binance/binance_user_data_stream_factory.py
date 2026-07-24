from __future__ import annotations

from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
from execution_gateway.listeners.binance.binance_user_data_stream import BinanceUserDataStreamListener
from execution_gateway.listeners.user_data_stream import UserDataStreamListener
from execution_gateway.listeners.user_data_stream_factory import (
    UserDataStreamListenerFactory,
)
from schemas.market import Exchange, MarketType


class BinanceUserDataStreamListenerFactory(UserDataStreamListenerFactory):
    exchange = Exchange.BINANCE
    market_type = MarketType.PERP

    def __init__(
        self,
        *,
        rest_adapter: BinanceRestAdapter,
        ws_base_url: str,
    ) -> None:
        self._rest_adapter = rest_adapter
        self._ws_base_url = ws_base_url

    def create(self) -> UserDataStreamListener:
        return BinanceUserDataStreamListener(
            rest_adapter=self._rest_adapter,
            ws_base_url=self._ws_base_url,
        )
