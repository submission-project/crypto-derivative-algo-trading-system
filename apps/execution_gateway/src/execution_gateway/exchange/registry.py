# execution_gateway/exchange/registry.py

from __future__ import annotations

from dataclasses import dataclass

from schemas.market import Exchange, MarketType

from execution_gateway.exchange.client import ExchangeExecutionClient


@dataclass(frozen=True, slots=True)
class ExchangeClientKey:
    exchange: Exchange
    market_type: MarketType


class ExchangeExecutionClientRegistry:
    def __init__(self) -> None:
        self._clients: dict[ExchangeClientKey, ExchangeExecutionClient] = {}

    def register(self, client: ExchangeExecutionClient) -> None:
        key = ExchangeClientKey(
            exchange=client.exchange,
            market_type=client.market_type,
        )

        if key in self._clients:
            raise ValueError(
                f"duplicate exchange client: "
                f"{client.exchange.value}/{client.market_type.value}"
            )

        self._clients[key] = client

    def get(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> ExchangeExecutionClient:
        key = ExchangeClientKey(exchange=exchange, market_type=market_type)

        try:
            return self._clients[key]
        except KeyError as e:
            raise ValueError(
                f"unsupported exchange client: "
                f"{exchange.value}/{market_type.value}"
            ) from e

    async def close_all(self) -> None:
        for client in self._clients.values():
            await client.close()