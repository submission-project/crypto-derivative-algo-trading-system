from schemas.market import Exchange, MarketType

from execution_gateway.exchange.client import ExchangeExecutionClient

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry

import asyncio
from collections import defaultdict
from typing import Optional


class OrderLockManager:
    """
    order_id 단위 async lock.

    같은 주문에 대해 submit/cancel/verify/user-data-update가 동시에
    상태를 덮어쓰는 문제를 줄이기 위한 최소 동시성 보호 장치.
    """

    def __init__(self) -> None:
        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def lock(self, order_id: str) -> asyncio.Lock:
        return self.locks[order_id]


class GatewayContext:
    def __init__(
        self,
        *,
        exchange_clients: ExchangeExecutionClientRegistry,
    ) -> None:
        self.exchange_clients = exchange_clients
        self.locks = OrderLockManager()

    def client_for_market(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
    ) -> ExchangeExecutionClient:
        return self.exchange_clients.get(
            exchange=exchange,
            market_type=market_type,
        )
