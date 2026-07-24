from dataclasses import dataclass
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.listeners.user_data_stream_registry import UserDataStreamListenerRegistry
from execution_gateway.listeners.binance.binance_user_data_stream_factory import (
    BinanceUserDataStreamListenerFactory,
)
from schemas.market import Exchange, MarketType

from execution_gateway.listeners.user_data_stream_factory import (
    UserDataStreamListenerFactory,
)

from execution_gateway.exchange import ExchangeExecutionClient, AsyncClosable

from api_server.helper import create_binance_adapter, get_user_data_ws_base_url

# [claim] 현재는 binance를 직접 넣는 방식을 했는 데, 좀 더 팩토리 방식으로 수정 필요
async def build_exchange_runtime(
    markets: list[tuple[Exchange, MarketType]],
) -> tuple[
    ExchangeExecutionClientRegistry,
    UserDataStreamListenerRegistry,
    list[AsyncClosable],
]:
    clients = ExchangeExecutionClientRegistry()
    listeners = UserDataStreamListenerRegistry()
    closables: list[AsyncClosable] = []
    registered: set[tuple[Exchange, MarketType]] = set()

    for exchange, market_type in markets:
        if (exchange, market_type) == (Exchange.BINANCE, MarketType.PERP):
            runtime = build_binance_perp_runtime()
        elif (exchange, market_type) == (Exchange.OKX, MarketType.PERP):
            runtime = build_okx_perp_runtime()
        elif (exchange, market_type) == (Exchange.BITGET, MarketType.PERP):
            runtime = build_bitget_perp_runtime()
        else:
            raise RuntimeError(
                f"unsupported exchange runtime: {exchange.value}/{market_type.value}"
            )

        clients.register(runtime.client)
        listeners.register(runtime.listener_factory)
        closables.extend(runtime.closables)
        registered.add((exchange, market_type))

    return clients, listeners, closables

@dataclass(slots=True)
class ExchangeRuntime:
    client: ExchangeExecutionClient
    listener_factory: UserDataStreamListenerFactory
    closables: list[AsyncClosable]


def build_binance_perp_runtime() -> ExchangeRuntime:
    adapter = create_binance_adapter()

    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=BinanceOrderRouter(adapter),
    )

    listener_factory = BinanceUserDataStreamListenerFactory(
        rest_adapter=adapter,
        ws_base_url=get_user_data_ws_base_url(),
    )

    return ExchangeRuntime(
        client=client,
        listener_factory=listener_factory,
        closables=[adapter],
    )


def build_okx_perp_runtime() -> ExchangeRuntime:
    raise NotImplementedError("close()는 아직 구현되지 않았습니다.")

def build_bitget_perp_runtime() -> ExchangeRuntime:
    raise NotImplementedError("close()는 아직 구현되지 않았습니다.")