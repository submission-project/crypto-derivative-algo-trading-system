from schemas.market import Exchange, MarketType

from execution_gateway.exchange.types import ExchangeLeverageResult

from execution_gateway.gateway.context import GatewayContext


class GatewayAccountService:
    def __init__(self, ctx: GatewayContext) -> None:
        self.ctx = ctx

    async def change_leverage(
        self,
        *,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
        leverage: int,
    ) -> ExchangeLeverageResult:
        # version 0.1
        # """
        # 특정 symbol의 initial leverage 변경.

        # Perp/Futures에서 leverage는 주문별 값이 아니라
        # symbol/account 설정값이다.
        # """
        # if leverage < 1 or leverage > 125:
        #     raise ValueError(f"leverage must be between 1 and 125: {leverage}")

        # await self.rate_limiter.acquire_request_weight(weight=1)

        # resp = await self.adapter.change_leverage(
        #     symbol=symbol,
        #     leverage=leverage,
        # )

        # logger.info(
        #     f"레버리지 변경 완료: "
        #     f"symbol={symbol.upper()}, leverage={leverage}, resp={resp}"
        # )

        # version 0.2
        client = self.ctx.client_for_market(exchange=exchange, market_type=market_type)
        return await client.change_leverage(symbol=symbol, leverage=leverage)