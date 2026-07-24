from __future__ import annotations

from typing import Any

from common.logging import setup_logger
from execution_gateway.adapters.binance.binance_rest_adapter import BinanceRestAdapter
from schemas.order import Order, OrderRoute, OrderType
from schemas.position import PositionSide

from execution_gateway.adapters.binance.dto.resp.OrderResponseDto import OrderRespDto
from execution_gateway.adapters.binance.dto.resp.AlgoOrderResponseDto import AlgoOrderRespDto

from .mapper.binance_order_type_mapper import map_binance_order_type

logger = setup_logger(__name__)


class BinanceOrderRouter:
    """
    Binance USD-M Futures 주문 route 분기.

    REGULAR:
      MARKET / LIMIT
      -> POST /fapi/v1/order

    CONDITIONAL:
      STOP_MARKET / STOP_LIMIT
      -> POST /fapi/v1/algoOrder
    """

    def __init__(self, adapter: BinanceRestAdapter) -> None:
        self.adapter = adapter

    async def place_regular_order(self, order: Order) -> OrderRespDto:
        if order.order_route != OrderRoute.REGULAR:
            raise ValueError("place_regular_order requires a REGULAR order")
        
        params = self._map_regular_order_params(order)
        return await self.adapter.place_regular_order(params)

    async def place_conditional_order(self, order: Order) -> AlgoOrderRespDto:
        if order.order_route != OrderRoute.CONDITIONAL:
            raise ValueError("place_conditional_order requires a CONDITIONAL order")
        
        params = self._map_conditional_order_params(order)
        return await self.adapter.place_algo_order(params)

    # async def place(self, order: Order) -> OrderRespDto | AlgoOrderRespDto:
    #     if order.order_route == OrderRoute.REGULAR:
    #         params = self._map_regular_order_params(order)
    #         return await self.adapter.place_order(params)

    #     if order.order_route == OrderRoute.CONDITIONAL:
    #         params = self._map_conditional_order_params(order)
    #         return await self.adapter.place_algo_order(params)

    #     raise ValueError(f"unsupported order_route: {order.order_route}")

    def _map_regular_order_params(self, order: Order) -> dict[str, Any]:
        """
        Takora REGULAR order -> Binance /fapi/v1/order params.
        """
        if order.order_type not in {OrderType.MARKET, OrderType.LIMIT}:
            raise ValueError(
                f"REGULAR route does not support order_type={order.order_type.value}"
            )

        params: dict[str, Any] = {
            "symbol": order.symbol,
            "side": order.side.value,
            "type": map_binance_order_type(order_type=order.order_type),
            "quantity": order.quantity,
            "newClientOrderId": order.client_order_id or order.order_id,
            "positionSide": order.position_side.value,
        }

        if order.order_type == OrderType.LIMIT:
            if not order.price:
                raise ValueError("LIMIT order requires price")

            params["price"] = order.price

            if order.time_in_force is not None:
                params["timeInForce"] = order.time_in_force.value

        # Hedge Mode에서는 reduceOnly를 보내면 안 됨.
        if order.reduce_only and order.position_side == PositionSide.BOTH:
            params["reduceOnly"] = "true"

        return params

    # [claim] 바이낸스 요청 및 응답 속성을 변수로 관리하지 고민
    def _map_conditional_order_params(self, order: Order) -> dict[str, Any]:
        """
        Binance /fapi/v1/algoOrder params.

        내부 타입 -> Binance:
          STOP_MARKET -> Binance STOP_MARKET
          STOP_LIMIT  -> Binance STOP

        내부 trigger_price:
          -> Binance triggerPrice
        """
        if order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
            binance_type = map_binance_order_type(order_type=order.order_type)
        else:
            raise ValueError(
                f"CONDITIONAL route does not support order_type={order.order_type.value}"
            )

        if not order.trigger_price:
            raise ValueError(
                f"{order.order_type.value} requires trigger_price"
            )

        params: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": order.symbol,
            "side": order.side.value,
            "positionSide": order.position_side.value,
            "type": binance_type,
            "triggerPrice": order.trigger_price,
            "clientAlgoId": order.client_conditional_id or order.order_id,
        }

        # closePosition=true 주문은 quantity를 보내지 않는다.
        if order.close_position:
            params["closePosition"] = "true"
        else:
            params["quantity"] = order.quantity

        if order.order_type == OrderType.STOP_LIMIT:
            if not order.price:
                raise ValueError("STOP_LIMIT order requires price")

            params["price"] = order.price

            if order.time_in_force is not None:
                params["timeInForce"] = order.time_in_force.value

        # closePosition과 reduceOnly는 같이 보내지 않는다.
        # Hedge Mode에서는 reduceOnly도 보내지 않는다.
        if (
            not order.close_position
            and order.reduce_only
            and order.position_side == PositionSide.BOTH
        ):
            params["reduceOnly"] = "true"

        return params