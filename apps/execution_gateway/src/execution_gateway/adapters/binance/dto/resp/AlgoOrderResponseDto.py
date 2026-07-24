"""
Binance USD-M Futures Algo Order Response DTOs.

Binance Docs:
  - POST /fapi/v1/algoOrder         (New Algo Order)
  - GET  /fapi/v1/openAlgoOrders    (Current All Algo Open Orders)
  - GET  /fapi/v1/allAlgoOrders     (Query All Algo Orders)
  - DELETE /fapi/v1/algoOpenOrders  (Cancel All Algo Open Orders)
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class AlgoOrderRespDto:
    """
    알고 주문 생성 / 조회 응답 DTO.

    POST /fapi/v1/algoOrder (New Algo Order)
    GET  /fapi/v1/openAlgoOrders
    GET  /fapi/v1/allAlgoOrders

    Response Example (New Algo Order):
    {
        "algoId": 123456,
        "clientAlgoId": "my_algo_order_1",
        "algoType": "CONDITIONAL",
        "orderType": "STOP_MARKET",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "positionSide": "BOTH",
        "timeInForce": "GTC",
        "quantity": "1.0",
        "algoStatus": "NEW",
        "actualOrderId": "",
        "actualPrice": "0.0",
        "triggerPrice": "50000.0",
        "price": "0.0"
    }

    openAlgoOrders / allAlgoOrders Response (리스트):
    [
      {
        "algoId": 123456,
        "clientAlgoId": "my_algo_order_1",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "positionSide": "BOTH",
        "algoStatus": "NEW",
        "algoType": "CONDITIONAL",
        "orderType": "STOP_MARKET",
        "quantity": "1.0",
        "triggerPrice": "50000.0",
        "price": "0.0",
        "timeInForce": "GTC",
        "bookTime": 1699999999000,
        "updateTime": 1699999999000
      }
    ]
    """
    algoId: Optional[int]
    clientAlgoId: Optional[str]
    algoType: Optional[str]
    orderType: Optional[str]
    symbol: Optional[str]
    side: Optional[str]
    positionSide: Optional[str]
    timeInForce: Optional[str]
    quantity: Optional[str]
    algoStatus: Optional[str]
    actualOrderId: Optional[str]
    actualPrice: Optional[str]
    triggerPrice: Optional[str]
    price: Optional[str]
    reduceOnly: Optional[bool]
    workingType: Optional[str]
    priceProtect: Optional[bool]
    activatePrice: Optional[str]                # TRAILING_STOP_MARKET 전용
    priceRate: Optional[str]                     # TRAILING_STOP_MARKET 전용
    bookTime: Optional[int]
    updateTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "AlgoOrderRespDto":
        return cls(
            algoId=row.get("algoId"),
            clientAlgoId=row.get("clientAlgoId"),
            algoType=row.get("algoType"),
            orderType=row.get("orderType"),
            symbol=row.get("symbol"),
            side=row.get("side"),
            positionSide=row.get("positionSide"),
            timeInForce=row.get("timeInForce"),
            quantity=row.get("quantity"),
            algoStatus=row.get("algoStatus"),
            actualOrderId=row.get("actualOrderId"),
            actualPrice=row.get("actualPrice"),
            triggerPrice=row.get("triggerPrice"),
            price=row.get("price"),
            reduceOnly=row.get("reduceOnly"),
            workingType=row.get("workingType"),
            priceProtect=row.get("priceProtect"),
            activatePrice=row.get("activatePrice"),
            priceRate=row.get("priceRate"),
            bookTime=row.get("bookTime"),
            updateTime=row.get("updateTime"),
            raw=row,
        )


@dataclass(frozen=True)
class CancelAllAlgoOpenOrdersRespDto:
    """
    전체 알고 미체결 취소 응답 DTO.

    DELETE /fapi/v1/algoOpenOrders

    Response Example:
    {
        "code": 200,
        "msg": "The operation of cancel all open algo orders is done."
    }
    """
    code: Optional[int]
    msg: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "CancelAllAlgoOpenOrdersRespDto":
        return cls(
            code=row.get("code"),
            msg=row.get("msg"),
            raw=row,
        )
