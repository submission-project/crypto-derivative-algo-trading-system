"""
Binance USD-M Futures Order Response DTOs.

Binance Docs:
  - POST   /fapi/v1/order       (New Order)
  - POST   /fapi/v1/batchOrders (Place Multiple Orders)
  - PUT    /fapi/v1/order       (Modify Order)
  - PUT    /fapi/v1/batchOrders (Modify Multiple Orders)
  - DELETE /fapi/v1/order       (Cancel Order)
  - DELETE /fapi/v1/batchOrders (Cancel Multiple Orders)
  - GET    /fapi/v1/order       (Query Order)
  - GET    /fapi/v1/openOrders  (Current All Open Orders)
  - GET    /fapi/v1/allOrders   (Query All Orders)

주의:
  - New Order / Query Order / Cancel Order / Modify Order의 응답은 동일 스키마를 공유한다.
  - activatePrice, priceRate는 TRAILING_STOP_MARKET 전용 필드이다.
  - cumQuote, avgPrice는 CM migration 후 제거 예정이다.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class OrderRespDto:
    """
    주문 응답 DTO (공통 스키마).

    POST /fapi/v1/order (New Order)
    GET  /fapi/v1/order (Query Order)
    GET  /fapi/v1/openOrders
    GET  /fapi/v1/allOrders

    Response Example:
    {
        "clientOrderId": "testOrder",
        "cumQty": "0",
        "cumQuote": "0",
        "executedQty": "0",
        "orderId": 22542179,
        "avgPrice": "0.00000",
        "origQty": "10",
        "price": "0",
        "reduceOnly": false,
        "side": "BUY",
        "positionSide": "SHORT",
        "status": "NEW",
        "stopPrice": "0",
        "closePosition": false,
        "symbol": "BTCUSDT",
        "timeInForce": "GTD",
        "type": "LIMIT",
        "origType": "LIMIT",
        "updateTime": 1566818724722,
        "workingType": "CONTRACT_PRICE",
        "priceProtect": false,
        "priceMatch": "NONE",
        "selfTradePreventionMode": "NONE",
        "goodTillDate": 1693207680000,
        "activatePrice": "9020",       // TRAILING_STOP_MARKET 전용
        "priceRate": "0.3"             // TRAILING_STOP_MARKET 전용
    }
    """
    clientOrderId: Optional[str]
    cumQty: Optional[str]
    cumQuote: Optional[str]                 # CM migration 후 제거 예정
    executedQty: Optional[str]
    orderId: Optional[int]
    avgPrice: Optional[str]                 # CM migration 후 제거 예정
    origQty: Optional[str]
    price: Optional[str]
    reduceOnly: Optional[bool]
    side: Optional[str]
    positionSide: Optional[str]
    status: Optional[str]
    stopPrice: Optional[str]
    closePosition: Optional[bool]
    symbol: Optional[str]
    time: Optional[int]                     # Query Order 전용 (order time)
    timeInForce: Optional[str]
    type: Optional[str]
    origType: Optional[str]
    updateTime: Optional[int]
    workingType: Optional[str]
    priceProtect: Optional[bool]
    priceMatch: Optional[str]
    selfTradePreventionMode: Optional[str]
    goodTillDate: Optional[int]
    activatePrice: Optional[str]            # TRAILING_STOP_MARKET 전용
    priceRate: Optional[str]                # TRAILING_STOP_MARKET 전용
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "OrderRespDto":
        return cls(
            clientOrderId=row.get("clientOrderId"),
            cumQty=row.get("cumQty"),
            cumQuote=row.get("cumQuote"),
            executedQty=row.get("executedQty"),
            orderId=row.get("orderId"),
            avgPrice=row.get("avgPrice"),
            origQty=row.get("origQty"),
            price=row.get("price"),
            reduceOnly=row.get("reduceOnly"),
            side=row.get("side"),
            positionSide=row.get("positionSide"),
            status=row.get("status"),
            stopPrice=row.get("stopPrice"),
            closePosition=row.get("closePosition"),
            symbol=row.get("symbol"),
            time=row.get("time"),
            timeInForce=row.get("timeInForce"),
            type=row.get("type"),
            origType=row.get("origType"),
            updateTime=row.get("updateTime"),
            workingType=row.get("workingType"),
            priceProtect=row.get("priceProtect"),
            priceMatch=row.get("priceMatch"),
            selfTradePreventionMode=row.get("selfTradePreventionMode"),
            goodTillDate=row.get("goodTillDate"),
            activatePrice=row.get("activatePrice"),
            priceRate=row.get("priceRate"),
            raw=row,
        )


# CancelOrderRespDto는 OrderRespDto와 동일한 스키마
CancelOrderRespDto = OrderRespDto


@dataclass(frozen=True)
class ModifyOrderRespDto:
    """
    주문 수정 응답 DTO.

    PUT /fapi/v1/order (Modify Order)

    Response Example:
    {
        "orderId": 20072994037,
        "symbol": "BTCUSDT",
        "pair": "BTCUSDT",
        "status": "NEW",
        "clientOrderId": "LJ9R4QZDihCaS8UAOOLpgW",
        "price": "30005",
        "avgPrice": "0.0",
        "origQty": "1",
        "executedQty": "0",
        "cumQty": "0",
        "cumBase": "0",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "reduceOnly": false,
        "closePosition": false,
        "side": "BUY",
        "positionSide": "LONG",
        "stopPrice": "0",
        "workingType": "CONTRACT_PRICE",
        "priceProtect": false,
        "origType": "LIMIT",
        "priceMatch": "NONE",
        "selfTradePreventionMode": "NONE",
        "goodTillDate": 0,
        "updateTime": 1629182711600
    }
    """
    orderId: Optional[int]
    symbol: Optional[str]
    pair: Optional[str]
    status: Optional[str]
    clientOrderId: Optional[str]
    price: Optional[str]
    avgPrice: Optional[str]
    origQty: Optional[str]
    executedQty: Optional[str]
    cumQty: Optional[str]
    cumBase: Optional[str]
    timeInForce: Optional[str]
    type: Optional[str]
    reduceOnly: Optional[bool]
    closePosition: Optional[bool]
    side: Optional[str]
    positionSide: Optional[str]
    stopPrice: Optional[str]
    workingType: Optional[str]
    priceProtect: Optional[bool]
    origType: Optional[str]
    priceMatch: Optional[str]
    selfTradePreventionMode: Optional[str]
    goodTillDate: Optional[int]
    updateTime: Optional[int]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "ModifyOrderRespDto":
        return cls(
            orderId=row.get("orderId"),
            symbol=row.get("symbol"),
            pair=row.get("pair"),
            status=row.get("status"),
            clientOrderId=row.get("clientOrderId"),
            price=row.get("price"),
            avgPrice=row.get("avgPrice"),
            origQty=row.get("origQty"),
            executedQty=row.get("executedQty"),
            cumQty=row.get("cumQty"),
            cumBase=row.get("cumBase"),
            timeInForce=row.get("timeInForce"),
            type=row.get("type"),
            reduceOnly=row.get("reduceOnly"),
            closePosition=row.get("closePosition"),
            side=row.get("side"),
            positionSide=row.get("positionSide"),
            stopPrice=row.get("stopPrice"),
            workingType=row.get("workingType"),
            priceProtect=row.get("priceProtect"),
            origType=row.get("origType"),
            priceMatch=row.get("priceMatch"),
            selfTradePreventionMode=row.get("selfTradePreventionMode"),
            goodTillDate=row.get("goodTillDate"),
            updateTime=row.get("updateTime"),
            raw=row,
        )


@dataclass(frozen=True)
class CancelAllOpenOrdersRespDto:
    """
    전체 미체결 취소 응답 DTO.

    DELETE /fapi/v1/allOpenOrders

    Response Example:
    {
        "code": 200,
        "msg": "The operation of cancel all open order is done."
    }
    """
    code: Optional[int]
    msg: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "CancelAllOpenOrdersRespDto":
        return cls(
            code=row.get("code"),
            msg=row.get("msg"),
            raw=row,
        )


@dataclass(frozen=True)
class CancelAlgoOrderRespDto:
    """
    알고 주문 취소 응답 DTO.

    DELETE /fapi/v1/algoOrder

    Response Example:
    {
        "algoId": 123456,
        "clientAlgoId": "my_algo_order_1",
        "code": "000000",
        "msg": "success"
    }
    """
    algoId: Optional[int]
    clientAlgoId: Optional[str]
    code: Optional[str]
    msg: Optional[str]
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, row: dict[str, Any]) -> "CancelAlgoOrderRespDto":
        return cls(
            algoId=row.get("algoId"),
            clientAlgoId=row.get("clientAlgoId"),
            code=row.get("code"),
            msg=row.get("msg"),
            raw=row,
        )
