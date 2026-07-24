from schemas.order import OrderType

_MAPPER_ORDER_TYPE_TO_BINANCE_ORDER_TYPE: dict[OrderType, str] = {
    OrderType.MARKET: OrderType.MARKET.value,
    OrderType.LIMIT: OrderType.LIMIT.value,
    OrderType.STOP_MARKET: OrderType.STOP_MARKET.value,
    OrderType.STOP_LIMIT: "STOP"  # STOP LIMIT,
}

def map_binance_order_type(order_type: OrderType) -> str | None:
    if not order_type:
        return None

    return _MAPPER_ORDER_TYPE_TO_BINANCE_ORDER_TYPE.get(order_type)
