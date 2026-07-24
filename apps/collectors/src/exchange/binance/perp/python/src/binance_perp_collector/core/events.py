"""
Binance WebSocket 이벤트 타입 정의.

msgspec.Struct를 사용해 JSON 파싱과 타입 검증을 C 레벨에서 한 번에 처리합니다.
스트림 파일에서는 orjson.loads → msgspec.convert 순서로 사용합니다.

  packet = orjson.loads(msg)
  event  = msgspec.convert(packet["data"], WsTradeEvent)   # raises msgspec.ValidationError

필드 검증 실패 시 msgspec.ValidationError가 발생합니다.
"""

import msgspec

_TRADE_RENAME = {
    "symbol": "s",
    "event_type": "e",
    "trade_id": "t",
    "trade_time_ms": "T",
    "event_time_ms": "E",
    "price": "p",
    "quantity": "q",
    "is_buyer_maker": "m",
}

_AGG_TRADE_RENAME = {
    "symbol": "s",
    "event_type": "e",
    "agg_trade_id": "a",
    "first_trade_id": "f",
    "last_trade_id": "l",
    "trade_time_ms": "T",
    "event_time_ms": "E",
    "price": "p",
    "quantity": "q",
    "is_buyer_maker": "m",
}


class WsTradeEvent(msgspec.Struct, frozen=True, rename=_TRADE_RENAME):
    """Binance @trade 스트림 단일 이벤트."""

    symbol: str
    event_type: str
    trade_id: int
    trade_time_ms: int
    price: str
    quantity: str
    is_buyer_maker: bool
    event_time_ms: int | None = None


class WsAggTradeEvent(msgspec.Struct, frozen=True, rename=_AGG_TRADE_RENAME):
    """Binance @aggTrade 스트림 단일 이벤트."""

    symbol: str
    event_type: str
    agg_trade_id: int
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    price: str
    quantity: str
    is_buyer_maker: bool
    event_time_ms: int | None = None
