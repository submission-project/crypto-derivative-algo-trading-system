from __future__ import annotations

from typing import Any

from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.market import Exchange, MarketType
from schemas.order import ConditionalStatus

from ..constant.binance_constant import BinanceConditionalOrderState

_BINANCE_ALGO_STATUS_MAP: dict[str, ConditionalStatus] = {
    BinanceConditionalOrderState.new: ConditionalStatus.NEW,
     # Binance TRIGGERING은 조건이 충족되어 matching engine으로 전달 중인 상태.
    # 내부적으로는 trigger 발생으로 본다.
    BinanceConditionalOrderState.triggering: ConditionalStatus.TRIGGERED,
    BinanceConditionalOrderState.triggered: ConditionalStatus.TRIGGERED,
    BinanceConditionalOrderState.finished: ConditionalStatus.FINISHED,
    BinanceConditionalOrderState.canceled: ConditionalStatus.CANCELLED,
    BinanceConditionalOrderState.expired: ConditionalStatus.EXPIRED,
    BinanceConditionalOrderState.rejected: ConditionalStatus.REJECTED,
}

_MAPPER_INTERNAL_TO_BINANCE_ALGO_STATUS: dict[ConditionalStatus, str] = {
    ConditionalStatus.NEW: BinanceConditionalOrderState.new,
    ConditionalStatus.TRIGGERED: BinanceConditionalOrderState.triggered,
    ConditionalStatus.FINISHED: BinanceConditionalOrderState.finished,
    ConditionalStatus.CANCELLED: BinanceConditionalOrderState.canceled,
    ConditionalStatus.EXPIRED: BinanceConditionalOrderState.expired,
    ConditionalStatus.REJECTED: BinanceConditionalOrderState.rejected,
}


def map_binance_algo_status(raw_status: str | None) -> ConditionalStatus:
    if not raw_status:
        return ConditionalStatus.UNKNOWN

    return _BINANCE_ALGO_STATUS_MAP.get(
        str(raw_status).upper(),
        ConditionalStatus.UNKNOWN,
    )


def normalize_binance_algo_update(
    *,
    raw_event: dict[str, Any],
    market_type: MarketType,
) -> NormalizedConditionalOrderEvent:
    """
    Binance ALGO_UPDATE raw event를 Takora 내부 표준 이벤트로 변환.

    Binance payload 예:
      {
        "e": "ALGO_UPDATE",
        "E": 1750515742303,
        "T": 1750515742297,
        "o": {
          "caid": "...",  # client algo id
          "aid": 123,    # algo id
          "X": "NEW",    # algo status
          "ai": "...",   # actual order id
          "s": "BTCUSDT"
        }
      }
    """
    algo = raw_event.get("o", {})  # algo
    if not isinstance(algo, dict):
        raise ValueError(f"Invalid ALGO_UPDATE payload: {raw_event}")

    raw_status = algo.get("X")  # algo status
    target_status = map_binance_algo_status(raw_status)

    symbol = str(algo.get("s") or "").upper()  # symbol
    if not symbol:
        raise ValueError(f"ALGO_UPDATE missing symbol: {raw_event}")

    client_conditional_id = algo.get("caid")  # client algo id
    exchange_conditional_id = algo.get("aid")  # algo id

    triggered_order_id = algo.get("ai")  # actual order id
    if triggered_order_id in (None, "", "0"):
        triggered_order_id = None  # 정합성 체크

    return NormalizedConditionalOrderEvent(
        exchange=Exchange.BINANCE,
        market_type=market_type,
        symbol=symbol,
        client_conditional_id=(
            str(client_conditional_id)
            if client_conditional_id not in (None, "")
            else None
        ),
        exchange_conditional_id=(
            str(exchange_conditional_id)
            if exchange_conditional_id not in (None, "")
            else None
        ),
        target_status=target_status,
        exchange_conditional_status=str(raw_status) if raw_status else None,
        triggered_order_id=str(triggered_order_id) if triggered_order_id else None,
        triggered_client_order_id=None,
        filled_quantity=str(algo.get("aq")) if algo.get("aq") not in (None, "") else None,
        avg_fill_price=str(algo.get("ap")) if algo.get("ap") not in (None, "") else None,
        reject_reason_text=str(algo.get("rm")) if algo.get("rm") not in (None, "") else None,
        event_time=int(raw_event.get("E") or 0),
        transaction_time=(
            int(raw_event["T"])
            if raw_event.get("T") is not None
            else None
        ),
        raw=raw_event,
    )

def normalize_binance_algo_rest_row(
    row: dict[str, Any],
    *,
    market_type: MarketType = MarketType.PERP,
    event_time: int | None = None,
) -> NormalizedConditionalOrderEvent:
    """
    Binance openAlgoOrders / allAlgoOrders row를 Takora 표준 이벤트로 변환.

    REST row 주요 필드:
      algoId
      clientAlgoId
      algoStatus
      actualOrderId
      symbol
      updateTime
      triggerTime
    """
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        raise ValueError(f"Algo REST row missing symbol: {row}")

    raw_status = row.get("algoStatus")
    exchange_conditional_status = str(raw_status) if raw_status else None
    target_status = map_binance_algo_status(
        str(raw_status) if raw_status is not None else None
    )

    update_time = (
        row.get("updateTime")
        or row.get("triggerTime")
        or row.get("createTime")
        or event_time
        or 0
    )

    executed_qty = row.get("executedQty")
    filled_quantity = str(executed_qty) if executed_qty not in (None, "") else None

    actual_price = row.get("actualPrice")
    avg_fill_price = str(actual_price) if actual_price not in (None, "", "0", "0.0", "0.00000") else None

    reason = row.get("reason")
    reject_reason_text = str(reason) if reason not in (None, "") else None

    exchange_conditional_id = row.get("algoId")
    exchange_conditional_id = str(exchange_conditional_id) if exchange_conditional_id not in (None, "") else None

    client_conditional_id = row.get("clientAlgoId")
    client_conditional_id = str(client_conditional_id) if client_conditional_id not in (None, "") else None

    actual_order_id = row.get("actualOrderId")
    actual_order_id = str(actual_order_id) if actual_order_id not in (None, "", "0", 0) else None

    return NormalizedConditionalOrderEvent(
        exchange=Exchange.BINANCE,
        market_type=market_type,
        symbol=symbol,
        client_conditional_id=client_conditional_id,
        exchange_conditional_id=exchange_conditional_id,
        target_status=target_status,
        exchange_conditional_status=exchange_conditional_status,
        triggered_order_id=actual_order_id,
        triggered_client_order_id=None,
        filled_quantity=filled_quantity,
        avg_fill_price=avg_fill_price,
        reject_reason_text=reject_reason_text,
        event_time=int(update_time),
        transaction_time=None,
        raw=row,
    )