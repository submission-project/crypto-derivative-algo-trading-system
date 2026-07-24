from enum import Enum
from dataclasses import dataclass
from typing import Any

from schemas.order import Order

from common.converters import enum_value

# [claim] json 라이브러리를 쓰는 데, 이후에는 orjson 으로 바꿀 수 있으면 그걸로 바꾸삼
import json

class OrderColumn(str, Enum):
    ORDER_ID = "order_id"

    EXCHANGE = "exchange"
    MARKET_TYPE = "market_type"
    SYMBOL = "symbol"

    SIDE = "side"
    ORDER_ROUTE = "order_route"
    ORDER_TYPE = "order_type"

    TIME_IN_FORCE = "time_in_force"
    
    STATUS = "status"

    POSITION_SIDE = "position_side"
    POSITION_ACTION = "position_action"

    QUANTITY = "quantity"
    PRICE = "price"
    TRIGGER_PRICE = "trigger_price"

    REDUCE_ONLY = "reduce_only"
    CLOSE_POSITION = "close_position"

    CLIENT_ORDER_ID = "client_order_id"
    EXCHANGE_ORDER_ID = "exchange_order_id"

    CLIENT_CONDITIONAL_ID = "client_conditional_id"
    EXCHANGE_CONDITIONAL_ID = "exchange_conditional_id"

    CONDITIONAL_STATUS = "conditional_status"
    EXCHANGE_CONDITIONAL_STATUS = "exchange_conditional_status"

    TRIGGERED_ORDER_ID = "triggered_order_id"
    TRIGGERED_CLIENT_ORDER_ID = "triggered_client_order_id"

    REJECT_REASON = "reject_reason"
    EXCHANGE_ERROR_CODE = "exchange_error_code"
    DETAIL_MSG = "detail_msg"

    FILLED_QUANTITY = "filled_quantity"
    AVG_FILL_PRICE = "avg_fill_price"

    CREATED_TS = "created_ts"
    SUBMITTED_TS = "submitted_ts"
    ACKNOWLEDGED_TS = "acknowledged_ts"
    TRIGGERED_TS = "triggered_ts"
    FILLED_TS = "filled_ts"
    CANCELLED_TS = "cancelled_ts"
    EXPIRED_TS = "expired_ts"
    UPDATED_TS = "updated_ts"

    RAW_EXCHANGE_RESPONSE = "raw_exchange_response"

    VERSION = "version"

@dataclass(frozen=True)
class PgColumn:
    column: OrderColumn
    insert_cast: str | None = None


class OrderTableSchema:
    table_name = "orders"
    c = OrderColumn

    INSERT_COLUMNS = (
        PgColumn(c.ORDER_ID),

        PgColumn(c.EXCHANGE),
        PgColumn(c.MARKET_TYPE),
        PgColumn(c.SYMBOL),

        PgColumn(c.ORDER_ROUTE),
        PgColumn(c.ORDER_TYPE),
        PgColumn(c.STATUS),

        PgColumn(c.SIDE),
        
        PgColumn(c.POSITION_SIDE),
        PgColumn(c.POSITION_ACTION),

        PgColumn(c.QUANTITY, "::numeric"),
        PgColumn(c.PRICE, "::numeric"),
        PgColumn(c.TRIGGER_PRICE, "::numeric"),

        PgColumn(c.REDUCE_ONLY),
        PgColumn(c.CLOSE_POSITION),

        PgColumn(c.CLIENT_ORDER_ID),
        PgColumn(c.EXCHANGE_ORDER_ID),

        PgColumn(c.CLIENT_CONDITIONAL_ID),
        PgColumn(c.EXCHANGE_CONDITIONAL_ID),

        PgColumn(c.CONDITIONAL_STATUS),
        PgColumn(c.EXCHANGE_CONDITIONAL_STATUS),

        PgColumn(c.TRIGGERED_ORDER_ID),
        PgColumn(c.TRIGGERED_CLIENT_ORDER_ID),

        PgColumn(c.REJECT_REASON),
        PgColumn(c.EXCHANGE_ERROR_CODE),
        PgColumn(c.DETAIL_MSG),

        PgColumn(c.FILLED_QUANTITY),
        PgColumn(c.AVG_FILL_PRICE),

        PgColumn(c.CREATED_TS, "::bigint"),
        PgColumn(c.SUBMITTED_TS, "::bigint"),
        PgColumn(c.ACKNOWLEDGED_TS, "::bigint"),
        PgColumn(c.TRIGGERED_TS, "::bigint"),
        PgColumn(c.FILLED_TS, "::bigint"),
        PgColumn(c.CANCELLED_TS, "::bigint"),
        PgColumn(c.EXPIRED_TS, "::bigint"),
        PgColumn(c.UPDATED_TS, "::bigint"),

        PgColumn(c.RAW_EXCHANGE_RESPONSE, "::jsonb"),
        
        PgColumn(c.VERSION, "::bigint"),

        PgColumn(c.TIME_IN_FORCE)
    )

    TRANSITION_UPDATE_COLUMNS = (
        PgColumn(c.STATUS),

        PgColumn(c.CLIENT_ORDER_ID),
        PgColumn(c.EXCHANGE_ORDER_ID),

        PgColumn(c.CLIENT_CONDITIONAL_ID),
        PgColumn(c.EXCHANGE_CONDITIONAL_ID),

        PgColumn(c.CONDITIONAL_STATUS),
        PgColumn(c.EXCHANGE_CONDITIONAL_STATUS),

        PgColumn(c.TRIGGERED_ORDER_ID),
        PgColumn(c.TRIGGERED_CLIENT_ORDER_ID),

        PgColumn(c.REJECT_REASON),
        PgColumn(c.EXCHANGE_ERROR_CODE, "::bigint"),
        PgColumn(c.DETAIL_MSG),

        PgColumn(c.FILLED_QUANTITY),
        PgColumn(c.AVG_FILL_PRICE),

        PgColumn(c.SUBMITTED_TS, "::bigint"),
        PgColumn(c.ACKNOWLEDGED_TS, "::bigint"),
        PgColumn(c.TRIGGERED_TS, "::bigint"),
        PgColumn(c.FILLED_TS, "::bigint"),
        PgColumn(c.CANCELLED_TS, "::bigint"),
        PgColumn(c.EXPIRED_TS, "::bigint"),

        PgColumn(c.UPDATED_TS, "::bigint"),

        PgColumn(c.RAW_EXCHANGE_RESPONSE, "::jsonb"),
    )

    @classmethod
    def names(cls, columns: tuple[PgColumn, ...]) -> str:
        return ",\n".join(col.column.value for col in columns)

    @classmethod
    def placeholders(cls, columns: tuple[PgColumn, ...]) -> str:
        values = []
        for idx, col in enumerate(columns, start=1):
            cast = col.insert_cast or ""
            values.append(f"${idx}{cast}")
        return ",\n".join(values)

    @classmethod
    def row_get(cls, row: dict[str, Any], column: OrderColumn) -> Any:
        return row[column.value]

    @classmethod
    def insert_values(cls, order: Order) -> tuple[Any, ...]:
        return tuple(
            cls.value_from_order(order=order, column=col.column)
            for col in cls.INSERT_COLUMNS
        )

    @classmethod
    def value_from_order(cls, *, order: Order, column: OrderColumn) -> Any:
        if column == OrderColumn.RAW_EXCHANGE_RESPONSE:
            if order.raw_exchange_response is None:
                return None
            return json.dumps(order.raw_exchange_response, ensure_ascii=False)

        attr_name = column.value

        if not hasattr(order, attr_name):
            raise AttributeError(
                f"Order has no field for postgres column: {column.value}"
            )

        return enum_value(getattr(order, attr_name))

    @classmethod
    def update_set_clause(
        cls,
        *,
        columns: tuple[PgColumn, ...],
        start_index: int,
    ) -> str:
        lines = []

        for idx, col in enumerate(columns, start=start_index):
            cast = col.insert_cast or ""
            lines.append(f"{col.column.value} = ${idx}{cast}")

        return ",\n".join(lines)

    @classmethod
    def update_values(cls, order: Order) -> tuple[Any, ...]:
        return tuple(
            cls.value_from_order(order=order, column=col.column)
            for col in cls.TRANSITION_UPDATE_COLUMNS
        )