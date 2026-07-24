from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from common.converters import enum_value
from schemas.order import Order


class OrderIntentColumn(str, Enum):
    ORDER_ID = "order_id"

    SOURCE = "source"
    SIGNAL_ID = "signal_id"
    STRATEGY_NAME = "strategy_name"

    EXCHANGE = "exchange"
    MARKET_TYPE = "market_type"
    SYMBOL = "symbol"

    SIDE = "side"
    ORDER_TYPE = "order_type"
    ORDER_ROUTE = "order_route"

    TIME_IN_FORCE = "time_in_force"

    QUANTITY = "quantity"
    PRICE = "price"
    TRIGGER_PRICE = "trigger_price"

    REDUCE_ONLY = "reduce_only"
    CLOSE_POSITION = "close_position"

    POSITION_SIDE = "position_side"
    POSITION_ACTION = "position_action"

    CLIENT_ORDER_ID = "client_order_id"
    CLIENT_CONDITIONAL_ID = "client_conditional_id"

    RAW_REQUEST = "raw_request"
    CREATED_TS = "created_ts"


@dataclass(frozen=True)
class PgColumn:
    column: OrderIntentColumn
    insert_cast: str | None = None


class OrderIntentTableSchema:
    table_name = "order_intents"
    c = OrderIntentColumn

    INSERT_COLUMNS = (
        PgColumn(c.ORDER_ID),
        PgColumn(c.SOURCE),
        PgColumn(c.SIGNAL_ID),
        PgColumn(c.STRATEGY_NAME),
        PgColumn(c.EXCHANGE),
        PgColumn(c.MARKET_TYPE),
        PgColumn(c.SYMBOL),
        PgColumn(c.SIDE),
        PgColumn(c.ORDER_TYPE),
        PgColumn(c.ORDER_ROUTE),
        PgColumn(c.TIME_IN_FORCE),
        PgColumn(c.QUANTITY),
        PgColumn(c.PRICE),
        PgColumn(c.TRIGGER_PRICE),
        PgColumn(c.REDUCE_ONLY),
        PgColumn(c.CLOSE_POSITION),
        PgColumn(c.POSITION_SIDE),
        PgColumn(c.POSITION_ACTION),
        PgColumn(c.CLIENT_ORDER_ID),
        PgColumn(c.CLIENT_CONDITIONAL_ID),
        PgColumn(c.RAW_REQUEST, "::jsonb"),
        PgColumn(c.CREATED_TS, "::bigint"),
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
    def row_get(cls, row: dict[str, Any], column: OrderIntentColumn) -> Any:
        return row[column.value]

    @classmethod
    def insert_values(cls, order: Order) -> tuple[Any, ...]:
        return tuple(
            cls.value_from_order(order=order, column=col.column)
            for col in cls.INSERT_COLUMNS
        )

    @classmethod
    def value_from_order(cls, *, order: Order, column: OrderIntentColumn) -> Any:
        if column == OrderIntentColumn.RAW_REQUEST:
            raw_request = order.model_dump(mode="json", exclude_none=True)
            return json.dumps(raw_request, ensure_ascii=False)

        attr_name = column.value

        if not hasattr(order, attr_name):
            raise AttributeError(
                f"Order has no field for postgres column: {column.value}"
            )

        return enum_value(getattr(order, attr_name))
