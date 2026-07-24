from __future__ import annotations

import json
from typing import Any, Optional

import asyncpg

from schemas.order import Order

from .domain.order_intent_table import OrderIntentTableSchema, OrderIntentColumn

def _decode_jsonb(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, (bytes, bytearray)):
        return json.loads(bytes(value).decode("utf-8"))
    raise TypeError(f"unsupported jsonb type: {type(value)!r}")

def _order_from_intent_row(row: asyncpg.Record) -> Order:
    data = _decode_jsonb(row[OrderIntentColumn.RAW_REQUEST.value])

    for column in OrderIntentColumn:
        if column == OrderIntentColumn.RAW_REQUEST:
            continue
        data[column.value] = row[column.value]

    return Order.model_validate(data)


class OrderIntentPostgresRepository:
    """
    order_intents 테이블 접근.

    최초 주문 의도는 생성 이후 거의 불변으로 본다.
    """

    def __init__(self) -> None:
        self._table_schema = OrderIntentTableSchema()
        self.table_name = self._table_schema.table_name
        self.insert_order_intent_columns = self._table_schema.INSERT_COLUMNS

        self.insert_order_intents_query = f"""
            INSERT INTO {self.table_name} (
                {self._table_schema.names(self.insert_order_intent_columns)}
            )
            VALUES (
                {self._table_schema.placeholders(self.insert_order_intent_columns)}
            )
        """
        self.insert_order_intents_returning_query = (
            self.insert_order_intents_query + "\nRETURNING *"
        )

        self.find_all_order_intents_by_order_id_query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {OrderIntentColumn.ORDER_ID.value} = $1
        """

    async def insert(
        self,
        *,
        conn: asyncpg.Connection,
        order: Order,
    ) -> None:
        # version: 1
        # raw_request = order.model_dump(mode="json", exclude_none=True)

        # await conn.execute(
        #     f"""
        #     INSERT INTO {PostgresTable.ORDER_INTENTS} (
        #         order_id,

        #         source,
        #         signal_id,
        #         strategy_name,

        #         exchange,
        #         market_type,
        #         symbol,

        #         side,
        #         order_type,
        #         order_route,

        #         time_in_force,

        #         quantity,
        #         price,
        #         trigger_price,

        #         reduce_only,
        #         close_position,

        #         position_side,
        #         position_action,

        #         client_order_id,
        #         client_conditional_id,

        #         raw_request,
        #         created_ts
        #     )
        #     VALUES (
        #         $1,

        #         $2, $3, $4,

        #         $5, $6, $7,

        #         $8, $9, $10,

        #         $11,

        #         $12, $13, $14,

        #         $15, $16,

        #         $17, $18,

        #         $19, $20,

        #         $21::jsonb,
        #         $22::bigint
        #     )
        #     """,
        #     order.order_id,

        #     _enum_value(order.source),
        #     order.signal_id,
        #     order.strategy_name,

        #     _enum_value(order.exchange),
        #     _enum_value(order.market_type),
        #     order.symbol,

        #     _enum_value(order.side),
        #     _enum_value(order.order_type),
        #     _enum_value(order.order_route),

        #     _enum_value(order.time_in_force) if order.time_in_force else None,

        #     order.quantity,
        #     order.price,
        #     order.trigger_price,

        #     order.reduce_only,
        #     order.close_position,

        #     _enum_value(order.position_side),
        #     _enum_value(order.position_action),

        #     order.client_order_id,
        #     order.client_conditional_id,

        #     json.dumps(raw_request),
        #     order.created_ts,
        # )

        # version: 2
        await conn.execute(
            self.insert_order_intents_query,
            *self._table_schema.insert_values(order),
        )

    async def insert_returning(
        self,
        *,
        conn: asyncpg.Connection,
        order: Order,
    ) -> Order:
        row = await conn.fetchrow(
            self.insert_order_intents_returning_query,
            *self._table_schema.insert_values(order),
        )

        if row is None:
            raise RuntimeError(
                f"order intent insert did not return row: order_id={order.order_id}"
            )
        
        return _order_from_intent_row(row)

    async def get(
        self,
        conn: asyncpg.Connection,
        order_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self.find_all_order_intents_by_order_id_query,
            order_id,
        )
        return dict(row) if row else None
