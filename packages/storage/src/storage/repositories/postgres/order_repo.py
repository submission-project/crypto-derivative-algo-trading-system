from __future__ import annotations

from typing import Any, Optional

import asyncpg

from schemas.order import Order, OrderRoute, TERMINAL_STATUSES
from common.converters import enum_value

from schemas.market import Exchange, MarketType

from .domain.order_table import OrderTableSchema, OrderColumn
from .domain.order_intent_table import OrderIntentTableSchema


# TERMINAL_STATUSES → SQL 'NOT IN (...)' 절 (모듈 로드 시 1회만 생성)
_TERMINAL_STATUS_SQL = "(" + ", ".join(f"'{s.value}'" for s in TERMINAL_STATUSES) + ")"


def _enum_value(value: Any) -> Any:
    return enum_value(value)

def _row_to_dict(row: asyncpg.Record | None) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    return dict(row)

class StaleOrderVersionError(RuntimeError):
    """
    PostgreSQL optimistic lock 충돌.

    기대한 version과 실제 DB version이 다르면 발생.
    """

    def __init__(
        self,
        *,
        order_id: str,
        expected_version: int,
        actual_version: int,
        actual_status: str,
    ) -> None:
        self.order_id = order_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        self.actual_status = actual_status

        super().__init__(
            f"stale order version: "
            f"order_id={order_id}, "
            f"expected_version={expected_version}, "
            f"actual_version={actual_version}, "
            f"actual_status={actual_status}"
        )


class OrderPostgresRepository:
    """
    orders 테이블 접근.

    역할:
      - 최초 상태 insert
      - 현재 상태 조회
      - version 기반 상태 전이
    """

    def __init__(self):
        self._order_table_schema = OrderTableSchema()
        self.order_table_name = self._order_table_schema.table_name
        self.insert_order_columns = self._order_table_schema.INSERT_COLUMNS
        self.insert_order_column_names = self._order_table_schema.names(self.insert_order_columns)
        self.insert_order_column_placeholders =self._order_table_schema.placeholders(self.insert_order_columns)
        self.update_order_columns = self._order_table_schema.TRANSITION_UPDATE_COLUMNS

        self.insert_initial_query = f"""
            INSERT INTO {self.order_table_name} ({self.insert_order_column_names})
            VALUES ({self.insert_order_column_placeholders})
            """

        self.transition_query = f"""
            UPDATE {self.order_table_name}
            SET
                {
                    self._order_table_schema.update_set_clause(
                        columns=self.update_order_columns,
                        start_index=2
                    )
                },
                {OrderColumn.VERSION.value} = {OrderColumn.VERSION.value} + 1
            WHERE {OrderColumn.ORDER_ID.value} = $1
              AND {OrderColumn.VERSION.value} = ${len(self.update_order_columns) + 2}
            RETURNING *
            """

        self.find_all_order_by_order_id_query = f"""
            SELECT *
            FROM {self.order_table_name}
            WHERE {OrderColumn.ORDER_ID.value} = $1
            """
        
        

    async def insert_initial(
        self,
        *,
        conn: asyncpg.Connection,
        order: Order,
    ) -> None:
        # # version: 1
        # await conn.execute(
        #     f"""
        #     INSERT INTO {self.order_table_name} (
        #         order_id,

        #         exchange,
        #         market_type,
        #         symbol,

        #         order_route,
        #         order_type,
        #         status,

        #         side,

        #         position_side,
        #         position_action,

        #         quantity,
        #         price,
        #         trigger_price,

        #         reduce_only,
        #         close_position,

        #         client_order_id,
        #         exchange_order_id,

        #         client_conditional_id,
        #         exchange_conditional_id,

        #         conditional_status,
        #         exchange_conditional_status,

        #         triggered_order_id,
        #         triggered_client_order_id,

        #         reject_reason,
        #         exchange_error_code,
        #         detail_msg,

        #         filled_quantity,
        #         avg_fill_price,

        #         created_ts,
        #         submitted_ts,
        #         acknowledged_ts,
        #         triggered_ts,
        #         filled_ts,
        #         cancelled_ts,
        #         expired_ts,
        #         updated_ts,

        #         raw_exchange_response,

        #         version,

        #         time_in_force
        #     )
        #     VALUES (
        #         $1,

        #         $2, $3, $4,

        #         $5, $6, $7,

        #         $8,

        #         $9, $10,

        #         $11::numeric,
        #         $12::numeric,
        #         $13::numeric,

        #         $14, $15,

        #         $16, $17,

        #         $18, $19,

        #         $20, $21,

        #         $22, $23,

        #         $24, $25::bigint, $26,

        #         $27, $28,

        #         $29::bigint,
        #         $30::bigint,
        #         $31::bigint,
        #         $32::bigint,
        #         $33::bigint,
        #         $34::bigint,
        #         $35::bigint,
        #         $36::bigint,

        #         $37::jsonb,

        #         $38::bigint,
                
        #         $39
        #     )
        #     """,
        #     order.order_id,

        #     _enum_value(order.exchange),
        #     _enum_value(order.market_type),
        #     order.symbol,

        #     _enum_value(order.order_route),
        #     _enum_value(order.order_type),
        #     _enum_value(order.status),

        #     _enum_value(order.side),

        #     _enum_value(order.position_side),
        #     _enum_value(order.position_action),

        #     order.quantity,
        #     order.price,
        #     order.trigger_price,

        #     order.reduce_only,
        #     order.close_position,

        #     order.client_order_id,
        #     order.exchange_order_id,

        #     order.client_conditional_id,
        #     order.exchange_conditional_id,

        #     _enum_value(order.conditional_status) if order.conditional_status else None,
        #     order.exchange_conditional_status,

        #     order.triggered_order_id,
        #     order.triggered_client_order_id,

        #     _enum_value(order.reject_reason) if order.reject_reason else None,
        #     order.exchange_error_code,
        #     order.detail_msg,

        #     order.filled_quantity,
        #     order.avg_fill_price,

        #     order.created_ts,
        #     order.submitted_ts,
        #     order.acknowledged_ts,
        #     order.triggered_ts,
        #     order.filled_ts,
        #     order.cancelled_ts,
        #     order.expired_ts,
        #     order.updated_ts,

        #     json.dumps(order.raw_exchange_response)
        #     if order.raw_exchange_response is not None
        #     else None,

        #     order.version,

        #     _enum_value(order.time_in_force) if order.time_in_force else None,
        # )

        # version: 2
        await conn.execute(
            self.insert_initial_query,
            *self._order_table_schema.insert_values(order=order)
        )

    async def insert_initial_returning(
        self,
        *,
        conn: asyncpg.Connection,
        order: Order,
    ) -> Order:
        await self.insert_initial(conn=conn, order=order)
        row = await self.get_joined_order(conn=conn, order_id=order.order_id)
        if row is None:
            raise RuntimeError(
                f"inserted order cannot be loaded: order_id={order.order_id}"
            )
        return Order.model_validate(row)

    async def transition(
        self,
        *,
        conn: asyncpg.Connection,
        order: Order,
        expected_version: int,
    ) -> dict[str, Any]:
        """
        상태 전이 결과를 orders 테이블에 반영.

        optimistic lock:
          WHERE version = expected_version

        성공:
          version = version + 1

        실패:
          현재 DB version/status를 조회한 뒤 StaleOrderVersionError 발생
        """
        # # version: 1
        # row = await conn.fetchrow(
        #     f"""
        #     UPDATE {self.order_table_name}
        #     SET
        #         status = $2,

        #         client_order_id = $3,
        #         exchange_order_id = $4,

        #         client_conditional_id = $5,
        #         exchange_conditional_id = $6,

        #         conditional_status = $7,
        #         exchange_conditional_status = $8,

        #         triggered_order_id = $9,
        #         triggered_client_order_id = $10,

        #         reject_reason = $11,
        #         exchange_error_code = $12::bigint,
        #         detail_msg = $13,

        #         filled_quantity = $14,
        #         avg_fill_price = $15,

        #         submitted_ts = $16::bigint,
        #         acknowledged_ts = $17::bigint,
        #         triggered_ts = $18::bigint,
        #         filled_ts = $19::bigint,
        #         cancelled_ts = $20::bigint,
        #         expired_ts = $21::bigint,

        #         updated_ts = $22::bigint,

        #         raw_exchange_response = $23::jsonb,

        #         version = version + 1
        #     WHERE order_id = $1
        #       AND version = $24
        #     RETURNING *
        #     """,
        #     order.order_id,

        #     _enum_value(order.status),

        #     order.client_order_id,
        #     order.exchange_order_id,

        #     order.client_conditional_id,
        #     order.exchange_conditional_id,

        #     _enum_value(order.conditional_status) if order.conditional_status else None,
        #     order.exchange_conditional_status,

        #     order.triggered_order_id,
        #     order.triggered_client_order_id,

        #     _enum_value(order.reject_reason) if order.reject_reason else None,
        #     order.exchange_error_code,
        #     order.detail_msg,

        #     order.filled_quantity,
        #     order.avg_fill_price,

        #     order.submitted_ts,
        #     order.acknowledged_ts,
        #     order.triggered_ts,
        #     order.filled_ts,
        #     order.cancelled_ts,
        #     order.expired_ts,

        #     order.updated_ts,

        #     json.dumps(order.raw_exchange_response)
        #     if order.raw_exchange_response is not None
        #     else None,

        #     expected_version,
        # )

        # version: 2
        row = await conn.fetchrow(
            self.transition_query,
            order.order_id,
            *self._order_table_schema.update_values(order=order),
            expected_version,
        )

        if row is None:
            current = await conn.fetchrow(
                f"""
                SELECT {OrderColumn.VERSION.value}, {OrderColumn.STATUS.value}
                FROM {self.order_table_name}
                WHERE {OrderColumn.ORDER_ID.value} = $1
                """,
                order.order_id,
            )

            if current is not None:
                raise StaleOrderVersionError(
                    order_id=order.order_id,
                    expected_version=expected_version,
                    actual_version=int(current[OrderColumn.VERSION.value]),
                    actual_status=str(current[OrderColumn.STATUS.value]),
                )

            raise RuntimeError(
                f"order transition missing row: order_id={order.order_id}"
            )

        return dict(row)

    async def get(
        self,
        conn: asyncpg.Connection,
        order_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self.find_all_order_by_order_id_query,
            order_id,
        )
        return dict(row) if row else None

    async def get_joined_order(
        self,
        *,
        conn: asyncpg.Connection,
        order_id: str,
    ) -> Optional[dict[str, Any]]:
        """
        order_intents + orders를 조인해서 Order 모델로 복원 가능한 dict 반환.

        Redis projection miss 시 PostgreSQL 원본에서 주문을 복구하는 데 사용.
        """
        row = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.order_id = $1
            """,
            order_id,
        )

        return dict(row) if row else None

    async def list_non_terminal_joined_orders(
        self,
        conn: asyncpg.Connection,
    ) -> list[Order]:
        """
        PostgreSQL 원본 기준으로 terminal이 아닌 모든 주문 조회.

        startup Redis projection rebuild에 사용.
        """

        rows = await conn.fetch(
            self._joined_order_select_sql()
            + f"""
            WHERE o.status NOT IN {_TERMINAL_STATUS_SQL}
            ORDER BY o.updated_ts ASC, i.order_id ASC
            """
        )

        return [Order.model_validate(dict(row)) for row in rows]

    async def list_orders(
        self,
        conn: asyncpg.Connection,
        *,
        exchange: Exchange,
        market_type: MarketType,
        status: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
    ) -> list[Order]:
        """
        PostgreSQL 데이터베이스에서 주문 이력 조회 (필터링 지원).
        """
        query_parts = [
            self._joined_order_select_sql(),
            "WHERE o.exchange = $1 AND o.market_type = $2"
        ]
        params = [_enum_value(exchange), _enum_value(market_type)]
        param_idx = 3

        if symbol:
            query_parts.append(f"AND o.symbol = ${param_idx}")
            params.append(symbol.upper())
            param_idx += 1

        if status:
            if status.upper() == "OPEN":
                query_parts.append(f"AND o.status NOT IN {_TERMINAL_STATUS_SQL}")
            else:
                query_parts.append(f"AND o.status = ${param_idx}")
                params.append(status.upper())
                param_idx += 1

        query_parts.append(f"ORDER BY o.updated_ts DESC LIMIT ${param_idx}")
        params.append(limit)

        query = "\n".join(query_parts)
        rows = await conn.fetch(query, *params)
        return [Order.model_validate(dict(row)) for row in rows]

    async def list_non_terminal_joined_orders_page(
        self,
        conn: asyncpg.Connection,
        *,
        exchange: Exchange,
        market_type: MarketType,
        order_route: OrderRoute | None = None,
        cursor_updated_ts: int | None = None,
        cursor_order_id: str | None = None,
        limit: int = 1000,
    ) -> list[Order]:
        rows = await conn.fetch(
            self._joined_order_select_sql()
            + f"""
            WHERE o.exchange = $1
            AND o.market_type = $2
            AND o.status NOT IN {_TERMINAL_STATUS_SQL}
            AND ($3::text IS NULL OR i.order_route = $3)
            AND (
                    $4::bigint IS NULL
                OR (o.updated_ts, i.order_id) > ($4::bigint, $5::text)
            )
            ORDER BY o.updated_ts ASC, i.order_id ASC
            LIMIT $6
            """,
            _enum_value(exchange),
            _enum_value(market_type),
            _enum_value(order_route) if order_route else None,
            cursor_updated_ts,
            cursor_order_id,
            limit,
        )

        return [Order.model_validate(dict(row)) for row in rows]


    async def list_open_joined_by_symbol(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: Exchange,
        market_type: MarketType,
        symbol: str,
    ) -> list[Order]:
        """
        PostgreSQL 원본 기준으로 terminal이 아닌 특정 심볼 주문 조회.
        """

        rows = await conn.fetch(
            self._joined_order_select_sql()
            + f"""
            WHERE o.exchange = $1
            AND o.market_type = $2
            AND o.symbol = $3
            AND o.status NOT IN {_TERMINAL_STATUS_SQL}
            ORDER BY o.updated_ts DESC
            """,
            _enum_value(exchange),
            _enum_value(market_type),
            symbol.upper(),
        )

        return [Order.model_validate(dict(row)) for row in rows]

    async def list_open_joined_by__exchange_market_type_symbol(
        self,
        conn: asyncpg.Connection,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
    ) -> list[dict[str, Any]]:
        """
        PostgreSQL 원본 기준으로 terminal이 아닌 특정 심볼 주문 조회.
        """

        rows = await conn.fetch(
            self._joined_order_select_sql()
            + f"""
            WHERE o.exchange = $1
            AND o.market_type = $2
            AND o.symbol = $3
            AND o.status NOT IN {_TERMINAL_STATUS_SQL}
            ORDER BY o.updated_ts DESC
            """,
            exchange,
            market_type,
            symbol,
        )

        return [dict(row) for row in rows]

    async def get_joined_orders_by_ids(
        self,
        conn: asyncpg.Connection,
        order_ids: list[str],
    ) -> list[dict[str, Any]]:
        """
        여러 order_id를 한 번의 SQL로 조회.

        Redis projection 누락 복구, reconciliation stale projection 검증 등에 사용.
        """
        if not order_ids:
            return []

        rows = await conn.fetch(
            self._joined_order_select_sql()
            + """
            WHERE o.order_id = ANY($1::text[])
            """,
            order_ids,
        )

        return [dict(row) for row in rows]

    async def get_by_exchange_order_id(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: str,
        market_type: str,
        exchange_order_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.exchange = $1
            AND o.market_type = $2
            AND o.exchange_order_id = $3
            """,
            exchange,
            market_type,
            exchange_order_id,
        )

        return dict(row) if row else None

    async def get_by_triggered_order_id(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: str,
        market_type: str,
        triggered_order_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.exchange = $1
            AND o.market_type = $2
            AND o.triggered_order_id = $3
            """,
            exchange,
            market_type,
            triggered_order_id,
        )

        return dict(row) if row else None

    async def get_by_client_order_id(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: str,
        market_type: str,
        client_order_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.exchange = $1
              AND o.market_type = $2
              AND o.client_order_id = $3
            """,
            exchange,
            market_type,
            client_order_id,
        )

        return dict(row) if row else None

    async def get_by_client_conditional_id(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: str,
        market_type: str,
        client_conditional_id: str,
    ) -> Optional[dict[str, Any]]:
        row: asyncpg.Record | None = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.exchange = $1
              AND o.market_type = $2
              AND o.client_conditional_id = $3
            """,
            exchange,
            market_type,
            client_conditional_id,
        )

        return dict(row) if row else None

    async def get_by_exchange_conditional_id(
        self,
        *,
        conn: asyncpg.Connection,
        exchange: str,
        market_type: str,
        exchange_conditional_id: str,
    ) -> Optional[dict[str, Any]]:
        row: asyncpg.Record | None = await conn.fetchrow(
            self._joined_order_select_sql()
            + """
            WHERE o.exchange = $1
              AND o.market_type = $2
              AND o.exchange_conditional_id = $3
            """,
            exchange,
            market_type,
            exchange_conditional_id,
        )

        return dict(row) if row else None


    # [claim] 매번 int -> str 변환하면, 굳이 int로 저장하는 게 의미 있나?
    def _joined_order_select_sql(self) -> str:
        """
        Order.model_validate(row)에 바로 넣기 위한 joined select.

        orders의 numeric 컬럼은 Pydantic str 필드와 맞추기 위해 text로 cast한다.
        """
        return f"""
        SELECT
            o.order_id,

            i.source,
            i.signal_id,
            i.strategy_name,

            o.exchange,
            o.market_type,
            o.symbol,

            o.side,
            o.order_type,
            o.order_route,

            i.time_in_force,

            o.quantity::text AS quantity,
            o.price::text AS price,
            o.trigger_price::text AS trigger_price,

            o.reduce_only,
            o.close_position,

            o.position_side,
            o.position_action,

            o.client_order_id,
            o.exchange_order_id,

            o.client_conditional_id,
            o.exchange_conditional_id,

            o.conditional_status,
            o.exchange_conditional_status,

            o.triggered_order_id,
            o.triggered_client_order_id,

            o.status,
            o.reject_reason,
            o.exchange_error_code,
            o.detail_msg,

            o.filled_quantity,
            o.avg_fill_price,

            o.created_ts,
            o.submitted_ts,
            o.acknowledged_ts,
            o.triggered_ts,
            o.filled_ts,
            o.cancelled_ts,
            o.expired_ts,
            o.updated_ts,

            o.raw_exchange_response,

            o.version
        FROM {self.order_table_name} o
        JOIN {OrderIntentTableSchema.table_name} i 
        ON o.order_id = i.order_id
        """
