from __future__ import annotations

from typing import Any, Optional

import asyncpg

from decimal import Decimal

from schemas.position import Position, PositionStatus


from .domain.position_table import PositionTableSchema, PositionColumn

def _decimal_str(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _row_to_dict(row: asyncpg.Record | None) -> Optional[dict[str, Any]]:
    if row is None:
        return None

    result = dict(row)

    for key, value in list(result.items()):
        if isinstance(value, Decimal):
            result[key] = _decimal_str(value)

    return result


class PositionPostgresRepository:
    """
    positions 테이블 접근.

    PostgreSQL = position source of truth.
    """

    def __init__(self):
        self._table_schema = PositionTableSchema()
        self.table_name = self._table_schema.table_name

        self.upsert_insert_columns = self._table_schema.UPSERT_INSERT_COLUMNS
        self.upsert_update_columns = self._table_schema.UPSERT_UPDATE_COLUMNS
        self.projection_select_columns = self._table_schema.PROJECTION_SELECT_COLUMNS

        self.upsert_query = f"""
            INSERT INTO {self.table_name} (
                {self._table_schema.names(self.upsert_insert_columns)}
            )
            VALUES (
                {self._table_schema.placeholders(self.upsert_insert_columns)}
            )
            ON CONFLICT ({PositionColumn.POSITION_ID.value})
            DO UPDATE SET
                {self._table_schema.excluded_update_set_clause(
                    columns=self.upsert_update_columns
                )},

                {PositionColumn.OPENED_TS.value} =
                    CASE
                        WHEN {self.table_name}.{PositionColumn.POSITION_AMT.value} = 0
                         AND EXCLUDED.{PositionColumn.POSITION_AMT.value} <> 0
                        THEN EXCLUDED.{PositionColumn.UPDATED_TS.value}
                        WHEN {self.table_name}.{PositionColumn.OPENED_TS.value} IS NULL
                         AND EXCLUDED.{PositionColumn.POSITION_AMT.value} <> 0
                        THEN EXCLUDED.{PositionColumn.UPDATED_TS.value}
                        ELSE {self.table_name}.{PositionColumn.OPENED_TS.value}
                    END,

                {PositionColumn.CLOSED_TS.value} =
                    CASE
                        WHEN EXCLUDED.{PositionColumn.POSITION_AMT.value} = 0
                        THEN EXCLUDED.{PositionColumn.UPDATED_TS.value}
                        ELSE NULL::bigint
                    END,

                {PositionColumn.VERSION.value} = {self.table_name}.{PositionColumn.VERSION.value} + 1
            WHERE {self.table_name}.{PositionColumn.LAST_EVENT_TIME.value} IS NULL
               OR EXCLUDED.{PositionColumn.LAST_EVENT_TIME.value} IS NULL
               OR EXCLUDED.{PositionColumn.LAST_EVENT_TIME.value} >= {self.table_name}.{PositionColumn.LAST_EVENT_TIME.value}
            RETURNING *
            """

        self.list_open_for_projection_sql = f"""
            SELECT
                {self._table_schema.projection_select_names()}
            FROM {self.table_name}
            WHERE {PositionColumn.STATUS.value} = '{PositionStatus.OPEN.value}'
            ORDER BY {PositionColumn.UPDATED_TS.value} ASC, {PositionColumn.POSITION_ID.value} ASC
            """

        self.list_open_query = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {PositionColumn.STATUS.value} = '{PositionStatus.OPEN.value}'
            ORDER BY {PositionColumn.UPDATED_TS.value} DESC
            """

        self.list_all_query = f"""
            SELECT *
            FROM {self.table_name}
            ORDER BY {PositionColumn.UPDATED_TS.value} DESC
            """

        self.find_all_by_position_id = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {PositionColumn.POSITION_ID.value} = $1
            """

    async def upsert(
        self,
        conn: asyncpg.Connection,
        *,
        position: Position,
    ) -> Position:
        """
        포지션 patch/snapshot 반영.

        stale guard:
          기존 last_event_time보다 오래된 이벤트는 상태를 덮어쓰지 않는다.

        opened_ts / closed_ts:
          - 0 -> non-zero가 되면 opened_ts 설정
          - non-zero -> 0이 되면 closed_ts 설정
        """
        # row = await conn.fetchrow(
        #     f"""
        #     INSERT INTO {self.table_name} (
        #         position_id,
        #         exchange,
        #         market_type,
        #         symbol,
        #         position_side,
        #         status,

        #         position_amt,
        #         entry_price,
        #         break_even_price,
        #         mark_price,
        #         unrealized_pnl,
        #         isolated_margin,
        #         isolated_wallet,
        #         margin_type,
        #         leverage,
        #         liquidation_price,
        #         notional,

        #         update_reason,
        #         last_event_time,
        #         last_transaction_time,

        #         opened_ts,
        #         closed_ts,
        #         updated_ts,
        #         version
        #     )
        #     VALUES (
        #         $1, $2, $3, $4, $5, $6,
        #         $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
        #         $18, $19, $20,
        #         CASE WHEN $7::numeric <> 0 THEN $21::bigint ELSE NULL::bigint END,
        #         CASE WHEN $7::numeric = 0 THEN $21::bigint ELSE NULL::bigint END,
        #         $21::bigint,
        #         1
        #     )
        #     ON CONFLICT (position_id)
        #     DO UPDATE SET
        #         status = EXCLUDED.status,
        #         position_amt = EXCLUDED.position_amt,
        #         entry_price = EXCLUDED.entry_price,
        #         break_even_price = EXCLUDED.break_even_price,
        #         mark_price = EXCLUDED.mark_price,
        #         unrealized_pnl = EXCLUDED.unrealized_pnl,
        #         isolated_margin = EXCLUDED.isolated_margin,
        #         isolated_wallet = EXCLUDED.isolated_wallet,
        #         margin_type = EXCLUDED.margin_type,
        #         leverage = EXCLUDED.leverage,
        #         liquidation_price = EXCLUDED.liquidation_price,
        #         notional = EXCLUDED.notional,

        #         update_reason = EXCLUDED.update_reason,
        #         last_event_time = EXCLUDED.last_event_time,
        #         last_transaction_time = EXCLUDED.last_transaction_time,

        #         opened_ts =
        #             CASE
        #                 WHEN positions.position_amt = 0
        #                  AND EXCLUDED.position_amt <> 0
        #                 THEN EXCLUDED.updated_ts
        #                 WHEN positions.opened_ts IS NULL
        #                  AND EXCLUDED.position_amt <> 0
        #                 THEN EXCLUDED.updated_ts
        #                 ELSE positions.opened_ts
        #             END,

        #         closed_ts =
        #             CASE
        #                 WHEN EXCLUDED.position_amt = 0
        #                 THEN EXCLUDED.updated_ts
        #                 ELSE NULL::bigint
        #             END,

        #         updated_ts = EXCLUDED.updated_ts,
        #         version = positions.version + 1
        #     WHERE positions.last_event_time IS NULL
        #        OR EXCLUDED.last_event_time IS NULL
        #        OR EXCLUDED.last_event_time >= positions.last_event_time
        #     RETURNING *
        #     """,
        #     position.position_id,
        #     _enum_value(position.exchange),
        #     _enum_value(position.market_type),
        #     position.symbol.upper(),
        #     _enum_value(position.position_side),
        #     _enum_value(position.status),
        #     _num(position.position_amt),
        #     _num(position.entry_price),
        #     _num(position.break_even_price),
        #     _num(position.mark_price),
        #     _num(position.unrealized_pnl),
        #     _num(position.isolated_margin),
        #     _num(position.isolated_wallet),
        #     position.margin_type,
        #     position.leverage,
        #     _num(position.liquidation_price),
        #     _num(position.notional),
        #     position.update_reason,
        #     position.last_event_time,
        #     position.last_transaction_time,
        #     position.updated_ts,
        # )
        row = await conn.fetchrow(
            self.upsert_query,
            *self._table_schema.upsert_insert_values(position),
        )

        data = _row_to_dict(row)

        if data is None:
            current = await self.get(conn, position.position_id)
            if current is None:
                raise RuntimeError(
                    f"position upsert ignored but current row not found: "
                    f"{position.position_id}"
                )
            return Position.model_validate(current)

        return Position.model_validate(data)

    async def get(
        self,
        conn: asyncpg.Connection,
        position_id: str,
    ) -> Optional[dict[str, Any]]:
        row = await conn.fetchrow(
            self.find_all_by_position_id,
            position_id,
        )
        return _row_to_dict(row)

    async def list_all(
        self,
        conn: asyncpg.Connection,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(self.list_all_query)
        return [_row_to_dict(row) for row in rows if row is not None]

    async def list_open(
        self,
        conn: asyncpg.Connection,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(self.list_open_query)
        return [_row_to_dict(row) for row in rows if row is not None]

    async def list_open_for_projection(
        self,
        conn: asyncpg.Connection,
    ) -> list[Position]:
        """
        PostgreSQL 원본 기준 OPEN position 목록 조회.

        Redis position projection rebuild에 사용한다.

        numeric 컬럼은 Position.model_validate()와 Redis 저장을 쉽게 하기 위해
        text로 cast한다.
        """
        # rows = await conn.fetch(
        #     f"""
        #     SELECT
        #         position_id,
        #         exchange,
        #         market_type,
        #         symbol,
        #         position_side,
        #         status,

        #         position_amt::text AS position_amt,
        #         entry_price::text AS entry_price,
        #         break_even_price::text AS break_even_price,
        #         mark_price::text AS mark_price,

        #         unrealized_pnl::text AS unrealized_pnl,
        #         isolated_margin::text AS isolated_margin,
        #         isolated_wallet::text AS isolated_wallet,
        #         margin_type,
        #         leverage,
        #         liquidation_price::text AS liquidation_price,
        #         notional::text AS notional,

        #         update_reason,
        #         last_event_time,
        #         last_transaction_time,

        #         opened_ts,
        #         closed_ts,
        #         updated_ts,
        #         version
        #     FROM {PositionTableSchema.table_name}
        #     WHERE status = 'OPEN'
        #     ORDER BY updated_ts ASC, position_id ASC
        #     """
        # )

        rows = await conn.fetch(self.list_open_for_projection_sql)

        return [Position.model_validate(dict(row)) for row in rows]