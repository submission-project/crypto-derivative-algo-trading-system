from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from common.converters import enum_value
from schemas.position import Position

from decimal import Decimal

class PositionColumn(str, Enum):
    POSITION_ID = "position_id"

    EXCHANGE = "exchange"
    MARKET_TYPE = "market_type"
    SYMBOL = "symbol"
    POSITION_SIDE = "position_side"

    STATUS = "status"

    POSITION_AMT = "position_amt"
    ENTRY_PRICE = "entry_price"
    BREAK_EVEN_PRICE = "break_even_price"
    MARK_PRICE = "mark_price"

    UNREALIZED_PNL = "unrealized_pnl"
    ISOLATED_MARGIN = "isolated_margin"
    ISOLATED_WALLET = "isolated_wallet"
    MARGIN_TYPE = "margin_type"
    LEVERAGE = "leverage"
    LIQUIDATION_PRICE = "liquidation_price"
    NOTIONAL = "notional"

    UPDATE_REASON = "update_reason"
    LAST_EVENT_TIME = "last_event_time"
    LAST_TRANSACTION_TIME = "last_transaction_time"

    OPENED_TS = "opened_ts"
    CLOSED_TS = "closed_ts"
    UPDATED_TS = "updated_ts"

    VERSION = "version"


@dataclass(frozen=True)
class PgColumn:
    column: PositionColumn
    insert_cast: str | None = None


class PositionTableSchema:
    table_name = "positions"
    c = PositionColumn

    UPSERT_INSERT_COLUMNS = (
        PgColumn(c.POSITION_ID),
        PgColumn(c.EXCHANGE),
        PgColumn(c.MARKET_TYPE),
        PgColumn(c.SYMBOL),
        PgColumn(c.POSITION_SIDE),
        PgColumn(c.STATUS),

        PgColumn(c.POSITION_AMT, "::numeric"),
        PgColumn(c.ENTRY_PRICE, "::numeric"),
        PgColumn(c.BREAK_EVEN_PRICE, "::numeric"),
        PgColumn(c.MARK_PRICE, "::numeric"),
        PgColumn(c.UNREALIZED_PNL, "::numeric"),
        PgColumn(c.ISOLATED_MARGIN, "::numeric"),
        PgColumn(c.ISOLATED_WALLET, "::numeric"),
        PgColumn(c.MARGIN_TYPE),
        PgColumn(c.LEVERAGE, "::integer"),
        PgColumn(c.LIQUIDATION_PRICE, "::numeric"),
        PgColumn(c.NOTIONAL, "::numeric"),

        PgColumn(c.UPDATE_REASON),
        PgColumn(c.LAST_EVENT_TIME, "::bigint"),
        PgColumn(c.LAST_TRANSACTION_TIME, "::bigint"),

        PgColumn(c.OPENED_TS, "::bigint"),
        PgColumn(c.CLOSED_TS, "::bigint"),
        PgColumn(c.UPDATED_TS, "::bigint"),
        PgColumn(c.VERSION, "::bigint"),
    )

    UPSERT_UPDATE_COLUMNS = (
        PgColumn(c.STATUS),
        PgColumn(c.POSITION_AMT),
        PgColumn(c.ENTRY_PRICE),
        PgColumn(c.BREAK_EVEN_PRICE),
        PgColumn(c.MARK_PRICE),
        PgColumn(c.UNREALIZED_PNL),
        PgColumn(c.ISOLATED_MARGIN),
        PgColumn(c.ISOLATED_WALLET),
        PgColumn(c.MARGIN_TYPE),
        PgColumn(c.LEVERAGE),
        PgColumn(c.LIQUIDATION_PRICE),
        PgColumn(c.NOTIONAL),
        PgColumn(c.UPDATE_REASON),
        PgColumn(c.LAST_EVENT_TIME),
        PgColumn(c.LAST_TRANSACTION_TIME),
        PgColumn(c.UPDATED_TS),
    )

    NUMERIC_TEXT_COLUMNS = {
        PositionColumn.POSITION_AMT,
        PositionColumn.ENTRY_PRICE,
        PositionColumn.BREAK_EVEN_PRICE,
        PositionColumn.MARK_PRICE,
        PositionColumn.UNREALIZED_PNL,
        PositionColumn.ISOLATED_MARGIN,
        PositionColumn.ISOLATED_WALLET,
        PositionColumn.LIQUIDATION_PRICE,
        PositionColumn.NOTIONAL,
    }

    PROJECTION_SELECT_COLUMNS = (
        PgColumn(c.POSITION_ID),
        PgColumn(c.EXCHANGE),
        PgColumn(c.MARKET_TYPE),
        PgColumn(c.SYMBOL),
        PgColumn(c.POSITION_SIDE),
        PgColumn(c.STATUS),
        PgColumn(c.POSITION_AMT),
        PgColumn(c.ENTRY_PRICE),
        PgColumn(c.BREAK_EVEN_PRICE),
        PgColumn(c.MARK_PRICE),
        PgColumn(c.UNREALIZED_PNL),
        PgColumn(c.ISOLATED_MARGIN),
        PgColumn(c.ISOLATED_WALLET),
        PgColumn(c.MARGIN_TYPE),
        PgColumn(c.LEVERAGE),
        PgColumn(c.LIQUIDATION_PRICE),
        PgColumn(c.NOTIONAL),
        PgColumn(c.UPDATE_REASON),
        PgColumn(c.LAST_EVENT_TIME),
        PgColumn(c.LAST_TRANSACTION_TIME),
        PgColumn(c.OPENED_TS),
        PgColumn(c.CLOSED_TS),
        PgColumn(c.UPDATED_TS),
        PgColumn(c.VERSION),
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
    def row_get(cls, row: dict[str, Any], column: PositionColumn) -> Any:
        return row[column.value]

    @classmethod
    def value_from_position(
        cls,
        *,
        position: Position,
        column: PositionColumn,
    ) -> Any:
        attr_name = column.value

        if not hasattr(position, attr_name):
            raise AttributeError(
                f"Position has no field for postgres column: {column.value}"
            )

        value = getattr(position, attr_name)
        if column == PositionColumn.SYMBOL and value is not None:
            return str(value).upper()

        return enum_value(value)

    
    @classmethod
    def excluded_update_set_clause(
        cls,
        *,
        columns: tuple[PgColumn, ...],
    ) -> str:
        return ",\n".join(
            f"{col.column.value} = EXCLUDED.{col.column.value}"
            for col in columns
        )

    @classmethod
    def upsert_insert_values(cls, position: Position) -> tuple[Any, ...]:
        position_amt = Decimal(str(position.position_amt))
        is_open = position_amt != Decimal("0")

        special_values = {
            PositionColumn.OPENED_TS: position.updated_ts if is_open else None,
            PositionColumn.CLOSED_TS: position.updated_ts if not is_open else None,
            PositionColumn.VERSION: 1,
        }

        return tuple(
            special_values.get(
                col.column,
                cls.value_from_position(
                    position=position,
                    column=col.column,
                ),
            )
            for col in cls.UPSERT_INSERT_COLUMNS
        )

    @classmethod
    def projection_select_names(cls) -> str:
        values = []

        for col in cls.PROJECTION_SELECT_COLUMNS:
            name = col.column.value
            if col.column in cls.NUMERIC_TEXT_COLUMNS:
                values.append(f"{name}::text AS {name}")
            else:
                values.append(name)

        return ",\n".join(values)
