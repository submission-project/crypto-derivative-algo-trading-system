from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class OutboxColumn(str, Enum):
    EVENT_ID = "event_id"

    AGGREGATE_TYPE = "aggregate_type"
    AGGREGATE_ID = "aggregate_id"

    EVENT_TYPE = "event_type"
    PAYLOAD = "payload"

    CREATED_TS = "created_ts"

    PUBLISHED_TS = "published_ts"
    RETRY_COUNT = "retry_count"
    LAST_ERROR = "last_error"

    NEXT_ATTEMPT_TS = "next_attempt_ts"
    LOCKED_BY = "locked_by"
    LOCKED_UNTIL_TS = "locked_until_ts"


@dataclass(frozen=True)
class PgColumn:
    column: OutboxColumn
    insert_cast: str | None = None


class OutboxTableSchema:
    table_name = "outbox_events"
    c = OutboxColumn

    INSERT_COLUMNS = (
        PgColumn(c.AGGREGATE_TYPE),
        PgColumn(c.AGGREGATE_ID),
        PgColumn(c.EVENT_TYPE),
        PgColumn(c.PAYLOAD, "::jsonb"),
        PgColumn(c.CREATED_TS, "::bigint"),
    )

    CLAIM_RETURNING_COLUMNS = (
        PgColumn(c.EVENT_ID),
        PgColumn(c.AGGREGATE_TYPE),
        PgColumn(c.AGGREGATE_ID),
        PgColumn(c.EVENT_TYPE),
        PgColumn(c.PAYLOAD),
        PgColumn(c.CREATED_TS),
        PgColumn(c.RETRY_COUNT),
    )

    CLAIM_UPDATE_COLUMNS = (
        PgColumn(c.LOCKED_BY),
        PgColumn(c.LOCKED_UNTIL_TS, "::bigint"),
    )

    MARK_PUBLISHED_UPDATE_COLUMNS = (
        PgColumn(c.PUBLISHED_TS, "::bigint"),
        PgColumn(c.LOCKED_BY),
        PgColumn(c.LOCKED_UNTIL_TS),
        PgColumn(c.LAST_ERROR),
    )

    MARK_FAILED_UPDATE_COLUMNS = (
        PgColumn(c.RETRY_COUNT),
        PgColumn(c.LAST_ERROR),
        PgColumn(c.NEXT_ATTEMPT_TS, "::bigint"),
        PgColumn(c.LOCKED_BY),
        PgColumn(c.LOCKED_UNTIL_TS),
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
    def returning_names(cls, columns: tuple[PgColumn, ...]) -> str:
        return ",\n".join(f"o.{col.column.value}" for col in columns)

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
    def insert_values(
        cls,
        *,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_ts: int,
        aggregate_type: str = "ORDER",
    ) -> tuple[Any, ...]:
        return (
            aggregate_type,
            aggregate_id,
            event_type,
            json.dumps(payload, ensure_ascii=False),
            created_ts,
        )

    @classmethod
    def row_get(cls, row: dict[str, Any], column: OutboxColumn) -> Any:
        return row[column.value]
