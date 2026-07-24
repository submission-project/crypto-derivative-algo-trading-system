from __future__ import annotations

import json
from typing import Any

import asyncpg

from schemas.outbox import OutboxEvent

from .domain.outbox_table import OutboxTableSchema, OutboxColumn


def _decode_jsonb_payload(value: Any) -> dict[str, Any]:
    """
    asyncpg는 json/jsonb를 보통 dict로 돌려주지만,
    연결/드라이버 설정에 따라 str로 올 수 있어 양쪽을 지원한다.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, (bytes, bytearray)):
        return json.loads(bytes(value).decode("utf-8"))
    raise TypeError(f"unsupported outbox payload type: {type(value)!r}")


def _outbox_event_from_row(row: asyncpg.Record) -> OutboxEvent:
    data = dict(row)
    data["payload"] = _decode_jsonb_payload(data["payload"])
    return OutboxEvent.model_validate(data)


class OutboxPostgresRepository:
    """
    transactional outbox 테이블 접근.

    역할:
      - 같은 PG 트랜잭션 안에서 이벤트 insert
      - 발행 대상 이벤트 claim
      - 발행 성공/실패 기록
    """

    def __init__(self) -> None:
        self._table_schema = OutboxTableSchema()
        self.table_name = self._table_schema.table_name

        self.insert_columns = self._table_schema.INSERT_COLUMNS
        self.claim_returning_columns = self._table_schema.CLAIM_RETURNING_COLUMNS

        self.insert_sql = f"""
            INSERT INTO {self.table_name} (
                {self._table_schema.names(self.insert_columns)}
            )
            VALUES (
                {self._table_schema.placeholders(self.insert_columns)}
            )
            RETURNING *
            """

        self.claim_unpublished_sql = f"""
            WITH candidate AS (
                SELECT {OutboxColumn.EVENT_ID.value}
                FROM {self.table_name}
                WHERE {OutboxColumn.PUBLISHED_TS.value} IS NULL
                  AND {OutboxColumn.RETRY_COUNT.value} < $1
                  AND ({OutboxColumn.NEXT_ATTEMPT_TS.value} IS NULL OR {OutboxColumn.NEXT_ATTEMPT_TS.value} <= $2)
                  AND ({OutboxColumn.LOCKED_UNTIL_TS.value} IS NULL OR {OutboxColumn.LOCKED_UNTIL_TS.value} <= $2)
                ORDER BY {OutboxColumn.CREATED_TS.value} ASC, {OutboxColumn.EVENT_ID.value} ASC
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {self.table_name} o
            SET
                {OutboxColumn.LOCKED_BY.value} = $4,
                {OutboxColumn.LOCKED_UNTIL_TS.value} = $5::bigint
            FROM candidate c
            WHERE o.{OutboxColumn.EVENT_ID.value} = c.{OutboxColumn.EVENT_ID.value}
            RETURNING
                {self._table_schema.returning_names(
                    self.claim_returning_columns
                )}
            """

        self.mark_published_sql = f"""
            UPDATE {self.table_name}
            SET
                {OutboxColumn.PUBLISHED_TS.value} = $3::bigint,
                {OutboxColumn.LOCKED_BY.value} = NULL,
                {OutboxColumn.LOCKED_UNTIL_TS.value} = NULL,
                {OutboxColumn.LAST_ERROR.value} = NULL
            WHERE {OutboxColumn.EVENT_ID.value} = $1
              AND {OutboxColumn.LOCKED_BY.value} = $2
              AND {OutboxColumn.PUBLISHED_TS.value} IS NULL
            """

        self.mark_failed_sql = f"""
            UPDATE {self.table_name}
            SET
                {OutboxColumn.RETRY_COUNT.value} = {OutboxColumn.RETRY_COUNT.value} + 1,
                {OutboxColumn.LAST_ERROR.value} = $3,
                {OutboxColumn.NEXT_ATTEMPT_TS.value} = $4,
                {OutboxColumn.LOCKED_BY.value} = NULL,
                {OutboxColumn.LOCKED_UNTIL_TS.value} = NULL
            WHERE {OutboxColumn.EVENT_ID.value} = $1
              AND {OutboxColumn.LOCKED_BY.value} = $2
              AND {OutboxColumn.PUBLISHED_TS.value} IS NULL
            """

        self.count_unpublished_sql = f"""
            SELECT count(*)
            FROM {self.table_name}
            WHERE {OutboxColumn.PUBLISHED_TS.value} IS NULL
            """

    async def insert(
        self,
        *,
        conn: asyncpg.Connection,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
        created_ts: int,
        aggregate_type: str = "ORDER",
    ) -> OutboxEvent:
        row = await conn.fetchrow(
            self.insert_sql,
            *self._table_schema.insert_values(
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=payload,
                created_ts=created_ts,
                aggregate_type=aggregate_type,
            ),
        )
        if row is None:
            raise RuntimeError(
                f"outbox insert did not return row: "
                f"aggregate_id={aggregate_id}, event_type={event_type}"
            )
        return _outbox_event_from_row(row)

    async def claim_unpublished(
        self,
        *,
        conn: asyncpg.Connection,
        publisher_id: str,
        now_ms: int,
        batch_size: int,
        lock_ttl_ms: int,
        max_retry_count: int,
    ) -> list[OutboxEvent]:
        

        """
        아직 발행되지 않은 outbox 이벤트를 claim한다.

        조건:
          - published_ts IS NULL
          - retry_count < max_retry_count
          - next_attempt_ts가 없거나 현재보다 과거
          - lock이 없거나 만료됨

        SKIP LOCKED를 사용해서 여러 publisher가 있어도 같은 row를
        동시에 claim하지 않게 한다.
        """
        # SQL 쿼리 분석:
        #   1. (next_attempt_ts IS NULL OR next_attempt_ts <= $2) => 재시도 스케줄링(Retry Scheduling)을 관리
        #      next_attempt_ts IS NULL: 이전에 실패한 적이 없는 "따끈따끈한 새 이벤트"라는 뜻입니다. 당연히 지금 바로 가져와야 합니다.
        #      next_attempt_ts <= $2 (현재 시간): 이전에 처리에 실패해서 "다음엔 $2 시점 이후에 다시 시도해라"라고 예약된 이벤트입니다. 
        #       현재 시간이 그 예약 시간보다 지나갔으므로, "다시 시도할 때가 되었다"는 뜻

        #   2. (locked_until_ts IS NULL OR locked_until_ts <= $2) => 분산 락의 타임아웃(Lock Timeout)을 관리
        #      locked_until_ts IS NULL: 아무도 이 데이터를 잡고 있지 않습니다. 즉시 가져가도 안전
        #      locked_until_ts <= $2 (현재 시간): 어떤 워커가 이 이벤트를 가져가서 잠금을 걸었었지만, 설정한 시간(TTL) 내에 처리를 완료하지 못하고 시간이 초과된 경우
        #        예를 들어, 워커가 이벤트를 가져가자마자 서버가 다운(Crash)되었다면 이 이벤트는 영원히 잠겨있을 수 있습니다.
        #        이 조건 덕분에 다른 워커가 "어, 이거 시간이 지났는데 아직 처리가 안 됐네? 내가 대신 할게" 하고 죽은 워커의 작업을 회수(Recovery)할 수 있게 됨
        rows = await conn.fetch(
            self.claim_unpublished_sql,
            max_retry_count,
            now_ms,
            batch_size,
            publisher_id,
            now_ms + lock_ttl_ms,
        )

        return [_outbox_event_from_row(row) for row in rows]

    async def mark_published(
        self,
        *,
        conn: asyncpg.Connection,
        event_id: int,
        publisher_id: str,
        published_ts: int,
    ) -> bool:
        """
        발행 성공 처리.

        locked_by가 현재 publisher와 일치할 때만 published_ts를 찍는다.
        """
        result = await conn.execute(
            self.mark_published_sql,
            event_id,
            publisher_id,
            published_ts,
        )

        return result.endswith("1")

    async def mark_failed(
        self,
        *,
        conn: asyncpg.Connection,
        event_id: int,
        publisher_id: str,
        now_ms: int,
        error: str,
        retry_delay_ms: int,
    ) -> bool:
        """
        발행 실패 처리.

        retry_count 증가 후 next_attempt_ts를 설정한다.
        """
        result = await conn.execute(
            self.mark_failed_sql,
            event_id,
            publisher_id,
            error[:2000],
            now_ms + retry_delay_ms,
        )

        return result.endswith("1")

    async def count_unpublished(
        self,
        *,
        conn: asyncpg.Connection,
    ) -> int:
        value = await conn.fetchval(
            self.count_unpublished_sql
        )
        return int(value or 0)
