from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class OutboxEvent(BaseModel):
    event_id: int
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    created_ts: int
    published_ts: int | None = None
    retry_count: int = 0
    last_error: str | None = None
    next_attempt_ts: int | None = None
    locked_by: str | None = None
    locked_until_ts: int | None = None
