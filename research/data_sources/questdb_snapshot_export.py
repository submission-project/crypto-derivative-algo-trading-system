"""
Export QuestDB time-series tables to reproducible parquet snapshots.

The production path stores market data in QuestDB.  Research notebooks should
use immutable snapshot files, so this module bridges the two layers with an
explicit time range and query audit trail.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx
import pandas as pd


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class QuestDBExportResult:
    table: str
    query: str
    output_path: Path
    rows: int


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe QuestDB identifier: {identifier!r}")
    return identifier


def build_time_range_query(
    *,
    table: str,
    start: str,
    end: str,
    timestamp_col: str = "timestamp",
    columns: Iterable[str] | str = "*",
) -> str:
    """Build a bounded QuestDB SQL query for snapshot export."""
    table_name = _quote_identifier(table)
    ts_col = _quote_identifier(timestamp_col)
    if isinstance(columns, str):
        column_sql = columns if columns == "*" else _quote_identifier(columns)
    else:
        column_sql = ", ".join(_quote_identifier(column) for column in columns)
    return (
        f"SELECT {column_sql} FROM {table_name} "
        f"WHERE {ts_col} >= '{start}' AND {ts_col} <= '{end}' "
        f"ORDER BY {ts_col}"
    )


def questdb_exec_json_to_frame(payload: dict) -> pd.DataFrame:
    """Convert QuestDB /exec JSON response into a pandas DataFrame."""
    columns = [column["name"] for column in payload.get("columns", [])]
    dataset = payload.get("dataset", [])
    if not columns:
        return pd.DataFrame(dataset)
    return pd.DataFrame(dataset, columns=columns)


def fetch_questdb_query(
    *,
    base_url: str,
    query: str,
    timeout_s: float = 60.0,
) -> pd.DataFrame:
    """Run a QuestDB HTTP /exec query and return a DataFrame."""
    url = base_url.rstrip("/") + "/exec"
    response = httpx.get(url, params={"query": query}, timeout=timeout_s)
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(f"QuestDB query failed: {payload['error']}")
    return questdb_exec_json_to_frame(payload)


def export_questdb_table_to_parquet(
    *,
    base_url: str,
    table: str,
    output_path: str | Path,
    start: str,
    end: str,
    timestamp_col: str = "timestamp",
    columns: Iterable[str] | str = "*",
    timeout_s: float = 60.0,
) -> QuestDBExportResult:
    """Export one QuestDB table to a parquet file with a fixed time range."""
    query = build_time_range_query(
        table=table,
        start=start,
        end=end,
        timestamp_col=timestamp_col,
        columns=columns,
    )
    frame = fetch_questdb_query(base_url=base_url, query=query, timeout_s=timeout_s)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return QuestDBExportResult(
        table=table,
        query=query,
        output_path=path,
        rows=len(frame),
    )
