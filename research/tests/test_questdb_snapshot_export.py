import pytest

from research.data_sources.questdb_snapshot_export import (
    build_time_range_query,
    questdb_exec_json_to_frame,
)


def test_build_time_range_query_with_safe_identifiers():
    query = build_time_range_query(
        table="market_open_interest",
        columns=("timestamp", "exchange", "symbol", "open_interest_value_usd"),
        timestamp_col="timestamp",
        start="2026-04-15T00:00:00.000000Z",
        end="2026-07-21T05:00:00.000000Z",
    )

    assert query == (
        "SELECT timestamp, exchange, symbol, open_interest_value_usd FROM market_open_interest "
        "WHERE timestamp >= '2026-04-15T00:00:00.000000Z' "
        "AND timestamp <= '2026-07-21T05:00:00.000000Z' "
        "ORDER BY timestamp"
    )


def test_build_time_range_query_rejects_unsafe_identifier():
    with pytest.raises(ValueError):
        build_time_range_query(
            table="market_open_interest; DROP TABLE x",
            start="2026-04-15",
            end="2026-04-16",
        )


def test_questdb_exec_json_to_frame():
    frame = questdb_exec_json_to_frame(
        {
            "columns": [{"name": "timestamp"}, {"name": "open_interest"}],
            "dataset": [["2026-04-15T00:00:00.000000Z", 100.0]],
        }
    )

    assert list(frame.columns) == ["timestamp", "open_interest"]
    assert frame.iloc[0]["open_interest"] == 100.0
