import pytest
from common.time import current_time_ms, current_time_ns, format_ts_iso

def test_current_time_ms():
    ts = current_time_ms()
    assert isinstance(ts, int)
    assert ts > 0

def test_current_time_ns():
    ms = current_time_ms()
    ns = current_time_ns()

    assert isinstance(ns, int)
    assert ns > 0
    assert ns // 1_000_000 >= ms

def test_format_ts_iso():
    ts = 1700000000000  # Example ms timestamp
    iso = format_ts_iso(ts)
    assert "2023-11-14" in iso
    assert "T" in iso
