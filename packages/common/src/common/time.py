import time
from datetime import datetime, timezone


def current_time_ms() -> int:
    """Returns current UTC time in milliseconds."""
    return int(time.time() * 1000)


def current_time_ns() -> int:
    """Returns current UTC time in nanoseconds."""
    return time.time_ns()


def format_ts_iso(ts_ms: int) -> str:
    """Formats a millisecond timestamp to ISO 8601 string."""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def epoch_ms() -> int:
    """Returns epoch time in milliseconds."""
    return time.time_ns() // 1_000_000
