import os
import re
import time
import secrets
import threading

from .env import ENV_KEY_APP_NODE_ID
from .time import current_time_ms


"""
# DO NOT CHANGE.

"""
# ID format
"""
bit: 43                          33    22   12   0

|---------- timestamp ----------| node | seq | id |

timestamp:  milliseconds since custom_epoch_ms
node:       0 ~ 1023
seq:        0 ~ 4095
"""
# 2024-01-01T00:00:00Z
CUSTOM_EPOCH_MS = 1704067200000

NODE_ID_BITS = 10
SEQUENCE_BITS = 12
_BASE36_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

MAX_NODE_ID = (1 << NODE_ID_BITS) - 1 # 1023
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1 # 4095

NODE_ID_SHIFT = SEQUENCE_BITS
TIMESTAMP_SHIFT = NODE_ID_BITS + SEQUENCE_BITS

_PREFIX_RE = re.compile(r"^[A-Z0-9\-_]+$")


def _base36(n: int) -> str:
    if n < 0:
        raise ValueError("base36 only supports non-negative integers")

    if n == 0:
        return "0"

    chars: list[str] = []

    while n:
        n, r = divmod(n, 36)
        chars.append(_BASE36_ALPHABET[r])

    return "".join(reversed(chars))


def _parse_node_id(value: str | int | None) -> int:
    """
    Convert node id to 0~1023 integer.

    APP_NODE_ID must be explicitly set and numeric.

    Valid:
        APP_NODE_ID=0
        APP_NODE_ID=1
        APP_NODE_ID=1023

    Invalid:
        APP_NODE_ID missing
        APP_NODE_ID=""
        APP_NODE_ID=EX01
        APP_NODE_ID=abc
        APP_NODE_ID=1024
    """
    if value is None:
        value = os.getenv(ENV_KEY_APP_NODE_ID)

    if value is None:
        raise ValueError(
            f"{ENV_KEY_APP_NODE_ID} is required. "
            f"Set it to an integer between 0 and {MAX_NODE_ID}."
        )

    if isinstance(value, int):
        node_id = value
    else:
        value = value.strip()

        if value == "":
            raise ValueError(
                f"{ENV_KEY_APP_NODE_ID} is required. "
                f"Set it to an integer between 0 and {MAX_NODE_ID}."
            )

        if not value.isdigit():
            raise ValueError(
                f"{ENV_KEY_APP_NODE_ID} must be an integer between 0 and {MAX_NODE_ID}. "
                f"Got: {value!r}"
            )

        node_id = int(value)

    if not 0 <= node_id <= MAX_NODE_ID:
        raise ValueError(
            f"{ENV_KEY_APP_NODE_ID} must be between 0 and {MAX_NODE_ID}. "
            f"Got: {node_id}"
        )

    return node_id


# 입력받은 접두사(예: "ORD")가 규칙에 맞는지 확인하고 대문자로 바꿈
def _validate_prefix(prefix: str) -> str:
    prefix = prefix.upper()

    if not _PREFIX_RE.match(prefix):
        raise ValueError(f"Invalid prefix: {prefix}")

    return prefix


class SnowflakeIdGenerator:
    """
    HFT-friendly sortable integer ID generator.

    64-bit layout:
        timestamp_ms_since_custom_epoch | node_id | sequence

    Capacity per node:
        4096 IDs / millisecond
        about 4,096,000 IDs / second
    """

    def __init__(self, node_id: str | int | None = None) -> None:
        self.node_id = _parse_node_id(node_id)

        self._lock = threading.Lock()
        self._last_ts_ms = -1
        self._sequence = 0

    def _now_ms(self) -> int:
        return current_time_ms()

    def _wait_next_ms(self, last_ts_ms: int) -> int:
        ts_ms = self._now_ms()

        # Clock rollback protection
        while ts_ms <= last_ts_ms:
            time.sleep(0)
            ts_ms = self._now_ms()

        return ts_ms

    def generate_int(self) -> int:
        """
        Generate sortable int64-like ID.
        """

        # lock 확보
        with self._lock:
            ts_ms = self._now_ms() - CUSTOM_EPOCH_MS

            if ts_ms < 0:
                raise ValueError("current time is earlier than CUSTOM_EPOCH_MS")

            # Clock rollback protection
            if ts_ms < self._last_ts_ms:
                ts_ms = self._last_ts_ms

            if ts_ms == self._last_ts_ms:
                self._sequence = (self._sequence + 1) & MAX_SEQUENCE

                # Sequence exhausted in the same millisecond
                if self._sequence == 0:
                    absolute_last_ms = self._last_ts_ms + CUSTOM_EPOCH_MS
                    absolute_next_ms = self._wait_next_ms(absolute_last_ms)
                    ts_ms = absolute_next_ms - CUSTOM_EPOCH_MS
            else:
                self._sequence = 0

            self._last_ts_ms = ts_ms

            return (
                (ts_ms << TIMESTAMP_SHIFT)
                | (self.node_id << NODE_ID_SHIFT)
                | self._sequence
            )

    def generate_str(self, prefix: str = "") -> str:
        """
        Generate compact base36 string ID.

        Example:
            O2B9X6V9M3QO0
            S2B9X6V9M3QO1
            
        S: Signal
        O: Order
        X: Execution
        E: Internal Order
        """
        raw_id = self.generate_int()
        encoded = _base36(raw_id)

        if prefix:
            prefix = _validate_prefix(prefix)
            return f"{prefix}{encoded}"

        return encoded


class DebugIdGenerator:
    """
    Human-readable debug ID generator.

    Format:
        PREFIX-TS_MS-SEQUENCE-NODE-RANDOM

    Example:
        ORD-1714280000123-0001-N0001-a3f91c2e77aa
    """

    def __init__(
        self,
        node_id: str | int | None = None,
        random_bytes: int = 6,
    ) -> None:
        self.node_id = _parse_node_id(node_id)
        self.node_name = f"N{self.node_id:04d}".upper()
        self.random_bytes = random_bytes
        self._lock = threading.Lock()
        self._last_ms = 0
        self._sequence = 0

    def _now_ms(self) -> int:
        return current_time_ms()

    def generate(self, prefix: str) -> str:
        prefix = _validate_prefix(prefix)

        # Thread-safety 확보
        with self._lock:
            now_ms = self._now_ms()

            # 시간 역행 방지
            if now_ms < self._last_ms:
                now_ms = self._last_ms

            # 같은 밀리초 동안 여러번 호출 시 시퀀스 증가
            if now_ms == self._last_ms:
                self._sequence += 1
            else:
                self._last_ms = now_ms
                self._sequence = 0

            # 시퀀스 한계 초과 시 다음 밀리초까지 대기
            if self._sequence > MAX_SEQUENCE:
                while True:
                    now_ms = self._now_ms()
                    if now_ms > self._last_ms:
                        self._last_ms = now_ms
                        self._sequence = 0
                        break
                    time.sleep(0)

            ts_ms = self._last_ms
            sequence = self._sequence

        rand_part = secrets.token_hex(self.random_bytes)

        # Ex) ORD-17764280000123-0001-N0001-a3f91c2e77aa
        return f"{prefix}-{ts_ms:013d}-{sequence:04d}-{self.node_name}-{rand_part}"


_snowflake_generator = SnowflakeIdGenerator()
_debug_generator = DebugIdGenerator()


# Hot-path internal IDs

def generate_id_int() -> int:
    return _snowflake_generator.generate_int()


def generate_signal_id_int() -> int:
    return _snowflake_generator.generate_int()


def generate_order_id_int() -> int:
    return _snowflake_generator.generate_int()


def generate_execution_id_int() -> int:
    return _snowflake_generator.generate_int()


# Compact string IDs

def _build_prefix(base: str, exchange: str | None, market_type: str | None) -> str:
    parts = [base]
    if exchange:
        parts.append(str(exchange).upper())
    if market_type:
        parts.append(str(market_type).upper())
    
    if len(parts) > 1:
        return "-".join(parts) + "-"
    return base


def generate_signal_id(exchange: str | None = None, market_type: str | None = None) -> str:
    return _snowflake_generator.generate_str(_build_prefix("S", exchange, market_type))


def generate_order_id(exchange: str | None = None, market_type: str | None = None) -> str:
    return _snowflake_generator.generate_str(_build_prefix("O", exchange, market_type))


def generate_execution_id(exchange: str | None = None, market_type: str | None = None) -> str:
    return _snowflake_generator.generate_str(_build_prefix("X", exchange, market_type))


def generate_event_id() -> str:
    return _snowflake_generator.generate_str("E")


# Debug-readable IDs

def generate_debug_signal_id() -> str:
    return _debug_generator.generate("SIG")


def generate_debug_order_id() -> str:
    return _debug_generator.generate("ORD")


def generate_debug_execution_id() -> str:
    return _debug_generator.generate("EXE")


# Market-data keys
# tick / trade / orderbook update에는 자체 UUID를 만들지 않고,
# 거래소가 제공하는 ID를 기반으로 key만 만든다.

def make_trade_key(exchange: str, symbol: str, trade_id: int) -> tuple[str, str, int]:
    return exchange, symbol, trade_id


def make_orderbook_update_key(
    exchange: str,
    symbol: str,
    first_update_id: int,
    last_update_id: int,
) -> tuple[str, str, int, int]:
    return exchange, symbol, first_update_id, last_update_id


def make_kline_key(
    exchange: str,
    symbol: str,
    interval: str,
    open_time_ms: int,
) -> tuple[str, str, str, int]:
    return exchange, symbol, interval, open_time_ms