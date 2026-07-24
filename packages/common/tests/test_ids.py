import pytest
import time
import re

from common.ids import (
    SnowflakeIdGenerator,
    DebugIdGenerator,
    generate_order_id,
    generate_order_id_int,
    generate_signal_id,
    generate_debug_signal_id,
    generate_debug_order_id,
    generate_debug_execution_id,
    make_trade_key,
    make_orderbook_update_key,
    make_kline_key
)

from common.ids import (
    CUSTOM_EPOCH_MS,
    NODE_ID_BITS,
    SEQUENCE_BITS,
    _BASE36_ALPHABET,
    MAX_NODE_ID,
    MAX_SEQUENCE,
    NODE_ID_SHIFT,
    TIMESTAMP_SHIFT,
    _PREFIX_RE
)

### CONSTANT TEST
def test_id_constant():
    assert CUSTOM_EPOCH_MS == 1704067200000
    assert NODE_ID_BITS == 10
    assert SEQUENCE_BITS == 12
    assert _BASE36_ALPHABET == "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    assert MAX_NODE_ID == (1 << NODE_ID_BITS) - 1 # 1023
    assert MAX_SEQUENCE == (1 << SEQUENCE_BITS) - 1 # 4095
    assert NODE_ID_SHIFT == SEQUENCE_BITS
    assert TIMESTAMP_SHIFT == NODE_ID_BITS + SEQUENCE_BITS
    assert _PREFIX_RE.pattern == r"^[A-Z0-9\-_]+$"

### Snowflake ID Test
def test_snowflake_unique_int_ids():
    gen = SnowflakeIdGenerator(node_id=1)

    ids = [gen.generate_int() for _ in range(300_000)]

    assert len(ids) == len(set(ids))
    assert ids == sorted(ids)


def test_snowflake_string_id():
    gen = SnowflakeIdGenerator(node_id=1)

    with pytest.raises(ValueError):
        gen.generate_str("order@id")

    oid = gen.generate_str("O")

    assert isinstance(oid, str)
    assert oid.startswith("O")
    assert len(oid) < 24

    oid = gen.generate_str("o")

    assert oid.startswith("O")

def test_snowflake_int_id_is_increasing():
    gen = SnowflakeIdGenerator(node_id=1)

    id1 = gen.generate_int()
    id2 = gen.generate_int()
    id3 = gen.generate_int()

    assert isinstance(id1, int)
    assert id1 < id2 < id3

def test_snowflake_sequence_increases_in_same_millisecond():
    class FixedTimeGenerator(SnowflakeIdGenerator):
        def _now_ms(self) -> int:
            return CUSTOM_EPOCH_MS + 1234

    gen = FixedTimeGenerator(node_id=1)

    id1 = gen.generate_int()
    id2 = gen.generate_int()
    id3 = gen.generate_int()

    assert id1 < id2 < id3

    seq1 = id1 & MAX_SEQUENCE
    seq2 = id2 & MAX_SEQUENCE
    seq3 = id3 & MAX_SEQUENCE

    assert seq1 == 0
    assert seq2 == 1
    assert seq3 == 2

def test_snowflake_id_contains_node_id():
    class FixedTimeGenerator(SnowflakeIdGenerator):
        def _now_ms(self) -> int:
            return CUSTOM_EPOCH_MS + 1234

    gen = FixedTimeGenerator(node_id=7)

    raw_id = gen.generate_int()

    node_id = (raw_id >> NODE_ID_SHIFT) & MAX_NODE_ID

    assert node_id == 7
    

def test_snowflake_clock_rollback_does_not_decrease_id():
    class RollbackTimeGenerator(SnowflakeIdGenerator):
        def __init__(self):
            super().__init__(node_id=1)
            self.times = iter([
                CUSTOM_EPOCH_MS + 1002,
                CUSTOM_EPOCH_MS + 1001,
                CUSTOM_EPOCH_MS + 1000,
            ])

        def _now_ms(self) -> int:
            return next(self.times)

    gen = RollbackTimeGenerator()

    id1 = gen.generate_int()
    id2 = gen.generate_int()
    id3 = gen.generate_int()

    assert id1 < id2 < id3


@pytest.mark.performance
def test_generate_int_id_speed():
    gen = SnowflakeIdGenerator(node_id=1)

    count = 100_000

    start = time.perf_counter()

    for _ in range(count):
        gen.generate_int()

    elapsed = time.perf_counter() - start
    ids_per_sec = count / elapsed

    print(f"\nint id speed: {ids_per_sec:,.0f} ids/sec")

    assert ids_per_sec > 100_000


@pytest.mark.performance
def test_generate_str_id_speed():
    gen = SnowflakeIdGenerator(node_id=1)

    count = 100_000

    start = time.perf_counter()

    for _ in range(count):
        gen.generate_str("O")

    elapsed = time.perf_counter() - start
    ids_per_sec = count / elapsed

    print(f"\nstr id speed: {ids_per_sec:,.0f} ids/sec")



def test_default_order_id_int():
    oid = generate_order_id_int()

    assert isinstance(oid, int)
    assert oid > 0


def test_default_order_id_string():
    oid = generate_order_id()

    assert isinstance(oid, str)
    assert oid.startswith("O")


def test_default_signal_id_string():
    sid = generate_signal_id()

    assert isinstance(sid, str)
    assert sid.startswith("S")



def test_trade_key():
    key = make_trade_key("BINANCE", "BTCUSDT", 123)

    assert key == ("BINANCE", "BTCUSDT", 123)


def test_orderbook_update_key():
    key = make_orderbook_update_key(
        "BINANCE",
        "BTCUSDT",
        100,
        120,
    )

    assert key == ("BINANCE", "BTCUSDT", 100, 120)

# DEBUG_ID
def test_debug_id_prefix_is_uppercased():
    gen = DebugIdGenerator(node_id=1, random_bytes=4)

    oid = gen.generate("ord")

    assert oid.startswith("ORD-")

def test_debug_order_id_format():
    gen = DebugIdGenerator(node_id=1, random_bytes=4)

    oid = gen.generate("ORD")

    parts = oid.split("-")

    assert parts[0] == "ORD"
    assert len(parts) == 5
    assert parts[1].isdigit()
    assert parts[2].isdigit()
    assert parts[3].startswith("N") and len(parts[3]) == 5 and parts[3][1:].isdigit()
    assert len(parts[4]) == 8

def test_default_debug_order_id_string():
    oid = generate_debug_order_id()

    parts = oid.split("-")

    assert parts[0] == "ORD"
    assert len(parts) == 5

    ts_ms = parts[1]
    sequence = parts[2]
    node_name = parts[3]
    rand_part = parts[4]

    assert ts_ms.isdigit()
    assert len(ts_ms) == 13

    assert sequence.isdigit()
    assert len(sequence) == 4

    assert len(node_name) > 0

    # DebugIdGenerator 기본 random_bytes=6
    # 6 bytes = 12 hex chars
    assert len(rand_part) == 12


def test_default_debug_signal_id_string():
    sid = generate_debug_signal_id()

    parts = sid.split("-")

    assert parts[0] == "SIG"
    assert len(parts) == 5
    assert parts[1].isdigit()
    assert parts[2].isdigit()
    assert len(parts[3]) > 0
    assert len(parts[4]) == 12


def test_default_debug_execution_id_string():
    eid = generate_debug_execution_id()

    parts = eid.split("-")

    assert parts[0] == "EXE"
    assert len(parts) == 5
    assert parts[1].isdigit()
    assert parts[2].isdigit()
    assert len(parts[3]) > 0
    assert len(parts[4]) == 12


def test_debug_id_sequence_increases_in_same_millisecond():
    class FixedTimeDebugIdGenerator(DebugIdGenerator):
        def _now_ms(self) -> int:
            return 1700000000000

    gen = FixedTimeDebugIdGenerator(node_id=1, random_bytes=4)

    id1 = gen.generate("ORD")
    id2 = gen.generate("ORD")
    id3 = gen.generate("ORD")

    assert id1.split("-")[2] == "0000"
    assert id2.split("-")[2] == "0001"
    assert id3.split("-")[2] == "0002"

def test_debug_node_id_format():
    gen = DebugIdGenerator(node_id=1, random_bytes=4)

    oid = gen.generate("ORD")

    parts = oid.split("-")

    assert parts[3] == "N0001"

def test_debug_node_id_format_max_node():
    gen = DebugIdGenerator(node_id=1023, random_bytes=4)

    oid = gen.generate("ORD")

    parts = oid.split("-")

    assert parts[3] == "N1023"

def test_default_generators_create_distinct_ids():
    oid = generate_order_id()
    sid = generate_signal_id()

    assert oid != sid
    assert oid.startswith("O")
    assert sid.startswith("S")


#### ID GENERATOR NODE ID TEST
def test_node_id_must_be_numeric():
    with pytest.raises(ValueError):
        SnowflakeIdGenerator(node_id="EX01")

    with pytest.raises(ValueError):
        DebugIdGenerator(node_id="EX01")

def test_node_id_must_be_in_range():
    with pytest.raises(ValueError):
        SnowflakeIdGenerator(node_id=-1)

    with pytest.raises(ValueError):
        SnowflakeIdGenerator(node_id=1024)

    with pytest.raises(ValueError):
        DebugIdGenerator(node_id=-1)

    with pytest.raises(ValueError):
        DebugIdGenerator(node_id=1024)


def test_node_id_accepts_numeric_string():
    gen = SnowflakeIdGenerator(node_id="7")
    debug = DebugIdGenerator(node_id="7", random_bytes=4)

    assert gen.node_id == 7
    assert debug.node_id == 7


### Market ID TEST
def test_trade_key():
    key = make_trade_key("BINANCE", "BTCUSDT", 123)

    assert isinstance(key, tuple)
    assert len(key) == 3

    exchange, symbol, trade_id = key

    assert exchange == "BINANCE"
    assert symbol == "BTCUSDT"
    assert trade_id == 123

    key1 = make_trade_key("BINANCE", "BTCUSDT", 123)
    key2 = make_trade_key("bybit", "BTCUSDT", 123)

    assert key1 != key2

    key1 = make_trade_key("BINANCE", "BTCUSDT", 123)
    key2 = make_trade_key("BINANCE", "ETHUSDT", 123)

    assert key1 != key2

    key1 = make_trade_key("BINANCE", "BTCUSDT", 123)
    key2 = make_trade_key("BINANCE", "BTCUSDT", 124)

    assert key1 != key2


def test_orderbook_update_key_structure():
    key = make_orderbook_update_key(
        "BINANCE",
        "BTCUSDT",
        100,
        120,
    )

    assert isinstance(key, tuple)
    assert len(key) == 4

    exchange, symbol, first_update_id, last_update_id = key

    assert exchange == "BINANCE"
    assert symbol == "BTCUSDT"
    assert first_update_id == 100
    assert last_update_id == 120

    key1 = make_orderbook_update_key("BINANCE", "BTCUSDT", 100, 120)
    key2 = make_orderbook_update_key("bybit", "BTCUSDT", 100, 120)

    assert key1 != key2

    key1 = make_orderbook_update_key("BINANCE", "BTCUSDT", 100, 120)
    key2 = make_orderbook_update_key("BINANCE", "ETHUSDT", 100, 120)

    assert key1 != key2

    key1 = make_orderbook_update_key("BINANCE", "BTCUSDT", 100, 120)
    key2 = make_orderbook_update_key("BINANCE", "BTCUSDT", 101, 120)
    key3 = make_orderbook_update_key("BINANCE", "BTCUSDT", 100, 121)

    assert key1 != key2
    assert key1 != key3


def test_kline_key():
    key = make_kline_key(
        "BINANCE",
        "BTCUSDT",
        "1m",
        1700000000000,
    )

    assert key == ("BINANCE", "BTCUSDT", "1m", 1700000000000)