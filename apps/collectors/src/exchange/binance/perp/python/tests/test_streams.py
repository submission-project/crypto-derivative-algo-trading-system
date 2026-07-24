"""
Stream 클래스 테스트 — TradeStream, AggTradeStream

- 앞쪽: mock 기반 유닛 테스트 (분기/계약 검증)
- 뒤쪽: 실 Binance WebSocket 통합 테스트
  (네트워크 필요. SKIP_BINANCE_INTEGRATION=1 시 skip)
"""
import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from binance_perp_trade.config import settings  # noqa: WPS433
from binance_perp_trade.streams.trade_stream import TradeStream
from binance_perp_trade.streams.aggtrade_stream import AggTradeStream
from binance_perp_trade.core.health_monitor import HealthMonitor, HealthStatus
from binance_perp_trade.core.gap_detector import GapDetector
from binance_perp_trade.core.fallback_controller import FallbackController

@pytest.fixture
def mock_producers():
    producer_raw = AsyncMock()
    producer_canonical = AsyncMock()
    producer_repair = AsyncMock()
    return producer_raw, producer_canonical, producer_repair


@pytest.fixture
def mock_core_components():
    health = MagicMock()
    health.on_message.return_value = HealthStatus.HEALTHY

    fallback = MagicMock()
    fallback.is_primary = True
    fallback.is_fallback = False

    gap = MagicMock()
    gap.check.return_value = (None, None)
    gap.should_fallback.return_value = False

    return health, fallback, gap


# ── TradeStream ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_trade_stream_healthy_message(mock_producers, mock_core_components):
    """정상 @trade 메시지 수신 → raw + canonical 발행 확인"""
    producer_raw, producer_canonical, _ = mock_producers
    health, fallback, gap = mock_core_components

    stream = TradeStream(
        symbols=["BTCUSDT"],
        base_ws_url="wss://test.com",
        producer_raw=producer_raw,
        producer_canonical=producer_canonical,
        health=health,
        fallback=fallback,
        gap=gap,
    )

    test_msg = json.dumps({
        "data": {
            "e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 100,
            "p": "70000", "q": "1.0", "m": False, "T": 123456780,
        }
    })

    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [test_msg, asyncio.CancelledError()]

    with patch("websockets.connect", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_ws))):
        try:
            await asyncio.wait_for(stream.run(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    # Raw producer: produce(symbol, data) 호출 확인
    producer_raw.produce.assert_called_once()
    key, data = producer_raw.produce.call_args[0]
    assert key == "BTCUSDT"
    assert data["s"] == "BTCUSDT"

    # Canonical producer: normalize_ws_trade 결과 확인
    producer_canonical.produce.assert_called_once()
    key, canonical = producer_canonical.produce.call_args[0]
    assert key == "BTCUSDT"
    assert canonical["trade_id"] == 100  # int (not str)
    assert canonical["price"] == "70000"  # 문자열 유지
    assert canonical["verified_by_rest"] is False
    assert canonical["reconstructed_from_agg"] is False


@pytest.mark.asyncio
async def test_trade_stream_gap_calls_symbol(mock_producers, mock_core_components):
    """GapDetector가 심볼별로 호출되는지 확인"""
    producer_raw, producer_canonical, _ = mock_producers
    health, fallback, gap = mock_core_components

    stream = TradeStream(
        symbols=["BTCUSDT"],
        base_ws_url="wss://test.com",
        producer_raw=producer_raw,
        producer_canonical=producer_canonical,
        health=health,
        fallback=fallback,
        gap=gap,
    )

    test_msg = json.dumps({
        "data": {
            "e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 100,
            "p": "70000", "q": "1.0", "m": False, "T": 123456780,
        }
    })

    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [test_msg, asyncio.CancelledError()]

    with patch("websockets.connect", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_ws))):
        try:
            await asyncio.wait_for(stream.run(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    # GapDetector.check가 심볼과 함께 호출됐는지 확인
    gap.check.assert_called_once_with("BTCUSDT", 100)
    gap.should_fallback.assert_called_once_with("BTCUSDT")


@pytest.mark.asyncio
async def test_trade_stream_fallback_skips_canonical(mock_producers, mock_core_components):
    """Fallback 모드에서는 canonical 발행 안 함"""
    producer_raw, producer_canonical, _ = mock_producers
    health, fallback, gap = mock_core_components
    fallback.is_primary = False
    fallback.is_fallback = True

    stream = TradeStream(
        symbols=["BTCUSDT"],
        base_ws_url="wss://test.com",
        producer_raw=producer_raw,
        producer_canonical=producer_canonical,
        health=health,
        fallback=fallback,
        gap=gap,
    )

    test_msg = json.dumps({
        "data": {
            "e": "trade", "E": 123456789, "s": "BTCUSDT", "t": 100,
            "p": "70000", "q": "1.0", "m": False, "T": 123456780,
        }
    })

    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [test_msg, asyncio.CancelledError()]

    with patch("websockets.connect", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_ws))):
        try:
            await asyncio.wait_for(stream.run(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    producer_raw.produce.assert_called_once()
    producer_canonical.produce.assert_not_called()


# ── AggTradeStream ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_aggtrade_stream_publishes_repair_job(mock_producers, mock_core_components):
    """Fallback 모드에서 aggTrade 수신 → repair job 발행 (canonical 아님)"""
    producer_raw, _, producer_repair = mock_producers
    _, fallback, _ = mock_core_components
    fallback.is_fallback = True
    fallback.is_primary = False

    stream = AggTradeStream(
        symbols=["BTCUSDT"],
        base_ws_url="wss://test.com",
        producer_raw=producer_raw,
        producer_repair=producer_repair,
        fallback=fallback,
    )

    test_msg = json.dumps({
        "data": {
            "e": "aggTrade", "E": 123456789, "s": "BTCUSDT",
            "a": 9999, "f": 100, "l": 105,
            "p": "70000", "q": "1.0", "m": False, "T": 123456780,
        }
    })

    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [test_msg, asyncio.CancelledError()]

    with patch("websockets.connect", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_ws))):
        try:
            await asyncio.wait_for(stream.run(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    # Raw producer: aggTrade raw 이벤트 발행
    producer_raw.produce.assert_called_once()

    # Repair producer: RepairJob dict 발행
    producer_repair.produce.assert_called_once()
    key, job_data = producer_repair.produce.call_args[0]
    assert key == "BTCUSDT"
    assert job_data["from_trade_id"] == 100
    assert job_data["to_trade_id"] == 105
    assert job_data["source_agg_trade_id"] == 9999
    assert job_data["reason"] == "agg_trade_fallback"


@pytest.mark.asyncio
async def test_aggtrade_stream_primary_no_repair(mock_producers, mock_core_components):
    """Primary 모드에서는 repair job 발행 안 함"""
    producer_raw, _, producer_repair = mock_producers
    _, fallback, _ = mock_core_components
    fallback.is_fallback = False
    fallback.is_primary = True

    stream = AggTradeStream(
        symbols=["BTCUSDT"],
        base_ws_url="wss://test.com",
        producer_raw=producer_raw,
        producer_repair=producer_repair,
        fallback=fallback,
    )

    test_msg = json.dumps({
        "data": {
            "e": "aggTrade", "E": 123456789, "s": "BTCUSDT",
            "a": 9999, "f": 100, "l": 105,
            "p": "70000", "q": "1.0", "m": False, "T": 123456780,
        }
    })

    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [test_msg, asyncio.CancelledError()]

    with patch("websockets.connect", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_ws))):
        try:
            await asyncio.wait_for(stream.run(), timeout=1.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    # Raw는 항상 발행
    producer_raw.produce.assert_called_once()
    # Primary 상태면 repair job 안 보냄
    producer_repair.produce.assert_not_called()


# ── 실 Binance 통합 테스트 ──────────────────────────────────────────────────
#
# 실제 wss://fstream.binance.com 에 붙어 메시지를 수신하고, 스트림 객체가
# 정상적으로 raw / canonical / repair_job 을 발행하는지 검증.
#
# 네트워크가 필요하므로 SKIP_BINANCE_INTEGRATION=1 이면 skip.
# BTCUSDT 는 거래량이 충분히 많아 수 초 내에 메시지를 받을 수 있음.

INTEGRATION_DISABLED = os.environ.get("SKIP_BINANCE_INTEGRATION") == "1"
INTEGRATION_RECV_TIMEOUT_SEC = 10.0


def _make_capturing_producer() -> tuple[AsyncMock, list[tuple[str, dict]]]:
    """produce(symbol, data) 호출을 그대로 list 에 적재하는 fake producer."""
    captured: list[tuple[str, dict]] = []

    async def _capture(symbol: str, data: dict):
        captured.append((symbol, data))

    producer = AsyncMock()
    producer.produce.side_effect = _capture
    return producer, captured


async def _run_until_first_message_or_timeout(
    stream, captured: list, timeout_sec: float
) -> None:
    """
    스트림을 실행하되,
      - captured 에 메시지가 1개 이상 들어오면 stop
      - timeout 까지 안 들어오면 강제 cancel
    """
    task = asyncio.create_task(stream.run())
    try:
        deadline = asyncio.get_event_loop().time() + timeout_sec
        while not captured and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)
    finally:
        await stream.stop()
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance WS integration disabled)",
)
@pytest.mark.asyncio
async def test_trade_stream_real_btcusdt():
    """
    실 Binance @trade 스트림에 연결하여 한 건 이상의 메시지를 수신하고,
    raw + canonical 발행이 모두 일어나는지 검증.
    """
    producer_raw, captured_raw = _make_capturing_producer()
    producer_canonical, captured_canonical = _make_capturing_producer()

    # health threshold 를 넉넉히 → CI/로컬 시계 skew 로 FAILED 트리거되지 않게.
    stream = TradeStream(
        symbols=["BTCUSDT"],
        base_ws_url=settings.binance_perp_ws,
        producer_raw=producer_raw,
        producer_canonical=producer_canonical,
        health=HealthMonitor(critical_lag_ms=60_000, degraded_lag_ms=30_000),
        fallback=FallbackController(),
        gap=GapDetector(),
    )

    await _run_until_first_message_or_timeout(
        stream, captured_canonical, INTEGRATION_RECV_TIMEOUT_SEC
    )

    assert captured_raw, (
        "실 Binance @trade 에서 raw 메시지를 한 건도 못 받음 — 네트워크/URL 확인"
    )
    assert captured_canonical, (
        "Primary 모드인데 canonical 발행이 안 됨 — fallback 으로 빠졌을 가능성"
    )

    sym, raw = captured_raw[0]
    assert sym == "BTCUSDT"
    assert raw["e"] == "trade"
    assert raw["s"] == "BTCUSDT"
    assert int(raw["t"]) > 0  # trade_id

    sym, canonical = captured_canonical[0]
    assert sym == "BTCUSDT"
    assert canonical["exchange"] == "binance"
    assert canonical["market_type"] == "perp"
    assert canonical["symbol"] == "BTCUSDT"
    assert isinstance(canonical["trade_id"], int)
    assert canonical["trade_id"] > 0
    assert float(canonical["price"]) > 0
    assert float(canonical["size"]) >= 0
    assert canonical["verified_by_rest"] is False
    assert canonical["reconstructed_from_agg"] is False
    assert canonical["lag_ms"] is not None and canonical["lag_ms"] >= 0


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance WS integration disabled)",
)
@pytest.mark.asyncio
async def test_aggtrade_stream_real_btcusdt_fallback_emits_repair_job():
    """
    실 Binance @aggTrade 스트림에 연결한 뒤, fallback 모드를 강제로 트리거.
    aggTrade 수신 시:
      - raw 는 항상 발행
      - fallback 상태이므로 repair_job 발행도 함께 일어나야 함
    """
    producer_raw, captured_raw = _make_capturing_producer()
    producer_repair, captured_repair = _make_capturing_producer()

    fallback = FallbackController()
    fallback.trigger_fallback("forced for integration test")
    assert fallback.is_fallback, "사전 조건: fallback 모드여야 repair_job 이 나감"

    stream = AggTradeStream(
        symbols=["BTCUSDT"],
        base_ws_url=settings.binance_perp_ws,
        producer_raw=producer_raw,
        producer_repair=producer_repair,
        fallback=fallback,
    )

    await _run_until_first_message_or_timeout(
        stream, captured_repair, INTEGRATION_RECV_TIMEOUT_SEC
    )

    assert captured_raw, (
        "실 Binance @aggTrade 에서 raw 를 한 건도 못 받음 — 네트워크/URL 확인"
    )
    assert captured_repair, (
        "fallback 모드인데 repair_job 발행이 안 됨 — AggTradeStream 로직 회귀"
    )

    sym, raw = captured_raw[0]
    assert sym == "BTCUSDT"
    assert raw["e"] == "aggTrade"
    assert raw["s"] == "BTCUSDT"
    assert int(raw["a"]) > 0  # agg_trade_id
    assert int(raw["f"]) <= int(raw["l"])

    sym, job = captured_repair[0]
    assert sym == "BTCUSDT"
    assert job["symbol"] == "BTCUSDT"
    assert isinstance(job["from_trade_id"], int)
    assert isinstance(job["to_trade_id"], int)
    assert job["from_trade_id"] <= job["to_trade_id"]
    assert isinstance(job["source_agg_trade_id"], int)
    assert job["source_agg_trade_id"] > 0
    assert job["reason"] == "agg_trade_fallback"


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance WS integration disabled)",
)
@pytest.mark.asyncio
async def test_aggtrade_stream_real_btcusdt_primary_no_repair_job():
    """
    실 Binance @aggTrade 스트림 — primary 모드에서는 raw 만 나오고 repair_job 은
    절대 안 나가야 함. (production 회귀 방지: fallback 안 켜졌는데 repair 가
    잘못 발행되면 repair_worker 가 불필요한 REST 호출을 하게 됨.)
    """
    producer_raw, captured_raw = _make_capturing_producer()
    producer_repair, captured_repair = _make_capturing_producer()

    fallback = FallbackController()
    assert fallback.is_primary

    stream = AggTradeStream(
        symbols=["BTCUSDT"],
        base_ws_url=settings.binance_perp_ws,
        producer_raw=producer_raw,
        producer_repair=producer_repair,
        fallback=fallback,
    )

    await _run_until_first_message_or_timeout(
        stream, captured_raw, INTEGRATION_RECV_TIMEOUT_SEC
    )

    assert captured_raw, (
        "실 Binance @aggTrade 에서 raw 를 한 건도 못 받음 — 네트워크/URL 확인"
    )
    assert not captured_repair, (
        "primary 모드인데 repair_job 이 발행됨 — AggTradeStream 분기 회귀"
    )
