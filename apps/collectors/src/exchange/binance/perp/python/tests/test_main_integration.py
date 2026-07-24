"""
통합 테스트 — main() 실행 시 각 컴포넌트 초기화 및 예외 시나리오 검증

- 앞쪽: mock 기반 — settings 와 모든 외부 의존성을 가짜로 패치하여 main() 의
  초기화/정리 흐름 자체를 빠르게 검증.
- 뒤쪽: 실 Binance WebSocket 통합 테스트 — 실 Kafka 또는 in-memory Kafka 와
  실 Binance WS 를 함께 띄워 main() 비즈니스 로직 전체가 실제로 도는지 검증.
  (네트워크 필요. SKIP_BINANCE_INTEGRATION=1 시 skip,
   RUN_REDPANDA_E2E=1 일 때만 실 Redpanda 변형 추가 실행)
"""

import asyncio
import os
import sys
import uuid
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from types import ModuleType


def _create_mock_settings():
    """테스트용 settings 객체 생성"""
    s = MagicMock()
    s.binance_perp_symbols = ["BTCUSDT"]
    s.redpanda_brokers = "localhost:9092"
    s.binance_perp_topic_raw_trades = "test.raw"
    s.binance_perp_topic_agg_trades = "test.agg"
    s.binance_perp_topic_canonical = "test.canonical"
    s.binance_perp_topic_repair_jobs = "test.repair"
    s.binance_perp_ws = "wss://test.com"
    return s


@pytest.fixture
def mock_env_setup():
    """
    테스트용 가짜 settings 주입.

    NOTE: autouse 가 아님. 실제 .env.dev 의 settings 를 사용해야 하는 통합
    테스트 (test_main_real_*) 에서는 이 fixture 를 받지 않아야 진짜 모듈이
    로드된다.
    """
    mock_settings = _create_mock_settings()
    mock_config = ModuleType("binance_perp_trade.config")
    mock_config.settings = mock_settings

    with patch.dict(sys.modules, {"binance_perp_trade.config": mock_config}):
        yield mock_settings


@pytest.mark.asyncio
async def test_main_initializes_all_components(mock_env_setup):
    """main()이 모든 producer/consumer/stream을 정상적으로 초기화하는지 확인"""
    mock_producer_instance = AsyncMock()
    mock_consumer_instance = AsyncMock()
    mock_rest_client_instance = AsyncMock()

    async def mock_consume():
        await asyncio.sleep(10)
        yield {}

    mock_consumer_instance.consume_stream = mock_consume

    import binance_perp_trade.main as main_mod

    # main_mod의 속성을 직접 패치
    with patch.object(
        main_mod, "KafkaProducer", return_value=mock_producer_instance
    ), patch.object(
        main_mod, "KafkaConsumer", return_value=mock_consumer_instance
    ), patch.object(
        main_mod, "RestTradeClient", return_value=mock_rest_client_instance
    ), patch.object(
        main_mod, "TradeStream"
    ) as MockTradeStream, patch.object(
        main_mod, "AggTradeStream"
    ) as MockAggTradeStream:

        mock_trade_instance = AsyncMock()
        mock_trade_instance.run = AsyncMock(side_effect=asyncio.CancelledError())
        MockTradeStream.return_value = mock_trade_instance

        mock_agg_instance = AsyncMock()
        mock_agg_instance.run = AsyncMock(side_effect=asyncio.CancelledError())
        MockAggTradeStream.return_value = mock_agg_instance

        try:
            await asyncio.wait_for(main_mod.main(), timeout=0.5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        assert mock_producer_instance.connect.call_count == 4
        mock_consumer_instance.connect.assert_called_once()
        mock_rest_client_instance.connect.assert_called_once()


@pytest.mark.asyncio
async def test_main_cleanup_on_shutdown(mock_env_setup):
    """main()이 종료 시 모든 리소스를 정리하는지 확인"""
    mock_producer_instance = AsyncMock()
    mock_consumer_instance = AsyncMock()
    mock_rest_client_instance = AsyncMock()

    async def mock_consume():
        await asyncio.sleep(10)
        yield {}

    mock_consumer_instance.consume_stream = mock_consume

    import binance_perp_trade.main as main_mod

    with patch.object(
        main_mod, "KafkaProducer", return_value=mock_producer_instance
    ), patch.object(
        main_mod, "KafkaConsumer", return_value=mock_consumer_instance
    ), patch.object(
        main_mod, "RestTradeClient", return_value=mock_rest_client_instance
    ), patch.object(
        main_mod, "TradeStream"
    ) as MockTradeStream, patch.object(
        main_mod, "AggTradeStream"
    ) as MockAggTradeStream:

        mock_trade_instance = AsyncMock()
        mock_trade_instance.run = AsyncMock(side_effect=asyncio.CancelledError())
        MockTradeStream.return_value = mock_trade_instance

        mock_agg_instance = AsyncMock()
        mock_agg_instance.run = AsyncMock(side_effect=asyncio.CancelledError())
        MockAggTradeStream.return_value = mock_agg_instance

        try:
            await asyncio.wait_for(main_mod.main(), timeout=0.5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        mock_rest_client_instance.close.assert_called_once()
        mock_consumer_instance.stop.assert_called_once()
        assert mock_producer_instance.stop.call_count >= 1


# ── 실 Binance 통합 테스트 ──────────────────────────────────────────────────
#
# main() 의 비즈니스 로직 (Stream → Producer → Consumer → RepairWorker) 이
# 실제 Binance WS 메시지를 받아 끝까지 흘리는지 검증.
#
# 두 가지 변형:
#   A. 실 Binance + in-memory Kafka (CI 친화적, 항상 실행)
#   B. 실 Binance + 실 Redpanda  (RUN_REDPANDA_E2E=1 일 때만, 로컬 인프라 필요)
#
# 실 settings (.env.dev) 가 필요하므로 mock_env_setup 을 받지 않는다.

INTEGRATION_DISABLED = os.environ.get("SKIP_BINANCE_INTEGRATION") == "1"
REDPANDA_E2E_ENABLED = os.environ.get("RUN_REDPANDA_E2E") == "1"

INTEGRATION_DURATION_SEC = 8.0
INTEGRATION_FALLBACK_DURATION_SEC = 12.0
INTEGRATION_SHUTDOWN_TIMEOUT_SEC = 5.0


# ── pytest-asyncio teardown race 노이즈 억제 ────────────────────────────
#
# pytest-asyncio 는 테스트 종료 시 곧바로 loop.close() 를 호출한다. 그런데
# aiohttp / aiokafka 의 SSL transport 와 finalizer 는 loop close 시점에
# 비동기로 cleanup 을 더 스케줄하는데, 이때 loop 이 이미 닫혀 있으면 다음 두
# 종류 노이즈가 stderr 로 출력된다:
#
#   1) loop 의 exception handler 경유:
#        "Fatal error on SSL transport ... RuntimeError: Event loop is closed"
#   2) 객체의 __del__ 등에서 발생한 unraisable exception (sys.unraisablehook 경유):
#        "RuntimeError: Event loop is closed"
#
# 모두 후처리 race 노이즈일 뿐이며 테스트 결과/production 로직과는 무관.
# 안정적 억제를 위해 loop exception handler + sys.unraisablehook 둘 다 패치.
@pytest_asyncio.fixture
async def _suppress_loop_close_race():
    """SSL transport / finalizer teardown 노이즈 억제."""
    import sys

    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    previous_unraisable = sys.unraisablehook

    def loop_handler(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        if isinstance(exc, RuntimeError) and "Event loop is closed" in str(exc):
            return
        if "SSL transport" in msg and isinstance(exc, (OSError, RuntimeError)):
            return
        if previous_handler is not None:
            previous_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    def unraisable(unraisable_args):
        exc = unraisable_args.exc_value
        if isinstance(exc, RuntimeError) and "Event loop is closed" in str(exc):
            return
        previous_unraisable(unraisable_args)

    loop.set_exception_handler(loop_handler)
    sys.unraisablehook = unraisable
    yield
    try:
        loop.set_exception_handler(previous_handler)
    except Exception:
        pass
    sys.unraisablehook = previous_unraisable


# ── In-memory Kafka 페이크 ──────────────────────────────────────────────────


class _InMemBus:
    """토픽 → asyncio.Queue 라우터 + history 캡처."""

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self.history: dict[str, list[tuple[str, dict]]] = {}

    def queue(self, topic: str) -> asyncio.Queue:
        q = self._queues.get(topic)
        if q is None:
            q = asyncio.Queue()
            self._queues[topic] = q
            self.history[topic] = []
        return q


class _InMemProducer:
    """messaging.producer.KafkaProducer 인터페이스 호환 in-memory 페이크."""

    def __init__(self, bus: _InMemBus, *, bootstrap_servers: str, topic: str):
        self._bus = bus
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    async def connect(self):
        # 큐를 미리 만들어 둠 (consumer 가 먼저 await 했을 때 race 방지).
        self._bus.queue(self.topic)

    async def produce(self, key: str, value: dict):
        self._bus.history.setdefault(self.topic, []).append((key, value))
        await self._bus.queue(self.topic).put((key, value))

    async def stop(self):
        pass


class _InMemConsumer:
    """messaging.consumer.KafkaConsumer 인터페이스 호환 in-memory 페이크."""

    def __init__(
        self, bus: _InMemBus, *, bootstrap_servers: str, topic: str, group_id: str
    ):
        self._bus = bus
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._stopped = False

    async def connect(self):
        self._bus.queue(self.topic)

    async def consume_stream(self):
        q = self._bus.queue(self.topic)
        while not self._stopped:
            _, value = await q.get()
            if self._stopped:
                break
            yield value

    async def stop(self):
        self._stopped = True


def _make_kafka_factories(bus: _InMemBus):
    def producer_factory(*, bootstrap_servers, topic):
        return _InMemProducer(bus, bootstrap_servers=bootstrap_servers, topic=topic)

    def consumer_factory(*, bootstrap_servers, topic, group_id):
        return _InMemConsumer(
            bus,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
        )

    return producer_factory, consumer_factory


async def _run_main_for(main_mod, duration_sec: float) -> None:
    """
    main() 을 task 로 띄워 duration_sec 동안 메시지를 흘린 뒤,
    cancel + finally 블록 cleanup 까지 기다린다.
    """
    task = asyncio.create_task(main_mod.main())
    try:
        await asyncio.sleep(duration_sec)
    finally:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=INTEGRATION_SHUTDOWN_TIMEOUT_SEC)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            # main() 의 finally cleanup 중 발생하는 예외는 무시 (best-effort).
            pass

        # asyncio 가 스케줄해 둔 잔여 콜백 (SSL transport close, aiohttp pool
        # cleanup 등) 이 pytest-asyncio 의 loop close 전에 flush 될 시간을
        # 준다. 이거 없으면 'Event loop is closed' race 가 가끔 출력됨.
        await asyncio.sleep(0.3)


# ── A: 실 Binance + In-memory Kafka ──────────────────────────────────────


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance WS integration disabled)",
)
@pytest.mark.asyncio
async def test_main_real_binance_inmem_kafka_primary_flow(_suppress_loop_close_race):
    """
    실 Binance WS + in-memory Kafka 로 main() 을 ~8s 가동.

    Primary 모드 정상 흐름:
      - @trade  raw → binance_perp_topic_raw_trades   (N >= 5)
      - @trade canonical → binance_perp_topic_canonical (N >= 5)
      - @aggTrade raw → binance_perp_topic_agg_trades (N >= 1)
      - repair_jobs 토픽: 0 건 (gap 발생 안 했으므로)
    """
    import binance_perp_trade.main as main_mod
    from binance_perp_trade.config import settings

    bus = _InMemBus()
    producer_factory, consumer_factory = _make_kafka_factories(bus)

    with patch.object(main_mod, "KafkaProducer", producer_factory), patch.object(
        main_mod, "KafkaConsumer", consumer_factory
    ):
        await _run_main_for(main_mod, INTEGRATION_DURATION_SEC)

    raw_trades = bus.history.get(settings.binance_perp_topic_raw_trades, [])
    canonical = bus.history.get(settings.binance_perp_topic_canonical, [])
    agg_raw = bus.history.get(settings.binance_perp_topic_agg_trades, [])
    repair_jobs = bus.history.get(settings.binance_perp_topic_repair_jobs, [])

    assert (
        len(raw_trades) >= 5
    ), f"@trade raw 발행 부족: {len(raw_trades)} (Binance @trade 수신 실패?)"
    assert len(canonical) >= 5, (
        f"canonical 발행 부족: {len(canonical)} "
        "(primary 모드인데 canonical 안 나옴 — fallback 으로 빠졌을 가능성)"
    )
    assert (
        len(agg_raw) >= 1
    ), f"@aggTrade raw 발행 부족: {len(agg_raw)} (URL/경로 회귀?)"

    # primary 모드에서는 repair_job 이 발행돼선 안 됨.
    assert repair_jobs == [], (
        f"primary 모드인데 repair_jobs 발행됨: {len(repair_jobs)} "
        f"건 — AggTradeStream fallback 분기 회귀"
    )

    # canonical 페이로드 구조 sanity check.
    sym, c0 = canonical[0]
    assert sym == "BTCUSDT"
    assert c0["exchange"] == "binance" and c0["market_type"] == "perp"
    assert c0["symbol"] == "BTCUSDT"
    assert isinstance(c0["trade_id"], int) and c0["trade_id"] > 0
    assert float(c0["price"]) > 0
    assert c0["verified_by_rest"] is False
    assert c0["reconstructed_from_agg"] is False


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance WS integration disabled)",
)
@pytest.mark.asyncio
async def test_main_real_binance_inmem_kafka_fallback_flow(_suppress_loop_close_race):
    """
    실 Binance WS + in-memory Kafka 로 fallback 모드를 강제 → repair 흐름 검증.

    FallbackController 를 처음부터 fallback 상태로 시작하도록 패치.
      - @aggTrade raw 가 발행되고
      - repair_jobs 토픽으로 RepairJob 이 발행돼야 함.
      - BINANCE_API_KEY 가 있으면 repair_worker 가 REST 호출 → canonical 발행.

    NOTE: FallbackController.recovery_cooldown_sec=30 이라 12s 동안은
    fallback 상태가 유지된다.
    """
    import binance_perp_trade.main as main_mod
    from binance_perp_trade.config import settings
    from binance_perp_trade.core.fallback_controller import FallbackController

    bus = _InMemBus()
    producer_factory, consumer_factory = _make_kafka_factories(bus)

    original_init = FallbackController.__init__

    def forced_fallback_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.trigger_fallback("forced for integration test")

    with patch.object(main_mod, "KafkaProducer", producer_factory), patch.object(
        main_mod, "KafkaConsumer", consumer_factory
    ), patch.object(FallbackController, "__init__", forced_fallback_init):
        await _run_main_for(main_mod, INTEGRATION_FALLBACK_DURATION_SEC)

    agg_raw = bus.history.get(settings.binance_perp_topic_agg_trades, [])
    repair_jobs = bus.history.get(settings.binance_perp_topic_repair_jobs, [])

    assert (
        len(agg_raw) >= 1
    ), f"@aggTrade raw 발행 부족: {len(agg_raw)} (URL/경로 회귀?)"
    assert (
        len(repair_jobs) >= 1
    ), f"repair_jobs 발행 안 됨: fallback 강제가 풀렸거나 AggTradeStream 분기 회귀"

    sym, job = repair_jobs[0]
    assert sym == "BTCUSDT"
    assert job["symbol"] == "BTCUSDT"
    assert job["reason"] == "agg_trade_fallback"
    assert isinstance(job["from_trade_id"], int)
    assert isinstance(job["to_trade_id"], int)
    assert job["from_trade_id"] <= job["to_trade_id"]
    assert isinstance(job["source_agg_trade_id"], int)
    assert job["source_agg_trade_id"] > 0

    # API key 가 있으면 repair_worker 가 REST 로 복원 후 canonical 발행까지 가야 함.
    if settings.binance_api_key:
        canonical = bus.history.get(settings.binance_perp_topic_canonical, [])
        assert len(canonical) >= 1, (
            "BINANCE_API_KEY 가 있는데 repair_worker 가 canonical 을 발행 못함 — "
            "REST 호출 실패 또는 worker 사이클 부족 (timeout 늘려보세요)"
        )
        sym, c0 = canonical[0]
        assert (
            c0["source_agg_trade_id"] is not None
        ), "REST_GAP_FILL canonical 인데 source_agg_trade_id 가 None"
        assert c0["reconstructed_from_agg"] is True
        assert c0["verified_by_rest"] is True


# ── B: 실 Binance + 실 Redpanda (opt-in) ────────────────────────────────


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not REDPANDA_E2E_ENABLED,
    reason=(
        "Real Redpanda E2E — RUN_REDPANDA_E2E=1 + 로컬 Redpanda(localhost:9092) 필요. "
        "SKIP_BINANCE_INTEGRATION=1 또는 RUN_REDPANDA_E2E 미설정 시 skip."
    ),
)
@pytest.mark.asyncio
async def test_main_real_binance_real_redpanda_e2e(_suppress_loop_close_race):
    """
    실 Binance WS + 실 Redpanda 로 main() E2E.

    별도 KafkaConsumer 를 unique group_id 로 띄워 raw_trades / canonical 토픽에
    main() 이 정말로 메시지를 produce 하는지 확인. 실 production 토픽을 그대로
    공유하므로 group_id 만 unique 하게 잡는다 (자동 commit 영향 없음).
    """
    from messaging.consumer import KafkaConsumer
    import binance_perp_trade.main as main_mod
    from binance_perp_trade.config import settings

    suffix = uuid.uuid4().hex[:8]

    raw_consumer = KafkaConsumer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_raw_trades,
        group_id=f"e2e-test-raw-{suffix}",
    )
    canonical_consumer = KafkaConsumer(
        bootstrap_servers=settings.redpanda_brokers,
        topic=settings.binance_perp_topic_canonical,
        group_id=f"e2e-test-canonical-{suffix}",
    )

    raw_count = 0
    canonical_count = 0
    sample_canonical: dict | None = None

    def _is_plausible_canonical(value: dict) -> bool:
        """
        dev Redpanda 토픽은 다른 process / 과거 run 의 메시지가 섞일 수 있어
        (legacy normalize_trade 가 price=0 을 흘렸을 가능성), 샘플링은 명백히
        유효한 메시지만 골라 정확도를 높인다.
        """
        try:
            return (
                value.get("exchange") == "binance"
                and value.get("symbol") == "BTCUSDT"
                and isinstance(value.get("trade_id"), int)
                and value.get("trade_id", 0) > 0
                and float(value.get("price", 0)) > 0
            )
        except (TypeError, ValueError):
            return False

    async def drain(consumer, counter_name: str):
        nonlocal raw_count, canonical_count, sample_canonical
        async for value in consumer.consume_stream():
            if counter_name == "raw":
                raw_count += 1
            else:
                canonical_count += 1
                if sample_canonical is None and _is_plausible_canonical(value):
                    sample_canonical = value

    await raw_consumer.connect()
    await canonical_consumer.connect()

    main_task = asyncio.create_task(main_mod.main())
    raw_task = asyncio.create_task(drain(raw_consumer, "raw"))
    can_task = asyncio.create_task(drain(canonical_consumer, "canonical"))

    try:
        # aiokafka 첫 partition assignment 시간 + WS warmup 고려하여 더 길게.
        await asyncio.sleep(INTEGRATION_DURATION_SEC + 4.0)
    finally:
        main_task.cancel()
        raw_task.cancel()
        can_task.cancel()
        for t in (main_task, raw_task, can_task):
            try:
                await asyncio.wait_for(t, timeout=INTEGRATION_SHUTDOWN_TIMEOUT_SEC)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        await raw_consumer.stop()
        await canonical_consumer.stop()
        # aiokafka 의 잔여 콜백이 pytest-asyncio 의 loop close 전에 flush 되도록
        # 짧게 양보. (in-mem 테스트와 동일 race 의 E2E 변형)
        await asyncio.sleep(0.5)

    assert raw_count >= 3, (
        f"실 Redpanda raw_trades 토픽에서 받은 메시지 부족: {raw_count} "
        "(Redpanda 미기동 또는 main() 이 produce 못함)"
    )
    assert (
        canonical_count >= 3
    ), f"실 Redpanda canonical 토픽 메시지 부족: {canonical_count}"
    assert sample_canonical is not None, (
        "canonical 토픽에서 본 test run 의 valid 한 메시지를 한 건도 못 잡음. "
        "토픽에 stale/legacy 데이터가 너무 많거나 main() 출력 형식이 회귀했을 수 있음."
    )
    assert sample_canonical["market_type"] == "perp"
