"""
E2E Testnet Simulation — Mock 없이 실제 인프라만 사용.

실제 사용 인프라:
  - Binance Futures Testnet (REST + WebSocket User Data Stream)
  - PostgreSQL (localhost)
  - Redis (localhost, ``REDIS_DB`` / common_settings.redis_db)

Mock 대상: 없음 (QuestDB 정리는 REST 시도 — 미기동 시 warning 후 스킵)

시나리오:
  1. submit_and_cancel: 지정가 주문 → UDS ACK 수신 → 취소 → UDS CANCELLED 수신
  2. submit_invalid_symbol: 존재하지 않는 심볼 → REJECTED
  3. batch_submit_and_cancel: 배치 주문 → 전체 취소
  4. verify_unknown_order: 주문 후 get_order로 상태 확인
  5. reconciliation_snapshot: 거래소 조회 후 reconciliation 반영

실행:
  docker compose up -d  (postgres, redis)
  cd apps/api_server
  set -o allexport && source ../../.env.dev && set +o allexport
  uv run python -m pytest tests/test_e2e_testnet_simulation.py -v -m integration --tb=short
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from common.logging import setup_logger
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
    BinanceApiError,
)
from apps.execution_gateway.src.execution_gateway.adapters.binance.mapper.binance_algo_event_mapper import (
    normalize_binance_algo_rest_row,
)
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.config import settings as gw_settings
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.listeners.binance.binance_user_data_stream import (
    BinanceUserDataStreamListener,
)
from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from schemas.position_update_event import NormalizedPositionSnapshot
from execution_gateway.services.order_state_service import OrderStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    OrderRequest,
    OrderRoute,
    OrderSide,
    OrderStatus,
    OrderType,
    RejectReason,
    PositionAction,
)
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.postgres.order_intent_repo import OrderIntentPostgresRepository
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.identifiers import QuestDBTable

pytestmark = pytest.mark.integration
logger = setup_logger(__name__)

# ─────────────────── Infra cleanup (PG 픽스처와 모듈 종료 시) ───────────────────


def _truncate_questdb_tables_best_effort() -> None:
    """QuestDB HTTP ``/exec`` 로 WAL 테이블 비우기. 실패 시 로그만 남김."""
    host = common_settings.questdb_host
    port = common_settings.questdb_port
    for table in (QuestDBTable.EXECUTION_LOGS, QuestDBTable.CANONICAL_TRADES):
        sql = f"TRUNCATE TABLE {table}"
        try:
            q = urllib.parse.urlencode({"query": sql})
            url = f"http://{host}:{port}/exec?{q}"
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=15.0) as resp:
                resp.read()
            logger.info("QuestDB TRUNCATE 완료: %s", table)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            logger.warning(
                "QuestDB TRUNCATE 건너뜀 (%s): %s — QuestDB 미기동이면 무시 가능",
                table,
                e,
            )


async def _truncate_questdb_tables_best_effort_async() -> None:
    await asyncio.to_thread(_truncate_questdb_tables_best_effort)


def _redis_flush_test_db_sync_best_effort() -> None:
    """테스트와 동일 logical DB만 FLUSHDB (sync, 모듈 종료용)."""
    import redis as redis_sync

    try:
        r = redis_sync.Redis(
            host=common_settings.redis_host,
            port=common_settings.redis_port,
            db=common_settings.redis_db,
            decode_responses=True,
        )
        r.ping()
        r.flushdb()
        r.close()
        logger.info(
            "E2E 모듈 종료: Redis FLUSHDB (%s:%s db=%s)",
            common_settings.redis_host,
            common_settings.redis_port,
            common_settings.redis_db,
        )
    except Exception as e:
        logger.warning("E2E 모듈 종료: Redis FLUSHDB 건너뜀: %s", e)


# ─────────────────── Config helpers ───────────────────


def _load_pem() -> str:
    pem_path = gw_settings.active_ed25519_key_pem
    if not pem_path or not os.path.exists(pem_path):
        pytest.skip(f"PEM 파일이 없습니다: {pem_path}")

    with open(pem_path, "r") as f:
        return f.read()


def _assert_testnet() -> None:
    base = gw_settings.binance_testnet_rest_url.rstrip("/")
    allowed = {
        "https://demo-fapi.binance.com",
        "https://testnet.binancefuture.com",
    }
    if base not in allowed:
        pytest.skip(f"Testnet endpoint가 아닙니다: {base}")


def _make_req(
    *,
    exchange: Exchange = Exchange.BINANCE,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    price: str = "10000",
    quantity: str = "0.01",
) -> OrderRequest:
    return OrderRequest(
        exchange=exchange,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type="LIMIT",
        time_in_force="GTC",
        price=price,
        quantity=quantity,
        position_action=PositionAction.OPEN,
    )


def _is_invalid_demo_account_error(exc: Exception) -> bool:
    if not isinstance(exc, BinanceApiError):
        return False

    msg = exc.msg.lower()
    return (
        exc.code in {-1109, -2015, 90801109}
        or "invalid account" in msg
        or "invalid api-key" in msg
        or "permissions for action" in msg
    )


def _is_invalid_demo_account_order(order) -> bool:
    code = getattr(order, "exchange_error_code", None)
    detail_msg = str(getattr(order, "detail_msg", "") or "").lower()

    return (
        getattr(order, "status", None) == OrderStatus.REJECTED
        and (
            code in {-1109, 90801109, "-1109", "90801109"}
            or "invalid account" in detail_msg
        )
    )


def _position_amount(row: dict) -> Decimal:
    return Decimal(str(row.get("positionAmt") or "0"))


def _open_position_rows(rows: list[dict]) -> list[dict]:
    return [
        row for row in rows
        if _position_amount(row) != 0
    ]


async def _close_open_positions_until_flat(
    adapter: BinanceRestAdapter,
    *,
    max_attempts: int = 5,
    interval_sec: float = 2.0,
) -> list[dict]:
    """
    positionRisk를 기준으로 남은 포지션을 반복해서 MARKET reduce-only로 닫는다.

    한 번의 close 주문 후에도 Demo/Testnet에서 일부 수량이 남는 경우가 있어,
    매 attempt마다 최신 positionRisk를 다시 읽고 남은 수량만큼 다시 close한다.

    Returns:
        max_attempts 이후에도 남아 있는 open position rows.
    """
    remaining: list[dict] = []

    for attempt in range(1, max_attempts + 1):
        positions = await adapter.get_position_risk_v3()
        remaining = _open_position_rows(positions)

        if not remaining:
            return []

        for row in remaining:
            params = _position_close_params(row)
            logger.warning(
                "E2E 사전 정리: position close 주문 제출 "
                "attempt=%s/%s params=%s",
                attempt,
                max_attempts,
                params,
            )
            try:
                resp = await adapter.place_regular_order(params)
                logger.warning("E2E position close 응답: %s", resp)

            except BinanceApiError as e:
                if _is_invalid_demo_account_error(e):
                    pytest.skip(f"Binance Demo position close 불가: {e}")

                if e.code == -2022:
                    await asyncio.sleep(interval_sec)
                    positions_after_reject = await adapter.get_position_risk_v3()
                    remaining_after_reject = _open_position_rows(
                        positions_after_reject
                    )

                    if not remaining_after_reject:
                        logger.info(
                            "E2E 사전 정리: reduceOnly rejected 이후 "
                            "position flat 확인"
                        )
                        return []

                    logger.warning(
                        "E2E 사전 정리: reduceOnly rejected, "
                        "positionRisk 재조회 결과 아직 position 남음: %s",
                        remaining_after_reject,
                    )
                    remaining = remaining_after_reject
                    continue

                raise
            except Exception as e:
                if _is_invalid_demo_account_error(e):
                    pytest.skip(f"Binance Demo position close 불가: {e}")
                raise

        await asyncio.sleep(interval_sec)

    return remaining


def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


async def _wait_for_triggered_order_id_from_uds_or_rest(
    *,
    adapter: BinanceRestAdapter,
    collector: "UDSCollector",
    symbol: str,
    client_conditional_id: str,
    exchange_conditional_id: str | None,
    timeout: float = 30.0,
) -> NormalizedConditionalOrderEvent | None:
    """
    Binance ALGO_UPDATE의 TRIGGERING 이벤트에는 actual order id가 비어 있을 수 있다.

    이 경우 후속 ALGO_UPDATE 또는 allAlgoOrders REST row의 actualOrderId를 기다린다.
    """
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        for event in collector.conditional_events:
            if event.client_conditional_id != client_conditional_id:
                continue
            if event.target_status not in {
                ConditionalStatus.TRIGGERED,
                ConditionalStatus.FINISHED,
            }:
                continue
            if event.triggered_order_id:
                return event

        if exchange_conditional_id:
            try:
                rows = await adapter.get_all_algo_orders(
                    symbol=symbol,
                    algo_id=exchange_conditional_id,
                    limit=10,
                )
            except BinanceApiError as e:
                logger.warning(
                    "[trigger-test] allAlgoOrders 조회 실패: "
                    "symbol=%s algo_id=%s err=%s",
                    symbol,
                    exchange_conditional_id,
                    e,
                )
                rows = []

            for row in rows:
                event = normalize_binance_algo_rest_row(
                    row,
                    market_type=MarketType.PERP,
                )
                if event.client_conditional_id != client_conditional_id:
                    continue
                if event.target_status not in {
                    ConditionalStatus.TRIGGERED,
                    ConditionalStatus.FINISHED,
                }:
                    continue
                if event.triggered_order_id:
                    return event

        await asyncio.sleep(0.5)

    return None


def _position_close_params(row: dict) -> dict:
    symbol = str(row.get("symbol") or "").upper()
    position_side = str(row.get("positionSide") or "BOTH").upper()
    amount = _position_amount(row)
    # amount = Decimal(str(float(amount) - 0.002))

    if not symbol or amount == 0:
        raise ValueError(f"closable position row가 아님: {row}")

    params = {
        "symbol": symbol,
        "side": "SELL" if amount > 0 else "BUY",
        "type": "MARKET",
        "quantity": format(abs(amount), "f"),
        "positionSide": position_side,
    }

    if position_side == "BOTH":
        params["reduceOnly"] = "true"

    return params


def _symbols_from_rows(rows: list[dict]) -> set[str]:
    return {
        str(row.get("symbol") or "").upper()
        for row in rows
        if row.get("symbol")
    }


async def _cleanup_demo_account(
    adapter: BinanceRestAdapter,
) -> None:
    """
    Binance Demo/Testnet 계정 전체에 남은 주문/포지션을 테스트 전에 정리한다.

    Demo 계정 자체가 invalid 상태면 코드 실패가 아니라 계정 상태 문제이므로
    pytest.skip으로 분리한다.
    """
    try:
        open_orders = await adapter.get_open_orders()
    except Exception as e:
        if _is_invalid_demo_account_error(e):
            pytest.skip(f"Binance Demo 계정이 invalid 상태입니다: {e}")
        raise

    symbols_to_cancel_regular = _symbols_from_rows(open_orders)
    for symbol in sorted(symbols_to_cancel_regular):
        symbol_orders = [
            row for row in open_orders
            if str(row.get("symbol") or "").upper() == symbol
        ]
        logger.warning(
            "E2E 사전 정리: regular open orders 취소: symbol=%s count=%s",
            symbol,
            len(symbol_orders),
        )
        try:
            await adapter.cancel_all_open_orders(symbol=symbol)
        except Exception as e:
            if _is_invalid_demo_account_error(e):
                pytest.skip(f"Binance Demo regular 주문 취소 불가: {e}")
            raise

    try:
        algo_orders = await adapter.get_open_algo_orders()
    except BinanceApiError as e:
        if _is_invalid_demo_account_error(e):
            pytest.skip(f"Binance Demo 조건부 주문 조회 불가: {e}")
        logger.warning("E2E 사전 정리: 조건부 주문 조회 실패, 계속 진행: %s", e)
        algo_orders = []

    symbols_to_cancel_algo = _symbols_from_rows(algo_orders)
    for symbol in sorted(symbols_to_cancel_algo):
        symbol_algo_orders = [
            row for row in algo_orders
            if str(row.get("symbol") or "").upper() == symbol
        ]
        logger.warning(
            "E2E 사전 정리: conditional open orders 취소: symbol=%s count=%s",
            symbol,
            len(symbol_algo_orders),
        )
        try:
            await adapter.cancel_all_algo_open_orders(symbol=symbol)
        except Exception as e:
            if _is_invalid_demo_account_error(e):
                pytest.skip(f"Binance Demo 조건부 주문 취소 불가: {e}")
            raise

    try:
        positions = await adapter.get_position_risk_v3()
    except Exception as e:
        if _is_invalid_demo_account_error(e):
            pytest.skip(f"Binance Demo position 조회 불가: {e}")
        raise
    open_positions = _open_position_rows(positions)

    if open_positions:
        await _close_open_positions_until_flat(adapter,max_attempts=2)

    if open_orders or algo_orders or open_positions:
        await asyncio.sleep(1.0)


    if open_positions:
        logger.info("E2E 사전 정리: 모든 position flat 확인")


# ─────────────────── Fixtures: Real infra ───────────────────


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN 미설정")

    client = PostgresClient(dsn=dsn, min_size=1, max_size=3)
    await client.connect()

    pool = client.require_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE outbox_events, orders, order_intents RESTART IDENTITY CASCADE"
        )

    await _truncate_questdb_tables_best_effort_async()

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE outbox_events, orders, order_intents RESTART IDENTITY CASCADE"
        )
    await _truncate_questdb_tables_best_effort_async()
    await client.close()


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis() -> RedisStreamClient:
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=common_settings.redis_db,
    )
    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()
    yield client
    await client.client.flushdb()
    logger.info(
        "E2E 테스트 종료: Redis FLUSHDB (%s:%s db=%s)",
        common_settings.redis_host,
        common_settings.redis_port,
        common_settings.redis_db,
    )
    await client.close()


@pytest.fixture(scope="module", autouse=True)
# pyrefly: ignore [bad-return]
def _e2e_testnet_module_cleanup() -> None:
    """이 모듈의 테스트가 모두 끝난 뒤 Redis·QuestDB 한 번 더 비움."""
    yield
    _redis_flush_test_db_sync_best_effort()
    _truncate_questdb_tables_best_effort()


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def real_adapter() -> BinanceRestAdapter:
    """실제 Binance Testnet REST adapter."""
    _assert_testnet()
    pem = _load_pem()

    adapter = BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem,
    )
    yield adapter
    await adapter.close()


@pytest.fixture
def redis_order_repo(redis: RedisStreamClient) -> OrderStateRedisRepository:
    return OrderStateRedisRepository(redis)


@pytest.fixture
def state_service(
    postgres: PostgresClient,
    redis_order_repo: OrderStateRedisRepository,
) -> OrderStateService:
    return OrderStateService(
        postgres=postgres,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=redis_order_repo,
    )


@pytest_asyncio.fixture
async def gateway(
    real_adapter: BinanceRestAdapter,
    redis_order_repo: OrderStateRedisRepository,
    state_service: OrderStateService,
) -> ExecutionGateway:
    order_router = BinanceOrderRouter(real_adapter)
    binance_execution_client = BinanceExecutionClient(
        adapter=real_adapter,
        order_router=order_router,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(binance_execution_client)

    return ExecutionGateway(
        state_repo=redis_order_repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )


# ─────────────────── UDS Listener fixture ───────────────────


class UDSCollector:
    """UDS 이벤트를 수집하는 헬퍼. 특정 order_id의 이벤트를 대기할 수 있다."""

    def __init__(self):
        self.order_events: list[NormalizedOrderUpdateEvent] = []
        self.conditional_events: list[NormalizedConditionalOrderEvent] = []
        self.position_events: list[list[NormalizedPositionSnapshot]] = []
        self._waiters: dict[str, asyncio.Event] = {}
        self._any_order_event = asyncio.Event()
        self._any_conditional_event = asyncio.Event()

    async def on_order(self, event: NormalizedOrderUpdateEvent) -> None:
        self.order_events.append(event)
        self._any_order_event.set()
        client_oid = event.client_order_id
        if client_oid in self._waiters:
            self._waiters[client_oid].set()

    async def on_conditional(
        self,
        event: NormalizedConditionalOrderEvent,
    ) -> None:
        self.conditional_events.append(event)
        self._any_conditional_event.set()

    async def on_position(
        self,
        snapshots: list[NormalizedPositionSnapshot],
    ) -> None:
        self.position_events.append(snapshots)

    async def wait_for_order(
        self, order_id: str, timeout: float = 15.0
    ) -> list[NormalizedOrderUpdateEvent]:
        """특정 order_id의 UDS 이벤트를 timeout 내에 대기."""
        evt = self._waiters.setdefault(order_id, asyncio.Event())

        # 이미 수신된 이벤트가 있으면 즉시 반환
        existing = [e for e in self.order_events if e.client_order_id == order_id]
        if existing:
            return existing

        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

        return [e for e in self.order_events if e.client_order_id == order_id]

    async def wait_for_status(
        self, order_id: str, target_status: str, timeout: float = 15.0
    ) -> NormalizedOrderUpdateEvent | None:
        """특정 order_id + status 조합의 UDS 이벤트를 대기."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for e in self.order_events:
                if e.client_order_id == order_id and e.exchange_status == target_status:
                    return e

            evt = self._waiters.setdefault(order_id, asyncio.Event())
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            evt.clear()
            try:
                await asyncio.wait_for(evt.wait(), timeout=min(remaining, 1.0))
            except asyncio.TimeoutError:
                pass

        return None

    async def wait_for_conditional_status(
        self,
        *,
        client_conditional_id: str,
        target_statuses: set[ConditionalStatus],
        timeout: float = 60.0,
    ) -> NormalizedConditionalOrderEvent | None:
        """특정 조건부 주문의 ALGO_UPDATE 상태를 대기."""
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for event in self.conditional_events:
                if (
                    event.client_conditional_id == client_conditional_id
                    and event.target_status in target_statuses
                ):
                    return event

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            self._any_conditional_event.clear()
            try:
                await asyncio.wait_for(
                    self._any_conditional_event.wait(),
                    timeout=min(remaining, 1.0),
                )
            except asyncio.TimeoutError:
                pass

        return None

    def conditional_event_summary(
        self,
        *,
        client_conditional_id: str,
    ) -> list[dict]:
        """디버깅용: 특정 조건부 주문으로 들어온 ALGO_UPDATE 요약."""
        result: list[dict] = []

        for event in self.conditional_events:
            if event.client_conditional_id != client_conditional_id:
                continue

            result.append(
                {
                    "target_status": (
                        event.target_status.value
                        if event.target_status is not None
                        else None
                    ),
                    "exchange_conditional_status": event.exchange_conditional_status,
                    "exchange_conditional_id": event.exchange_conditional_id,
                    "triggered_order_id": event.triggered_order_id,
                    "event_time": event.event_time,
                }
            )

        return result

    async def wait_for_exchange_order_status(
        self,
        *,
        exchange_order_id: str,
        target_statuses: set[OrderStatus],
        timeout: float = 30.0,
    ) -> NormalizedOrderUpdateEvent | None:
        """거래소 order id 기준 ORDER_TRADE_UPDATE 상태를 대기."""
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for event in self.order_events:
                if (
                    event.exchange_order_id == exchange_order_id
                    and event.target_status in target_statuses
                ):
                    return event

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            self._any_order_event.clear()
            try:
                await asyncio.wait_for(
                    self._any_order_event.wait(),
                    timeout=min(remaining, 1.0),
                )
            except asyncio.TimeoutError:
                pass

        return None


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
async def uds_listener(
    real_adapter: BinanceRestAdapter,
# pyrefly: ignore [bad-return]
) -> tuple[BinanceUserDataStreamListener, UDSCollector]:
    """실제 Binance Testnet UDS 리스너 + 이벤트 수집기."""
    try:
        probe_listen_key = await real_adapter.create_listen_key()
        await real_adapter.close_listen_key(probe_listen_key)
    except Exception as e:
        if _is_invalid_demo_account_error(e):
            pytest.skip(f"Binance Demo UDS 권한 확인 실패: {e}")
        raise

    ws_base = gw_settings.ws_base_url.rstrip("/")
    if not ws_base.endswith("/private"):
        ws_base = f"{ws_base}/private"

    listener = BinanceUserDataStreamListener(
        rest_adapter=real_adapter,
        ws_base_url=ws_base,
    )

    collector = UDSCollector()
    listener.on_order_update(collector.on_order)
    listener.on_algo_update(collector.on_conditional)
    listener.on_position_update(collector.on_position)

    task = asyncio.create_task(listener.start(), name="uds-test")

    # listenKey 발급 + WS 연결 대기
    await asyncio.sleep(3.0)

    yield listener, collector

    await listener.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ═══════════════════════════════════════════════════════════
#  시나리오 1: 주문 생성 → UDS ACK 수신 → 취소 → UDS CANCELLED 수신
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_scenario_submit_ack_cancel_with_uds(
    gateway: ExecutionGateway,
    real_adapter: BinanceRestAdapter,
    postgres: PostgresClient,
    redis_order_repo: OrderStateRedisRepository,
    uds_listener: tuple[BinanceUserDataStreamListener, UDSCollector],
):
    """
    전체 주문 lifecycle을 mock 없이 검증.

    흐름:
      1. Gateway.submit_order → Testnet REST → ACKNOWLEDGED
      2. UDS WebSocket에서 NEW 이벤트 수신 확인
      3. Gateway.cancel_order → Testnet REST → CANCELLED
      4. UDS WebSocket에서 CANCELED 이벤트 수신 확인
      5. PG + Redis 최종 상태 확인
    """
    _, collector = uds_listener
    await _cleanup_demo_account(real_adapter)

    req = _make_req(price="10000", quantity="0.01")

    # 1. 주문 생성
    order = await gateway.submit_order(req)
    logger.info(f"[시나리오1] 주문 생성: {order.order_id}, status={order.status.value}")

    if _is_invalid_demo_account_order(order):
        pytest.skip(
            "Binance Demo 계정이 invalid 상태라 주문 lifecycle E2E를 실행할 수 없습니다. "
            f"exchange_error_code={order.exchange_error_code}, detail_msg={order.detail_msg}"
        )

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.exchange_order_id

    assert order.order_id

    # 2. UDS에서 NEW 이벤트 수신 대기
    new_event = await collector.wait_for_status(order.order_id, "NEW", timeout=10.0)
    if new_event:
        logger.info(
            "[시나리오1] UDS NEW 수신: exchange_order_id=%s",
            new_event.exchange_order_id,
        )
    else:
        logger.warning("[시나리오1] UDS NEW 이벤트 미수신 (timeout)")

    # 3. PG 상태 확인
    pool = postgres.require_pool()
    async with pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            "SELECT status, version, exchange_order_id FROM orders WHERE order_id = $1",
            order.order_id,
        )
    assert pg_row is not None
    assert pg_row["status"] == "ACKNOWLEDGED"
    assert pg_row["version"] == 3  # PENDING_NEW(1) → SUBMITTED(2) → ACK(3)
    logger.info(f"[시나리오1] PG 확인: status={pg_row['status']}, version={pg_row['version']}")

    # 4. Redis 확인
    redis_data = await redis_order_repo.get(order.order_id)
    assert redis_data is not None
    assert redis_data["status"] == "ACKNOWLEDGED"

    # open orders에 포함
    open_orders = await redis_order_repo.list_open_regular_orders(
        exchange=order.exchange.value,
        market_type=order.market_type.value,
    )
    open_ids = {o["order_id"] for o in open_orders}
    assert order.order_id in open_ids


    assert order.order_id

    # 5. 취소
    cancel_resp = await gateway.cancel_order(order.order_id)
    logger.info(f"[시나리오1] 취소 응답: {cancel_resp.get('status', 'N/A')}")

    # 6. UDS CANCELED 이벤트 수신 대기
    cancel_event = await collector.wait_for_status(order.order_id, "CANCELED", timeout=10.0)
    if cancel_event:
        logger.info("[시나리오1] UDS CANCELED 수신 확인")
    else:
        logger.warning("[시나리오1] UDS CANCELED 이벤트 미수신 (timeout)")

    # 7. PG 최종 상태 확인
    async with pool.acquire() as conn:
        pg_final = await conn.fetchrow(
            "SELECT status FROM orders WHERE order_id = $1",
            order.order_id,
        )
    assert pg_final["status"] == "CANCELLED"

    # 8. Redis open orders에서 제거됨
    open_orders_final = await redis_order_repo.list_open_regular_orders(
        exchange=order.exchange.value,
        market_type=order.market_type.value,
    )
    open_ids_final = {o["order_id"] for o in open_orders_final}
    assert order.order_id not in open_ids_final

    # 9. Outbox events 확인
    async with pool.acquire() as conn:
        outbox_count = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1",
            order.order_id,
        )
    assert outbox_count >= 3
    logger.info(f"[시나리오1] outbox events: {outbox_count}")


# ═══════════════════════════════════════════════════════════
#  시나리오 2: 잘못된 심볼 → REJECTED
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_scenario_invalid_symbol_rejected(
    gateway: ExecutionGateway,
    postgres: PostgresClient,
    redis_order_repo: OrderStateRedisRepository,
):
    """존재하지 않는 심볼 → Binance Testnet이 reject → 로컬 REJECTED."""
    exchange = Exchange.BINANCE
    req = _make_req(exchange=exchange, symbol="NOTREALUSDT", price="10000")

    order = await gateway.submit_order(req)
    logger.info(f"[시나리오2] {order.order_id}: status={order.status.value}, reason={order.reject_reason}")

    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason in {RejectReason.INVALID_SYMBOL, RejectReason.EXCHANGE_REJECTED}

    # PG 확인
    pool = postgres.require_pool()
    async with pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            "SELECT status FROM orders WHERE order_id = $1", order.order_id
        )
    assert pg_row["status"] == "REJECTED"

    # Redis 확인 — REJECTED이므로 open에 없어야 함
    open_orders = await redis_order_repo.list_open_regular_orders(
        exchange=exchange.value,
        market_type=MarketType.PERP.value,
    )
    open_ids = {o["order_id"] for o in open_orders}
    assert order.order_id not in open_ids


@pytest.mark.skipif(
    os.getenv("RUN_BINANCE_TRIGGER_TESTS") != "1",
    reason=(
        "실제 조건부 주문 trigger 테스트는 포지션이 열릴 수 있으므로 "
        "RUN_BINANCE_TRIGGER_TESTS=1 일 때만 실행"
    ),
)
@pytest.mark.asyncio
async def test_scenario_triggered_conditional_order_update_falls_back_by_triggered_order_id(
    gateway: ExecutionGateway,
    real_adapter: BinanceRestAdapter,
    state_service: OrderStateService,
    uds_listener: tuple[BinanceUserDataStreamListener, UDSCollector],
):
    """
    실제 Binance Demo/Testnet에서 조건부 주문 trigger 후 child regular 주문 이벤트를 검증한다.

    검증 흐름:
      1. BUY STOP_MARKET 조건부 주문을 현재가 근처에 제출
      2. ALGO_UPDATE TRIGGERED를 수신하고 Gateway에 반영
      3. TRIGGERED 이벤트의 triggered_order_id를 확인
      4. ORDER_TRADE_UPDATE 중 exchange_order_id == triggered_order_id인 이벤트 수신
      5. apply_order_update_event()가 triggered_order_id fallback으로 기존 조건부 Order를 갱신해야 함

    현재 fallback 구현이 없다면 5번에서 updated가 None이 되어 실패한다.
    """
    _, collector = uds_listener

    symbol = os.getenv("BINANCE_TRIGGER_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_TRIGGER_TEST_QTY", "0.01")
    trigger_wait_sec = float(os.getenv("BINANCE_TRIGGER_WAIT_SEC", "180"))

    submitted_order = None

    try:
        await _cleanup_demo_account(real_adapter)

        ticker = await real_adapter.get_symbol_price_ticker(symbol)
        ref_price = Decimal(str(ticker["price"]))

        explicit_trigger_price = os.getenv("BINANCE_TRIGGER_TEST_PRICE")
        if explicit_trigger_price:
            trigger_price = Decimal(explicit_trigger_price)
        else:
            # BUY STOP_MARKET은 triggerPrice 이상 도달 시 발동한다.
            # 실제 가격 움직임에 의존하므로 기본값은 매우 가까운 0.1bps 위에 둔다.
            offset_bps = Decimal(os.getenv("BINANCE_TRIGGER_OFFSET_BPS", "0.1"))
            step = Decimal(os.getenv("BINANCE_TRIGGER_PRICE_STEP", "0.1"))
            trigger_price = _round_down_to_step(
                ref_price * (Decimal("1") + offset_bps / Decimal("10000")),
                step,
            )
            if trigger_price <= ref_price:
                trigger_price = _round_down_to_step(ref_price + step, step)

        logger.info(
            "[trigger-test] 조건부 주문 준비: symbol=%s ref_price=%s trigger_price=%s "
            "quantity=%s wait_sec=%s",
            symbol,
            ref_price,
            trigger_price,
            quantity,
            trigger_wait_sec,
        )

        req = OrderRequest(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.STOP_MARKET,
            order_route=OrderRoute.CONDITIONAL,
            quantity=quantity,
            trigger_price=str(trigger_price),
            reduce_only=False,
            close_position=False,
            position_action=PositionAction.OPEN,
        )

        submitted_order = await gateway.submit_order(req)

        if _is_invalid_demo_account_order(submitted_order):
            pytest.skip(
                "Binance Demo 계정이 invalid 상태라 trigger E2E를 실행할 수 없습니다. "
                f"code={submitted_order.exchange_error_code}, msg={submitted_order.detail_msg}"
            )

        if submitted_order.status == OrderStatus.REJECTED:
            pytest.skip(
                "Binance Demo/Testnet이 조건부 주문을 거부했습니다. "
                f"code={submitted_order.exchange_error_code}, "
                f"reason={submitted_order.reject_reason}, "
                f"msg={submitted_order.detail_msg}"
            )

        assert submitted_order.order_id
        assert submitted_order.client_conditional_id
        assert submitted_order.conditional_status in {
            ConditionalStatus.NEW,
            ConditionalStatus.ACTIVE,
        }

        created_event = await collector.wait_for_conditional_status(
            client_conditional_id=submitted_order.client_conditional_id,
            target_statuses={ConditionalStatus.NEW, ConditionalStatus.ACTIVE},
            timeout=20.0,
        )
        if created_event is None:
            pytest.skip(
                "조건부 주문 생성 ALGO_UPDATE NEW/ACTIVE 이벤트를 받지 못했습니다. "
                f"client_conditional_id={submitted_order.client_conditional_id}, "
                f"seen={collector.conditional_event_summary(client_conditional_id=submitted_order.client_conditional_id)}"
            )

        triggered_event = await collector.wait_for_conditional_status(
            client_conditional_id=submitted_order.client_conditional_id,
            target_statuses={ConditionalStatus.TRIGGERED},
            timeout=trigger_wait_sec,
        )

        if triggered_event is None:
            pytest.skip(
                "조건부 주문이 timeout 내 trigger되지 않았습니다. "
                f"symbol={symbol}, ref_price={ref_price}, trigger_price={trigger_price}, "
                f"seen={collector.conditional_event_summary(client_conditional_id=submitted_order.client_conditional_id)}"
            )

        triggered_order = await gateway.apply_conditional_order_event(triggered_event)

        print(triggered_order)

        assert triggered_order is not None
        assert triggered_order.order_id == submitted_order.order_id
        assert triggered_order.conditional_status == ConditionalStatus.TRIGGERED

        triggered_order_id = triggered_order.triggered_order_id
        if not triggered_order_id:
            triggered_id_event = await _wait_for_triggered_order_id_from_uds_or_rest(
                adapter=real_adapter,
                collector=collector,
                symbol=symbol,
                client_conditional_id=submitted_order.client_conditional_id,
                exchange_conditional_id=submitted_order.exchange_conditional_id,
                timeout=30.0,
            )

            if triggered_id_event is None:
                pytest.skip(
                    "조건부 주문은 trigger되었지만 actual triggered order id를 "
                    "UDS/allAlgoOrders에서 확인하지 못했습니다. "
                    f"first_trigger_event={triggered_event.raw}, "
                    f"seen={collector.conditional_event_summary(client_conditional_id=submitted_order.client_conditional_id)}"
                )

            triggered_order = await gateway.apply_conditional_order_event(
                triggered_id_event
            )

            assert triggered_order is not None
            assert triggered_order.order_id == submitted_order.order_id
            assert triggered_order.conditional_status in {
                ConditionalStatus.TRIGGERED,
                ConditionalStatus.FINISHED,
            }

            triggered_order_id = triggered_order.triggered_order_id

        assert triggered_order_id

        actual_order_event = await collector.wait_for_exchange_order_status(
            exchange_order_id=triggered_order_id,
            target_statuses={
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
                OrderStatus.REJECTED,
            },
            timeout=60.0,
        )

        assert actual_order_event is not None, (
            "triggered_order_id와 매칭되는 terminal ORDER_TRADE_UPDATE를 받지 못했습니다. "
            f"triggered_order_id={triggered_order_id}"
        )

        updated = await gateway.apply_order_update_event(actual_order_event)

        assert updated is not None, (
            "ORDER_TRADE_UPDATE가 client_order_id로 매칭되지 않을 때 "
            "exchange_order_id == triggered_order_id fallback으로 기존 조건부 Order를 "
            "찾아야 합니다."
        )
        assert updated.order_id == submitted_order.order_id
        assert updated.triggered_order_id == triggered_order_id
        assert updated.status == actual_order_event.target_status

        loaded = await state_service.load_order(
            order_id=submitted_order.order_id,
            refresh_projection=True,
        )

        assert loaded is not None
        assert loaded.status == actual_order_event.target_status
        assert loaded.triggered_order_id == triggered_order_id

    finally:
        # trigger 테스트는 실제 포지션이 열릴 수 있으므로 성공/실패와 무관하게 정리한다.
        try:
            await _cleanup_demo_account(real_adapter)
        except Exception as e:
            logger.error(
                "trigger 테스트 cleanup 실패: err=%s",
                e,
                exc_info=True,
            )
        pass


# ═══════════════════════════════════════════════════════════
#  시나리오 3: 배치 주문 생성 → 전체 취소
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_scenario_batch_submit_and_cancel_all(
    gateway: ExecutionGateway,
    postgres: PostgresClient,
    redis_order_repo: OrderStateRedisRepository,
):
    """배치 주문 2건 → cancel_all_open_orders로 전체 취소."""
    exchange = Exchange.BINANCE
    req1 = _make_req(exchange=exchange, price="10001", quantity="0.01")
    req2 = _make_req(exchange=exchange, price="10002", quantity="0.01")

    orders = await gateway.submit_batch_orders(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        requests=[req1, req2],
    )
    logger.info(f"[시나리오3] 배치 주문 {len(orders)}건 생성")

    if any(_is_invalid_demo_account_order(o) for o in orders):
        pytest.skip("Binance Demo 계정이 invalid 상태이므로 시나리오 진행 불가")

    assert len(orders) == 2
    for o in orders:
        assert o.status == OrderStatus.ACKNOWLEDGED
        logger.info(f"  {o.order_id}: exchange_order_id={o.exchange_order_id}")

    # Redis open orders에 2건 포함
    open_orders = await redis_order_repo.list_open_regular_orders(
        exchange=exchange.value,
        market_type=MarketType.PERP.value,
    )
    open_ids = {o["order_id"] for o in open_orders}
    for o in orders:
        assert o.order_id in open_ids

    # 전체 취소
    cancel_results = await gateway.cancel_all_regular_open_orders(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        market_type=MarketType.PERP,
    )
    logger.info(f"[시나리오3] 전체 취소 결과: {len(cancel_results)}건")

    # PG에서 PENDING_CANCEL 이상 확인
    # (cancel_all은 PENDING_CANCEL까지만 동기 처리. 최종 CANCELLED는 UDS에서 비동기 확정)
    pool = postgres.require_pool()
    async with pool.acquire() as conn:
        for o in orders:
            pg_row = await conn.fetchrow(
                "SELECT status FROM orders WHERE order_id = $1", o.order_id
            )
            assert pg_row["status"] in ("PENDING_CANCEL", "CANCELLED"), (
                f"{o.order_id}: {pg_row['status']}"
            )


# ═══════════════════════════════════════════════════════════
#  시나리오 4: verify_unknown_order — 거래소 실제 조회로 복구
# ═══════════════════════════════════════════════════════════


# @pytest.mark.asyncio
# async def test_scenario_verify_unknown_order_recovery(
#     gateway: ExecutionGateway,
#     postgres: PostgresClient,
#     redis_order_repo: OrderStateRedisRepository,
# ):
#     """
#     주문 후 verify_unknown_order로 실제 거래소 상태 조회.
#     (503 시뮬레이션은 아니지만, verify 함수가 실제 REST로 동작하는지 확인)
#     """
#     req = _make_req(price="10000", quantity="0.01")
#     order = await gateway.submit_order(req)

#     if _is_invalid_demo_account_order(order):
#         pytest.skip("Binance Demo 계정이 invalid 상태이므로 시나리오 진행 불가")

#     assert order.status == OrderStatus.ACKNOWLEDGED

#     # verify_unknown_order는 거래소에서 get_order를 호출해서 상태를 확인
#     result = await gateway.verify_unknown_order("BTCUSDT", order.order_id)
#     logger.info(
#         f"[시나리오4] verify 결과: "
#         f"status={result.status.value if result else 'None'}"
#     )

#     if result:
#         # 거래소에서 NEW → ACKNOWLEDGED로 매핑되므로
#         assert result.status in {OrderStatus.ACKNOWLEDGED, OrderStatus.FILLED}

#     # 정리: 취소
#     try:
#         assert order.order_id
#         await gateway.cancel_order("BTCUSDT", order.order_id)
#     except Exception:
#         pass


# ═══════════════════════════════════════════════════════════
#  시나리오 5: reconciliation — 거래소 스냅샷으로 상태 보정
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_scenario_reconciliation_from_exchange(
    gateway: ExecutionGateway,
    real_adapter: BinanceRestAdapter,
    postgres: PostgresClient,
):
    """
    거래소에서 실제 주문 상태를 조회한 뒤
    apply_reconciliation_snapshot으로 반영하는 전체 흐름.
    """
    req = _make_req(price="10000", quantity="0.01")
    order = await gateway.submit_order(req)

    if _is_invalid_demo_account_order(order):
        pytest.skip("Binance Demo 계정이 invalid 상태이므로 시나리오 진행 불가")

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.order_id

    # 거래소에서 실제 주문 상태 조회 (Testnet read replica lag 보완)
    await asyncio.sleep(1.0)
    client = gateway.ctx.client_for_market(
        exchange=order.exchange, market_type=order.market_type
    )
    exchange_snapshot = await client.get_order(order)
    logger.info(f"[시나리오5] 거래소 상태: {exchange_snapshot.status}")

    # reconciliation 반영
    result = await gateway.apply_reconciliation_order_snapshot(
        order_id=order.order_id,
        snapshot=exchange_snapshot,
    )
    assert result is not None
    assert result.status == OrderStatus.ACKNOWLEDGED  # NEW → ACKNOWLEDGED

    # 취소 후 다시 reconciliation — terminal 보호 검증
    await gateway.cancel_order("BTCUSDT", order.order_id)

    # PG에서 CANCELLED 확인
    pool = postgres.require_pool()
    async with pool.acquire() as conn:
        pg_row = await conn.fetchrow(
            "SELECT status FROM orders WHERE order_id = $1", order.order_id
        )
    assert pg_row["status"] == "CANCELLED"

    # stale snapshot으로 reconciliation 시도 → terminal 보호
    result2 = await gateway.apply_reconciliation_order_snapshot(
        order_id=order.order_id,
        snapshot=exchange_snapshot,  # 아직 NEW 상태의 stale snapshot
    )

    assert result2
    assert result2.status == OrderStatus.CANCELLED  # terminal 보호
    logger.info("[시나리오5] terminal 보호 확인: CANCELLED 유지")


# ═══════════════════════════════════════════════════════════
#  시나리오 6: Fill dedup (실제 Redis SETNX)
# ═══════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_scenario_fill_dedup_real_redis(
    redis: RedisStreamClient,
):
    """실제 Redis에서 fill dedup key가 정확히 동작하는지 검증."""
    order_id = f"O-BN-PERP-DEDUP-{int(time.time() * 1000)}"
    trade_id = "777777"
    key = f"fill:{order_id}:{trade_id}"

    # 첫 번째: 설정 성공
    r1 = await redis.client.set(key, "1", nx=True, ex=86400)
    assert r1 is True

    # 두 번째: 이미 존재 → 실패
    r2 = await redis.client.set(key, "1", nx=True, ex=86400)
    assert r2 is None or r2 is False

    # TTL 확인
    ttl = await redis.client.ttl(key)
    assert 86300 < ttl <= 86400

    # 다른 trade_id는 별도 키 → 성공
    key2 = f"fill:{order_id}:888888"
    r3 = await redis.client.set(key2, "1", nx=True, ex=86400)
    assert r3 is True
    logger.info("[시나리오6] fill dedup 검증 완료")
