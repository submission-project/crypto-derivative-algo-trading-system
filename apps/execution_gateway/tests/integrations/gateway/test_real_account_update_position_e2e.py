from __future__ import annotations

import asyncio
import contextlib
import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from common.logging import setup_logger
from execution_gateway.config import settings as gw_settings
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.listeners.binance.binance_user_data_stream import (
    BinanceUserDataStreamListener,
)
from schemas.position_update_event import NormalizedPositionSnapshot
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.services.position_order_service import PositionOrderService
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
)
from schemas.position import (
    Position,
    PositionSide,
    PositionStatus,
    make_position_id,
)
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.position_repo import PositionPostgresRepository
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.redis.position_state_repo import PositionRedisRepository

from execution_gateway.adapters.binance.dto.resp.PositionResponseDto import (
    PositionRiskRespDto,
)

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter

pytestmark = pytest.mark.integration

logger = setup_logger(__name__)


# def _require_real_account_update_tests_enabled() -> None:
#     if os.getenv("RUN_BINANCE_REAL_TESTS") != "1":
#         pytest.skip(
#             "Real Binance testnet tests are disabled. "
#             "Set RUN_BINANCE_REAL_TESTS=1 to run."
#         )

#     if os.getenv("RUN_ACCOUNT_UPDATE_REAL_TESTS") != "1":
#         pytest.skip(
#             "Real ACCOUNT_UPDATE E2E tests are disabled. "
#             "Set RUN_ACCOUNT_UPDATE_REAL_TESTS=1 to run."
#         )

#     if os.getenv("POSITION_TEST_ALLOW_OPEN", "0") != "1":
#         pytest.skip(
#             "This test opens and closes a real Binance Futures Testnet MARKET position. "
#             "Set POSITION_TEST_ALLOW_OPEN=1 to allow it."
#         )


def _load_pem() -> str:
    pem_path = getattr(gw_settings, "active_ed25519_key_pem", None)

    if not pem_path:
        pytest.skip("gw_settings.active_ed25519_key_pem is not configured")

    path = Path(str(pem_path))

    if not path.exists():
        pytest.skip(f"ED25519 private key file does not exist: {path}")

    return path.read_text()


def _make_adapter() -> BinanceRestAdapter:
    return BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=_load_pem(),
    )


def _position_id(
    *,
    symbol: str,
    position_side: PositionSide = PositionSide.BOTH,
) -> str:
    return make_position_id(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=position_side,
    )


async def _get_position_risk_rows(
    adapter: BinanceRestAdapter,
    *,
    symbol: str,
) -> list[PositionRiskRespDto]:
    if hasattr(adapter, "get_position_risk_v3"):
        rows = await adapter.get_position_risk_v3(symbol=symbol)
    elif hasattr(adapter, "get_position_risk"):
        rows = await adapter.get_position_risk(symbol=symbol)
    
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected positionRisk response: {rows}")

    return rows


def _pick_nonzero_position_row(
    rows: list[PositionRiskRespDto],
    *,
    preferred_side: PositionSide = PositionSide.BOTH,
) -> PositionRiskRespDto | None:
    preferred: PositionRiskRespDto | None = None
    fallback: PositionRiskRespDto | None = None

    for row in rows:
        assert row.positionAmt
        amt = Decimal(row.positionAmt)

        if amt == 0:
            continue

        assert row.positionSide
        raw_side = row.positionSide.upper()

        if raw_side == preferred_side.value:
            preferred = row
            break

        if fallback is None:
            fallback = row

    return preferred or fallback


async def _wait_for_position_risk_nonzero(
    adapter: BinanceRestAdapter,
    *,
    symbol: str,
    position_side: PositionSide,
    timeout_sec: float = 15.0,
    interval_sec: float = 0.5,
) -> PositionRiskRespDto:
    deadline = asyncio.get_running_loop().time() + timeout_sec
    last_rows: list[PositionRiskRespDto] = []

    while True:
        rows = await _get_position_risk_rows(adapter, symbol=symbol)
        last_rows = rows

        row = _pick_nonzero_position_row(
            rows,
            preferred_side=position_side,
        )

        if row is not None:
            return row

        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise AssertionError(
                {
                    "message": "Timed out waiting for nonzero positionRisk",
                    "symbol": symbol,
                    "position_side": position_side.value,
                    "last_rows": [row.__dict__ for row in last_rows],
                }
            )

        await asyncio.sleep(min(interval_sec, remaining))


def _assert_position_risk_flat(
    rows: list[PositionRiskRespDto],
    *,
    adapter: BinanceRestAdapter,
    position_side: PositionSide,
    context: str,
) -> None:
    existing_position = _pick_nonzero_position_row(
        rows,
        preferred_side=position_side,
    )

    if existing_position is not None:
        # pytest.skip(
        #     f"Existing testnet position detected during {context}: "
        #     f"{existing_position}. This E2E test expects a flat account."
        # )
        raise Exception(
            f"Existing testnet position detected during {context}: "
            f"{existing_position}. This E2E test expects a flat account."
        )


def _drain_position_queue(queue: asyncio.Queue[Position]) -> list[Position]:
    drained: list[Position] = []

    while True:
        try:
            drained.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return drained


def _is_reduce_only_rejected(order) -> bool:
    return (
        order.status == OrderStatus.REJECTED
        and str(order.exchange_error_code) == "-2022"
        and "ReduceOnly" in str(order.detail_msg or "")
    )


async def _close_testnet_position_without_reduce_only(
    *,
    adapter: BinanceRestAdapter,
    position_order_service: PositionOrderService,
    symbol: str,
    position_side: PositionSide,
) -> Any:
    rows = await _get_position_risk_rows(adapter, symbol=symbol)
    row = _pick_nonzero_position_row(rows, preferred_side=position_side)

    if row is None:
        raise AssertionError("reduceOnly close rejected but positionRisk is already flat")

    assert row.positionAmt
    amt = Decimal(row.positionAmt)
    side = OrderSide.SELL if amt > 0 else OrderSide.BUY

    logger.warning(
        "Binance testnet reduceOnly close rejected; "
        "fallback non-reduceOnly MARKET close 진행: symbol=%s, "
        "position_side=%s, position_amt=%s, side=%s",
        row.symbol,
        row.positionSide,
        row.positionAmt,
        side.value,
    )

    return await position_order_service.open_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        quantity=str(abs(amt)),
        position_side=position_side,
        source=OrderSource.MANUAL,
        strategy_name="account-update-e2e-close-testnet-fallback",
    )


async def _wait_for_position_update(
    queue: asyncio.Queue[Position],
    *,
    symbol: str,
    expected_status: PositionStatus,
    timeout_sec: float = 25.0,
) -> Position:
    deadline = asyncio.get_running_loop().time() + timeout_sec
    seen: list[dict[str, Any]] = []

    while True:
        remaining = deadline - asyncio.get_running_loop().time()

        if remaining <= 0:
            raise AssertionError(
                {
                    "message": "Timed out waiting for position update",
                    "symbol": symbol,
                    "expected_status": expected_status.value,
                    "seen": seen,
                }
            )

        position = await asyncio.wait_for(queue.get(), timeout=remaining)

        seen.append(
            {
                "position_id": position.position_id,
                "symbol": position.symbol,
                "position_side": position.position_side.value,
                "status": position.status.value,
                "position_amt": position.position_amt,
                "version": position.version,
            }
        )

        if position.symbol != symbol.upper():
            continue

        if position.status == expected_status:
            return position


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = common_settings.postgres_dsn

    assert dsn

    client = PostgresClient(
        dsn=dsn,
        min_size=1,
        max_size=3,
    )
    await client.connect()

    pool = client.require_pool()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox_events,
                orders,
                order_intents,
                positions
            RESTART IDENTITY CASCADE
            """
        )

    yield client

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox_events,
                orders,
                order_intents,
                positions
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis_client() -> RedisStreamClient:
    db = int(os.getenv("REDIS_TEST_DB", "15"))

    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=db,
    )

    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    await client.client.flushdb()

    yield client

    await client.client.flushdb()
    await client.close()


@pytest_asyncio.fixture
async def e2e_bundle(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
):
    # _require_real_account_update_tests_enabled()

    adapter = _make_adapter()

    order_state_repo = OrderStateRedisRepository(redis_client)
    position_redis_repo = PositionRedisRepository(redis_client)

    outbox_repo = OutboxPostgresRepository()

    order_state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=outbox_repo,
        redis_order_repo=order_state_repo,
    )

    position_state_service = PositionStateService(
        postgres=postgres_client,
        position_repo=PositionPostgresRepository(),
        outbox_repo=outbox_repo,
        redis_position_repo=position_redis_repo,
    )
    
    order_router = BinanceOrderRouter(adapter=adapter)
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=order_router,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        state_repo=order_state_repo,
        state_service=order_state_service,
        exchange_clients=exchange_clients,
    )

    position_order_service = PositionOrderService(
        position_state_service=position_state_service,
        gateway=gateway,
    )

    ws_base_url = os.getenv(
        "BINANCE_TESTNET_WS_PRIVATE_URL",
        gw_settings.binance_testnet_ws_url,
    )

    listener = BinanceUserDataStreamListener(
        rest_adapter=adapter,
        ws_base_url=ws_base_url,
    )

    try:
        yield {
            "adapter": adapter,
            "postgres": postgres_client,
            "redis": redis_client,
            "order_state_service": order_state_service,
            "position_state_service": position_state_service,
            "order_state_repo": order_state_repo,
            "position_redis_repo": position_redis_repo,
            "gateway": gateway,
            "position_order_service": position_order_service,
            "listener": listener,
        }

    finally:
        with contextlib.suppress(Exception):
            await listener.stop()

        await adapter.close()


@pytest.mark.asyncio
async def test_real_account_update_updates_position_postgres_and_redis_e2e(
    e2e_bundle,
) -> None:
    """
    실제 Binance Futures Testnet ACCOUNT_UPDATE E2E 테스트.

    이 테스트는 실제 MARKET 주문을 제출하므로 반드시 testnet에서만 실행한다.

    검증:
      1. BinanceUserDataStreamListener가 실제 ACCOUNT_UPDATE를 수신한다.
      2. PositionStateService.apply_position_snapshots()가 실제 호출된다.
      3. PostgreSQL positions에 OPEN 포지션이 저장된다.
      4. Redis position projection/index가 OPEN으로 갱신된다.
      5. close_position_market() 후 ACCOUNT_UPDATE로 FLAT이 반영된다.
      6. Redis open/symbol index에서 제거된다.
    """
    adapter: BinanceRestAdapter = e2e_bundle["adapter"]
    postgres: PostgresClient = e2e_bundle["postgres"]
    position_state_service: PositionStateService = e2e_bundle["position_state_service"]
    position_redis_repo: PositionRedisRepository = e2e_bundle["position_redis_repo"]
    position_order_service: PositionOrderService = e2e_bundle["position_order_service"]
    listener: BinanceUserDataStreamListener = e2e_bundle["listener"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")
    position_side = PositionSide.BOTH
    position_id = _position_id(symbol=symbol, position_side=position_side)

    # 테스트 시작 전, 기존 포지션이 있으면 위험하므로 중단.
    initial_rows = await _get_position_risk_rows(adapter, symbol=symbol)
    _assert_position_risk_flat(
        initial_rows,
        # symbol=symbol,
        adapter=adapter,
        position_side=position_side,
        context="initial setup",
    )

    queue: asyncio.Queue[Position] = asyncio.Queue()

    async def on_position_update(
        snapshots: list[NormalizedPositionSnapshot],
    ) -> None:
        updated_positions = await position_state_service.apply_position_snapshots(
            snapshots=snapshots,
        )

        for position in updated_positions:
            await queue.put(position)

    listener.on_position_update(on_position_update)

    listener_task: asyncio.Task | None = None
    opened = False

    try:
        listener_task = asyncio.create_task(
            listener.start(),
            name="real-account-update-position-e2e-listener",
        )

        # listenKey 생성 및 WebSocket 연결 시간 확보.
        await asyncio.sleep(2.0)

        post_listener_rows = await _get_position_risk_rows(adapter, symbol=symbol)
        _assert_position_risk_flat(
            post_listener_rows,
            adapter=adapter,
            position_side=position_side,
            context="post-listener warmup",
        )

        stale_positions = _drain_position_queue(queue)
        if stale_positions:
            logger.warning(
                "테스트 주문 전 stale position update queue drain: count=%s, rows=%s",
                len(stale_positions),
                [
                    {
                        "position_id": position.position_id,
                        "status": position.status.value,
                        "position_amt": position.position_amt,
                        "version": position.version,
                    }
                    for position in stale_positions
                ],
            )

        open_order = await position_order_service.open_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            position_side=position_side,
            source=OrderSource.MANUAL,
            strategy_name="account-update-e2e-open",
        )

        assert open_order.status == OrderStatus.ACKNOWLEDGED
        assert open_order.order_route == OrderRoute.REGULAR

        opened = True

        opened_position = await _wait_for_position_update(
            queue,
            symbol=symbol,
            expected_status=PositionStatus.OPEN,
            timeout_sec=30.0,
        )

        assert opened_position.position_id == position_id
        assert opened_position.symbol == symbol.upper()
        assert opened_position.position_side == position_side
        assert opened_position.status == PositionStatus.OPEN
        assert Decimal(opened_position.position_amt) != 0
        assert opened_position.opened_ts is not None
        assert opened_position.closed_ts is None

        loaded_position = await position_state_service.load_position(
            position_id=position_id,
            refresh_projection=True,
        )

        assert loaded_position is not None
        assert loaded_position.status == PositionStatus.OPEN
        assert Decimal(loaded_position.position_amt) != 0

        redis_row = await position_redis_repo.get(position_id)

        assert redis_row is not None
        assert redis_row["position_id"] == position_id
        assert redis_row["status"] == "OPEN"

        open_positions = await position_redis_repo.list_open_positions(
            exchange=Exchange.BINANCE.value,
        )

        assert any(row["position_id"] == position_id for row in open_positions)

        by_symbol = await position_redis_repo.list_by_symbol(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP.value,
            symbol=symbol,
        )

        assert any(row["position_id"] == position_id for row in by_symbol)

        risk_row = await _wait_for_position_risk_nonzero(
            adapter,
            symbol=symbol,
            position_side=position_side,
            timeout_sec=15.0,
        )
        logger.info(
            "positionRisk nonzero 확인 후 close 진행: symbol=%s, "
            "position_side=%s, position_amt=%s",
            risk_row.symbol,
            risk_row.positionSide,
            risk_row.positionAmt,
        )
        await asyncio.sleep(0.5)

        close_order = await position_order_service.close_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            position_side=position_side,
            source=OrderSource.MANUAL,
            strategy_name="account-update-e2e-close",
        )

        if _is_reduce_only_rejected(close_order):
            close_order = await _close_testnet_position_without_reduce_only(
                adapter=adapter,
                position_order_service=position_order_service,
                symbol=symbol,
                position_side=position_side,
            )

        assert close_order.status == OrderStatus.ACKNOWLEDGED
        assert close_order.order_route == OrderRoute.REGULAR

        flattened_position = await _wait_for_position_update(
            queue,
            symbol=symbol,
            expected_status=PositionStatus.FLAT,
            timeout_sec=30.0,
        )

        assert flattened_position.position_id == position_id
        assert flattened_position.status == PositionStatus.FLAT
        assert Decimal(flattened_position.position_amt) == 0
        assert flattened_position.opened_ts is not None
        assert flattened_position.closed_ts is not None

        loaded_flat = await position_state_service.load_position(
            position_id=position_id,
            refresh_projection=True,
        )

        assert loaded_flat is not None
        assert loaded_flat.status == PositionStatus.FLAT
        assert Decimal(loaded_flat.position_amt) == 0

        redis_flat = await position_redis_repo.get(position_id)

        assert redis_flat is not None
        assert redis_flat["status"] == "FLAT"
        assert redis_flat["position_amt"] == "0"

        open_positions_after_close = await position_redis_repo.list_open_positions(
            exchange=Exchange.BINANCE.value,
        )

        assert all(
            row["position_id"] != position_id for row in open_positions_after_close
        )

        by_symbol_after_close = await position_redis_repo.list_by_symbol(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP.value,
            symbol=symbol,
        )

        assert all(row["position_id"] != position_id for row in by_symbol_after_close)

        # PostgreSQL row 직접 확인.
        pool = postgres.require_pool()

        async with pool.acquire() as conn:
            pg_row = await conn.fetchrow(
                """
                SELECT position_id, status, position_amt, opened_ts, closed_ts
                FROM positions
                WHERE position_id = $1
                """,
                position_id,
            )

        assert pg_row is not None
        assert pg_row["status"] == "FLAT"
        assert Decimal(str(pg_row["position_amt"])) == 0
        assert pg_row["opened_ts"] is not None
        assert pg_row["closed_ts"] is not None

    finally:
        # 실패 중간에 포지션이 남으면 testnet에서 정리 시도.
        if opened:
            try:
                rows = await _get_position_risk_rows(adapter, symbol=symbol)
                row = _pick_nonzero_position_row(
                    rows,
                    preferred_side=position_side,
                )

                if row is not None:
                    assert row.positionAmt
                    amt = Decimal(row.positionAmt)

                    if amt != 0:
                        side = OrderSide.SELL if amt > 0 else OrderSide.BUY
                        cleanup_qty = str(abs(amt))

                        with contextlib.suppress(Exception):
                            await position_order_service.open_position_market(
                                exchange=Exchange.BINANCE,
                                market_type=MarketType.PERP,
                                symbol=symbol,
                                side=side,
                                quantity=cleanup_qty,
                                position_side=position_side,
                                source=OrderSource.MANUAL,
                                strategy_name="account-update-e2e-emergency-cleanup",
                            )

            except Exception:
                logger.exception("Failed to emergency cleanup testnet position")

        with contextlib.suppress(Exception):
            await listener.stop()

        if listener_task is not None:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task
