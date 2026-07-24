from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from execution_gateway.config import settings as gw_settings
from common.logging import setup_logger
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.services.order_state_service import OrderStateService
from execution_gateway.services.position_order_service import PositionOrderService
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
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


pytestmark = pytest.mark.integration

logger = setup_logger(__name__)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _require_real_tests_enabled() -> None:
    if os.getenv("RUN_BINANCE_REAL_TESTS") != "1":
        pytest.skip(
            "Real Binance testnet tests are disabled. "
            "Set RUN_BINANCE_REAL_TESTS=1 to run."
        )

    if os.getenv("RUN_POSITION_REAL_TESTS") != "1":
        pytest.skip(
            "Real position integration tests are disabled. "
            "Set RUN_POSITION_REAL_TESTS=1 to run."
        )


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


def _round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _enum_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


async def _get_position_risk_rows(
    adapter: BinanceRestAdapter,
    *,
    symbol: str,
) -> list[dict[str, Any]]:
    """
    실제 Binance Testnet positionRisk 조회.

    adapter.get_position_risk()가 있으면 그걸 사용하고,
    없으면 adapter._request()로 직접 호출한다.
    """
    if hasattr(adapter, "get_position_risk_v3"):
        rows = await adapter.get_position_risk_v3(symbol=symbol)
    elif hasattr(adapter, "get_position_risk"):
        rows = await adapter.get_position_risk(symbol=symbol)
    else:
        rows = await adapter._request(
            method="GET",
            path="/fapi/v3/positionRisk",
            params={"symbol": symbol},
        )

    if isinstance(rows, dict):
        rows = [rows]

    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected positionRisk response: {rows}")

    result = []
    for r in rows:
        if hasattr(r, "raw"):
            result.append(r.raw)
        else:
            result.append(r)
    return result


def _pick_nonzero_position_row(
    rows: list[dict[str, Any]],
    *,
    preferred_side: PositionSide = PositionSide.BOTH,
) -> dict[str, Any] | None:
    """
    positionRisk rows 중 positionAmt != 0인 row 선택.

    one-way 모드면 positionSide=BOTH row를 우선 선택한다.
    hedge 모드면 preferred_side와 맞는 row를 우선 선택하고,
    없으면 non-zero row 하나를 fallback으로 반환한다.
    """
    preferred: dict[str, Any] | None = None
    fallback: dict[str, Any] | None = None

    for row in rows:
        amt = Decimal(str(row.get("positionAmt", "0")))

        if amt == 0:
            continue

        raw_side = str(row.get("positionSide") or "BOTH").upper()

        if raw_side == preferred_side.value:
            preferred = row
            break

        if fallback is None:
            fallback = row

    return preferred or fallback


def _position_from_risk_row(
    row: dict[str, Any],
    *,
    exchange: Exchange = Exchange.BINANCE,
    market_type: MarketType = MarketType.PERP,
) -> Position:
    """
    Binance positionRisk row를 Takora Position 모델로 변환.
    """
    now = _now_ms()

    symbol = str(row["symbol"]).upper()
    position_side = PositionSide(str(row.get("positionSide") or "BOTH").upper())

    amt = Decimal(str(row.get("positionAmt", "0")))
    status = PositionStatus.OPEN if amt != 0 else PositionStatus.FLAT

    update_time_raw = row.get("updateTime")
    update_time = int(update_time_raw) if update_time_raw not in (None, "") else now

    return Position(
        position_id=make_position_id(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        ),
        exchange=exchange,
        market_type=market_type,
        symbol=symbol,
        position_side=position_side,
        status=status,
        position_amt=str(row.get("positionAmt", "0")),
        entry_price=(
            str(row.get("entryPrice"))
            if row.get("entryPrice") is not None
            else None
        ),
        break_even_price=(
            str(row.get("breakEvenPrice"))
            if row.get("breakEvenPrice") is not None
            else None
        ),
        mark_price=(
            str(row.get("markPrice"))
            if row.get("markPrice") is not None
            else None
        ),
        unrealized_pnl=(
            str(row.get("unRealizedProfit"))
            if row.get("unRealizedProfit") is not None
            else None
        ),
        isolated_margin=(
            str(row.get("isolatedMargin"))
            if row.get("isolatedMargin") is not None
            else None
        ),
        isolated_wallet=(
            str(row.get("isolatedWallet"))
            if row.get("isolatedWallet") is not None
            else None
        ),
        margin_type=(
            str(row.get("marginType"))
            if row.get("marginType") is not None
            else None
        ),
        leverage=(
            int(row["leverage"])
            if row.get("leverage") not in (None, "")
            else None
        ),
        liquidation_price=(
            str(row.get("liquidationPrice"))
            if row.get("liquidationPrice") is not None
            else None
        ),
        notional=(
            str(row.get("notional"))
            if row.get("notional") is not None
            else None
        ),
        update_reason="positionRisk",
        last_event_time=update_time,
        last_transaction_time=None,
        opened_ts=now if status == PositionStatus.OPEN else None,
        closed_ts=now if status == PositionStatus.FLAT else None,
        updated_ts=update_time,
        version=1,
    )


async def _seed_position_into_postgres(
    *,
    postgres: PostgresClient,
    position_repo: PositionPostgresRepository,
    position: Position,
) -> Position:
    """
    실제 PositionPostgresRepository.upsert()를 사용해
    positions 테이블에 테스트 position snapshot을 반영한다.

    raw INSERT를 직접 쓰지 않는다.
    """
    pool = postgres.require_pool()

    async with pool.acquire() as conn:
        persisted = await position_repo.upsert(
            conn,
            position=position,
        )

    return persisted


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN") or common_settings.postgres_dsn

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
async def real_modules_bundle(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
):
    """
    mock 없이 실제 모듈들만 조립한다.
    """
    # _require_real_tests_enabled()

    adapter = _make_adapter()

    order_state_repo = OrderStateRedisRepository(redis_client)

    order_state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=order_state_repo,
    )

    position_repo = PositionPostgresRepository()
    redis_position_repo = PositionRedisRepository(redis_client)

    position_state_service = PositionStateService(
        postgres=postgres_client,
        position_repo=position_repo,
        outbox_repo=OutboxPostgresRepository(),
        redis_position_repo=redis_position_repo,
    )

    from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
    from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
    from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter
    
    order_router = BinanceOrderRouter(adapter=adapter)
    client = BinanceExecutionClient(
        adapter=adapter,
        # rate_limiter=LocalBinanceRateLimiter(),
        order_router=order_router,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        # adapter=adapter,
        state_repo=order_state_repo,
        state_service=order_state_service,
        # rate_limiter=LocalBinanceRateLimiter(),
        exchange_clients=exchange_clients,
    )

    position_order_service = PositionOrderService(
        position_state_service=position_state_service,
        gateway=gateway,
    )

    try:
        yield {
            "adapter": adapter,
            "postgres": postgres_client,
            "redis": redis_client,
            "order_state_service": order_state_service,
            "position_state_service": position_state_service,
            "position_repo": position_repo,
            "redis_position_repo": redis_position_repo,
            "order_state_repo": order_state_repo,
            "gateway": gateway,
            "position_order_service": position_order_service,
        }

    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_real_position_order_service_open_then_protective_stop_then_cancel_then_close(
    real_modules_bundle,
) -> None:
    """
    mock 없이 실제 모듈만 사용하는 PositionOrderService 통합 테스트.

    실제 사용 모듈:
      - BinanceRestAdapter testnet
      - ExecutionGateway
      - OrderStateService
      - PositionStateService
      - PositionPostgresRepository
      - PositionRedisRepository
      - OrderPostgresRepository
      - OrderStateRedisRepository
      - PostgreSQL
      - Redis
      - PositionOrderService

    흐름:
      1. 실제 testnet positionRisk 조회
      2. 포지션이 없고 POSITION_TEST_ALLOW_OPEN=1이면 작은 MARKET 포지션 생성
      3. positionRisk를 Position 모델로 변환
      4. PositionPostgresRepository.upsert()로 positions에 반영
      5. PositionStateService.load_position(refresh_projection=True)로 실제 로드
      6. PositionOrderService.close_position_stop_market() 호출
      7. Gateway -> /fapi/v1/algoOrder 실제 보호 주문 생성
      8. Gateway.cancel_order()로 실제 algo order 취소
      9. Redis conditional open index 제거 확인
      10. 테스트가 열었던 포지션이면 close_position_market()으로 정리
    """
    adapter: BinanceRestAdapter = real_modules_bundle["adapter"]
    postgres: PostgresClient = real_modules_bundle["postgres"]
    position_repo: PositionPostgresRepository = real_modules_bundle["position_repo"]
    position_state_service: PositionStateService = real_modules_bundle[
        "position_state_service"
    ]
    order_state_service: OrderStateService = real_modules_bundle[
        "order_state_service"
    ]
    order_state_repo: OrderStateRedisRepository = real_modules_bundle[
        "order_state_repo"
    ]
    gateway: ExecutionGateway = real_modules_bundle["gateway"]
    position_order_service: PositionOrderService = real_modules_bundle[
        "position_order_service"
    ]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    qty = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    allow_open = os.getenv("POSITION_TEST_ALLOW_OPEN", "1") == "1"

    opened_by_test = False
    protective_order = None
    seeded_position: Position | None = None

    try:
        rows = await _get_position_risk_rows(adapter, symbol=symbol)
        row = _pick_nonzero_position_row(
            rows,
            preferred_side=PositionSide.BOTH,
        )

        if row is None:
            if not allow_open:
                pytest.skip(
                    "No existing testnet position. "
                    "Set POSITION_TEST_ALLOW_OPEN=1 to let the test open a small MARKET position."
                )

            open_order = await position_order_service.open_position_market(
                exchange=Exchange.BINANCE,
                market_type=MarketType.PERP,
                symbol=symbol,
                side=OrderSide.BUY,
                quantity=qty,
                position_side=PositionSide.BOTH,
                source=OrderSource.MANUAL,
                strategy_name="position-service-real-test-open",
            )

            assert open_order.status == OrderStatus.ACKNOWLEDGED
            assert open_order.order_route == OrderRoute.REGULAR

            opened_by_test = True

            await asyncio.sleep(2.0)

            rows = await _get_position_risk_rows(adapter, symbol=symbol)
            row = _pick_nonzero_position_row(
                rows,
                preferred_side=PositionSide.BOTH,
            )

        assert row is not None, rows

        seeded_position = _position_from_risk_row(row)
        assert seeded_position.status == PositionStatus.OPEN
        assert Decimal(seeded_position.position_amt) != 0

        persisted_position = await _seed_position_into_postgres(
            postgres=postgres,
            position_repo=position_repo,
            position=seeded_position,
        )

        assert persisted_position.position_id == seeded_position.position_id
        assert persisted_position.status == PositionStatus.OPEN

        assert seeded_position.position_id
        loaded_position = await position_state_service.load_position(
            position_id=seeded_position.position_id,
            refresh_projection=True,
        )

        assert loaded_position is not None
        assert loaded_position.position_id == seeded_position.position_id
        assert loaded_position.status == PositionStatus.OPEN
        assert Decimal(loaded_position.position_amt) != 0

        mark_price_raw = seeded_position.mark_price or row.get("markPrice")
        assert mark_price_raw is not None

        mark_price = Decimal(str(mark_price_raw))
        amt = Decimal(seeded_position.position_amt)

        # LONG이면 아래쪽 stop, SHORT이면 위쪽 stop.
        # 즉시 trigger되지 않도록 매우 멀리 둔다.
        if amt > 0:
            trigger_price = _round_down_to_step(
                mark_price * Decimal("0.5"),
                Decimal("0.1"),
            )
        else:
            trigger_price = _round_down_to_step(
                mark_price * Decimal("2"),
                Decimal("0.1"),
            )

        protective_order = await position_order_service.close_position_stop_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            trigger_price=str(trigger_price),
            position_side=seeded_position.position_side,
            source=OrderSource.MANUAL,
            use_close_position=True,
            strategy_name="position-service-real-test-stop",
        )

        assert protective_order.status == OrderStatus.ACKNOWLEDGED
        assert protective_order.order_route == OrderRoute.CONDITIONAL
        assert protective_order.conditional_status == ConditionalStatus.NEW
        assert protective_order.exchange_conditional_id not in (None, "")

        assert protective_order.order_id
        redis_projection = await order_state_repo.get(protective_order.order_id)

        assert redis_projection is not None
        assert redis_projection["order_id"] == protective_order.order_id
        assert redis_projection["order_route"] == "CONDITIONAL"
        assert redis_projection["conditional_status"] == "NEW"

        conditional_open = await order_state_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP,
        )

        assert any(
            row["order_id"] == protective_order.order_id
            for row in conditional_open
        )

        cancel_resp = await gateway.cancel_order(
            order_id=protective_order.order_id,
        )

        from execution_gateway.exchange import ExchangeCancelResult
        assert isinstance(cancel_resp, ExchangeCancelResult) or isinstance(cancel_resp, dict)

        loaded_order = await order_state_service.load_order(
            order_id=protective_order.order_id
        )

        assert loaded_order is not None
        assert loaded_order.status == OrderStatus.CANCELLED
        assert loaded_order.conditional_status == ConditionalStatus.CANCELLED

        conditional_open_after_cancel = (
            await order_state_repo.list_open_conditional_orders(
                exchange=Exchange.BINANCE.value,
                market_type=MarketType.PERP,
            )
        )

        assert all(
            row["order_id"] != protective_order.order_id
            for row in conditional_open_after_cancel
        )

    finally:
        # 보호 주문이 남아 있으면 반드시 취소 시도.
        if protective_order is not None:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=protective_order.client_conditional_id,
                    algo_id=protective_order.exchange_conditional_id,
                )
            except Exception:
                pass

        # 테스트가 실제로 포지션을 열었다면 MARKET close로 정리.
        # 기존에 사용자가 열어둔 testnet position은 닫지 않는다.
        if opened_by_test and seeded_position is not None:
            try:
                latest_rows = await _get_position_risk_rows(
                    adapter,
                    symbol=symbol,
                )

                latest_row = _pick_nonzero_position_row(
                    latest_rows,
                    preferred_side=seeded_position.position_side,
                )

                if latest_row is not None:
                    latest_position = _position_from_risk_row(latest_row)

                    await _seed_position_into_postgres(
                        postgres=postgres,
                        position_repo=position_repo,
                        position=latest_position,
                    )

                    await position_order_service.close_position_market(
                        exchange=Exchange.BINANCE,
                        market_type=MarketType.PERP,
                        symbol=symbol,
                        position_side=latest_position.position_side,
                        source=OrderSource.MANUAL,
                        strategy_name="position-service-real-test-cleanup",
                    )

            except Exception:
                logger.exception("Failed to cleanup real testnet position")