from __future__ import annotations

import os
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

import pytest
import pytest_asyncio

from common.time import epoch_ms
from execution_gateway.config import settings as gw_settings
from common.config import settings as common_settings
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
)
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.services.order_state_service import OrderStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    OrderRequest,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
)
from schemas.position import PositionSide
from storage.postgres_client import PostgresClient
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository

from execution_gateway.workers.reconciliation_worker import ReconciliationWorker

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter

from execution_gateway.exchange.capabilities import ExchangeCapabilities


pytestmark = pytest.mark.integration


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


async def _get_reference_price(
    adapter: BinanceRestAdapter,
    symbol: str,
) -> Decimal:
    ticker = await adapter.get_symbol_price_ticker(symbol)
    return Decimal(str(ticker.price))


async def _find_open_algo_order(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_algo_id: str,
):
    rows = await adapter.get_open_algo_orders(symbol=symbol)

    for row in rows:
        if row.clientAlgoId == client_algo_id:
            return row

    return None


async def _find_open_regular_order(
    *,
    adapter: BinanceRestAdapter,
    symbol: str,
    client_order_id: str,
):
    rows = await adapter.get_open_orders(symbol=symbol)

    for row in rows:
        if row.clientOrderId == client_order_id:
            return row

    return None


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = common_settings.postgres_dsn

    client = PostgresClient(
        # pyrefly: ignore [bad-argument-type]
        dsn=dsn,
        min_size=1,
        max_size=3,
    )
    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"PostgreSQL 연결 불가: {e}")

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
    db = common_settings.redis_db

    assert db

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
async def gateway_bundle(
    postgres_client: PostgresClient,
    redis_client: RedisStreamClient,
):

    adapter = _make_adapter()

    redis_order_repo = OrderStateRedisRepository(redis_client)

    state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=redis_order_repo,
    )
    
    order_router = BinanceOrderRouter(adapter=adapter)
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=order_router,
    )
    client.capabilities = ExchangeCapabilities(
        supports_bulk_order_lookup=True,
        bulk_order_lookup_threshold=3,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        state_repo=redis_order_repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )

    reconciliation_worker = ReconciliationWorker(
        exchange_clients=exchange_clients,
        gateway=gateway,
        order_state_service=state_service,
        redis_order_repo=redis_order_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        recent_grace_ms=0,
        all_orders_threshold=6,
    )
    
    try:
        yield {
            "adapter": adapter,
            "gateway": gateway,
            "state_service": state_service,
            "redis_order_repo": redis_order_repo,
            "reconciliation_worker": reconciliation_worker,
            "exchange_clients": exchange_clients,
        }

    finally:
        await adapter.close()



@pytest.mark.asyncio
async def test_real_conditional_reconciliation_keeps_open_algo_order(
    gateway_bundle,
) -> None:
    """실제 Binance 테스트넷 조건부 주문이 reconciliation 후에도 open 상태로 유지되는지 검증한다."""
    adapter = gateway_bundle["adapter"]
    gateway = gateway_bundle["gateway"]
    redis_order_repo = gateway_bundle["redis_order_repo"]
    reconciliation_worker = gateway_bundle["reconciliation_worker"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    ref_price = await _get_reference_price(adapter, symbol)
    trigger_price = _round_down_to_step(ref_price * Decimal("2"), Decimal("0.1"))

    order = await gateway.submit_order(
        req=OrderRequest(
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
            position_side=PositionSide.BOTH,
            position_action=PositionAction.OPEN,
        ),
        source=OrderSource.MANUAL,
        strategy_name="real-conditional-reconciliation-test",
    )

    try:
        result = await reconciliation_worker.reconcile_conditional_orders_once(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
        )

        assert result["checked"] >= 1

        loaded = await gateway.state_service.load_order_from_postgres(order_id=order.order_id)

        assert loaded is not None
        assert loaded.conditional_status in {
            ConditionalStatus.NEW,
            ConditionalStatus.ACTIVE,
        }

        rows = await redis_order_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP,
        )

        assert any(row["order_id"] == order.order_id for row in rows)

    finally:
        try:
            await gateway.cancel_order(order_id=order.order_id)
        except Exception:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=order.client_conditional_id,
                    algo_id=order.exchange_conditional_id,
                )
            except Exception:
                pass


@pytest.mark.asyncio
async def test_real_conditional_orphan_cancel_policy_cancels_exchange_order(
    gateway_bundle,
) -> None:
    """PG/Redis에 없는 실제 Binance 조건부 주문을 cancel 정책 reconciliation이 취소하는지 검증한다."""
    adapter = gateway_bundle["adapter"]
    gateway = gateway_bundle["gateway"]
    state_service = gateway_bundle["state_service"]
    redis_order_repo = gateway_bundle["redis_order_repo"]
    exchange_clients = gateway_bundle["exchange_clients"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")
    client_algo_id = f"ORPH{epoch_ms()}"

    ref_price = await _get_reference_price(adapter, symbol)
    trigger_price = _round_down_to_step(ref_price * Decimal("2"), Decimal("0.1"))

    anchor_order = await gateway.submit_order(
        req=OrderRequest(
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
            position_side=PositionSide.BOTH,
            position_action=PositionAction.OPEN,
        ),
        source=OrderSource.MANUAL,
        strategy_name="real-conditional-orphan-cancel-anchor",
    )

    assert anchor_order.status == OrderStatus.ACKNOWLEDGED

    created = await adapter.place_algo_order(
        {
            "algoType": "CONDITIONAL",
            "symbol": symbol,
            "side": "BUY",
            "positionSide": "BOTH",
            "type": "STOP_MARKET",
            "triggerPrice": str(trigger_price),
            "clientAlgoId": client_algo_id,
            "quantity": quantity,
        }
    )

    try:
        assert created.clientAlgoId == client_algo_id
        assert await _find_open_algo_order(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
        )

        cancel_worker = ReconciliationWorker(
            exchange_clients=exchange_clients,
            gateway=gateway,
            order_state_service=state_service,
            redis_order_repo=redis_order_repo,
            markets=[(Exchange.BINANCE, MarketType.PERP)],
            recent_grace_ms=0,
            external_orphan_policy="cancel",
        )

        result = await cancel_worker.reconcile_conditional_orders_once(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
        )

        assert result["orphan_exchange"] >= 1
        assert await _find_open_algo_order(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
        ) is None

    finally:
        try:
            await gateway.cancel_order(order_id=anchor_order.order_id)
        except Exception:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=anchor_order.client_conditional_id,
                    algo_id=anchor_order.exchange_conditional_id,
                )
            except Exception:
                pass

        try:
            await adapter.cancel_algo_order(
                symbol=symbol,
                client_algo_id=client_algo_id,
                algo_id=created.algoId,
            )
        except Exception:
            pass


@pytest.mark.asyncio
async def test_real_regular_orphan_cancel_policy_cancels_exchange_order(
    gateway_bundle,
) -> None:
    """PG/Redis에 없는 실제 Binance 일반 주문을 cancel 정책 reconciliation이 취소하는지 검증한다."""
    adapter = gateway_bundle["adapter"]
    gateway = gateway_bundle["gateway"]
    state_service = gateway_bundle["state_service"]
    redis_order_repo = gateway_bundle["redis_order_repo"]
    exchange_clients = gateway_bundle["exchange_clients"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")
    client_order_id = f"OREG{epoch_ms()}"

    ref_price = await _get_reference_price(adapter, symbol)
    limit_price = _round_down_to_step(ref_price * Decimal("0.9"), Decimal("0.1"))

    created = await adapter.place_regular_order(
        {
            "symbol": symbol,
            "side": "BUY",
            "type": "LIMIT",
            "quantity": quantity,
            "newClientOrderId": client_order_id,
            "positionSide": "BOTH",
            "price": str(limit_price),
            "timeInForce": "GTC",
        }
    )

    try:
        assert created.clientOrderId == client_order_id
        assert await _find_open_regular_order(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        )

        cancel_worker = ReconciliationWorker(
            exchange_clients=exchange_clients,
            gateway=gateway,
            order_state_service=state_service,
            redis_order_repo=redis_order_repo,
            markets=[(Exchange.BINANCE, MarketType.PERP)],
            recent_grace_ms=0,
            external_orphan_policy="cancel",
        )

        result = await cancel_worker.reconcile_regular_orders_once(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
        )

        assert result["exchange_extra_vs_pg"] >= 1
        assert await _find_open_regular_order(
            adapter=adapter,
            symbol=symbol,
            client_order_id=client_order_id,
        ) is None

    finally:
        try:
            await adapter.cancel_order(
                symbol=symbol,
                client_order_id=client_order_id,
            )
        except Exception:
            pass
