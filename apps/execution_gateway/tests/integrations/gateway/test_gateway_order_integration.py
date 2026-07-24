"""
ExecutionGateway 실제 연동 테스트 (Binance Testnet + Redis).

`test_gateway.py`의 mock 단위 테스트와 역할을 나눈다:
  - 단위: 게이트웨이 로직·분기·Redis 호출 횟수
  - 본 모듈: BinanceRestAdapter 서명·REST 응답과 OrderStateRedisRepository 저장이
    함께 맞물리는지 (실제 네트워크·Redis 필요)

실행: `make run-pytest-integration` (또는 `pytest -m integration`).
"""

from __future__ import annotations

from redis import asyncio

import os
import time

import pytest
import pytest_asyncio

from common.config import settings as common_settings
from common.logging import setup_logger
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceKeyType,
    BinanceRestAdapter,
    BinanceApiError,
)
from execution_gateway.config import settings as gw_settings
from execution_gateway.gateway import ExecutionGateway
from execution_gateway.gateway.cancellation_service import (
    CancelOrderSkipped,
    CancelSkipReason,
)
from execution_gateway.gateway.dto.cancel_service_resp import BatchCancelResultStatus
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRequest,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    TimeInForce,
    RejectReason,
    PositionAction,
)
from schemas.position import PositionSide

from execution_gateway.exchange import (
    ExchangeApiError,
    ExchangeCapabilities,
    ExchangeErrorCategory,
)
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from execution_gateway.services.order_state_service import OrderStateService

from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.binance_order_router import BinanceOrderRouter

from schemas.order_update_event import NormalizedOrderUpdateEvent
from common.time import epoch_ms

from execution_gateway.exchange import ExchangeOrderSnapshot, ExchangeCancelResult, ExchangeConditionalSnapshot

import os
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.order_intent_repo import (
    OrderIntentPostgresRepository,
)
from storage.repositories.postgres.order_repo import OrderPostgresRepository
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository

from schemas.conditional_order_event import NormalizedConditionalOrderEvent
from common.time import epoch_ms

import os
from decimal import Decimal, ROUND_DOWN
from pathlib import Path

from typing import Any

from execution_gateway.adapters.binance.constant.binance_constant import BinanceConditionalOrderState 


pytestmark = pytest.mark.integration
logger = setup_logger(__name__)


class RejectingExecutionClient:
    @property
    def exchange(self) -> Exchange:
        return Exchange.BINANCE

    @property
    def market_type(self) -> MarketType:
        return MarketType.PERP

    @property
    def capabilities(self) -> ExchangeCapabilities:
        return ExchangeCapabilities()

    async def place_order(self, order):
        raise ExchangeApiError(
            exchange=Exchange.BINANCE,
            category=ExchangeErrorCategory.EXCHANGE_REJECTED,
            code=-2010,
            status_code=400,
            message="forced exchange rejection for integration test",
            raw={
                "code": -2010,
                "msg": "forced exchange rejection for integration test",
            },
        )

    async def close(self) -> None:
        return None


def _load_pem() -> str:
    pem_path = gw_settings.active_ed25519_key_pem
    if not pem_path or not os.path.exists(pem_path):
        pytest.skip(f"PEM 파일이 없습니다: {pem_path}")
    with open(pem_path, "r") as f:
        return f.read()


def _assert_testnet_base_url() -> None:
    base = gw_settings.binance_testnet_rest_url.rstrip("/")
    allowed = {
        "https://demo-fapi.binance.com",
        "https://testnet.binancefuture.com",
    }
    if base not in allowed:
        pytest.skip(f"Testnet/Demo endpoint가 아닙니다: {base}")


def _row_status(row: dict) -> str:
    status = row.get("status")
    if hasattr(status, "value"):
        return status.value
    return str(status)


def _make_req(
    *,
    symbol: str = "BTCUSDT",
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.LIMIT,
    time_in_force: TimeInForce = TimeInForce.GTC,
    price: str = "10000",
    quantity: str = "0.01",
) -> OrderRequest:
    return OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        side=side,
        order_type=order_type,
        time_in_force=time_in_force,
        price=price,
        quantity=quantity,
        position_action=PositionAction.OPEN,
    )


def _make_cancel_skip_order(
    *,
    order_id: str,
    status: OrderStatus,
    order_route: OrderRoute = OrderRoute.REGULAR,
    conditional_status: ConditionalStatus | None = None,
) -> Order:
    now = epoch_ms()
    is_conditional = order_route == OrderRoute.CONDITIONAL

    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET if is_conditional else OrderType.LIMIT,
        order_route=order_route,
        time_in_force=None if is_conditional else TimeInForce.GTC,
        quantity="0.01",
        price=None if is_conditional else "10000",
        trigger_price="150000" if is_conditional else None,
        reduce_only=False,
        close_position=False,
        client_order_id=None if is_conditional else order_id,
        exchange_order_id=None if is_conditional else f"EX-{order_id}",
        client_conditional_id=order_id if is_conditional else None,
        exchange_conditional_id=f"ALGO-{order_id}" if is_conditional else None,
        conditional_status=conditional_status,
        exchange_conditional_status=(
            conditional_status.value if conditional_status else None
        ),
        triggered_order_id="TRIGGERED-ORDER-123" if is_conditional else None,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=status,
        created_ts=now,
        updated_ts=now,
        version=1,
    )


# 테스트 시작 전에 DB 15를 전부 비운다.
# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis_stream_client() -> RedisStreamClient:
    ################
    client = RedisStreamClient(
        host=common_settings.redis_host,
        port=common_settings.redis_port,
        db=15,  # integration test는 DB 15 고정 권장
    )
    try:
        await client.connect()
    except Exception as e:
        pytest.skip(f"Redis 연결 불가: {e}")

    # 테스트 격리: 테스트 시작 전에 DB 15를 전부 비운다.
    try:
        await client.client.flushdb()
    except Exception:
        pass

    # 테스트 실행 전 준비 코드
    yield client
    # 테스트 종료 후 정리 코드
    try:
        await client.client.flushdb()
    except Exception:
        pass

    await client.close()
    ################


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def postgres_client() -> PostgresClient:
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN이 설정되지 않았습니다.")

    client = PostgresClient(
        dsn=dsn,
        min_size=1,
        max_size=2,
    )
    await client.connect()

    pool = client.require_pool()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE TABLE
                outbox_events,
                orders,
                order_intents
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
                order_intents
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
async def real_gateway(
    redis_stream_client: RedisStreamClient, postgres_client: PostgresClient
# pyrefly: ignore [bad-return]
) -> ExecutionGateway:
    _assert_testnet_base_url()

    pem = _load_pem()

    rest = BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem,
    )
    repo = OrderStateRedisRepository(redis=redis_stream_client)

    state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=repo,
    )

    order_router = BinanceOrderRouter(adapter=rest)
    client = BinanceExecutionClient(
        adapter=rest,
        order_router=order_router,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        state_repo=repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )
    # gateway.adapter = rest

    try:
        yield gateway
    finally:
        await rest.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_submit_ack_and_cancel_roundtrip(real_gateway:ExecutionGateway):
    """
    실제 Testnet 주문 생성 후 취소.
    가격을 멀리 두어 체결 가능성을 낮춘다.
    """
    repo = real_gateway.state_repo

    req = _make_req(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        price="10000",
        quantity="0.01",
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)

        assert order.status == OrderStatus.ACKNOWLEDGED, order.status
        assert order.exchange_order_id

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.ACKNOWLEDGED.value
        assert row["symbol"] == "BTCUSDT"

        cancel_resp = await real_gateway.cancel_order(order.order_id)
        assert isinstance(cancel_resp, ExchangeCancelResult)
        assert cancel_resp.status == OrderStatus.CANCELLED or cancel_resp.raw_status in (BinanceConditionalOrderState.canceled, )

        row2 = await repo.get(order.order_id)
        assert row2 is not None
        assert _row_status(row2) == OrderStatus.CANCELLED.value

    finally:
        if order and order.order_id:
            try:
                await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(
                    order=order
                )
            except Exception:
                pass
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_submit_invalid_symbol_rejected(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo

    req = _make_req(
        symbol="NOTREALUSDT",
        price="10000",
        quantity="0.01",
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)

        assert order.status == OrderStatus.REJECTED
        assert order.reject_reason in {
            RejectReason.INVALID_SYMBOL,
            RejectReason.EXCHANGE_REJECTED,
        }
        assert order.exchange_error_code is not None
        assert order.detail_msg

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.REJECTED.value

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_gateway_submit_exchange_rejected_error_is_persisted(
    redis_stream_client: RedisStreamClient,
    postgres_client: PostgresClient,
):
    repo = OrderStateRedisRepository(redis=redis_stream_client)

    state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=repo,
    )

    exchange_clients = ExchangeExecutionClientRegistry()

    # pyrefly: ignore [bad-argument-type]
    exchange_clients.register(RejectingExecutionClient())

    gateway = ExecutionGateway(
        state_repo=repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )

    order = None
    try:
        order = await gateway.submit_order(_make_req())

        assert order.status == OrderStatus.REJECTED
        assert order.reject_reason == RejectReason.EXCHANGE_REJECTED
        assert order.exchange_error_code == -2010
        assert order.detail_msg == "forced exchange rejection for integration test"

        assert order.order_id

        redis_row = await repo.get(order.order_id)
        assert redis_row is not None
        assert _row_status(redis_row) == OrderStatus.REJECTED.value
        assert redis_row["reject_reason"] == RejectReason.EXCHANGE_REJECTED.value
        assert str(redis_row["exchange_error_code"]) == "-2010"
        assert redis_row["detail_msg"] == "forced exchange rejection for integration test"

        pg_order = await state_service.load_order(order_id=order.order_id)
        assert pg_order is not None
        assert pg_order.status == OrderStatus.REJECTED
        assert pg_order.reject_reason == RejectReason.EXCHANGE_REJECTED
        assert pg_order.exchange_error_code == -2010
        assert pg_order.detail_msg == "forced exchange rejection for integration test"

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_submit_invalid_precision_rejected(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo

    req = _make_req(
        symbol="BTCUSDT",
        price="10000.123456789123456789",
        quantity="0.010000000000000001",
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)

        assert order.status == OrderStatus.REJECTED
        assert order.reject_reason in {
            RejectReason.INVALID_SYMBOL,
            RejectReason.EXCHANGE_REJECTED,
        }
        assert order.exchange_error_code is not None
        assert order.detail_msg

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.REJECTED.value

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_submit_gtx_would_match_rejected(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo

    # BTCUSDT에서 BUY GTX를 말도 안 되게 높은 가격으로 넣으면 즉시 체결 가능성이 높아져 reject 예상
    req = _make_req(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTX,
        price="999999",
        quantity="0.01",
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)

        assert order.status == OrderStatus.REJECTED
        assert order.reject_reason in {
            RejectReason.EXCHANGE_REJECTED,
            RejectReason.INSUFFICIENT_BALANCE,
        }
        assert order.exchange_error_code is not None
        assert order.detail_msg

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.REJECTED.value

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_cancel_unknown_order_raises(real_gateway: ExecutionGateway):
    fake_client_order_id = f"NO-SUCH-ORDER-{int(time.time() * 1000)}"

    with pytest.raises(CancelOrderSkipped) as exc:
        await real_gateway.cancel_order(fake_client_order_id)

    assert exc.value.reason == CancelSkipReason.LOCAL_ORDER_NOT_FOUND
    assert exc.value.order_id == fake_client_order_id


@pytest.mark.stable
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_reason", "expected_http_status"),
    [
        (OrderStatus.CANCELLED, CancelSkipReason.ALREADY_CANCELLED, 200),
        (OrderStatus.FILLED, CancelSkipReason.ALREADY_FILLED, 409),
        (OrderStatus.REJECTED, CancelSkipReason.ALREADY_REJECTED, 409),
        (OrderStatus.EXPIRED, CancelSkipReason.ALREADY_EXPIRED, 409),
    ],
)
async def test_gateway_cancel_terminal_orders_raise_cancel_skip_reason(
    real_gateway: ExecutionGateway,
    status: OrderStatus,
    expected_reason: CancelSkipReason,
    expected_http_status: int,
):
    order_id = f"TEST-CANCEL-SKIP-{status.value}-{int(time.time() * 1000)}"
    order = _make_cancel_skip_order(order_id=order_id, status=status)

    await real_gateway.state_service.create_order(order)

    try:
        with pytest.raises(CancelOrderSkipped) as exc:
            await real_gateway.cancel_order(order_id)

        assert exc.value.reason == expected_reason
        assert exc.value.order_id == order_id
        assert exc.value.status == status
        assert exc.value.conditional_status is None
        assert exc.value.http_status == expected_http_status

        payload = exc.value.to_payload()
        assert payload["skipped"] is True
        assert payload["reason"] == expected_reason.value
        assert payload["order_id"] == order_id
        assert payload["status"] == status.value

    finally:
        await real_gateway.state_repo.delete(order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_gateway_cancel_triggered_conditional_order_raises_cancel_skip_reason(
    real_gateway: ExecutionGateway,
):
    order_id = f"TEST-COND-CANCEL-SKIP-{int(time.time() * 1000)}"
    order = _make_cancel_skip_order(
        order_id=order_id,
        status=OrderStatus.ACKNOWLEDGED,
        order_route=OrderRoute.CONDITIONAL,
        conditional_status=ConditionalStatus.TRIGGERED,
    )

    await real_gateway.state_service.create_order(order)

    try:
        with pytest.raises(CancelOrderSkipped) as exc:
            await real_gateway.cancel_order(order_id)

        assert exc.value.reason == CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE
        assert exc.value.order_id == order_id
        assert exc.value.status == OrderStatus.ACKNOWLEDGED
        assert exc.value.conditional_status == ConditionalStatus.TRIGGERED
        assert exc.value.triggered_order_id == "TRIGGERED-ORDER-123"
        assert exc.value.http_status == 409

        payload = exc.value.to_payload()
        assert payload["skipped"] is True
        assert payload["reason"] == CancelSkipReason.CONDITIONAL_ORDER_NOT_CANCELABLE.value
        assert payload["order_id"] == order_id
        assert payload["status"] == OrderStatus.ACKNOWLEDGED.value
        assert payload["conditional_status"] == ConditionalStatus.TRIGGERED.value
        assert payload["triggered_order_id"] == "TRIGGERED-ORDER-123"

    finally:
        await real_gateway.state_repo.delete(order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_cancel_already_cancelled_order_raises(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo

    req = _make_req(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        price="10000",
        quantity="0.01",
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)
        assert order.status == OrderStatus.ACKNOWLEDGED

        assert order.order_id
        cancel_resp = await real_gateway.cancel_order(order.order_id)
        assert isinstance(cancel_resp, ExchangeCancelResult)
        assert cancel_resp.status == OrderStatus.CANCELLED and cancel_resp.raw_status in (BinanceConditionalOrderState.canceled,)

        # Gateway는 로컬 terminal 상태면 skip할 수 있음.
        # 실제 Binance 에러를 보고 싶으면 adapter를 직접 호출한다.
        with pytest.raises(ExchangeApiError) as exc:
            await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(
                order=order,
            )

        assert exc.value.code and int(exc.value.code) < 0

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_adapter_invalid_api_key_raises():
    _assert_testnet_base_url()

    pem = _load_pem()

    bad_rest = BinanceRestAdapter(
        base_url=gw_settings.binance_testnet_rest_url,
        api_key="INVALID_API_KEY_FOR_TEST",
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem,
    )

    try:
        with pytest.raises(BinanceApiError) as exc:
            await bad_rest.get_open_orders("BTCUSDT")

        assert exc.value.code in (-2014, -2015)

    finally:
        await bad_rest.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_network_connection_error_becomes_internal_error(
    redis_stream_client, postgres_client
):
    pem = _load_pem()

    bad_rest = BinanceRestAdapter(
        base_url="http://127.0.0.1:9",  # 보통 discard port, 연결 실패 가능
        api_key=gw_settings.active_api_key,
        key_type=BinanceKeyType.ED25519,
        private_key_pem=pem,
        timeout=0.2,
    )

    repo = OrderStateRedisRepository(redis=redis_stream_client)

    state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=repo,
    )

    order_router = BinanceOrderRouter(adapter=bad_rest)
    client = BinanceExecutionClient(
        adapter=bad_rest,
        order_router=order_router,
    )
    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(client)

    gateway = ExecutionGateway(
        state_repo=repo,
        state_service=state_service,
        exchange_clients=exchange_clients,
    )
    # gateway.adapter = bad_rest

    order = None
    try:
        order = await gateway.submit_order(_make_req())

        assert order.status == OrderStatus.UNKNOWN
        assert order.reject_reason == RejectReason.UNKNOWN_EXECUTION

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.UNKNOWN.value

    finally:
        if order and order.order_id:
            await repo.delete(order.order_id)
        await bad_rest.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_batch_submit_and_cancel(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    req1 = _make_req(symbol="BTCUSDT", price="10000", quantity="0.01")
    req2 = _make_req(symbol="BTCUSDT", price="10001", quantity="0.01")

    orders = []
    try:
        orders = await real_gateway.submit_batch_orders(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            requests=[req1, req2],
        )
        assert len(orders) == 2

        assert orders[0].status == OrderStatus.ACKNOWLEDGED
        assert orders[1].status == OrderStatus.ACKNOWLEDGED

        order_ids = [o.order_id for o in orders if o.order_id]

        # Verify in redis
        for oid in order_ids:
            row = await repo.get(oid)
            assert row is not None
            assert _row_status(row) == OrderStatus.ACKNOWLEDGED.value

        # time.sleep(5)

        cancel_results = await real_gateway.cancel_batch_orders(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            order_ids=order_ids,
        )
        assert len(cancel_results) == 2
        for res in cancel_results:
            assert res.result != BatchCancelResultStatus.SKIPPED
            assert res.result == BatchCancelResultStatus.CANCELLED

        # Verify cancelled in redis
        for oid in order_ids:
            row = await repo.get(oid)
            assert row is not None
            assert _row_status(row) == OrderStatus.CANCELLED.value

    finally:
        for o in orders:
            if o and o.order_id:
                try:
                    await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(order=o)
                except Exception:
                    pass
                await repo.delete(o.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_change_leverage(real_gateway: ExecutionGateway):
    symbol = "BTCUSDT"
    # pyrefly: ignore [missing-attribute]
    adapter = real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).adapter
    rows = await adapter.get_symbol_config(symbol=symbol)
    original_leverage = rows[0].leverage

    target_leverage = 3 if original_leverage != 3 else 2

    try:
        res = await real_gateway.change_leverage(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            leverage=target_leverage,
        )
        assert res.leverage == target_leverage

        # Verify on Binance
        updated_rows = await adapter.get_symbol_config(symbol=symbol)

        print(updated_rows)
        assert updated_rows[0].leverage == target_leverage
    finally:
        # Restore
        await real_gateway.change_leverage(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            leverage=original_leverage,
        )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_cancel_all_regular_and_open_orders(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    req1 = _make_req(symbol="BTCUSDT", price="10000", quantity="0.01")
    req2 = _make_req(symbol="BTCUSDT", price="10001", quantity="0.01")

    orders = []
    try:
        orders = [
            await real_gateway.submit_order(req1),
            await real_gateway.submit_order(req2),
        ]
        assert orders[0].status == OrderStatus.ACKNOWLEDGED
        assert orders[1].status == OrderStatus.ACKNOWLEDGED

        # Cancel all regular open orders
        res = await real_gateway.cancel_all_regular_open_orders(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
        )
        # Verify in redis - should be in PENDING_CANCEL state before WS execution report confirms cancel
        for o in orders:
            assert o.order_id
            row = await repo.get(o.order_id)
            assert row is not None
            assert _row_status(row) == OrderStatus.PENDING_CANCEL.value

        # Place another order to test cancel_all_open_orders
        order3 = await real_gateway.submit_order(req1)
        assert order3.status == OrderStatus.ACKNOWLEDGED

        res_all = await real_gateway.cancel_all_open_orders(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
        )

        assert order3.order_id
        row3 = await repo.get(order3.order_id)
        assert row3 is not None
        assert _row_status(row3) == OrderStatus.PENDING_CANCEL.value

    finally:
        for o in orders:
            if o and o.order_id:
                try:
                    await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(order=o)
                except Exception:
                    pass
                await repo.delete(o.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_cancel_all_conditional_open_orders(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET,
        time_in_force=TimeInForce.GTC,
        price=None,  # STOP_MARKET cannot have price
        stop_price="150000",  # Set far above current price to avoid immediate trigger
        quantity="0.01",
        position_action=PositionAction.OPEN,
        order_route=OrderRoute.CONDITIONAL,
    )

    order = None
    try:
        order = await real_gateway.submit_order(req)
        assert order.order_id

        # Verify in redis
        row = await repo.get(order.order_id)
        assert row is not None

        # Cancel all conditional open orders
        res:ExchangeCancelResult  = await real_gateway.cancel_all_conditional_open_orders(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
        )

        assert res.exchange == Exchange.BINANCE
        assert res.market_type == MarketType.PERP

        # Verify conditional order is PENDING_CANCEL in redis
        row2 = await repo.get(order.order_id)
        assert row2 is not None
        assert _row_status(row2) == OrderStatus.PENDING_CANCEL.value or row2.get("conditional_status") == "PENDING_CANCEL"

    finally:
        if order and order.order_id:
            try:
                # Cancel on adapter directly to be safe
                # pyrefly: ignore [missing-attribute]
                adapter = real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).adapter
                await adapter.cancel_algo_order(
                    symbol="BTCUSDT",
                    client_algo_id=order.client_conditional_id,
                    algo_id=order.exchange_conditional_id,
                )
            except Exception:
                pass
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_apply_order_update_event(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    req = _make_req(symbol="BTCUSDT", price="10000", quantity="0.01")

    order = None
    try:
        order = await real_gateway.submit_order(req)
        assert order.status == OrderStatus.ACKNOWLEDGED

        # Construct NormalizedOrderUpdateEvent

        assert order.order_id
        event = NormalizedOrderUpdateEvent(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            client_order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            target_status=OrderStatus.FILLED,
            filled_quantity="0.01",
            avg_fill_price="10000",
            event_time=epoch_ms(),
            raw={},
        )

        updated_order = await real_gateway.apply_order_update_event(event)
        assert updated_order is not None
        assert updated_order.status == OrderStatus.FILLED
        assert updated_order.filled_quantity == "0.01"
        assert updated_order.avg_fill_price == "10000"

        # Verify in redis

        assert order.order_id
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.FILLED.value

    finally:
        if order and order.order_id:
            try:
                await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(order=order)
            except Exception:
                pass
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_apply_reconciliation_order_snapshot(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    req = _make_req(symbol="BTCUSDT", price="10000", quantity="0.01")

    order = None
    try:
        order = await real_gateway.submit_order(req)
        assert order.status == OrderStatus.ACKNOWLEDGED

        # Construct ExchangeOrderSnapshot
        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            client_order_id=order.order_id,
            exchange_order_id=order.exchange_order_id,
            status=OrderStatus.FILLED,
            filled_quantity="0.01",
            avg_fill_price="10000",
            raw={},
        )

        assert order.order_id
        updated_order = await real_gateway.apply_reconciliation_order_snapshot(
            order_id=order.order_id,
            snapshot=snapshot,
        )
        assert updated_order is not None
        assert updated_order.status == OrderStatus.FILLED
        assert updated_order.filled_quantity == "0.01"
        assert updated_order.avg_fill_price == "10000"

        # Verify in redis
        row = await repo.get(order.order_id)
        assert row is not None
        assert _row_status(row) == OrderStatus.FILLED.value

    finally:
        if order and order.order_id:
            try:
                await real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).cancel_order(order=order)
            except Exception:
                pass
            await repo.delete(order.order_id)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_apply_conditional_order_snapshot_and_event(real_gateway: ExecutionGateway):
    repo = real_gateway.state_repo
    
    req = OrderRequest(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET,
        time_in_force=TimeInForce.GTC,
        price=None,  # STOP_MARKET cannot have price
        stop_price="150000",  # Set far above current price to avoid immediate trigger
        quantity="0.01",
        position_action=PositionAction.OPEN,
        order_route=OrderRoute.CONDITIONAL,
    )

    order = None
    order2 = None
    try:
        order = await real_gateway.submit_order(req)
        assert order.order_id

        # 1. Test apply_conditional_order_snapshot (transition to TRIGGERED)
        snapshot = ExchangeConditionalSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            client_conditional_id=order.client_conditional_id,
            exchange_conditional_id=order.exchange_conditional_id,
            conditional_status=ConditionalStatus.TRIGGERED,
            triggered_order_id="triggered-123",
            triggered_client_order_id="triggered-client-123",
            filled_quantity="0.01",
            avg_fill_price="150000",
            raw_status="NEW",
            raw={},
        )

        updated_order = await real_gateway.apply_conditional_order_snapshot(snapshot=snapshot)
        assert updated_order is not None
        assert updated_order.conditional_status == ConditionalStatus.TRIGGERED
        assert updated_order.triggered_order_id == snapshot.triggered_order_id
        assert updated_order.triggered_client_order_id == snapshot.triggered_client_order_id
        assert updated_order.filled_quantity == "0.01"
        assert updated_order.avg_fill_price == "150000"

        # Verify in redis
        row = await repo.get(order.order_id)
        assert row is not None
        assert row.get("conditional_status") == ConditionalStatus.TRIGGERED.value

        # 2. Test apply_conditional_order_event (transition to CANCELLED)
        order2 = await real_gateway.submit_order(req)
        assert order2.order_id

        event = NormalizedConditionalOrderEvent(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            client_conditional_id=order2.client_conditional_id,
            exchange_conditional_id=order2.exchange_conditional_id,
            target_status=ConditionalStatus.CANCELLED,
            exchange_conditional_status=BinanceConditionalOrderState.canceled,
            event_time=epoch_ms(),
            raw={},
        )

        updated_order2 = await real_gateway.apply_conditional_order_event(event)
        assert updated_order2 is not None
        assert updated_order2.conditional_status == ConditionalStatus.CANCELLED

        # Verify in redis
        row2 = await repo.get(order2.order_id)
        assert row2 is not None
        assert row2.get("conditional_status") == ConditionalStatus.CANCELLED.value

    finally:
        for o in (order, order2):
            if o and o.order_id:
                try:
                    # pyrefly: ignore [missing-attribute]
                    adapter = real_gateway.exchange_clients.get(exchange=Exchange.BINANCE, market_type=MarketType.PERP).adapter
                    await adapter.cancel_algo_order(
                        symbol="BTCUSDT",
                        client_algo_id=o.client_conditional_id,
                        algo_id=o.exchange_conditional_id,
                    )
                except Exception:
                    pass
                await repo.delete(o.order_id)




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
    db = common_settings.redis_db or 15

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
    # rate_limiter = LocalBinanceRateLimiter()

    redis_order_repo = OrderStateRedisRepository(redis_client)

    state_service = OrderStateService(
        postgres=postgres_client,
        intent_repo=OrderIntentPostgresRepository(),
        postgres_order_repo=OrderPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        redis_order_repo=redis_order_repo,
    )

    exchange_clients = ExchangeExecutionClientRegistry()
    exchange_clients.register(
        BinanceExecutionClient(
            adapter=adapter,
            # rate_limiter=rate_limiter,
            order_router=BinanceOrderRouter(adapter),
        )
    )

    gateway = ExecutionGateway(
        # adapter=adapter,
        state_repo=redis_order_repo,
        state_service=state_service,
        # rate_limiter=rate_limiter,
        exchange_clients=exchange_clients,
    )

    try:
        yield {
            "adapter": adapter,
            "gateway": gateway,
            "state_service": state_service,
            "redis_order_repo": redis_order_repo,
            "redis_client":redis_client,
        }

    finally:
        await adapter.close()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_cancel_conditional_order_route(
    gateway_bundle,
) -> None:
    """
    실제 Binance Futures Testnet 조건부 주문 생성 후 Gateway.cancel_order()로 취소.

    검증:
      - 생성은 /fapi/v1/algoOrder
      - 취소도 /fapi/v1/algoOrder
      - PostgreSQL status=CANCELLED
      - PostgreSQL conditional_status=CANCELED/CANCELLED
      - Redis conditional open index에서 제거
    """
    adapter: BinanceRestAdapter = gateway_bundle["adapter"]
    gateway: ExecutionGateway = gateway_bundle["gateway"]
    state_service: OrderStateService = gateway_bundle["state_service"]
    redis_order_repo: OrderStateRedisRepository = gateway_bundle["redis_order_repo"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    submitted_order = None

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        trigger_price = _round_down_to_step(
            ref_price * Decimal("2"),
            Decimal("0.1"),
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
            position_side=PositionSide.BOTH,
            position_action=PositionAction.OPEN,
        )

        submitted_order = await gateway.submit_order(
            req=req,
            source=OrderSource.MANUAL,
            strategy_name="real-cancel-test",
        )

        assert submitted_order.status == OrderStatus.ACKNOWLEDGED
        assert submitted_order.order_route == OrderRoute.CONDITIONAL
        assert submitted_order.conditional_status == ConditionalStatus.NEW
        assert submitted_order.exchange_conditional_id not in (None, "")

        conditional_open = await redis_order_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP
        )

        assert len(conditional_open) == 1
        assert all(
            row["order_id"] == submitted_order.order_id for row in conditional_open
        )

        assert submitted_order.order_id

        cancel_resp = await gateway.cancel_order(
            order_id=submitted_order.order_id,
        )

        
        assert isinstance(cancel_resp, ExchangeCancelResult)

        loaded = await state_service.load_order_from_postgres(
            order_id=submitted_order.order_id
        )

        assert loaded is not None
        assert loaded.status == OrderStatus.CANCELLED
        assert loaded.conditional_status == ConditionalStatus.CANCELLED

        conditional_open = await redis_order_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP
        )

        assert len(conditional_open) == 0

    finally:
        if submitted_order is not None:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=submitted_order.client_conditional_id,
                    algo_id=submitted_order.exchange_conditional_id,
                )
            except Exception:
                pass




@pytest.mark.stable
@pytest.mark.asyncio
async def test_real_gateway_submit_stop_market_algo_order_and_cancel(
    gateway_bundle,
) -> None:
    """
    실제 Binance Futures Testnet + 실제 PostgreSQL + 실제 Redis 통합 테스트.

    검증:
      1. Gateway.submit_order(STOP_MARKET)가 실제 /fapi/v1/algoOrder를 호출한다.
      2. PostgreSQL orders/order_intents에 CONDITIONAL 주문이 저장된다.
      3. Redis orders:conditional:open:{exchange} index에 들어간다.
      4. Binance openAlgoOrders에서 clientAlgoId로 조회된다.
      5. cancel_algo_order로 테스트 주문을 정리한다.
      6. cancel 결과를 NormalizedConditionalOrderEvent로 Gateway에 반영한다.
    """
    adapter: BinanceRestAdapter = gateway_bundle["adapter"]
    gateway: ExecutionGateway = gateway_bundle["gateway"]
    state_service: OrderStateService = gateway_bundle["state_service"]
    redis_order_repo: OrderStateRedisRepository = gateway_bundle["redis_order_repo"]

    symbol = os.getenv("BINANCE_REAL_TEST_SYMBOL", "BTCUSDT")
    quantity = os.getenv("BINANCE_REAL_TEST_QTY", "0.001")

    client_algo_id: str | None = None
    exchange_algo_id: str | None = None
    submitted_order: Order | None = None

    def _client_algo_id() -> str:
        # Binance client id 제한에 걸리지 않게 짧게 유지.
        return f"TKSTP{int(time.time() * 1000)}"

    def _is_target_algo_order(
        item: Any,
        *,
        client_algo_id: str | None,
        exchange_algo_id: str | None,
    ) -> bool:
        item_client_algo_id = item.clientAlgoId or ""
        item_algo_id = str(item.algoId or "")

        return bool(
            (client_algo_id and item_client_algo_id == client_algo_id)
            or (exchange_algo_id and item_algo_id == exchange_algo_id)
        )

    
    async def _wait_until_algo_order_visible(
        *,
        adapter: BinanceRestAdapter,
        symbol: str,
        client_algo_id: str | None,
        exchange_algo_id: str | None,
        timeout_sec: float = 5.0,
        interval_sec: float = 0.5,
    ) -> Any | None:
        """
        Binance openAlgoOrders에 방금 생성한 algo order가 보일 때까지 polling.

        실제 테스트넷에서는 주문 생성 직후 openAlgoOrders에 즉시 반영되지 않을 수 있다.
        """
        import asyncio

        deadline = time.monotonic() + timeout_sec

        while time.monotonic() < deadline:
            rows = await adapter.get_open_algo_orders(symbol=symbol)

            for item in rows:
                if _is_target_algo_order(
                    item,
                    client_algo_id=client_algo_id,
                    exchange_algo_id=exchange_algo_id,
                ):
                    return item

            await asyncio.sleep(interval_sec)

        return None

    async def _wait_until_algo_order_absent(
        *,
        adapter: BinanceRestAdapter,
        symbol: str,
        client_algo_id: str | None,
        exchange_algo_id: str | None,
        timeout_sec: float = 10.0,
        interval_sec: float = 0.5,
    ) -> list[Any]:
        """
        Binance openAlgoOrders에서 대상 algo order가 사라질 때까지 polling.

        cancel 응답 직후에도 openAlgoOrders 조회 결과가 잠시 stale할 수 있으므로
        마지막 거래소 검증은 짧게 재시도한다.
        """
        import asyncio

        deadline = time.monotonic() + timeout_sec
        last_rows: list[Any] = []

        while time.monotonic() < deadline:
            rows = await adapter.get_open_algo_orders(symbol=symbol)
            last_rows = rows

            exists = any(
                _is_target_algo_order(
                    item,
                    client_algo_id=client_algo_id,
                    exchange_algo_id=exchange_algo_id,
                )
                for item in rows
            )

            if not exists:
                return rows

            await asyncio.sleep(interval_sec)

        return last_rows

    try:
        ref_price = await _get_reference_price(adapter, symbol)

        # BUY STOP_MARKET은 가격이 triggerPrice 이상이 되면 발동.
        # 현재가 2배로 두면 테스트 중 발동 가능성이 낮다.
        trigger_price = _round_down_to_step(
            ref_price * Decimal("2"),
            Decimal("0.1"),
        )

        client_algo_id = _client_algo_id()

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
            position_side=PositionSide.BOTH,
            position_action=PositionAction.OPEN,
        )

        # order_id를 직접 지정해야 clientAlgoId를 예측할 수 있다.
        # Gateway 내부 _create_internal_order가 order_id를 생성하므로,
        # 테스트에서는 submit 후 반환된 client_conditional_id를 사용한다.
        submitted_order = await gateway.submit_order(
            req=req,
            source=OrderSource.MANUAL,
            signal_id=None,
            strategy_name="real-test",
        )

        assert submitted_order.status == OrderStatus.ACKNOWLEDGED
        assert submitted_order.order_route == OrderRoute.CONDITIONAL
        assert submitted_order.conditional_status == ConditionalStatus.NEW
        assert submitted_order.client_conditional_id is not None
        assert submitted_order.exchange_conditional_id not in (None, "")

        client_algo_id = submitted_order.client_conditional_id
        exchange_algo_id = submitted_order.exchange_conditional_id

        # PostgreSQL source of truth 확인
        assert submitted_order.order_id
        loaded_pg = await state_service.load_order_from_postgres(order_id=submitted_order.order_id)

        assert loaded_pg is not None
        assert loaded_pg.order_id == submitted_order.order_id
        assert loaded_pg.order_route == OrderRoute.CONDITIONAL
        assert loaded_pg.conditional_status == ConditionalStatus.NEW
        assert loaded_pg.exchange_conditional_id == exchange_algo_id

        # Redis projection 확인
        loaded_redis = await redis_order_repo.get(submitted_order.order_id)

        assert loaded_redis is not None
        assert loaded_redis["order_id"] == submitted_order.order_id
        assert loaded_redis["order_route"] == "CONDITIONAL"
        assert loaded_redis["conditional_status"] == "NEW"

        conditional_open = await redis_order_repo.list_open_conditional_orders(
            exchange=Exchange.BINANCE.value,
            market_type=MarketType.PERP
        )   

        assert len(conditional_open) > 0

        assert any(
            row["order_id"] == submitted_order.order_id for row in conditional_open
        )
        

        # Binance openAlgoOrders 실제 조회
        found_algo = await _wait_until_algo_order_visible(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            exchange_algo_id=exchange_algo_id,
            timeout_sec=15.0,
            interval_sec=0.5,
        )

        assert found_algo is not None
        assert str(found_algo.algoId) == exchange_algo_id 
        assert str(found_algo.clientAlgoId) == client_algo_id

        # cancel 직전 Binance openAlgoOrders 목록에 실제로 남아 있는지 재확인
        exchange_open_algo_before_cancel = await adapter.get_open_algo_orders(
            symbol=symbol
        )

        assert any(
            _is_target_algo_order(
                item,
                client_algo_id=client_algo_id,
                exchange_algo_id=exchange_algo_id,
            )
            for item in exchange_open_algo_before_cancel
        )

        # 실제 취소
        cancel_resp = await adapter.cancel_algo_order(
            symbol=symbol,
            client_algo_id=client_algo_id,
            algo_id=exchange_algo_id,
        )

        assert cancel_resp is not None

        # cancel response를 표준 conditional event로 변환해서 Gateway에 반영
        cancel_event = NormalizedConditionalOrderEvent(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol=symbol,
            client_conditional_id=client_algo_id,
            exchange_conditional_id=str(cancel_resp.algoId or exchange_algo_id),
            target_status=ConditionalStatus.CANCELLED,
            exchange_conditional_status=str(
                cancel_resp.raw.get("algoStatus") or cancel_resp.raw.get("status") or BinanceConditionalOrderState.canceled
            ),
            triggered_order_id=None,
            triggered_client_order_id=None,
            filled_quantity=None,
            avg_fill_price=None,
            reject_reason_text=None,
            event_time=epoch_ms(),
            transaction_time=None,
            raw=cancel_resp.raw,
        )

        canceled_order = await gateway.apply_conditional_order_event(cancel_event)
        assert canceled_order is not None
        assert canceled_order.order_id == submitted_order.order_id
        assert canceled_order.conditional_status == ConditionalStatus.CANCELLED
        assert canceled_order.exchange_conditional_status == BinanceConditionalOrderState.canceled
        assert canceled_order.exchange_conditional_id == submitted_order.exchange_conditional_id
        assert canceled_order.client_conditional_id == submitted_order.client_conditional_id

        # Redis conditional open index에서 제거됐는지 확인
        conditional_open_after_cancel = (
            await redis_order_repo.list_open_conditional_orders(
                exchange=Exchange.BINANCE.value,
                market_type=MarketType.PERP
            )
        )

        assert all(
            row["order_id"] != submitted_order.order_id
            for row in conditional_open_after_cancel
        )

        # PostgreSQL 최종 상태 확인
        loaded_after_cancel = await state_service.load_order_from_postgres(
            order_id=submitted_order.order_id
        )

        assert loaded_after_cancel is not None
        assert loaded_after_cancel.conditional_status in {
            ConditionalStatus.CANCELLED,
        }

        # Binance openAlgoOrders에서도 테스트 조건부 주문이 제거됐는지 확인
        exchange_open_algo_after_cancel = await _wait_until_algo_order_absent(
            adapter=adapter,
            symbol=symbol,
            client_algo_id=client_algo_id,
            exchange_algo_id=exchange_algo_id,
            timeout_sec=15.0,
            interval_sec=0.5,
        )

        assert all(
            not _is_target_algo_order(
                item,
                client_algo_id=client_algo_id,
                exchange_algo_id=exchange_algo_id,
            )
            for item in exchange_open_algo_after_cancel
        )

    finally:
        # 테스트 중간 실패 시에도 실제 testnet 주문 정리 시도
        if client_algo_id is not None or exchange_algo_id is not None:
            try:
                await adapter.cancel_algo_order(
                    symbol=symbol,
                    client_algo_id=client_algo_id,
                    algo_id=exchange_algo_id,
                )
            except Exception:
                pass



@pytest.mark.stable
@pytest.mark.asyncio
async def test_gateway_load_order_rebuilds_redis_projection_from_postgres(
    gateway_bundle
) -> None:
    def make_order(
        *,
        order_id: str = "ORD-FALLBACK-IT-001",
    ) -> Order:
        return Order(
            order_id=order_id,
            source=OrderSource.MANUAL,
            signal_id=None,
            strategy_name=None,
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GTC,
            quantity="0.1",
            price="60000",
            stop_price=None,
            reduce_only=False,
            position_action=PositionAction.OPEN,
            created_ts=1_700_000_000_000,
            submitted_ts=None,
            filled_ts=None,
            updated_ts=1_700_000_000_000,
            status=OrderStatus.PENDING_NEW,
            version=1,
        )

    redis_repo = gateway_bundle["redis_order_repo"]
    order_state_service = gateway_bundle["state_service"]

    order = make_order()
    await order_state_service.create_order(order)

    assert order.order_id
    await redis_repo.delete(order.order_id)
    assert await redis_repo.get(order.order_id) is None

    gateway = ExecutionGateway(
        state_repo=redis_repo,
        state_service=order_state_service,
        exchange_clients=ExchangeExecutionClientRegistry(),
    )

    loaded = await gateway.transitions._load_order_from_repo(order.order_id)

    assert loaded is not None
    assert loaded.order_id == order.order_id
    assert loaded.status == OrderStatus.PENDING_NEW
    assert loaded.version == 1

    redis_row = await redis_repo.get(order.order_id)

    assert redis_row is not None
    assert redis_row["order_id"] == order.order_id
    assert redis_row["status"] == OrderStatus.PENDING_NEW.value
    assert redis_row["version"] == 1

    open_orders = await redis_repo.list_open_regular_orders(
        exchange=order.exchange.value,
        market_type=order.market_type.value,
    )
    open_order_ids = {row["order_id"] for row in open_orders}

    assert order.order_id in open_order_ids
