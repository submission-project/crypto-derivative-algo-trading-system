from __future__ import annotations

import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from execution_gateway.services.position_order_service import (
    PositionCloseError,
    PositionFlipError,
    PositionOpenError,
    PositionOrderError,
    PositionOrderService,
)
from execution_gateway.services.position_state_service import PositionStateService
from schemas.market import Exchange, MarketType
from schemas.order import (
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderType,
    PositionAction,
    TimeInForce,
)
from schemas.position import (
    Position,
    PositionSide,
    PositionStatus,
    make_position_id,
)
from storage.postgres_client import PostgresClient
from storage.repositories.postgres.outbox_repo import OutboxPostgresRepository
from storage.repositories.postgres.position_repo import PositionPostgresRepository


_NOW_MS = lambda: time.time_ns() // 1_000_000


class DummyGateway:
    def __init__(self) -> None:
        self.calls = []

    async def submit_order(
        self,
        *,
        req,
        source,
        signal_id=None,
        strategy_name=None,
    ):
        self.calls.append(
            {
                "req": req,
                "source": source,
                "signal_id": signal_id,
                "strategy_name": strategy_name,
            }
        )

        return SimpleNamespace(
            req=req,
            source=source,
            signal_id=signal_id,
            strategy_name=strategy_name,
        )


def make_position(
    *,
    exchange: Exchange = Exchange.BINANCE,
    market_type: MarketType = MarketType.PERP,
    symbol: str = "BTCUSDT",
    position_side: PositionSide = PositionSide.BOTH,
    status: PositionStatus = PositionStatus.OPEN,
    position_amt: str = "0.01",
) -> Position:
    now = _NOW_MS()

    return Position(
        position_id=make_position_id(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            position_side=position_side,
        ),
        exchange=exchange,
        market_type=market_type,
        symbol=symbol.upper(),
        position_side=position_side,
        status=status,
        position_amt=position_amt,
        entry_price="50000",
        break_even_price=None,
        mark_price="50500",
        unrealized_pnl=None,
        isolated_margin=None,
        isolated_wallet=None,
        margin_type=None,
        leverage=10,
        liquidation_price=None,
        notional=None,
        update_reason="test",
        last_event_time=None,
        last_transaction_time=None,
        opened_ts=now,
        closed_ts=None,
        updated_ts=now,
        version=1,
    )


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
                positions
            RESTART IDENTITY CASCADE
            """
        )

    await client.close()


@pytest.fixture
def gateway() -> DummyGateway:
    return DummyGateway()


@pytest.fixture
def redis_position_repo() -> SimpleNamespace:
    return SimpleNamespace(save=AsyncMock())


@pytest.fixture
def position_state_service(
    postgres_client: PostgresClient,
    redis_position_repo: SimpleNamespace,
) -> PositionStateService:
    return PositionStateService(
        postgres=postgres_client,
        position_repo=PositionPostgresRepository(),
        outbox_repo=OutboxPostgresRepository(),
        # pyrefly: ignore [bad-argument-type]
        redis_position_repo=redis_position_repo,
    )


@pytest.fixture
def service(
    *,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> PositionOrderService:
    return PositionOrderService(
        position_state_service=position_state_service,
        # pyrefly: ignore [bad-argument-type]
        gateway=gateway,
    )


def last_req(gateway: DummyGateway):
    assert gateway.calls
    return gateway.calls[-1]["req"]


async def seed_position(
    position_state_service: PositionStateService,
    position: Position,
) -> Position:
    pool = position_state_service.postgres.require_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            return await position_state_service.position_repo.upsert(
                conn,
                position=position,
            )


# ---------------------------------------------------------------------
# OPEN
# ---------------------------------------------------------------------

@pytest.mark.stable
@pytest.mark.asyncio
async def test_open_position_market_builds_regular_open_order(
    service: PositionOrderService,
    gateway: DummyGateway,
) -> None:
    await service.open_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="btcusdt",
        side=OrderSide.BUY,
        quantity="0.01",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.exchange == Exchange.BINANCE
    assert req.market_type == MarketType.PERP
    assert req.symbol == "BTCUSDT"
    assert req.side == OrderSide.BUY
    assert req.order_type == OrderType.MARKET
    assert req.order_route == OrderRoute.REGULAR
    assert req.quantity == "0.01"
    assert req.price is None
    assert req.trigger_price is None
    assert req.reduce_only is False
    assert req.close_position is False
    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.OPEN

@pytest.mark.stable
@pytest.mark.asyncio
async def test_open_position_market_rejects_invalid_hedge_side(
    service: PositionOrderService,
) -> None:
    with pytest.raises(PositionOpenError):
        await service.open_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            quantity="0.01",
            position_side=PositionSide.LONG,
            source=OrderSource.MANUAL,
        )

        await service.open_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            quantity="0.01",
            position_side=PositionSide.SHORT,
            source=OrderSource.MANUAL,
        )


# ---------------------------------------------------------------------
# INCREASE / OPEN OR INCREASE
# ---------------------------------------------------------------------

@pytest.mark.stable
@pytest.mark.asyncio
async def test_increase_position_market_one_way_long_builds_buy_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.01",
        )
    )

    await service.increase_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity="0.02",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.MARKET
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.BUY
    assert req.quantity == "0.02"
    assert req.reduce_only is False
    assert req.close_position is False
    assert req.position_action == PositionAction.INCREASE


@pytest.mark.asyncio
async def test_increase_position_market_rejects_opposite_side(
    service: PositionOrderService,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.01",
        )
    )

    with pytest.raises(PositionCloseError):
        await service.increase_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            quantity="0.02",
            position_side=PositionSide.BOTH,
            source=OrderSource.MANUAL,
        )


@pytest.mark.asyncio
async def test_open_or_increase_position_market_opens_when_position_missing(
    service: PositionOrderService,
    gateway: DummyGateway,
) -> None:
    await service.open_or_increase_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity="0.01",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.position_action == PositionAction.OPEN
    assert req.order_route == OrderRoute.REGULAR
    assert req.order_type == OrderType.MARKET
    assert req.side == OrderSide.BUY
    assert req.quantity == "0.01"


@pytest.mark.asyncio
async def test_open_or_increase_position_market_increases_when_position_open(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.01",
        )
    )

    await service.open_or_increase_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity="0.02",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.position_action == PositionAction.INCREASE
    assert req.order_route == OrderRoute.REGULAR
    assert req.order_type == OrderType.MARKET
    assert req.side == OrderSide.BUY
    assert req.quantity == "0.02"


# ---------------------------------------------------------------------
# REDUCE
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reduce_position_market_one_way_long_builds_sell_reduce_only_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.reduce_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        quantity="0.01",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.MARKET
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.01"
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.REDUCE


@pytest.mark.asyncio
async def test_reduce_position_market_rejects_quantity_larger_than_position(
    service: PositionOrderService,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.01",
        )
    )

    with pytest.raises(PositionCloseError):
        await service.reduce_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            quantity="0.02",
            position_side=PositionSide.BOTH,
            source=OrderSource.MANUAL,
        )


@pytest.mark.asyncio
async def test_reduce_position_limit_builds_limit_reduce_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.reduce_position_limit(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        quantity="0.01",
        price="60000",
        position_side=PositionSide.BOTH,
        time_in_force=TimeInForce.GTC,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.LIMIT
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.01"
    assert req.price == "60000"
    assert req.time_in_force == TimeInForce.GTC
    assert req.reduce_only is True
    assert req.position_action == PositionAction.REDUCE


# ---------------------------------------------------------------------
# CLOSE
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_market_one_way_long_builds_sell_reduce_only_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.MARKET
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.03"
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_close_position_market_hedge_long_does_not_use_reduce_only(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.LONG,
            position_amt="0.03",
        )
    )

    await service.close_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        position_side=PositionSide.LONG,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.side == OrderSide.SELL
    assert req.position_side == PositionSide.LONG
    assert req.reduce_only is False
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_close_position_limit_builds_limit_close_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="-0.02",
        )
    )

    await service.close_position_limit(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        price="50000",
        position_side=PositionSide.BOTH,
        time_in_force=TimeInForce.GTC,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.LIMIT
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.BUY
    assert req.quantity == "0.02"
    assert req.price == "50000"
    assert req.reduce_only is True
    assert req.position_action == PositionAction.CLOSE


# ---------------------------------------------------------------------
# STOP MARKET protection
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_stop_market_uses_close_position_true_by_default(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.close_position_stop_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        trigger_price="59000",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.STOP_MARKET
    assert req.order_route == OrderRoute.CONDITIONAL
    assert req.side == OrderSide.SELL
    assert req.quantity == "0"
    assert req.trigger_price == "59000"
    assert req.price is None
    assert req.reduce_only is False
    assert req.close_position is True
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_close_position_stop_market_can_use_explicit_quantity(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.close_position_stop_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        trigger_price="59000",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
        use_close_position=False,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.STOP_MARKET
    assert req.order_route == OrderRoute.CONDITIONAL
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.03"
    assert req.trigger_price == "59000"
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_reduce_position_stop_market_builds_conditional_reduce_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.reduce_position_stop_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        quantity="0.01",
        trigger_price="59000",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.STOP_MARKET
    assert req.order_route == OrderRoute.CONDITIONAL
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.01"
    assert req.trigger_price == "59000"
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.REDUCE


# ---------------------------------------------------------------------
# STOP LIMIT protection
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_stop_limit_builds_conditional_close_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.close_position_stop_limit(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        trigger_price="59000",
        price="58950",
        position_side=PositionSide.BOTH,
        time_in_force=TimeInForce.GTC,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.STOP_LIMIT
    assert req.order_route == OrderRoute.CONDITIONAL
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.03"
    assert req.trigger_price == "59000"
    assert req.price == "58950"
    assert req.time_in_force == TimeInForce.GTC
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.CLOSE


@pytest.mark.asyncio
async def test_reduce_position_stop_limit_builds_conditional_reduce_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.03",
        )
    )

    await service.reduce_position_stop_limit(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        quantity="0.01",
        trigger_price="59000",
        price="58950",
        position_side=PositionSide.BOTH,
        time_in_force=TimeInForce.GTC,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.STOP_LIMIT
    assert req.order_route == OrderRoute.CONDITIONAL
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.01"
    assert req.trigger_price == "59000"
    assert req.price == "58950"
    assert req.time_in_force == TimeInForce.GTC
    assert req.reduce_only is True
    assert req.close_position is False
    assert req.position_action == PositionAction.REDUCE


# ---------------------------------------------------------------------
# FLIP
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flip_position_market_one_way_long_builds_total_sell_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0.01",
        )
    )

    await service.flip_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        target_quantity="0.02",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.order_type == OrderType.MARKET
    assert req.order_route == OrderRoute.REGULAR
    assert req.side == OrderSide.SELL
    assert req.quantity == "0.03"
    assert req.reduce_only is False
    assert req.close_position is False
    assert req.position_side == PositionSide.BOTH
    assert req.position_action == PositionAction.FLIP


@pytest.mark.asyncio
async def test_flip_position_market_one_way_short_builds_total_buy_order(
    service: PositionOrderService,
    gateway: DummyGateway,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="-0.01",
        )
    )

    await service.flip_position_market(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        target_quantity="0.02",
        position_side=PositionSide.BOTH,
        source=OrderSource.MANUAL,
    )

    req = last_req(gateway)

    assert req.side == OrderSide.BUY
    assert req.quantity == "0.03"
    assert req.reduce_only is False
    assert req.position_action == PositionAction.FLIP


@pytest.mark.asyncio
async def test_flip_position_market_rejects_hedge_mode(
    service: PositionOrderService,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.LONG,
            position_amt="0.01",
        )
    )

    with pytest.raises(PositionFlipError):
        await service.flip_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            target_quantity="0.02",
            position_side=PositionSide.LONG,
            source=OrderSource.MANUAL,
        )


# ---------------------------------------------------------------------
# Common errors
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_position_market_rejects_missing_position(
    service: PositionOrderService,
) -> None:
    with pytest.raises(PositionCloseError):
        await service.close_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            source=OrderSource.MANUAL,
        )


@pytest.mark.asyncio
async def test_close_position_market_rejects_flat_position(
    service: PositionOrderService,
    position_state_service: PositionStateService,
) -> None:
    await seed_position(
        position_state_service,
        make_position(
            position_side=PositionSide.BOTH,
            position_amt="0",
            status=PositionStatus.FLAT,
        )
    )

    with pytest.raises(PositionCloseError):
        await service.close_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            source=OrderSource.MANUAL,
        )


@pytest.mark.asyncio
async def test_position_order_service_rejects_spot_market_type(
    service: PositionOrderService,
) -> None:
    with pytest.raises(PositionOrderError):
        await service.open_position_market(
            exchange=Exchange.BINANCE,
            market_type=MarketType.SPOT,
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            quantity="0.01",
            position_side=PositionSide.BOTH,
            source=OrderSource.MANUAL,
        )
