from __future__ import annotations

import time

import pytest
import pytest_asyncio

from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderRoute,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TERMINAL_STATUSES,
    RECOVERY_STATUSES,
    UNKNOWN_STATUSES
)
from schemas.position import PositionSide
from storage.redis_client import RedisStreamClient
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository

from common.config import settings as common_settings


pytestmark = pytest.mark.integration


_NOW_MS = lambda: time.time_ns() // 1_000_000


# pyrefly: ignore [no-matching-overload]
@pytest_asyncio.fixture
# pyrefly: ignore [bad-return]
async def redis_client() -> RedisStreamClient:
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
    await client.close()


@pytest_asyncio.fixture
async def repo(redis_client: RedisStreamClient) -> OrderStateRedisRepository:
    return OrderStateRedisRepository(redis_client)


def make_regular_order(
    *,
    order_id: str = "ORD-REG-001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 1,
) -> Order:
    now = _NOW_MS()

    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        order_route=OrderRoute.REGULAR,
        quantity="0.01",
        price=None,
        trigger_price=None,
        reduce_only=False,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.OPEN,
        status=status,
        created_ts=now,
        updated_ts=now,
        version=version,
    )


def make_conditional_order(
    *,
    order_id: str = "ORD-COND-001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    conditional_status: ConditionalStatus | None = ConditionalStatus.NEW,
    version: int = 1,
    updated_ts: int | None = None,
) -> Order:
    now = _NOW_MS()

    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        order_route=OrderRoute.CONDITIONAL,
        quantity="0.01",
        price=None,
        trigger_price="59000",
        reduce_only=True,
        close_position=False,
        position_side=PositionSide.BOTH,
        position_action=PositionAction.CLOSE,
        status=status,
        conditional_status=conditional_status,
        created_ts=now,
        updated_ts=updated_ts or now,
        version=version,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_save_accepts_order_object(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_regular_order()

    applied = await repo.save(order)
    assert applied is True

    assert order.order_id
    loaded = await repo.get(order.order_id)

    assert loaded is not None
    assert loaded["order_id"] == order.order_id
    assert loaded["status"] == OrderStatus.ACKNOWLEDGED.value
    assert loaded["order_route"] == OrderRoute.REGULAR.value
    assert loaded["version"] == order.version
    assert loaded["source"] == order.source.value
    assert loaded["exchange"] == order.exchange.value
    assert loaded["market_type"] == order.market_type.value
    assert loaded["symbol"] == order.symbol
    assert loaded["side"] == order.side.value
    assert loaded["order_type"] == order.order_type.value
    assert loaded["reduce_only"] == order.reduce_only
    assert loaded["close_position"] == order.close_position
    assert loaded["position_side"] == order.position_side.value
    assert loaded["position_action"] == order.position_action.value
    assert loaded["filled_quantity"] == order.filled_quantity
    assert loaded["avg_fill_price"] == order.avg_fill_price
    assert loaded["created_ts"] == order.created_ts
    assert loaded["updated_ts"] == order.updated_ts

@pytest.mark.stable
@pytest.mark.asyncio
async def test_save_accepts_conditional_order_object(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_conditional_order()

    applied = await repo.save(order)
    assert applied is True

    assert order.order_id

    loaded = await repo.get(order.order_id)

    assert loaded is not None
    assert loaded["order_id"] == order.order_id
    assert loaded["client_conditional_id"] == order.client_conditional_id
    assert loaded["status"] == OrderStatus.ACKNOWLEDGED.value
    assert loaded["conditional_status"] == ConditionalStatus.NEW.value
    assert loaded["order_route"] == OrderRoute.CONDITIONAL.value
    assert loaded["version"] == order.version
    assert loaded["source"] == order.source.value
    assert loaded["exchange"] == order.exchange.value
    assert loaded["market_type"] == order.market_type.value
    assert loaded["symbol"] == order.symbol
    assert loaded["side"] == order.side.value
    assert loaded["order_type"] == order.order_type.value
    assert loaded["reduce_only"] == order.reduce_only
    assert loaded["close_position"] == order.close_position
    assert loaded["position_side"] == order.position_side.value
    assert loaded["position_action"] == order.position_action.value
    assert loaded["filled_quantity"] == order.filled_quantity
    assert loaded["avg_fill_price"] == order.avg_fill_price
    assert loaded["created_ts"] == order.created_ts
    assert loaded["updated_ts"] == order.updated_ts
    assert loaded["trigger_price"] == order.trigger_price
    # assert loaded["price"] == order.price

@pytest.mark.stable
@pytest.mark.asyncio
async def test_regular_order_goes_to_regular_open_index(
    repo: OrderStateRedisRepository,
) -> None:
    status = OrderStatus.ACKNOWLEDGED
    order = make_regular_order(status=status)

    applied = await repo.save(order)

    assert applied is True

    regular = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert len(regular) == 1
    assert regular[0]["order_id"] == order.order_id
    assert regular[0]["order_route"] == OrderRoute.REGULAR.value
    assert regular[0]["status"] == status.value
    assert conditional == []

@pytest.mark.stable
@pytest.mark.asyncio
async def test_conditional_order_goes_to_conditional_open_index(
    repo: OrderStateRedisRepository,
) -> None:
    conditional_status = ConditionalStatus.NEW
    order = make_conditional_order(conditional_status=conditional_status)

    applied = await repo.save(order)

    assert applied is True

    regular = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert regular == []
    assert len(conditional) == 1
    assert conditional[0]["order_id"] == order.order_id
    assert conditional[0]["order_route"] == OrderRoute.CONDITIONAL.value
    assert conditional[0]["conditional_status"] == conditional_status.value

@pytest.mark.stable
@pytest.mark.asyncio
async def test_conditional_triggered_removed_from_conditional_open_index(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.NEW,
        version=1,
    )

    await repo.save(order)

    conditional_status = ConditionalStatus.TRIGGERED
    triggered_order_id = "123456"

    triggered = order.model_copy(deep=True)
    triggered.version = 2
    triggered.conditional_status = conditional_status
    triggered.triggered_order_id = triggered_order_id
    triggered.updated_ts = _NOW_MS()

    applied = await repo.save(triggered)

    assert applied is True
    conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert conditional == []

    assert order.order_id
    loaded = await repo.get(order.order_id)

    assert loaded is not None
    assert loaded["conditional_status"] == conditional_status.value
    assert loaded["triggered_order_id"] == triggered_order_id
    assert loaded["version"] > order.version

# [claim] 
# @pytest.mark.stable
@pytest.mark.asyncio
async def test_terminal_conditional_order_removed_from_conditional_indexes(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_conditional_order(
        conditional_status=ConditionalStatus.NEW,
        version=1,
    )

    await repo.save(order)

    expired = order.model_copy(deep=True)
    expired.version = 2
    expired.status = OrderStatus.EXPIRED
    expired.conditional_status = ConditionalStatus.EXPIRED
    expired.expired_ts = _NOW_MS()
    expired.updated_ts = _NOW_MS()

    applied = await repo.save(expired)

    assert applied is True

    regular = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)   
    recovery = await repo.list_recovery_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    unknown = await repo.list_unknown_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert regular == []
    assert conditional == []
    assert recovery == []
    assert unknown == []

    assert order.order_id
    loaded = await repo.get(order.order_id)

    assert loaded is not None
    assert loaded["status"] == "EXPIRED"
    assert loaded["conditional_status"] == "EXPIRED"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_terminal_order_removed_from_open_indexes(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_regular_order(version=1)

    await repo.save(order)

    async def check_terminal_status(order: Order, new_status: OrderStatus):
        terminal = order.model_copy(deep=True)
        terminal.version += 1

        assert new_status in TERMINAL_STATUSES
        terminal.status = new_status
        terminal.updated_ts = _NOW_MS()

        applied = await repo.save(terminal)

        assert applied is True

        regular = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
        conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
        recovery = await repo.list_recovery_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
        unknown = await repo.list_unknown_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

        assert regular == []
        assert conditional == []
        assert recovery == []
        assert unknown == []

        assert order.order_id
        loaded = await repo.get(order.order_id)

        assert loaded is not None
        assert loaded["status"] == new_status.value

        return terminal

    for status in TERMINAL_STATUSES:
        order = await check_terminal_status(order, status)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_stale_version_is_ignored(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_regular_order(version=2)

    applied1 = await repo.save(order)

    stale = order.model_copy(deep=True)
    stale.version = 1
    stale.status = OrderStatus.SUBMITTED
    stale.updated_ts = _NOW_MS()

    applied2 = await repo.save(stale)


    assert order.order_id
    loaded = await repo.get(order.order_id)

    assert applied1 is True
    assert applied2 is False
    assert loaded is not None
    assert loaded["version"] == 2
    assert loaded["status"] == "ACKNOWLEDGED"

@pytest.mark.stable
@pytest.mark.asyncio
async def test_unknown_order_goes_to_unknown_and_recovery_indexes(
    repo: OrderStateRedisRepository,
) -> None:
    status = OrderStatus.UNKNOWN
    order = make_regular_order(
        status=status,
        version=1,
    )

    await repo.save(order)

    assert status in UNKNOWN_STATUSES
    unknown = await repo.list_unknown_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    assert status in RECOVERY_STATUSES
    recovery = await repo.list_recovery_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert len(unknown) == 1
    assert unknown[0]["order_id"] == order.order_id

    assert len(recovery) == 1
    assert recovery[0]["order_id"] == order.order_id


@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_failure_counter_can_be_incremented_and_cleared(
    repo: OrderStateRedisRepository,
) -> None:
    """reconciliation get_order 실패 횟수는 Redis에서 TTL 카운터로 누적하고 성공 시 제거한다."""
    kwargs = {
        "exchange": Exchange.BINANCE.value,
        "market_type": MarketType.PERP.value,
        "order_id": "ORD-RECONCILE-FAILURE-001",
        "ttl_sec": 60,
    }

    count1 = await repo.increment_reconcile_failure(**kwargs)
    count2 = await repo.increment_reconcile_failure(**kwargs)

    await repo.clear_reconcile_failure(
        exchange=kwargs["exchange"],
        market_type=kwargs["market_type"],
        order_id=kwargs["order_id"],
    )

    count_after_clear = await repo.increment_reconcile_failure(**kwargs)

    assert count1 == 1
    assert count2 == 2
    assert count_after_clear == 1


@pytest.mark.stable
@pytest.mark.asyncio
async def test_postpone_recovery_and_unknown_orders_moves_retry_score(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_regular_order(
        order_id="ORD-POSTPONE-001",
        status=OrderStatus.UNKNOWN,
        version=1,
    )
    order.updated_ts = 1_700_000_000_000

    await repo.save(order)

    recovery_before = await repo.list_recovery_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_000_001,
    )
    unknown_before = await repo.list_unknown_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_000_001,
    )

    assert [row["order_id"] for row in recovery_before] == [order.order_id]
    assert [row["order_id"] for row in unknown_before] == [order.order_id]

    assert order.order_id

    await repo.postpone_recovery_order(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        order_id=order.order_id,
        next_attempt_ts=1_700_000_010_000,
    )
    await repo.postpone_unknown_order(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        order_id=order.order_id,
        next_attempt_ts=1_700_000_010_000,
    )

    recovery_deferred = await repo.list_recovery_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_000_001,
    )
    unknown_deferred = await repo.list_unknown_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_000_001,
    )

    assert recovery_deferred == []
    assert unknown_deferred == []

    recovery_after_backoff = await repo.list_recovery_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_010_000,
    )
    unknown_after_backoff = await repo.list_unknown_orders(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        older_than_ts=1_700_000_010_000,
    )

    assert [row["order_id"] for row in recovery_after_backoff] == [order.order_id]
    assert [row["order_id"] for row in unknown_after_backoff] == [order.order_id]

# test_unknown_conditional_status_goes_to_unknown_index
@pytest.mark.stable
@pytest.mark.asyncio
async def unknown_conditional_status는_open_conditional_recovery_및_unknown_인덱스에등록된다(
    repo: OrderStateRedisRepository,
) -> None:
    order = make_conditional_order(
        status=OrderStatus.ACKNOWLEDGED,
        conditional_status=ConditionalStatus.UNKNOWN,
        version=1,
    )

    await repo.save(order)

    unknown = await repo.list_unknown_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    recovery = await repo.list_recovery_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    conditional = await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert len(unknown) == 1
    assert unknown[0]["order_id"] == order.order_id

    assert len(recovery) == 1
    assert recovery[0]["order_id"] == order.order_id

    assert len(conditional) == 1
    assert conditional[0]["order_id"] == order.order_id

# [claim] 통과는 하였으나 사용되지 않는 메소드, 왜 사용하는 지 파악 필요 이후에 stable 처리 바람
@pytest.mark.asyncio
async def test_list_open_by_symbol_excludes_conditional_by_default(
    repo: OrderStateRedisRepository,
) -> None:
    regular = make_regular_order(order_id="ORD-REG-001")
    conditional = make_conditional_order(order_id="ORD-COND-001")

    await repo.save(
        regular
    )
    await repo.save(
        conditional
    )

    rows = await repo.list_open_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="BTCUSDT",
        include_conditional=False,
    )

    assert len(rows) == 1
    assert rows[0]["order_id"] == "ORD-REG-001"

    rows_with_conditional = await repo.list_open_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="BTCUSDT",
        include_conditional=True,
    )

    assert {row["order_id"] for row in rows_with_conditional} == {
        "ORD-REG-001",
        "ORD-COND-001",
    }
    
@pytest.mark.stable
@pytest.mark.asyncio
async def test_delete_removes_live_hash_and_indexes(
    repo: OrderStateRedisRepository,
) -> None:
    regular = make_regular_order(order_id="ORD-REG-DELETE")

    await repo.save(regular)

    rows_before = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)

    assert len(rows_before) == 1

    assert regular.order_id
    await repo.delete(regular.order_id)

    loaded = await repo.get(regular.order_id)
    rows_after = await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value)
    by_symbol = await repo.list_open_by_symbol(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        symbol="BTCUSDT",
        include_conditional=True,
    )

    assert loaded is None
    assert rows_after == []
    assert by_symbol == []

@pytest.mark.stable
@pytest.mark.asyncio
async def test_clear_projection_removes_all_projection_keys(
    repo: OrderStateRedisRepository,
) -> None:
    regular = make_regular_order(order_id="ORD-REG-CLEAR")
    conditional = make_conditional_order(order_id="ORD-COND-CLEAR")

    await repo.save(regular)
    await repo.save(conditional)

    result = await repo.clear_projection(include_live_hashes=True)

    assert result.total_deleted > 0

    assert regular.order_id
    assert conditional.order_id
    assert await repo.get(regular.order_id) is None
    assert await repo.get(conditional.order_id) is None

    assert await repo.list_open_regular_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value) == []
    assert await repo.list_open_conditional_orders(exchange=Exchange.BINANCE.value, market_type=MarketType.PERP.value) == []
