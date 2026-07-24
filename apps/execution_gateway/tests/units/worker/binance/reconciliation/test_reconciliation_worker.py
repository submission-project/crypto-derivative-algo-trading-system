from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from execution_gateway.exchange import (
    ExchangeCapabilities,
    ExchangeApiError,
    ExchangeErrorCategory,
    ExchangeConditionalSnapshot,
    ExchangeOrderSnapshot,
)
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.workers.reconciliation_worker import ReconciliationWorker, ExternalOrphanPolicy
from schemas.market import Exchange, MarketType
from schemas.order import (
    ConditionalStatus,
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    OrderRoute,
    PositionAction,
    TimeInForce,
)
def make_order(
    *,
    order_id: str,
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    updated_ts: int = 1_700_000_000_000,
    version: int = 2,
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
        submitted_ts=1_700_000_000_100,
        filled_ts=None,
        updated_ts=updated_ts,
        status=status,
        version=version,
    )


def make_conditional_order(
    *,
    order_id: str,
    client_conditional_id: str,
    exchange_conditional_id: str = "2001",
    conditional_status: ConditionalStatus = ConditionalStatus.NEW,
) -> Order:
    order = make_order(order_id=order_id)
    order.order_type = OrderType.STOP_MARKET
    order.order_route = OrderRoute.CONDITIONAL
    order.price = None
    order.trigger_price = "59000"
    order.client_conditional_id = client_conditional_id
    order.exchange_conditional_id = exchange_conditional_id
    order.conditional_status = conditional_status
    order.exchange_conditional_status = conditional_status.value
    return order


def order_snapshot(
    *,
    client_order_id: str,
    status: OrderStatus = OrderStatus.FILLED,
    raw_status: str = "FILLED",
) -> ExchangeOrderSnapshot:
    return ExchangeOrderSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id=client_order_id,
        exchange_order_id="123",
        status=status,
        filled_quantity="0.1",
        avg_fill_price="60000",
        raw_status=raw_status,
        raw={
            "clientOrderId": client_order_id,
            "symbol": "BTCUSDT",
            "status": raw_status,
            "orderId": 123,
            "executedQty": "0.1",
            "avgPrice": "60000",
        },
    )


def conditional_snapshot(
    *,
    client_conditional_id: str,
    exchange_conditional_id: str = "1001",
    status: ConditionalStatus = ConditionalStatus.NEW,
    raw_status: str = "NEW",
) -> ExchangeConditionalSnapshot:
    return ExchangeConditionalSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        conditional_status=status,
        client_conditional_id=client_conditional_id,
        exchange_conditional_id=exchange_conditional_id,
        raw_status=raw_status,
        raw={
            "clientAlgoId": client_conditional_id,
            "algoId": exchange_conditional_id,
            "algoStatus": raw_status,
            "symbol": "BTCUSDT",
        },
    )


def make_client(
    *,
    open_orders: list[ExchangeOrderSnapshot] | None = None,
    get_order_result: ExchangeOrderSnapshot | None = None,
    open_conditionals: list[ExchangeConditionalSnapshot] | None = None,
    conditional_result: ExchangeConditionalSnapshot | None = None,
    bulk_results: dict[str, ExchangeOrderSnapshot] | None = None,
    supports_conditional_reconciliation: bool = True,
) -> MagicMock:
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities(
        supports_conditional_reconciliation=supports_conditional_reconciliation,
        supports_bulk_order_lookup=True,
    )
    client.get_open_orders = AsyncMock(return_value=open_orders or [])
    client.get_order = AsyncMock(return_value=get_order_result)
    client.find_order_snapshots = AsyncMock(return_value=bulk_results or {})
    client.get_open_conditional_orders = AsyncMock(return_value=open_conditionals or [])
    client.get_conditional_order = AsyncMock(return_value=conditional_result)
    client.cancel_regular_order_by_client_id = AsyncMock()
    client.cancel_conditional_order_by_id = AsyncMock()
    client.close = AsyncMock()
    return client


def make_registry(client: MagicMock) -> ExchangeExecutionClientRegistry:
    registry = ExchangeExecutionClientRegistry()
    registry.register(client)
    return registry


def make_worker(
    *,
    client: MagicMock | None = None,
    gateway: MagicMock | None = None,
    order_state_service: MagicMock | None = None,
    redis_repo: MagicMock | None = None,
    external_orphan_policy: ExternalOrphanPolicy = "log",
    all_orders_threshold: int = 6,
    reconcile_not_found_threshold: int = 3,
) -> ReconciliationWorker:
    client = client or make_client()
    gateway = gateway or MagicMock()
    order_state_service = order_state_service or MagicMock()
    redis_repo = redis_repo or MagicMock()

    if not isinstance(redis_repo.clear_reconcile_failure, AsyncMock):
        redis_repo.clear_reconcile_failure = AsyncMock()
    if not isinstance(redis_repo.increment_reconcile_failure, AsyncMock):
        redis_repo.increment_reconcile_failure = AsyncMock(return_value=1)

    return ReconciliationWorker(
        exchange_clients=make_registry(client),
        gateway=gateway,
        order_state_service=order_state_service,
        redis_order_repo=redis_repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        recent_grace_ms=0,
        external_orphan_policy=external_orphan_policy,
        all_orders_threshold=all_orders_threshold,
        reconcile_not_found_threshold=reconcile_not_found_threshold,
    )


def iter_order_batches(*batches: list[dict]):
    async def _iter(*args, **kwargs):
        for batch in batches:
            yield batch

    return _iter

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_repairs_pg_missing_in_redis() -> None:
    """PG에는 open 주문이 있지만 Redis projection이 없는 경우 projection을 복구한다."""
    order = make_order(order_id="ORD-PG-ONLY-001")

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(return_value=None)

    state_service = MagicMock()
    # pyrefly: ignore [bad-argument-type]
    state_service.iter_open_order_batches = iter_order_batches([order])
    state_service.refresh_order_projection = AsyncMock(return_value=True)

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(return_value=[])
    redis_repo.list_open_conditional_orders = AsyncMock(return_value=[])
    redis_repo.iter_open_conditional_order_batches = iter_order_batches()

    worker = make_worker(
        gateway=gateway,
        order_state_service=state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    # '단 한 번이라도' 특정 인자값과 함께 호출(await)된 적이 있는지 검증
    state_service.refresh_order_projection.assert_any_await(order)

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_repairs_pg_open_missing_from_exchange_open() -> None:
    """PG/Redis에는 open인데 거래소 open 목록에 없으면 단건 조회 후 Gateway 보정 경로를 탄다."""
    order = make_order(order_id="ORD-MISSING-EXCHANGE-001")
    # pyrefly: ignore [bad-argument-type]
    snapshot = order_snapshot(client_order_id=order.order_id)
    updated_order = make_order(
        # pyrefly: ignore [bad-argument-type]
        order_id=order.order_id,
        status=OrderStatus.FILLED,
        version=3,
    )

    client = make_client(get_order_result=snapshot)

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(return_value=updated_order)

    state_service = MagicMock()
    # pyrefly: ignore [bad-argument-type]
    state_service.iter_open_order_batches = iter_order_batches([order])

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(
        return_value=[order.model_dump(mode="json", exclude_none=True)]
    )
    redis_repo.list_open_conditional_orders = AsyncMock(return_value=[])
    redis_repo.iter_open_conditional_order_batches = iter_order_batches()

    worker = make_worker(
        client=client,
        gateway=gateway,
        order_state_service=state_service,
        redis_repo=redis_repo,
    )

    # await worker.reconcile_once()
    # client.get_order.assert_awaited_once_with(order)

    with patch.object(
        worker,
        "_repair_missing_open_by_single_get_order",
        wraps=worker._repair_missing_open_by_single_get_order,
    ) as single_get_order_spy:
        await worker.reconcile_once()

        single_get_order_spy.assert_awaited_once_with(
            client=client,
            symbol="BTCUSDT",
            orders=[order],
        )
        client.get_order.assert_awaited_once_with(order)

    gateway.apply_reconciliation_order_snapshot.assert_awaited_once_with(
        order_id=order.order_id,
        snapshot=snapshot,
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_single_get_order_not_found_before_threshold_does_not_mark_unresolved() -> None:
    """거래소 주문 없음 오류가 임계치 전이면 unresolved 전환 없이 재시도 대상으로 둔다."""
    order = make_order(order_id="ORD-NOT-FOUND-BEFORE-THRESHOLD")
    error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.ORDER_NOT_FOUND,
        code=-2013,
        message="Order does not exist.",
        status_code=400,
        raw={"code": -2013, "msg": "Order does not exist."},
    )

    client = make_client()
    client.get_order = AsyncMock(side_effect=error)

    gateway = MagicMock()
    gateway.mark_reconciliation_unresolved = AsyncMock()

    redis_repo = MagicMock()
    redis_repo.increment_reconcile_failure = AsyncMock(return_value=1)
    redis_repo.clear_reconcile_failure = AsyncMock()

    worker = make_worker(
        client=client,
        gateway=gateway,
        redis_repo=redis_repo,
        reconcile_not_found_threshold=3,
    )

    await worker._repair_missing_open_by_single_get_order(
        client=client,
        symbol="BTCUSDT",
        orders=[order],
    )

    redis_repo.increment_reconcile_failure.assert_awaited_once_with(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        order_id=order.order_id,
        ttl_sec=worker.reconcile_failure_ttl_sec,
    )
    gateway.mark_reconciliation_unresolved.assert_not_awaited()


@pytest.mark.stable
@pytest.mark.asyncio
async def test_single_get_order_not_found_at_threshold_marks_unresolved() -> None:
    """거래소 주문 없음 오류가 임계치에 도달하면 로컬 주문을 reconciliation unresolved로 격리한다."""
    order = make_order(order_id="ORD-NOT-FOUND-AT-THRESHOLD")
    error = ExchangeApiError(
        exchange=Exchange.BINANCE,
        category=ExchangeErrorCategory.ORDER_NOT_FOUND,
        code=-2013,
        message="Order does not exist.",
        status_code=400,
        raw={"code": -2013, "msg": "Order does not exist."},
    )

    client = make_client()
    client.get_order = AsyncMock(side_effect=error)

    gateway = MagicMock()
    gateway.mark_reconciliation_unresolved = AsyncMock()

    redis_repo = MagicMock()
    redis_repo.increment_reconcile_failure = AsyncMock(return_value=3) # 3번 이상이면 reconciliation unresolved로 이동
    redis_repo.clear_reconcile_failure = AsyncMock()

    worker = make_worker(
        client=client,
        gateway=gateway,
        redis_repo=redis_repo,
        reconcile_not_found_threshold=3,
    )

    await worker._repair_missing_open_by_single_get_order(
        client=client,
        symbol="BTCUSDT",
        orders=[order],
    )

    gateway.mark_reconciliation_unresolved.assert_awaited_once_with(
        order_id=order.order_id,
        exchange_error_code=-2013,
        detail_msg="Order does not exist.",
        raw_exchange_response={"code": -2013, "msg": "Order does not exist."},
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_detects_exchange_orphan_without_cancel_by_default() -> None:
    """거래소에만 있는 오픈 고아인 일반 주문은 기본 log 정책에서 자동 취소하지 않는다."""
    client = make_client(
        open_orders=[
            order_snapshot(
                client_order_id="EXTERNAL-ORDER-001",
                status=OrderStatus.ACKNOWLEDGED,
                raw_status="NEW",
            )
        ]
    )

    state_service = MagicMock()
    state_service.iter_open_order_batches = iter_order_batches()

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(return_value=[])
    redis_repo.list_open_conditional_orders = AsyncMock(return_value=[])
    redis_repo.iter_open_conditional_order_batches = iter_order_batches()

    worker = make_worker(
        client=client,
        order_state_service=state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    # cancel_regular_order_by_client_id 메소드는 실행되지 않는다.
    client.cancel_regular_order_by_client_id.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_cancels_regular_exchange_orphan_when_policy_cancel() -> None:
    """regular orphan 주문은 cancel 정책에서 regular client id 기반 취소를 요청한다."""
    open_orders=[
        order_snapshot(
            client_order_id="EXTERNAL-ORDER-001",
            status=OrderStatus.ACKNOWLEDGED,
            raw_status="NEW",
        )
    ]

    client = make_client(
        open_orders=open_orders
    )

    state_service = MagicMock()
    state_service.iter_open_order_batches = iter_order_batches()

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(return_value=[])
    redis_repo.list_open_conditional_orders = AsyncMock(return_value=[])
    redis_repo.iter_open_conditional_order_batches = iter_order_batches()

    worker = make_worker(
        client=client,
        order_state_service=state_service,
        redis_repo=redis_repo,
        external_orphan_policy="cancel",
    )

    # await worker.reconcile_once()

    # client.cancel_regular_order_by_client_id.assert_awaited_once_with(
    #     symbol="BTCUSDT",
    #     client_order_id="EXTERNAL-ORDER-001",
    # )
    with patch.object(
        worker,
        "_repair_redis_extra_vs_pg",
        wraps=worker._repair_redis_extra_vs_pg,
    ) as repair_redis_extra_spy:
        await worker.reconcile_once()

        repair_redis_extra_spy.assert_awaited_once()
        client.cancel_regular_order_by_client_id.assert_awaited_once_with(
            symbol="BTCUSDT",
            client_order_id="EXTERNAL-ORDER-001",
        )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_deletes_redis_projection_when_not_in_postgres() -> None:
    """Redis에는 open projection이 있지만 PG 원본이 없으면 stale projection을 삭제한다."""
    state_service = MagicMock()
    state_service.iter_open_order_batches = iter_order_batches()
    state_service.load_orders_from_postgres = AsyncMock(return_value={})

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(
        return_value=[
            make_order(order_id="ORD-REDIS-ONLY-001").model_dump(
                mode="json",
                exclude_none=True,
            )
        ]
    )
    redis_repo.list_open_conditional_orders = AsyncMock(return_value=[])
    redis_repo.iter_open_conditional_order_batches = iter_order_batches()
    redis_repo.delete = AsyncMock()

    worker = make_worker(
        order_state_service=state_service,
        redis_repo=redis_repo,
    )

    # await worker.reconcile_once()

    # redis_repo.delete.assert_awaited_once_with(order_id="ORD-REDIS-ONLY-001")

    with patch.object(
        worker,
        "_repair_redis_extra_vs_pg",
        wraps=worker._repair_redis_extra_vs_pg,
    ) as repair_redis_extra_spy:
        await worker.reconcile_once()

        repair_redis_extra_spy.assert_awaited_once()
        redis_repo.delete.assert_awaited_once_with(order_id="ORD-REDIS-ONLY-001")

@pytest.mark.stable
@pytest.mark.asyncio
async def test_repair_missing_open_uses_bulk_lookup_when_many_orders() -> None:
    """같은 심볼의 누락 open 주문이 threshold 이상이면 client bulk lookup을 사용한다."""
    orders = [make_order(order_id=f"ORD-ALL-{i}") for i in range(6)]
    order_ids = {order.order_id for order in orders if order.order_id}
    pg_open_by_id = {order.order_id: order for order in orders if order.order_id}
    bulk_results = {
        order.order_id: order_snapshot(client_order_id=order.order_id)
        for order in orders
        if order.order_id
    }

    client = make_client(bulk_results=bulk_results)

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        return_value=make_order(order_id="ORD-UPDATED", status=OrderStatus.FILLED)
    )

    all_orders_threshold = 6

    worker = make_worker(
        client=client,
        gateway=gateway,
        all_orders_threshold=all_orders_threshold,
    )

    # await worker._repair_pg_missing_from_exchange_open(
    #     client,
    #     order_ids,
    #     pg_open_by_id,
    # )

    # client.find_order_snapshots.assert_awaited_once()
    # client.get_order.assert_not_awaited()
    # assert gateway.apply_reconciliation_order_snapshot.await_count == all_orders_threshold

    with patch.object(
        worker,
        "_try_repair_missing_open_by_bulk_lookup",
        wraps=worker._try_repair_missing_open_by_bulk_lookup,
    ) as bulk_lookup_spy:
        await worker._repair_pg_missing_from_exchange_open(
            client,
            order_ids,
            pg_open_by_id,
        )

        bulk_lookup_spy.assert_awaited_once()
        client.find_order_snapshots.assert_awaited_once()
        client.get_order.assert_not_awaited()

    assert gateway.apply_reconciliation_order_snapshot.await_count == all_orders_threshold

@pytest.mark.stable
@pytest.mark.asyncio
async def test_repair_missing_open_uses_get_order_when_few_orders() -> None:
    """누락 open 주문 수가 적으면 bulk lookup 대신 주문별 get_order 단건 조회를 사용한다."""

    all_orders_threshold = 6
    open_order_num = 3
    assert open_order_num < all_orders_threshold

    orders = [make_order(order_id=f"ORD-SINGLE-{i}") for i in range(open_order_num)]
    order_ids = {order.order_id for order in orders if order.order_id}
    pg_open_by_id = {order.order_id: order for order in orders if order.order_id}
    snapshots = [
        order_snapshot(client_order_id=order.order_id)
        for order in orders
        if order.order_id
    ]

    client = make_client()
    client.get_order = AsyncMock(side_effect=snapshots)

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        return_value=make_order(order_id="ORD-UPDATED", status=OrderStatus.FILLED)
    )

    worker = make_worker(
        client=client,
        gateway=gateway,
        all_orders_threshold=all_orders_threshold,
    )

    await worker._repair_pg_missing_from_exchange_open(
        client,
        order_ids,
        pg_open_by_id,
    )

    client.find_order_snapshots.assert_not_awaited()
    assert client.get_order.await_count == 3
    assert gateway.apply_reconciliation_order_snapshot.await_count == 3

@pytest.mark.stable
@pytest.mark.asyncio
async def test_bulk_lookup_missing_items_fallback_to_get_order() -> None:
    """bulk lookup에서 일부 주문을 못 찾으면 못 찾은 주문만 get_order로 fallback한다."""
    orders = [make_order(order_id=f"ORD-FALLBACK-{i}") for i in range(6)]
    order_ids = {order.order_id for order in orders if order.order_id}
    pg_open_by_id = {order.order_id: order for order in orders if order.order_id}
    bulk_results = {
        order.order_id: order_snapshot(client_order_id=order.order_id)
        for order in orders[:5]
        if order.order_id
    }

    client = make_client(bulk_results=bulk_results)
    # pyrefly: ignore [bad-argument-type]
    client.get_order = AsyncMock(return_value=order_snapshot(client_order_id=orders[5].order_id))
    client.capabilities = ExchangeCapabilities(
        supports_bulk_order_lookup=True,
        bulk_order_lookup_threshold=3,
    )

    gateway = MagicMock()
    gateway.apply_reconciliation_order_snapshot = AsyncMock(
        return_value=make_order(order_id="ORD-UPDATED", status=OrderStatus.FILLED)
    )

    worker = make_worker(
        client=client,
        gateway=gateway,
        all_orders_threshold=6,
    )

    # await worker._repair_pg_missing_from_exchange_open(
    #     client,
    #     order_ids,
    #     pg_open_by_id,
    # )


    # client.find_order_snapshots.assert_awaited_once()
    # client.get_order.assert_awaited_once_with(orders[5])
    # assert gateway.apply_reconciliation_order_snapshot.await_count == 6


    with patch.object(
        worker,
        "_try_repair_missing_open_by_bulk_lookup",
        wraps=worker._try_repair_missing_open_by_bulk_lookup,
    ) as bulk_lookup_spy, patch.object(
        worker,
        "_repair_missing_open_by_single_get_order",
        wraps=worker._repair_missing_open_by_single_get_order,
    ) as single_get_spy:
        await worker._repair_pg_missing_from_exchange_open(
            client,
            order_ids,
            pg_open_by_id,
        )

        bulk_lookup_spy.assert_awaited_once()
        bulk_call = bulk_lookup_spy.await_args
        assert bulk_call is not None
        assert bulk_call.kwargs["client"] is client
        assert bulk_call.kwargs["symbol"] == "BTCUSDT"
        assert {o.order_id for o in bulk_call.kwargs["orders"]} == order_ids

        single_get_spy.assert_awaited_once()
        single_call = single_get_spy.await_args
        assert single_call is not None
        assert single_call.kwargs["client"] is client
        assert single_call.kwargs["symbol"] == "BTCUSDT"
        assert [o.order_id for o in single_call.kwargs["orders"]] == [orders[5].order_id]
        client.find_order_snapshots.assert_awaited_once()
        client.get_order.assert_awaited_once_with(orders[5])
    assert gateway.apply_reconciliation_order_snapshot.await_count == 6

@pytest.mark.stable
@pytest.mark.asyncio
async def test_conditional_orphan_log_policy_does_not_cancel() -> None:
    """거래소에만 있는 conditional orphan 주문은 log 정책에서 자동 취소하지 않는다."""
    local_order = make_conditional_order(
        order_id="COND-LOCAL-001",
        client_conditional_id="COND-LOCAL-001",
    )
    orphan_snapshot = conditional_snapshot(client_conditional_id="COND-ORPHAN-001")
    client = make_client(
        open_conditionals=[
            conditional_snapshot(client_conditional_id="COND-LOCAL-001"),
            orphan_snapshot,
        ]
    )

    state_service = MagicMock()
    state_service.iter_open_order_batches = iter_order_batches()
    state_service.load_order_by_client_conditional_id = AsyncMock(return_value=None)
    state_service.load_order_by_exchange_conditional_id = AsyncMock(return_value=None)

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(return_value=[])
    redis_repo.list_open_conditional_orders = AsyncMock(
        return_value=[local_order.model_dump(mode="json", exclude_none=True)]
    )
    redis_repo.iter_open_conditional_order_batches = iter_order_batches(
        [local_order.model_dump(mode="json", exclude_none=True)]
    )

    worker = make_worker(
        client=client,
        order_state_service=state_service,
        redis_repo=redis_repo,
    )

    await worker.reconcile_once()

    client.cancel_conditional_order_by_id.assert_not_awaited()

@pytest.mark.stable
@pytest.mark.asyncio
async def test_conditional_orphan_cancel_policy_uses_conditional_cancel() -> None:
    """conditional orphan 주문은 cancel 정책에서 조건부 주문 id 기반 취소를 요청한다."""
    local_order = make_conditional_order(
        order_id="COND-LOCAL-001",
        client_conditional_id="COND-LOCAL-001",
    )
    orphan_snapshot = conditional_snapshot(client_conditional_id="COND-ORPHAN-001")
    client = make_client(
        open_conditionals=[
            conditional_snapshot(client_conditional_id="COND-LOCAL-001"),
            orphan_snapshot,
        ]
    )

    state_service = MagicMock()
    state_service.iter_open_order_batches = iter_order_batches()
    state_service.load_order_by_client_conditional_id = AsyncMock(return_value=None)
    state_service.load_order_by_exchange_conditional_id = AsyncMock(return_value=None)

    redis_repo = MagicMock()
    redis_repo.list_open_regular_orders = AsyncMock(return_value=[])
    redis_repo.list_open_conditional_orders = AsyncMock(
        return_value=[local_order.model_dump(mode="json", exclude_none=True)]
    )
    redis_repo.iter_open_conditional_order_batches = iter_order_batches(
        [local_order.model_dump(mode="json", exclude_none=True)]
    )

    worker = make_worker(
        client=client,
        order_state_service=state_service,
        redis_repo=redis_repo,
        external_orphan_policy="cancel",
    )

    # await worker.reconcile_once()

    # client.cancel_conditional_order_by_id.assert_awaited_once_with(
    #     symbol="BTCUSDT",
    #     client_conditional_id="COND-ORPHAN-001",
    #     exchange_conditional_id="1001",
    # )

    with patch.object(
        worker,
        "_repair_exchange_conditional_orphan",
        wraps=worker._repair_exchange_conditional_orphan,
    ) as repair_conditional_orphan_spy:
        await worker.reconcile_once()

        repair_conditional_orphan_spy.assert_awaited_once()
        client.cancel_conditional_order_by_id.assert_awaited_once_with(
            symbol="BTCUSDT",
            client_conditional_id="COND-ORPHAN-001",
            exchange_conditional_id="1001",
        )
