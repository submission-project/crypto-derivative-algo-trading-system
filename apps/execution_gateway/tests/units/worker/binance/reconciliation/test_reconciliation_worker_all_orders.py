from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.adapters.binance.binance_execution_client import BinanceExecutionClient
from execution_gateway.adapters.binance.dto.resp.OrderResponseDto import OrderRespDto
from schemas.market import Exchange, MarketType
from schemas.order import (
    Order,
    OrderSide,
    OrderSource,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)


def make_order(
    order_id: str,
    *,
    symbol: str = "BTCUSDT",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    created_ts: int = 1_700_000_000_000,
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
        symbol=symbol,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.1",
        price="60000",
        stop_price=None,
        reduce_only=False,
        position_action=PositionAction.OPEN,
        created_ts=created_ts,
        submitted_ts=created_ts + 100,
        filled_ts=None,
        updated_ts=updated_ts,
        status=status,
        version=version,
    )


def make_client(*, adapter: MagicMock | None = None) -> BinanceExecutionClient:
    adapter = adapter or MagicMock()
    client = BinanceExecutionClient(
        adapter=adapter,
        order_router=MagicMock(),
    )
    client.rate_limiter = MagicMock()
    client.rate_limiter.acquire_request_weight = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_fetch_all_orders_until_client_ids_found_advances_order_id() -> None:
    """Binance allOrders pagination에서 다음 조회 order_id가 마지막 orderId + 1로 진행된다."""
    adapter = MagicMock()
    adapter.get_all_orders = AsyncMock(
        side_effect=[
            [
                OrderRespDto.from_response({
                    "orderId": 10,
                    "clientOrderId": "OTHER-1",
                    "symbol": "BTCUSDT",
                    "status": "FILLED",
                }),
                OrderRespDto.from_response({
                    "orderId": 11,
                    "clientOrderId": "ORD-TARGET-1",
                    "symbol": "BTCUSDT",
                    "status": "FILLED",
                }),
            ],
            [
                OrderRespDto.from_response({
                    "orderId": 12,
                    "clientOrderId": "ORD-TARGET-2",
                    "symbol": "BTCUSDT",
                    "status": "FILLED",
                })
            ],
        ]
    )
    client = make_client(adapter=adapter)

    rows = await client._fetch_all_orders_until_client_ids_found(
        symbol="BTCUSDT",
        start_time=1_700_000_000_000,
        end_time=1_700_000_100_000,
        target_client_ids={"ORD-TARGET-1", "ORD-TARGET-2"},
        limit=2,
    )

    assert len(rows) == 3
    assert adapter.get_all_orders.await_count == 2
    # pyrefly: ignore [missing-attribute]
    assert client.rate_limiter.acquire_request_weight.await_count == 2

    first_call = adapter.get_all_orders.await_args_list[0]
    second_call = adapter.get_all_orders.await_args_list[1]

    assert first_call.kwargs["symbol"] == "BTCUSDT"
    assert first_call.kwargs.get("order_id") is None
    assert second_call.kwargs["order_id"] == 12


@pytest.mark.asyncio
async def test_fetch_all_orders_stops_when_page_is_short() -> None:
    """allOrders 응답 페이지가 limit보다 짧으면 더 이상 다음 페이지를 조회하지 않는다."""
    adapter = MagicMock()
    adapter.get_all_orders = AsyncMock(
        return_value=[
            OrderRespDto.from_response({
                "orderId": 10,
                "clientOrderId": "OTHER-1",
                "symbol": "BTCUSDT",
                "status": "FILLED",
            })
        ]
    )
    client = make_client(adapter=adapter)

    rows = await client._fetch_all_orders_until_client_ids_found(
        symbol="BTCUSDT",
        start_time=1_700_000_000_000,
        end_time=1_700_000_100_000,
        target_client_ids={"ORD-NOT-FOUND"},
        limit=2,
    )

    assert len(rows) == 1
    assert adapter.get_all_orders.await_count == 1
    # pyrefly: ignore [missing-attribute]
    assert client.rate_limiter.acquire_request_weight.await_count == 1


@pytest.mark.asyncio
async def test_fetch_all_orders_stops_when_all_targets_found_even_if_page_full() -> None:
    """페이지가 가득 차도 찾으려던 client id를 모두 찾으면 pagination을 종료한다."""
    adapter = MagicMock()
    adapter.get_all_orders = AsyncMock(
        return_value=[
            OrderRespDto.from_response({
                "orderId": 10,
                "clientOrderId": "ORD-TARGET-1",
                "symbol": "BTCUSDT",
                "status": "FILLED",
            }),
            OrderRespDto.from_response({
                "orderId": 11,
                "clientOrderId": "ORD-TARGET-2",
                "symbol": "BTCUSDT",
                "status": "FILLED",
            }),
        ]
    )
    client = make_client(adapter=adapter)

    rows = await client._fetch_all_orders_until_client_ids_found(
        symbol="BTCUSDT",
        start_time=1_700_000_000_000,
        end_time=1_700_000_100_000,
        target_client_ids={"ORD-TARGET-1", "ORD-TARGET-2"},
        limit=2,
    )

    assert len(rows) == 2
    assert adapter.get_all_orders.await_count == 1


@pytest.mark.asyncio
async def test_find_order_snapshots_returns_snapshots_keyed_by_local_order_id() -> None:
    """find_order_snapshots는 allOrders 응답을 local order_id 기준 snapshot dict로 변환한다."""
    orders = [make_order(f"ORD-ALL-{i}") for i in range(3)]
    adapter = MagicMock()
    adapter.get_all_orders = AsyncMock(
        return_value=[
            OrderRespDto.from_response({
                "clientOrderId": order.order_id,
                "symbol": order.symbol,
                "status": "FILLED",
                "orderId": i,
                "executedQty": "0.1",
                "avgPrice": "60000",
            })
            for i, order in enumerate(orders)
        ]
    )
    client = make_client(adapter=adapter)

    result = await client.find_order_snapshots(
        symbol="BTCUSDT",
        orders=orders,
        lookback_ms=60_000,
        limit=1000,
    )

    assert set(result) == {order.order_id for order in orders}
    assert all(snapshot.status == OrderStatus.FILLED for snapshot in result.values())
    adapter.get_all_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_find_order_snapshots_omits_orders_not_found_in_all_orders() -> None:
    """allOrders에서 찾지 못한 주문은 결과 dict에서 제외해 worker fallback 대상이 되게 한다."""
    orders = [make_order(f"ORD-FALLBACK-{i}") for i in range(2)]
    adapter = MagicMock()
    adapter.get_all_orders = AsyncMock(
        return_value=[
            OrderRespDto.from_response({
                "clientOrderId": orders[0].order_id,
                "symbol": orders[0].symbol,
                "status": "FILLED",
                "orderId": 1,
                "executedQty": "0.1",
                "avgPrice": "60000",
            })
        ]
    )
    client = make_client(adapter=adapter)

    result = await client.find_order_snapshots(
        symbol="BTCUSDT",
        orders=orders,
        lookback_ms=60_000,
        limit=1000,
    )

    assert set(result) == {orders[0].order_id}
