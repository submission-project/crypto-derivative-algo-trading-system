from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from execution_gateway.workers.recovery_worker import RecoveryWorker
from execution_gateway.exchange import ExchangeCapabilities, ExchangeOrderSnapshot
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from schemas.market import Exchange, MarketType
from schemas.order import OrderStatus


def _make_order_row(
    *,
    order_id: str,
    symbol: str = "BTCUSDT",
    status: str = "SUBMITTED",
    updated_ts: int = 1_700_000_000_000,
) -> dict:
    """RecoveryWorker가 Order.model_validate()할 수 있는 최소 row."""
    return {
        "order_id": order_id,
        "source": "MANUAL",
        "exchange": "BINANCE",
        "market_type": "PERP",
        "symbol": symbol,
        "side": "BUY",
        "order_type": "MARKET",
        "order_route": "REGULAR",
        "quantity": "0.001",
        "status": status,
        "created_ts": updated_ts,
        "updated_ts": updated_ts,
    }


def _make_worker(
    *,
    gateway: MagicMock,
    repo: MagicMock,
    client: MagicMock | None = None,
    failure_backoff_ms: int = 10_000,
) -> RecoveryWorker:
    if client is None:
        client = MagicMock()
        client.exchange = Exchange.BINANCE
        client.market_type = MarketType.PERP
        client.capabilities = ExchangeCapabilities(
            supports_conditional_reconciliation=True,
        )
        client.get_order = AsyncMock()
        client.get_conditional_order = AsyncMock()

    registry = ExchangeExecutionClientRegistry()
    registry.register(client)

    repo.postpone_recovery_order = AsyncMock()

    return RecoveryWorker(
        exchange_clients=registry,
        gateway=gateway,
        repo=repo,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        interval_sec=1,
        older_than_ms=2_000,
        batch_size=100,
        failure_backoff_ms=failure_backoff_ms,
    )


@pytest.mark.asyncio
async def test_recover_once_no_targets() -> None:
    gateway = MagicMock()
    repo = MagicMock()
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities()
    client.get_order = AsyncMock()

    repo.list_recovery_orders = AsyncMock(return_value=[])
    gateway.apply_reconciliation_order_snapshot = AsyncMock()

    worker = _make_worker(gateway=gateway, repo=repo, client=client)

    await worker.recover_once(exchange=Exchange.BINANCE, market_type=MarketType.PERP)

    repo.list_recovery_orders.assert_awaited_once()

    
    assert repo.list_recovery_orders.await_args is not None
    assert repo.list_recovery_orders.await_args.kwargs["batch_size"] == 100
    client.get_order.assert_not_awaited()
    gateway.apply_reconciliation_order_snapshot.assert_not_awaited()
    repo.postpone_recovery_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_once_calls_binance_and_gateway() -> None:
    gateway = MagicMock()
    repo = MagicMock()
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities()

    row = _make_order_row(order_id="ORD-RECOVERY-001")

    repo.list_recovery_orders = AsyncMock(return_value=[row])

    exchange_snapshot = ExchangeOrderSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        client_order_id="ORD-RECOVERY-001",
        exchange_order_id="123456",
        status=OrderStatus.ACKNOWLEDGED,
        raw_status="NEW",
        raw={
            "clientOrderId": "ORD-RECOVERY-001",
            "symbol": "BTCUSDT",
            "status": "NEW",
            "orderId": 123456,
            "executedQty": "0",
            "avgPrice": "0",
        },
    )
    client.get_order = AsyncMock(return_value=exchange_snapshot)

    updated_order = MagicMock()
    updated_order.status.value = "ACKNOWLEDGED"

    gateway.apply_reconciliation_order_snapshot = AsyncMock(return_value=updated_order)

    worker = _make_worker(gateway=gateway, repo=repo, client=client)

    await worker.recover_once(exchange=Exchange.BINANCE, market_type=MarketType.PERP)

    repo.list_recovery_orders.assert_awaited_once()

    assert repo.list_recovery_orders.await_args is not None
    
    assert repo.list_recovery_orders.await_args.kwargs["batch_size"] == 100

    client.get_order.assert_awaited_once()
    # pyrefly: ignore [missing-attribute]
    assert client.get_order.await_args.args[0].order_id == "ORD-RECOVERY-001"

    gateway.apply_reconciliation_order_snapshot.assert_awaited_once_with(
        order_id="ORD-RECOVERY-001",
        snapshot=exchange_snapshot,
    )
    repo.postpone_recovery_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_once_skips_order_with_parse_failure() -> None:
    """필수 필드가 누락된 row는 파싱 실패하고 건너뛴다."""
    gateway = MagicMock()
    repo = MagicMock()
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities()
    client.get_order = AsyncMock()

    # 필수 필드 누락 row
    repo.list_recovery_orders = AsyncMock(
        return_value=[
            {
                "order_id": "ORD-RECOVERY-002",
                "status": "UNKNOWN",
                "updated_ts": 1_700_000_000_000,
            }
        ]
    )

    gateway.apply_reconciliation_order_snapshot = AsyncMock()

    worker = _make_worker(gateway=gateway, repo=repo, client=client)

    await worker.recover_once(exchange=Exchange.BINANCE, market_type=MarketType.PERP)

    # 파싱 실패 → Binance API 호출 없음
    client.get_order.assert_not_awaited()
    gateway.apply_reconciliation_order_snapshot.assert_not_awaited()
    repo.postpone_recovery_order.assert_awaited_once()


@pytest.mark.asyncio
async def test_recover_once_deduplicates_order_ids() -> None:
    gateway = MagicMock()
    repo = MagicMock()
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities()

    row1 = _make_order_row(order_id="ORD-DUP-001", status="SUBMITTED")
    row2 = _make_order_row(order_id="ORD-DUP-001", status="UNKNOWN")

    repo.list_recovery_orders = AsyncMock(return_value=[row1, row2])

    client.get_order = AsyncMock(
        return_value=ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            client_order_id="ORD-DUP-001",
            exchange_order_id="1",
            status=OrderStatus.ACKNOWLEDGED,
            raw_status="NEW",
            raw={
                "clientOrderId": "ORD-DUP-001",
                "symbol": "BTCUSDT",
                "status": "NEW",
                "orderId": 1,
                "executedQty": "0",
                "avgPrice": "0",
            },
        )
    )

    updated_order = MagicMock()
    updated_order.status.value = "ACKNOWLEDGED"
    gateway.apply_reconciliation_order_snapshot = AsyncMock(return_value=updated_order)

    worker = _make_worker(gateway=gateway, repo=repo, client=client)

    await worker.recover_once(exchange=Exchange.BINANCE, market_type=MarketType.PERP)

    assert client.get_order.await_count == 1
    assert gateway.apply_reconciliation_order_snapshot.await_count == 1
    repo.postpone_recovery_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_once_postpones_failed_regular_order() -> None:
    gateway = MagicMock()
    repo = MagicMock()
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities()

    row = _make_order_row(order_id="ORD-FAIL-001")
    repo.list_recovery_orders = AsyncMock(return_value=[row])
    client.get_order = AsyncMock(side_effect=RuntimeError("temporary failure"))
    gateway.apply_reconciliation_order_snapshot = AsyncMock()

    worker = _make_worker(
        gateway=gateway,
        repo=repo,
        client=client,
        failure_backoff_ms=5_000,
    )

    with patch(
        "execution_gateway.workers.recovery_worker.epoch_ms",
        return_value=1_700_000_000_000,
    ):
        await worker.recover_once(exchange=Exchange.BINANCE, market_type=MarketType.PERP)

    client.get_order.assert_awaited_once()
    gateway.apply_reconciliation_order_snapshot.assert_not_awaited()
    repo.postpone_recovery_order.assert_awaited_once_with(
        exchange=Exchange.BINANCE.value,
        market_type=MarketType.PERP.value,
        order_id="ORD-FAIL-001",
        next_attempt_ts=1_700_000_005_000,
    )
