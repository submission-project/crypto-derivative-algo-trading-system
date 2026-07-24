from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from execution_gateway.exchange import ExchangeCapabilities, ExchangePositionSnapshot
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from execution_gateway.workers.position_reconciliation_worker import (
    PositionReconciliationWorker,
)
from schemas.market import Exchange, MarketType
from schemas.position import PositionSide


def make_snapshot(symbol: str = "BTCUSDT") -> ExchangePositionSnapshot:
    return ExchangePositionSnapshot(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=symbol,
        position_side=PositionSide.BOTH,
        position_amt="0.01",
        entry_price="60000",
        updated_ts=1_700_000_000_500,
        raw={"source": "unit-test"},
    )


def make_client(
    *,
    supports_position_snapshot: bool = True,
) -> MagicMock:
    client = MagicMock()
    client.exchange = Exchange.BINANCE
    client.market_type = MarketType.PERP
    client.capabilities = ExchangeCapabilities(
        supports_position_snapshot=supports_position_snapshot,
    )
    client.get_positions = AsyncMock(return_value=[make_snapshot()])
    return client


def make_registry(client: MagicMock) -> ExchangeExecutionClientRegistry:
    registry = ExchangeExecutionClientRegistry()
    registry.register(client)
    return registry

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_once_refreshes_all_positions_with_exchange_client() -> None:
    """active_symbols가 없으면 거래소 client의 전체 position snapshot을 service에 반영한다."""
    client = make_client()
    position_state_service = MagicMock()
    position_state_service.refresh_position_snapshots = AsyncMock(return_value=[MagicMock()])

    worker = PositionReconciliationWorker(
        exchange_clients=make_registry(client),
        position_state_service=position_state_service,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        active_symbols=None,
    )

    await worker.reconcile_once()

    client.get_positions.assert_awaited_once_with()
    position_state_service.refresh_position_snapshots.assert_awaited_once_with(
        [make_snapshot()]
    )

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_once_refreshes_active_symbols_individually() -> None:
    """active_symbols가 있으면 symbol별로 position snapshot을 조회한다."""
    client = make_client()
    position_state_service = MagicMock()
    position_state_service.refresh_position_snapshots = AsyncMock(return_value=[MagicMock()])

    worker = PositionReconciliationWorker(
        exchange_clients=make_registry(client),
        position_state_service=position_state_service,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
        active_symbols={"BTCUSDT", "ETHUSDT"},
    )

    await worker.reconcile_once()

    assert client.get_positions.await_count == 2
    assert position_state_service.refresh_position_snapshots.await_count == 2
    awaited_symbols = {
        call.kwargs["symbol"]
        for call in client.get_positions.await_args_list
    }
    assert awaited_symbols == {"BTCUSDT", "ETHUSDT"}

@pytest.mark.stable
@pytest.mark.asyncio
async def test_reconcile_once_skips_client_without_position_snapshot_capability() -> None:
    """position snapshot을 지원하지 않는 거래소 client는 조회하지 않는다."""
    client = make_client(supports_position_snapshot=False)
    position_state_service = MagicMock()
    position_state_service.refresh_position_snapshots = AsyncMock()

    worker = PositionReconciliationWorker(
        exchange_clients=make_registry(client),
        position_state_service=position_state_service,
        markets=[(Exchange.BINANCE, MarketType.PERP)],
    )

    await worker.reconcile_once()

    client.get_positions.assert_not_awaited()
    position_state_service.refresh_position_snapshots.assert_not_awaited()
