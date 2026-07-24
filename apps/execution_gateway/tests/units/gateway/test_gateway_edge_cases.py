"""
Gateway 엣지 케이스 테스트.

검증 대상:
  1. apply_reconciliation_snapshot — terminal 보호
  2. GatewayTransitionService._set_status — StaleOrderVersionError 처리
  3. UDS 이벤트 — terminal 주문 재처리 방지
  4. cancel — 이미 terminal인 주문 cancel 시도
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from execution_gateway.gateway import ExecutionGateway
from execution_gateway.exchange import ExchangeOrderSnapshot
from execution_gateway.adapters.binance.binance_rest_adapter import (
    BinanceRestAdapter,
)
from execution_gateway.exchange.registry import ExchangeExecutionClientRegistry
from schemas.order_update_event import NormalizedOrderUpdateEvent
from storage.repositories.redis.order_state_repo import OrderStateRedisRepository
from storage.repositories.postgres.order_repo import StaleOrderVersionError
from schemas.order import (
    Order,
    OrderSource,
    OrderStatus,
    PositionAction,
    RejectReason,
    TERMINAL_STATUSES,
)
from schemas.market import Exchange, MarketType



# ─────────────────────── Fixtures ───────────────────────


def _make_order(
    *,
    order_id: str = "O-BN-PERP-TEST001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    version: int = 3,
    exchange_order_id: str = "12345",
) -> Order:
    return Order(
        order_id=order_id,
        source=OrderSource.MANUAL,
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        time_in_force="GTC",
        quantity="0.1",
        price="60000",
        status=status,
        position_action=PositionAction.OPEN,
        version=version,
        exchange_order_id=exchange_order_id,
        created_ts=1000000,
        updated_ts=1000001,
    )


@pytest.fixture
def mock_adapter():
    adapter = MagicMock(spec=BinanceRestAdapter)
    adapter.exchange = Exchange.BINANCE
    adapter.market_type = MarketType.PERP
    return adapter


@pytest.fixture
def mock_repo():
    repo = MagicMock(spec=OrderStateRedisRepository)
    repo.save = AsyncMock()
    repo.get = AsyncMock(return_value=None)
    repo.update_status = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_state_service():
    service = MagicMock()

    async def transition_order(*, current_order, updated_order):
        updated = updated_order.model_copy(deep=True)
        updated.version = current_order.version + 1
        return updated

    service.create_order = AsyncMock(side_effect=lambda order: order)
    service.transition_order = AsyncMock(side_effect=transition_order)
    service.load_order = AsyncMock(return_value=None)
    return service


@pytest.fixture
def gateway(mock_adapter, mock_repo, mock_state_service):
    registry = ExchangeExecutionClientRegistry()
    registry.register(mock_adapter)

    return ExecutionGateway(
        state_repo=mock_repo,
        state_service=mock_state_service,
        exchange_clients=registry,
    )


# ─────────────────────── F-02: apply_reconciliation_snapshot terminal 보호 ───────────────────────


class TestReconciliationTerminalProtection:
    """이미 terminal 상태인 주문을 reconciliation으로 되돌리는 것을 방지."""

    @pytest.mark.asyncio
    async def test_filled_order_not_regressed_to_acknowledged(
        self, gateway, mock_state_service
    ):
        """FILLED 주문에 NEW(→ACKNOWLEDGED) snapshot이 오면 상태 변경 안 됨."""
        filled_order = _make_order(
            status=OrderStatus.FILLED,
            version=5,
        )
        mock_state_service.load_order = AsyncMock(return_value=filled_order)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.ACKNOWLEDGED,
            client_order_id=filled_order.order_id,
            exchange_order_id="12345",
            filled_quantity="0",
            avg_fill_price="0",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=filled_order.order_id,
            snapshot=snapshot,
        )

        assert result is not None
        assert result.status == OrderStatus.FILLED
        # transition_order가 호출되지 않아야 함
        mock_state_service.transition_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancelled_order_not_regressed_to_partially_filled(
        self, gateway, mock_state_service
    ):
        """CANCELLED 주문에 PARTIALLY_FILLED snapshot이 오면 상태 변경 안 됨."""
        cancelled_order = _make_order(
            status=OrderStatus.CANCELLED,
            version=4,
        )
        mock_state_service.load_order = AsyncMock(return_value=cancelled_order)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.PARTIALLY_FILLED,
            client_order_id=cancelled_order.order_id,
            exchange_order_id="12345",
            filled_quantity="0.05",
            avg_fill_price="60000",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=cancelled_order.order_id,
            snapshot=snapshot,
        )

        assert result is not None
        assert result.status == OrderStatus.CANCELLED
        mock_state_service.transition_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_filled_order_allows_same_terminal_reobservation(
        self, gateway, mock_state_service
    ):
        """FILLED 주문에 FILLED snapshot이 오면 idempotent하게 처리."""
        filled_order = _make_order(
            status=OrderStatus.FILLED,
            version=5,
        )
        mock_state_service.load_order = AsyncMock(return_value=filled_order)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.FILLED,
            client_order_id=filled_order.order_id,
            exchange_order_id="12345",
            filled_quantity="0.1",
            avg_fill_price="60000",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=filled_order.order_id,
            snapshot=snapshot,
        )

        assert result is not None
        # 같은 terminal 상태는 통과
        assert result.status == OrderStatus.FILLED

    @pytest.mark.asyncio
    @pytest.mark.parametrize("terminal_status", list(TERMINAL_STATUSES))
    async def test_all_terminal_statuses_protected_from_non_terminal_snapshots(
        self, gateway, mock_state_service, terminal_status
    ):
        """모든 terminal 상태가 non-terminal snapshot으로부터 보호됨."""
        order = _make_order(status=terminal_status, version=5)
        mock_state_service.load_order = AsyncMock(return_value=order)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.ACKNOWLEDGED,
            client_order_id=order.order_id,
            exchange_order_id="12345",
            filled_quantity="0",
            avg_fill_price="0",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=order.order_id,
            snapshot=snapshot,
        )

        assert result.status == terminal_status
        mock_state_service.transition_order.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_terminal_order_reconciled_normally(
        self, gateway, mock_state_service
    ):
        """non-terminal 주문은 정상적으로 reconciliation 반영."""
        acked_order = _make_order(
            status=OrderStatus.ACKNOWLEDGED,
            version=3,
        )
        mock_state_service.load_order = AsyncMock(return_value=acked_order)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.FILLED,
            client_order_id=acked_order.order_id,
            exchange_order_id="12345",
            filled_quantity="0.1",
            avg_fill_price="60000",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id=acked_order.order_id,
            snapshot=snapshot,
        )

        assert result.status == OrderStatus.FILLED
        mock_state_service.transition_order.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reconciliation_order_not_found(self, gateway, mock_state_service):
        """로컬에 없는 주문의 reconciliation은 None 반환."""
        mock_state_service.load_order = AsyncMock(return_value=None)

        snapshot = ExchangeOrderSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            status=OrderStatus.FILLED,
            client_order_id="O-BN-PERP-NONEXIST",
            exchange_order_id="12345",
            filled_quantity="0.1",
            avg_fill_price="60000",
        )

        result = await gateway.apply_reconciliation_order_snapshot(
            order_id="O-BN-PERP-NONEXIST",
            snapshot=snapshot,
        )

        assert result is None


# ─────────────────────── F-07: StaleOrderVersionError 처리 ───────────────────────


class TestStaleVersionHandling:
    """GatewayTransitionService._set_status에서 StaleOrderVersionError 발생 시 PG 재조회 후 반환."""

    @pytest.mark.asyncio
    async def test_stale_version_reloads_from_pg(self, gateway, mock_state_service):
        """version conflict 발생 시 PG에서 재조회한 order 반환."""
        original_order = _make_order(
            status=OrderStatus.ACKNOWLEDGED,
            version=3,
        )

        # PG에서 이미 FILLED로 업데이트된 상태
        reloaded_order = _make_order(
            status=OrderStatus.FILLED,
            version=5,
        )

        assert original_order.order_id

        mock_state_service.transition_order = AsyncMock(
            side_effect=StaleOrderVersionError(
                order_id=original_order.order_id,
                expected_version=3,
                actual_version=5,
                actual_status="FILLED",
            )
        )
        mock_state_service.load_order = AsyncMock(return_value=reloaded_order)

        result = await gateway.transitions._set_status(
            order=original_order,
            status=OrderStatus.PARTIALLY_FILLED,
            use_machine=False,
            protect_terminal=True,
        )

        assert result.status == OrderStatus.FILLED
        assert result.version == 5
        mock_state_service.load_order.assert_awaited_once_with(
            order_id=original_order.order_id,
        )

    @pytest.mark.asyncio
    async def test_stale_version_pg_also_missing_returns_original(
        self, gateway, mock_state_service
    ):
        """version conflict + PG에서도 찾지 못하면 원래 order 반환."""
        original_order = _make_order(
            status=OrderStatus.ACKNOWLEDGED,
            version=3,
        )

        assert original_order.order_id

        mock_state_service.transition_order = AsyncMock(
            side_effect=StaleOrderVersionError(
                order_id=original_order.order_id,
                expected_version=3,
                actual_version=5,
                actual_status="FILLED",
            )
        )
        mock_state_service.load_order = AsyncMock(return_value=None)

        result = await gateway.transitions._set_status(
            order=original_order,
            status=OrderStatus.PARTIALLY_FILLED,
            use_machine=False,
            protect_terminal=True,
        )

        # PG에서도 못 찾으면 원래 order 반환
        assert result.status == OrderStatus.ACKNOWLEDGED
        assert result.version == 3
        mock_state_service.load_order.assert_awaited_once_with(
            order_id=original_order.order_id,
        )


# ─────────────────────── Cancel — terminal 보호 ───────────────────────


class TestCancelTerminalProtection:
    """이미 terminal인 주문 cancel 시 거래소 호출 없이 skip."""

    @pytest.mark.asyncio
    async def test_cancel_filled_order_skips(
        self, gateway, mock_adapter, mock_state_service
    ):
        """FILLED 주문 cancel 시도 → 거래소 호출 없이 반환."""
        filled_order = _make_order(
            status=OrderStatus.FILLED,
            version=5,
        )
        mock_state_service.load_order = AsyncMock(return_value=filled_order)

        result = await gateway.cancel_order(filled_order.order_id)

        # cancel이 skip되면 dict 또는 Order가 반환될 수 있음
        # 핵심: adapter.cancel_order가 호출되지 않아야 함
        mock_adapter.cancel_order.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_cancelled_order_skips(
        self, gateway, mock_adapter, mock_state_service
    ):
        """CANCELLED 주문 cancel 시도 → 거래소 호출 없이 반환."""
        cancelled_order = _make_order(
            status=OrderStatus.CANCELLED,
            version=4,
        )
        mock_state_service.load_order = AsyncMock(return_value=cancelled_order)

        result = await gateway.cancel_order(cancelled_order.order_id)

        mock_adapter.cancel_order.assert_not_called()


# ─────────────────────── UDS — terminal 재처리 방지 ───────────────────────


class TestUDSTerminalProtection:
    """UDS 이벤트로 terminal 주문을 non-terminal로 되돌리지 않음."""

    @pytest.mark.asyncio
    async def test_uds_new_event_on_filled_order_no_regression(
        self, gateway, mock_state_service
    ):
        """FILLED 주문에 NEW UDS 이벤트 수신 → 상태 변경 없음."""
        filled_order = _make_order(
            status=OrderStatus.FILLED,
            version=5,
        )
        mock_state_service.load_order = AsyncMock(return_value=filled_order)

        event = NormalizedOrderUpdateEvent(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            # pyrefly: ignore [bad-argument-type]
            client_order_id=filled_order.order_id,
            exchange_order_id="12345",
            target_status=OrderStatus.ACKNOWLEDGED,
            exchange_status="NEW",
            execution_type="NEW",
            filled_quantity="0",
            avg_fill_price="0",
            event_time=1700000000000,
            transaction_time=1700000000001,
            raw={
                "e": "ORDER_TRADE_UPDATE",
                "E": 1700000000000,
                "T": 1700000000001,
            },
        )

        result = await gateway.apply_order_update_event(event)

        if result is not None:
            # terminal 보호에 의해 상태가 변경되지 않아야 함
            assert result.status == OrderStatus.FILLED
