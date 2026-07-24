"""
api_server/main.py 단위 테스트.

순수 함수 + mock 기반 async 함수를 검증한다.
외부 인프라(PG, Redis, QuestDB, Binance) 의존 없음.

테스트 대상 (ExecutionLogService / main 핸들러):
  1. ExecutionLogService._is_real_trade_fill() — 체결 판별 로직
  2. ExecutionLogService._build_execution_report() — QuestDB 저장 payload 생성
  3. ExecutionLogService._enum_value() — Enum → value 변환
  4. ExecutionLogService._is_real_trade_fill 엣지 케이스
  5. ExecutionLogService.save_if_needed() — dedup + QuestDB 저장 (mock)
  6. on_trade_update / on_position_update (handlers/user_data_stream_handler)
  8. get_user_data_ws_base_url() — WS URL 생성
"""

from __future__ import annotations

from enum import Enum
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from schemas.order import (
    Order,
    OrderSource,
    OrderStatus,
    PositionAction,
)
from schemas.market import Exchange, MarketType
from schemas.binance_usds_futures import (
    BinanceUsdsFuturesExecutionType,
)

from schemas.order_update_event import NormalizedOrderUpdateEvent
from schemas.position import PositionSide, PositionStatus
from schemas.position_update_event import NormalizedPositionSnapshot
from api_server.services.execution_log_service import ExecutionLogService


# ─────────────────────── Helpers ───────────────────────


def _make_order(
    *,
    order_id: str = "O-BN-PERP-TEST001",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
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
        version=3,
        exchange_order_id="12345",
        created_ts=1_700_000_000_000,
        updated_ts=1_700_000_000_001,
    )


def _make_fill_event(
    *,
    client_order_id: str = "O-BN-PERP-TEST001",
    execution_type: str = BinanceUsdsFuturesExecutionType.TRADE,
    fill_qty: str = "0.1",
    trade_id: int = 99999,
    fill_price: str = "60000",
    exchange_order_id: int = 12345,
    exchange_status: str = "FILLED",
) -> dict:
    """Binance ORDER_TRADE_UPDATE의 o (order) dict 생성."""
    return {
        "c": client_order_id,
        "i": exchange_order_id,
        "X": exchange_status,
        "x": execution_type,
        "l": fill_qty,
        "L": fill_price,
        "t": trade_id,
        "q": "0.1",
        "z": "0.1",
        "ap": fill_price,
        "n": "0.001",
        "N": "USDT",
        "m": False,
    }


def _enum_text(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _make_normalized_event(
    order_event: dict,
    event_time: int = 1_700_000_000_000,
    transaction_time: int | None = 1_700_000_000_001,
) -> NormalizedOrderUpdateEvent:
    trade_id = order_event.get("t")
    if trade_id in (None, "", 0, "0", -1, "-1"):
        trade_id = None

    return NormalizedOrderUpdateEvent(
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol=str(order_event.get("s") or "BTCUSDT").upper(),
        client_order_id=str(order_event.get("c") or "O-BN-PERP-TEST001"),
        exchange_order_id=(
            str(order_event["i"]) if order_event.get("i") is not None else None
        ),
        exchange_status=_enum_text(order_event.get("X")),
        execution_type=_enum_text(order_event.get("x")),
        filled_quantity=(
            str(order_event["z"]) if order_event.get("z") is not None else None
        ),
        avg_fill_price=(
            str(order_event["ap"]) if order_event.get("ap") is not None else None
        ),
        last_fill_quantity=(
            str(order_event["l"]) if order_event.get("l") is not None else None
        ),
        last_fill_price=(
            str(order_event["L"]) if order_event.get("L") is not None else None
        ),
        trade_id=str(trade_id) if trade_id is not None else None,
        commission=(
            str(order_event["n"]) if order_event.get("n") is not None else None
        ),
        commission_asset=(
            str(order_event["N"]) if order_event.get("N") is not None else None
        ),
        is_maker=order_event.get("m") if isinstance(order_event.get("m"), bool) else None,
        event_time=event_time,
        transaction_time=transaction_time,
        raw={
            "e": "ORDER_TRADE_UPDATE",
            "E": event_time,
            "T": transaction_time,
            "o": order_event,
        },
    )


# ─────────────────────── 1. _is_real_trade_fill ───────────────────────


class TestIsRealTradeFill:
    """ExecutionLogService._is_real_trade_fill() 순수 함수 테스트."""

    def test_normal_fill(self):
        event = _make_fill_event()
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is True

    def test_non_trade_execution_type(self):
        event = _make_fill_event(execution_type="NEW")
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_non_trade_execution_type(self):
        event = _make_fill_event(execution_type=BinanceUsdsFuturesExecutionType.NEW)
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_zero_fill_qty_string(self):
        event = _make_fill_event(fill_qty="0")
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_zero_fill_qty_float_string(self):
        event = _make_fill_event(fill_qty="0.00000000")
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_empty_fill_qty(self):
        event = _make_fill_event(fill_qty="")
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_none_fill_qty(self):
        event = {"x": "trade", "l": None, "t": 1}
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_zero_trade_id(self):
        event = _make_fill_event(trade_id=0)
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_none_trade_id(self):
        event = {"x": BinanceUsdsFuturesExecutionType.TRADE, "l": "0.1", "t": None}
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False

    def test_missing_execution_type(self):
        event = {"l": "0.1", "t": 1}
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is True

    def test_calculated_event(self):
        """CALCULATED 이벤트 (ADL 등)는 TRADE가 아니므로 False."""
        event = _make_fill_event(execution_type="CALCULATED")
        assert ExecutionLogService._is_real_trade_fill(_make_normalized_event(event)) is False


# ─────────────────────── 2. _enum_value ───────────────────────


class TestEnumValue:
    def test_enum_returns_value(self):
        class Color(Enum):
            RED = "red"

        assert ExecutionLogService._enum_value(Color.RED) == "red"

    def test_plain_string_returns_as_is(self):
        assert ExecutionLogService._enum_value("hello") == "hello"

    def test_int_returns_as_is(self):
        assert ExecutionLogService._enum_value(42) == 42

    def test_none_returns_none(self):
        assert ExecutionLogService._enum_value(None) is None


# ─────────────────────── 3. _build_execution_report ───────────────────────


class TestBuildExecutionReport:

    def test_basic_fields(self):
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        report = ExecutionLogService._build_execution_report(
            order=order,
            event_data=normalized,
        )

        assert report["exchange"] == "BINANCE"
        assert report["market_type"] == "PERP"
        assert report["symbol"] == "BTCUSDT"
        assert report["side"] == "BUY"
        assert report["source"] == "MANUAL"
        assert report["order_id"] == order.order_id
        assert report["exchange_order_id"] == "12345"
        assert report["exchange_trade_id"] == "99999"
        assert report["execution_id"] == "99999"
        assert report["fill_price"] == "60000"
        assert report["fill_quantity"] == "0.1"
        assert report["commission"] == "0.001"
        assert report["commission_asset"] == "USDT"
        assert report["is_maker"] is False
        assert report["exchange_ts"] == 1_700_000_000_001  # transaction_time
        assert isinstance(report["local_ts"], int)

    def test_uses_event_time_when_no_transaction_time(self):
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(
            event,
            event_time=1_700_000_000_000,
            transaction_time=None,
        )

        report = ExecutionLogService._build_execution_report(
            order=order,
            event_data=normalized,
        )

        assert report["exchange_ts"] == 1_700_000_000_000

    def test_maker_flag(self):
        order = _make_order()
        event = _make_fill_event()
        event["m"] = True  # maker
        normalized = _make_normalized_event(event)

        report = ExecutionLogService._build_execution_report(
            order=order,
            event_data=normalized,
        )

        assert report["is_maker"] is True

    def test_strategy_metadata(self):
        order = _make_order()
        order.signal_id = "SIG-001"
        order.strategy_name = "grid_v2"
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        report = ExecutionLogService._build_execution_report(
            order=order,
            event_data=normalized,
        )

        assert report["signal_id"] == "SIG-001"
        assert report["strategy_name"] == "grid_v2"


# ─────────────────────── 4. ExecutionLogService.save_if_needed ───────────────────────


class TestExecutionLogServiceSaveIfNeeded:
    """mock 기반 ExecutionLogService.save_if_needed 테스트."""

    @pytest.mark.asyncio
    async def test_non_trade_event_skips(self):
        order = _make_order()
        event = _make_fill_event(execution_type="NEW")
        normalized = _make_normalized_event(event)

        svc = ExecutionLogService(exec_repo=None, redis=None)

        await svc.save_if_needed(
            order=order,
            event_data=normalized,
        )

    @pytest.mark.asyncio
    async def test_real_fill_saves_to_questdb(self):
        """TRADE 이벤트 → QuestDB 저장 호출."""
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_exec_repo = MagicMock()
        mock_exec_repo.save = AsyncMock()

        mock_redis = MagicMock()
        mock_redis.client = MagicMock()
        mock_redis.client.set = AsyncMock(
            return_value=True
        )  # 첫 번째 호출 = dedup 성공

        svc = ExecutionLogService(exec_repo=mock_exec_repo, redis=mock_redis)

        await svc.save_if_needed(
            order=order,
            event_data=normalized,
        )

        mock_exec_repo.save.assert_awaited_once()
        saved_report = mock_exec_repo.save.call_args[0][0]
        assert saved_report["order_id"] == order.order_id
        assert saved_report["exchange_trade_id"] == "99999"

    @pytest.mark.asyncio
    async def test_duplicate_fill_skipped_by_dedup(self):
        """같은 trade_id로 두 번째 호출 → dedup에 의해 skip."""
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_exec_repo = MagicMock()
        mock_exec_repo.save = AsyncMock()

        mock_redis = MagicMock()
        mock_redis.client = MagicMock()
        mock_redis.client.set = AsyncMock(return_value=False)  # 이미 존재 = dedup 실패

        svc = ExecutionLogService(exec_repo=mock_exec_repo, redis=mock_redis)

        await svc.save_if_needed(
            order=order,
            event_data=normalized,
        )

        mock_exec_repo.save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_redis_failure_still_saves(self):
        """Redis 장애 시에도 fill log는 저장 (유실 방지 > 중복 허용)."""
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_exec_repo = MagicMock()
        mock_exec_repo.save = AsyncMock()

        mock_redis = MagicMock()
        mock_redis.client = MagicMock()
        mock_redis.client.set = AsyncMock(side_effect=ConnectionError("Redis down"))

        svc = ExecutionLogService(exec_repo=mock_exec_repo, redis=mock_redis)

        await svc.save_if_needed(
            order=order,
            event_data=normalized,
        )

        mock_exec_repo.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_questdb_failure_does_not_crash(self):
        """QuestDB 저장 실패 시에도 예외가 전파되지 않아야 함."""
        order = _make_order()
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_exec_repo = MagicMock()
        mock_exec_repo.save = AsyncMock(side_effect=RuntimeError("QuestDB timeout"))

        mock_redis = MagicMock()
        mock_redis.client = MagicMock()
        mock_redis.client.set = AsyncMock(return_value=True)
        mock_redis.client.delete = AsyncMock()

        svc = ExecutionLogService(exec_repo=mock_exec_repo, redis=mock_redis)

        await svc.save_if_needed(
            order=order,
            event_data=normalized,
        )

        mock_redis.client.delete.assert_awaited_once()


# ─────────────────────── 5. on_trade_update ───────────────────────


class TestOnTradeUpdate:

    @pytest.mark.asyncio
    async def test_missing_client_order_id_skips(self):
        """client_order_id 없는 이벤트 → skip."""
        from api_server.handlers.user_data_stream_handler import on_trade_update

        event = _make_fill_event()
        normalized = _make_normalized_event(event).model_copy(
            update={"client_order_id": ""}
        )

        # state.gateway를 mock하지 않아도 호출되지 않아야 함
        await on_trade_update(normalized)

    @pytest.mark.asyncio
    async def test_gateway_returns_none_logs_error(self):
        """gateway.apply_order_update_event가 None 반환 → 로깅만."""
        from api_server.handlers.user_data_stream_handler import on_trade_update
        from api_server.runtime import state

        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_gateway = MagicMock()
        mock_gateway.apply_order_update_event = AsyncMock(return_value=None)

        original_gw = state.gateway
        try:
            state.gateway = mock_gateway
            await on_trade_update(normalized)

            mock_gateway.apply_order_update_event.assert_awaited_once()
        finally:
            state.gateway = original_gw

    @pytest.mark.asyncio
    async def test_successful_trade_update_calls_save(self):
        """정상 체결 이벤트 → gateway 호출 + fill log 저장."""
        from api_server.handlers.user_data_stream_handler import on_trade_update
        from api_server.runtime import state

        order = _make_order(status=OrderStatus.FILLED)
        event = _make_fill_event()
        normalized = _make_normalized_event(event)

        mock_gateway = MagicMock()
        mock_gateway.apply_order_update_event = AsyncMock(return_value=order)

        mock_exec_repo = MagicMock()
        mock_exec_repo.save = AsyncMock()

        mock_redis = MagicMock()
        mock_redis.client = MagicMock()
        mock_redis.client.set = AsyncMock(return_value=True)

        originals = (state.gateway, state.execution_log_service)
        try:
            state.gateway = mock_gateway
            state.execution_log_service = ExecutionLogService(
                exec_repo=mock_exec_repo,
                redis=mock_redis,
            )

            await on_trade_update(normalized)

            mock_gateway.apply_order_update_event.assert_awaited_once()
            mock_exec_repo.save.assert_awaited_once()
        finally:
            state.gateway, state.execution_log_service = originals


# ─────────────────────── 6. on_position_update ───────────────────────


class TestOnPositionUpdate:

    @pytest.mark.asyncio
    async def test_position_update_does_not_crash_without_service(self):
        """position_state_service 미초기화여도 예외 없이 반환."""
        from api_server.handlers.user_data_stream_handler import on_position_update

        snapshot = NormalizedPositionSnapshot(
            exchange=Exchange.BINANCE,
            market_type=MarketType.PERP,
            symbol="BTCUSDT",
            position_side=PositionSide.BOTH,
            status=PositionStatus.OPEN,
            position_amt="0.1",
            entry_price="60000",
            update_reason="ORDER",
            event_time=1_700_000_000_000,
            transaction_time=1_700_000_000_001,
            raw={},
        )

        await on_position_update([snapshot])

    @pytest.mark.asyncio
    async def test_position_update_with_empty_data(self):
        """빈 snapshot list로도 크래시하지 않아야 함."""
        from api_server.handlers.user_data_stream_handler import on_position_update

        await on_position_update([])


# ─────────────────────── 7. _user_data_ws_base_url ───────────────────────


class TestUserDataWsBaseUrl:

    def test_appends_private_if_missing(self):
        from api_server.helper import get_user_data_ws_base_url
        from execution_gateway.config import settings as gw_settings

        original = gw_settings.ws_base_url

        # testnet URL은 보통 /private가 없음
        with patch.object(
            type(gw_settings),
            "ws_base_url",
            new_callable=lambda: property(
                lambda self: "wss://stream.binancefuture.com"
            ),
        ):
            url = get_user_data_ws_base_url()
            assert url.endswith("/private")
            assert url == "wss://stream.binancefuture.com/private"

    def test_preserves_existing_private(self):
        from api_server.helper import get_user_data_ws_base_url
        from execution_gateway.config import settings as gw_settings

        with patch.object(
            type(gw_settings),
            "ws_base_url",
            new_callable=lambda: property(
                lambda self: "wss://fstream.binance.com/private"
            ),
        ):
            url = get_user_data_ws_base_url()
            assert url == "wss://fstream.binance.com/private"
            # /private/private가 아닌지 확인
            assert not url.endswith("/private/private")
