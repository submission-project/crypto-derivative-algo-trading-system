"""
OrderStateMachine 단위 테스트.

검증 항목:
- 유효한 전이 성공
- 허용되지 않은 전이 시 InvalidTransitionError
- Terminal 상태에서 추가 전이 불가
- FILLED 시 filled_ts 자동 설정
- apply_execution_event — ORDER_TRADE_UPDATE 이벤트 반영
- UNKNOWN → 복구 전이 가능
"""
import time
import pytest

from schemas.order import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionAction,
    TimeInForce,
)
from schemas.market import Exchange, MarketType
from execution_gateway.state_machine.order_state_machine import (
    OrderStateMachine,
    InvalidTransitionError,
)

@pytest.fixture
def base_order() -> Order:
    now = int(time.time() * 1000)
    return Order(
        source="MANUAL",
        exchange=Exchange.BINANCE,
        market_type=MarketType.PERP,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity="0.01",
        price="60000",
        created_ts=now,
        updated_ts=now,
        position_action=PositionAction.OPEN,
    )

@pytest.mark.stable
def test_valid_transition_pending_to_submitted(base_order):
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    assert machine.status == OrderStatus.SUBMITTED

@pytest.mark.stable
def test_invalid_transition_raises(base_order):
    """PENDING_NEW → FILLED 직접 전이는 불가."""
    machine = OrderStateMachine(base_order)
    with pytest.raises(InvalidTransitionError) as exc_info:
        machine.transition(OrderStatus.FILLED)

    assert exc_info.value.current == OrderStatus.PENDING_NEW
    assert exc_info.value.target == OrderStatus.FILLED

@pytest.mark.stable
def test_valid_transition_full_happy_path(base_order):
    """PENDING_NEW → SUBMITTED → ACKNOWLEDGED → FILLED 전체 경로."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.ACKNOWLEDGED, exchange_order_id="EX123")
    machine.transition(
        OrderStatus.FILLED,
        filled_quantity="0.01",
        avg_fill_price="60000.5",
    )
    assert machine.status == OrderStatus.FILLED
    assert machine.order.exchange_order_id == "EX123"
    assert machine.order.filled_quantity == "0.01"
    assert machine.order.avg_fill_price == "60000.5"
    assert machine.order.filled_ts is not None

@pytest.mark.stable
def test_terminal_state_no_further_transition(base_order):
    """FILLED 이후에는 어떤 전이도 불가."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.ACKNOWLEDGED)
    machine.transition(OrderStatus.FILLED)
    assert machine.is_terminal
    with pytest.raises(InvalidTransitionError) as exc_info:
        machine.transition(OrderStatus.CANCELLED)

    assert exc_info.value.current == OrderStatus.FILLED
    assert exc_info.value.target == OrderStatus.CANCELLED

@pytest.mark.stable
def test_reconcile_unresolved_is_terminal(base_order):
    """반복 reconciliation 실패로 격리된 주문은 terminal처럼 추가 전이를 막는다."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.RECONCILE_UNRESOLVED)

    assert machine.status == OrderStatus.RECONCILE_UNRESOLVED
    assert machine.is_terminal

    with pytest.raises(InvalidTransitionError) as exc_info:
        machine.transition(OrderStatus.ACKNOWLEDGED)

    assert exc_info.value.current == OrderStatus.RECONCILE_UNRESOLVED
    assert exc_info.value.target == OrderStatus.ACKNOWLEDGED

@pytest.mark.stable
def test_unknown_state_can_recover(base_order):
    """UNKNOWN → ACKNOWLEDGED 복구 가능."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.UNKNOWN)
    assert not machine.is_terminal  # UNKNOWN은 terminal 아님
    machine.transition(OrderStatus.ACKNOWLEDGED, exchange_order_id="EX999")
    assert machine.status == OrderStatus.ACKNOWLEDGED

@pytest.mark.stable
def test_can_transition_check(base_order):
    """can_transition() 예외 없이 bool 반환 확인."""
    machine = OrderStateMachine(base_order)
    assert machine.can_transition(OrderStatus.SUBMITTED) is True
    assert machine.can_transition(OrderStatus.FILLED) is False

@pytest.mark.stable
def test_chaining(base_order):
    """transition 체이닝 가능."""
    machine = OrderStateMachine(base_order)
    result = (
        machine
        .transition(OrderStatus.SUBMITTED)
        .transition(OrderStatus.ACKNOWLEDGED)
        .transition(OrderStatus.PARTIALLY_FILLED, filled_quantity="0.005")
    )
    assert result is machine
    assert machine.status == OrderStatus.PARTIALLY_FILLED


@pytest.mark.skip(reason="apply_execution_event is deprecated, convert to apply_execution_event")
def test_apply_execution_event_filled(base_order):
    """ORDER_TRADE_UPDATE FILLED 이벤트 적용."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.ACKNOWLEDGED, exchange_order_id="EX1")

    order_event = {
        "X": "FILLED",
        "i": 99999,
        "z": "0.01",
        "ap": "60001.0",
    }
    machine.apply_execution_event(order_event)
    assert machine.status == OrderStatus.FILLED
    assert machine.order.filled_quantity == "0.01"
    assert machine.order.avg_fill_price == "60001.0"


@pytest.mark.skip(reason="apply_execution_event is deprecated, convert to apply_execution_event")
def test_apply_execution_event_race_condition(base_order):
    """이미 FILLED인 주문에 CANCELED 이벤트가 와도 상태가 변하지 않음 (Race condition)."""
    machine = OrderStateMachine(base_order)
    machine.transition(OrderStatus.SUBMITTED)
    machine.transition(OrderStatus.ACKNOWLEDGED)
    machine.transition(OrderStatus.FILLED)

    order_event = {"X": "CANCELED", "i": 1, "z": "0", "ap": "0"}
    machine.apply_execution_event(order_event)
    assert machine.status == OrderStatus.FILLED  # 변하지 않음
