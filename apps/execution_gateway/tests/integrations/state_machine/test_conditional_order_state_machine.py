from __future__ import annotations

import pytest

from execution_gateway.state_machine.conditional_order_state_machine import (
    ConditionalOrderStateMachine,
    InvalidConditionalTransitionError,
)
from schemas.order import ConditionalStatus


@pytest.mark.stable
def test_none_can_transition_to_new() -> None:
    machine = ConditionalOrderStateMachine(None)

    assert machine.can_transition(ConditionalStatus.NEW) is True

@pytest.mark.stable
def test_none_can_transition_to_triggered_for_recovery_case() -> None:
    """
    이벤트 순서 역전/reconciliation 복구 상황에서는
    local conditional_status=None인데 거래소 상태가 이미 TRIGGERED일 수 있다.
    """
    machine = ConditionalOrderStateMachine(None)

    assert machine.can_transition(ConditionalStatus.TRIGGERED) is True

@pytest.mark.stable
def test_new_can_transition_to_active() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.NEW)

    assert machine.can_transition(ConditionalStatus.ACTIVE) is True

@pytest.mark.stable
def test_new_can_transition_to_triggered() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.NEW)

    assert machine.can_transition(ConditionalStatus.TRIGGERED) is True

@pytest.mark.stable
def test_active_can_transition_to_triggered() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.ACTIVE)

    assert machine.can_transition(ConditionalStatus.TRIGGERED) is True

@pytest.mark.stable
def test_triggered_can_transition_to_finished() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.TRIGGERED)

    assert machine.can_transition(ConditionalStatus.FINISHED) is True

@pytest.mark.stable
def test_unknown_can_transition_to_finished() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.UNKNOWN)

    assert machine.can_transition(ConditionalStatus.FINISHED) is True

@pytest.mark.stable
def test_terminal_status_allows_same_status_reobservation() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.FINISHED)

    assert machine.can_transition(ConditionalStatus.FINISHED) is True

@pytest.mark.stable
def test_terminal_status_blocks_different_status() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.FINISHED)

    assert machine.can_transition(ConditionalStatus.TRIGGERED) is False

@pytest.mark.stable
def test_assert_can_transition_raises_on_invalid_transition() -> None:
    machine = ConditionalOrderStateMachine(ConditionalStatus.FINISHED)

    with pytest.raises(InvalidConditionalTransitionError) as exc:
        machine.assert_can_transition(ConditionalStatus.TRIGGERED)

    assert exc.value.current == ConditionalStatus.FINISHED
    assert exc.value.target == ConditionalStatus.TRIGGERED