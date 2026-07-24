from __future__ import annotations

from schemas.order import ConditionalStatus, CONDITIONAL_TERMINAL_STATUSES


class InvalidConditionalTransitionError(RuntimeError):
    def __init__(
        self,
        current: ConditionalStatus | None,
        target: ConditionalStatus,
    ) -> None:
        self.current = current
        self.target = target

        current_value = current.value if current else "None"

        super().__init__(
            f"invalid conditional transition: "
            f"{current_value} -> {target.value}"
        )


class ConditionalOrderStateMachine:
    """
    조건부 주문 상태 전이 검증.

    이 state machine은 Order.status가 아니라
    Order.conditional_status만 담당한다.

    기본 흐름:
      None -> NEW / ACTIVE / UNKNOWN / REJECTED
      NEW -> ACTIVE / TRIGGERED / CANCELLED / EXPIRED / REJECTED / UNKNOWN
      ACTIVE -> TRIGGERED / CANCELLED / EXPIRED / REJECTED / UNKNOWN
      TRIGGERED -> FINISHED / EXPIRED / REJECTED / UNKNOWN
      UNKNOWN -> NEW / ACTIVE / TRIGGERED / FINISHED / CANCELLED / EXPIRED / REJECTED
      FINISHED/CANCELLED/EXPIRED/REJECTED -> terminal
    """

    TERMINAL_STATUSES = CONDITIONAL_TERMINAL_STATUSES

    ALLOWED_TRANSITIONS: dict[
        ConditionalStatus | None,
        set[ConditionalStatus],
    ] = {
        # 이벤트 유실/순서 역전/reconciliation 복구를 위해 None은 넓게 허용.
        None: {
            ConditionalStatus.NEW,
            ConditionalStatus.ACTIVE,
            ConditionalStatus.TRIGGERED,
            ConditionalStatus.FINISHED,
            ConditionalStatus.CANCELLED,
            ConditionalStatus.EXPIRED,
            ConditionalStatus.REJECTED,
            ConditionalStatus.UNKNOWN,
        },
        ConditionalStatus.NEW: {
            ConditionalStatus.ACTIVE,
            ConditionalStatus.TRIGGERED,
            ConditionalStatus.CANCELLED,
            ConditionalStatus.EXPIRED,
            ConditionalStatus.REJECTED,
            ConditionalStatus.UNKNOWN,
        },
        ConditionalStatus.ACTIVE: {
            ConditionalStatus.TRIGGERED,
            ConditionalStatus.CANCELLED,
            ConditionalStatus.EXPIRED,
            ConditionalStatus.REJECTED,
            ConditionalStatus.UNKNOWN,
        },
        ConditionalStatus.TRIGGERED: {
            ConditionalStatus.FINISHED,
            ConditionalStatus.EXPIRED,
            ConditionalStatus.REJECTED,
            ConditionalStatus.UNKNOWN,
        },
        ConditionalStatus.UNKNOWN: {
            ConditionalStatus.NEW,
            ConditionalStatus.ACTIVE,
            ConditionalStatus.TRIGGERED,
            ConditionalStatus.FINISHED,
            ConditionalStatus.CANCELLED,
            ConditionalStatus.EXPIRED,
            ConditionalStatus.REJECTED,
        },
        **{s: set() for s in TERMINAL_STATUSES},
    }

    def __init__(self, current: ConditionalStatus | None) -> None:
        self.current = current

    def is_terminal(self) -> bool:
        return self.current in self.TERMINAL_STATUSES

    def can_transition(self, target: ConditionalStatus) -> bool:
        if self.current == target:
            return True

        allowed = self.ALLOWED_TRANSITIONS.get(self.current, set())
        return target in allowed

    def assert_can_transition(self, target: ConditionalStatus) -> None:
        if not self.can_transition(target):
            raise InvalidConditionalTransitionError(self.current, target)