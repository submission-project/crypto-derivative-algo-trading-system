from pytest import approx

from risk.constraints import PositionLimit, apply_position_limit
from risk.drawdown import calculate_drawdowns, max_drawdown, should_stop_trading


def test_calculate_drawdowns() -> None:
    assert calculate_drawdowns([100.0, 110.0, 99.0]) == approx([0.0, 0.0, -0.1])


def test_max_drawdown() -> None:
    assert max_drawdown([100.0, 120.0, 90.0, 130.0]) == -0.25


def test_should_stop_trading() -> None:
    assert should_stop_trading([100.0, 120.0, 90.0], 0.2)


def test_apply_position_limit() -> None:
    assert apply_position_limit(15_000.0, PositionLimit(max_abs_notional=10_000.0)) == 10_000.0
