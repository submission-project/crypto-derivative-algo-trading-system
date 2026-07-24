from risk.sizing import capped_kelly_fraction, fixed_fractional_notional, volatility_target_notional


def test_fixed_fractional_notional() -> None:
    assert fixed_fractional_notional(10_000.0, 0.1) == 1_000.0


def test_capped_kelly_fraction_caps_aggressive_estimate() -> None:
    fraction = capped_kelly_fraction(win_rate=0.7, avg_win=2.0, avg_loss=1.0, cap=0.2)
    assert fraction == 0.2


def test_capped_kelly_fraction_returns_zero_for_negative_edge() -> None:
    fraction = capped_kelly_fraction(win_rate=0.4, avg_win=1.0, avg_loss=2.0, cap=0.2)
    assert fraction == 0.0


def test_volatility_target_notional_reduces_size_when_realized_vol_is_high() -> None:
    notional = volatility_target_notional(
        equity=10_000.0,
        target_volatility=0.10,
        realized_volatility=0.20,
        max_fraction=1.0,
    )
    assert notional == 5_000.0
