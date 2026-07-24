from __future__ import annotations


def fixed_fractional_notional(equity: float, fraction: float) -> float:
    if equity < 0:
        raise ValueError("equity must be non-negative")
    if not 0 <= fraction <= 1:
        raise ValueError("fraction must be between 0 and 1")
    return equity * fraction


def capped_kelly_fraction(
    *,
    win_rate: float,
    avg_win: float,
    avg_loss: float,
    cap: float = 0.2,
) -> float:
    if not 0 <= win_rate <= 1:
        raise ValueError("win_rate must be between 0 and 1")
    if cap < 0:
        raise ValueError("cap must be non-negative")
    if avg_win <= 0 or avg_loss <= 0:
        return 0.0

    payoff_ratio = avg_win / avg_loss
    raw = win_rate - (1.0 - win_rate) / payoff_ratio
    return max(0.0, min(raw, cap))


def volatility_target_notional(
    *,
    equity: float,
    target_volatility: float,
    realized_volatility: float,
    max_fraction: float = 1.0,
) -> float:
    if equity < 0:
        raise ValueError("equity must be non-negative")
    if target_volatility < 0 or realized_volatility < 0:
        raise ValueError("volatility values must be non-negative")
    if not 0 <= max_fraction <= 1:
        raise ValueError("max_fraction must be between 0 and 1")
    if realized_volatility == 0:
        return equity * max_fraction

    fraction = min(target_volatility / realized_volatility, max_fraction)
    return equity * max(0.0, fraction)
