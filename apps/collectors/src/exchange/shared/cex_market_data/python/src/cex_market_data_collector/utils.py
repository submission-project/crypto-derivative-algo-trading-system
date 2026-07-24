from __future__ import annotations

from typing import Any, Mapping


def to_str(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, Mapping):
            return first
    if isinstance(value, Mapping):
        return value
    return {}


def first_float(mapping: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = to_float(mapping.get(key))
        if value is not None:
            return value
    return None


def first_str(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = to_str(mapping.get(key))
        if value is not None:
            return value
    return None


def notional_str(amount: str | None, price: str | None, multiplier: float = 1.0) -> str | None:
    amount_float = to_float(amount)
    price_float = to_float(price)
    if amount_float is None or price_float is None:
        return None
    return str(amount_float * price_float * multiplier)
