from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def normalize_symbol(symbol: Any) -> str:
    text = str(symbol or "").upper()
    return text.replace("-", "").replace("_", "").replace("SWAP", "").replace("PERP", "")
