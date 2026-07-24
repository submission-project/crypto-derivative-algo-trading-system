from __future__ import annotations

from importlib import import_module

from .adapter_base import ExchangeAdapter
from .module_loader import ensure_exchange_package_paths

# 거래소 adapter 등록 관리
DEFAULT_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "okx",
    "bitget",
    "gate",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "kraken",
    "bitfinex",
    "lbank",
)


def supported_exchanges() -> tuple[str, ...]:
    return DEFAULT_EXCHANGES


def build_adapter(exchange: str) -> ExchangeAdapter:
    key = exchange.lower()
    module_name = f"{key}_perp_collector.rest"
    ensure_exchange_package_paths()
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ValueError(f"unsupported exchange: {exchange}") from exc
    return module.build_rest_adapter()
