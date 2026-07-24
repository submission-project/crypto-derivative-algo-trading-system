from __future__ import annotations

from importlib import import_module

from .adapters import build_adapter
from .module_loader import ensure_exchange_package_paths
from .operational_helpers import topic
from .operational_models import MarketEvent, OperationalSpecs, RestPollSpec, WebSocketSpec


DEFAULT_OPERATIONAL_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "okx",
    "bitget",
    "gate",
    "mexc",
    "kraken",
    "htx",
    "lbank",
    "bitfinex",
    "bingx",
    "kucoin",
)


def build_operational_specs(
    exchanges: tuple[str, ...] = DEFAULT_OPERATIONAL_EXCHANGES,
    *,
    oi_interval_s: float = 60.0,
    rest_oi_fallback: bool = False,
) -> list[OperationalSpecs]:
    return [
        _build_one(exchange, oi_interval_s=oi_interval_s, rest_oi_fallback=rest_oi_fallback)
        for exchange in exchanges
    ]


def _build_one(exchange: str, *, oi_interval_s: float, rest_oi_fallback: bool) -> OperationalSpecs:
    key = exchange.lower()
    websocket_specs = _load_ws_specs(key)
    rest_poll_specs = ()
    if rest_oi_fallback or not _has_websocket_open_interest(websocket_specs):
        rest_poll_specs = (
            RestPollSpec(
                exchange=key,
                data_type="open_interest",
                topic=topic(key, "open_interest"),
                interval_s=oi_interval_s,
                poller=_open_interest_poller(key),
            ),
        )
    return OperationalSpecs(
        exchange=key,
        websocket_specs=websocket_specs,
        rest_poll_specs=rest_poll_specs,
    )


def _open_interest_poller(exchange: str):
    async def poll(client) -> list[MarketEvent]:
        adapter = build_adapter(exchange)
        snapshot = await adapter.fetch_open_interest(client)
        record = snapshot.to_record()
        record["data_type"] = "open_interest"
        return [record]

    return poll


def _empty_ws() -> tuple[WebSocketSpec, ...]:
    return ()


def _load_ws_specs(exchange: str) -> tuple[WebSocketSpec, ...]:
    module_name = f"{exchange}_perp_collector.operational"
    ensure_exchange_package_paths()
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:
        return _empty_ws()
    return module.build_ws_specs()


def _has_websocket_open_interest(websocket_specs: tuple[WebSocketSpec, ...]) -> bool:
    return any("open_interest" in spec.data_type or spec.data_type == "trade_orderbook_oi" for spec in websocket_specs)
