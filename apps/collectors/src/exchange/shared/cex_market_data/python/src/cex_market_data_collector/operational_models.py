from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from .adapter_base import JsonGetter


MarketEvent = dict[str, Any]
WsNormalizer = Callable[[Any], list[MarketEvent]]
RestPoller = Callable[[JsonGetter], Awaitable[list[MarketEvent]]]


@dataclass(frozen=True, slots=True)
class WebSocketSpec:
    exchange: str
    data_type: str
    url: str
    topic: str
    subscribe_messages: tuple[Mapping[str, Any], ...] = ()
    normalizer: WsNormalizer | None = None
    ping_message: Mapping[str, Any] | None = None
    gzip_binary: bool = False
    trade_repair: Any | None = None


@dataclass(frozen=True, slots=True)
class RestPollSpec:
    exchange: str
    data_type: str
    topic: str
    interval_s: float
    poller: RestPoller


@dataclass(frozen=True, slots=True)
class OperationalSpecs:
    exchange: str
    websocket_specs: tuple[WebSocketSpec, ...]
    rest_poll_specs: tuple[RestPollSpec, ...]
