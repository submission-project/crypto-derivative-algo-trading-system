from __future__ import annotations

from cex_market_data_collector.operational_models import WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    # KuCoin futures public WebSocket requires a bullet token from REST before
    # connecting. Keep OI in REST polling and add tokenized WS after validation.
    return ()
