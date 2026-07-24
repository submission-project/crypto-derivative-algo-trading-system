from __future__ import annotations

from cex_market_data_collector.operational_models import WebSocketSpec


def build_ws_specs() -> tuple[WebSocketSpec, ...]:
    # LBank public futures field names vary by product. Keep OI polling enabled
    # and validate trade/depth WS before production use.
    return ()
