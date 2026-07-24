from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExchangeCapabilities:
    """
    Feature flags and limits exposed by an exchange execution client.

    The gateway should use these values instead of hard-coded Binance limits
    when splitting batches or enabling optional routes.
    """

    supports_batch_order: bool = False
    max_batch_order_size: int = 1

    supports_batch_cancel: bool = False
    max_batch_cancel_size: int = 1

    supports_cancel_all: bool = False
    supports_ws_trade: bool = False

    supports_conditional_order: bool = False
    supports_conditional_batch: bool = False
    supports_conditional_reconciliation: bool = False

    supports_bulk_order_lookup: bool = False

    supports_hedge_mode: bool = False
    supports_reduce_only: bool = False
    supports_close_position: bool = False

    supports_leverage_change: bool = False
    supports_position_snapshot: bool = False

    bulk_order_lookup_threshold: int | None = None
