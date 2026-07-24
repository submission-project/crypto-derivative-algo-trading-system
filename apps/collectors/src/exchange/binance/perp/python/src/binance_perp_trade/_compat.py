from __future__ import annotations

import warnings


def warn_deprecated() -> None:
    warnings.warn(
        "binance_perp_trade is deprecated; use binance_perp_collector instead.",
        DeprecationWarning,
        stacklevel=3,
    )
