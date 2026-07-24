from ._compat import warn_deprecated

warn_deprecated()

from binance_perp_collector.config import Settings, settings

__all__ = ["Settings", "settings"]
