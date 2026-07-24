from __future__ import annotations

import sys
from pathlib import Path


def ensure_exchange_package_paths() -> None:
    exchange_root = Path(__file__).resolve().parents[5]
    for src in exchange_root.glob("*/perp/python/src"):
        src_text = str(src)
        if src_text not in sys.path:
            sys.path.insert(0, src_text)
