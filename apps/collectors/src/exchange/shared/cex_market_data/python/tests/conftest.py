from __future__ import annotations

import sys
from pathlib import Path


SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

REPO_ROOT = Path(__file__).resolve().parents[8]
for src in REPO_ROOT.glob("apps/collectors/src/exchange/*/perp/python/src"):
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
