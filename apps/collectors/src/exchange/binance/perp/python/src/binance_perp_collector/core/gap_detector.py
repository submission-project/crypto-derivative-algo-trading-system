from collections import defaultdict
from typing import Dict, Optional, Tuple

class GapDetector:
    def __init__(self, max_gaps_before_fallback: int = 5):
        self._last_ids: Dict[str, int] = {}
        self._gap_counts: Dict[str, int] = defaultdict(int)
        self._max_gaps = max_gaps_before_fallback

    def check(self, symbol: str, current_id: int) -> tuple[int | None, int | None]:
        last_id = self._last_ids.get(symbol)

        if last_id is None:
            self._last_ids[symbol] = current_id
            return None, None

        # duplicate / old / out-of-order
        if current_id <= last_id:
            return None, None

        missing_from = None
        missing_to = None

        if current_id > last_id + 1:
            missing_from = last_id + 1
            missing_to = current_id - 1
            self._gap_counts[symbol] += 1

        self._last_ids[symbol] = current_id
        return missing_from, missing_to

    def should_fallback(self, symbol: str) -> bool:
        """Fallback 전환이 필요한 상태인지 확인"""
        return self._gap_counts[symbol] >= self._max_gaps

    def reset(self, symbol: str | None = None):
        if symbol is None:
            self._last_ids.clear()
            self._gap_counts.clear()
            return

        self._last_ids.pop(symbol, None)
        self._gap_counts.pop(symbol, None)
