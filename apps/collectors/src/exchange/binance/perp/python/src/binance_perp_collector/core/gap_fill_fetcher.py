"""
GapFillFetcher — RepairJob 의 [from_id, to_id] 범위를 두 단계로 채우는 fetcher.

배경:
  Binance Futures 에는 trade 를 가져올 수 있는 두 endpoint 가 있고, 각각 trade-off
  가 있다.

  1) /fapi/v1/trades  (memory, "recent")
       장점: 색인 lag 없음. 빠르게 최신 데이터 조회 가능.
       제약: 가장 최근 max 1000건만. 그보다 오래된 ID는 못 본다.

  2) /fapi/v1/historicalTrades  (DB, "historical")
       장점: 한 달 이전까지 조회 가능.
       제약: DB 색인 lag 으로 가장 최신 ID는 잠시 안 잡힐 수 있음 (수 초~수십 초).

  ⚠️  실측 결과 두 endpoint 모두 응답 ID 시퀀스에 gap 이 발생한다 — 즉 일부
  trade 타입(ADL/insurance fund 등)이 양쪽 모두에서 제외되는 것으로 보인다.
  따라서 "recent endpoint 면 ADL 포함" 이라는 가정은 하지 않는다.
  결과적으로 어느 stage 든 restored < expected 가 정상 케이스로 발생할 수 있고,
  이는 호출자가 "ADL/insurance 제외분" 으로 해석해야 한다.

  본 fetcher 는 두 endpoint 를 결합하여 trade-off 를 보완한다:
    - Stage 1 (recent): 작은 gap (대부분의 케이스) 은 1번 호출로 lag 없이 처리.
    - Stage 2 (historical + retry/backoff): 큰 gap 만 사용. lag 흡수 위해 재시도.

설계 결정:
  * [from_id, to_id] 범위 필터링은 fetcher 내부에서 수행하여 호출자를 단순화.
  * 정렬은 ID 오름차순 보장 (호출자 가독성).
  * Stage 2 retry 도중 RestRateLimitError 는 backoff 더 기다리고 재시도.
    RestAuthError / RestApiError 는 즉시 raise (retry 무의미).
  * 모든 retry 후에도 to_id 까지 못 도달하면 PARTIAL 로 표시 (호출자가 경고 로깅).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

from common.logging import setup_logger

from ..trade_rest.trade_client import (
    RestApiError,
    RestRateLimitError,
    RestTradeClient,
)

logger = setup_logger(__name__)


class GapFillSource(str, Enum):
    """결과 trade 들이 어느 endpoint 에서 왔는지."""

    RECENT_MEMORY = "recent_memory"  # Stage 1 — fast path (lag 없음)
    HISTORICAL_DB = "historical_db"  # Stage 2 — to_id 까지 cover 됨
    PARTIAL = "partial"  # Stage 2 — 모든 retry 후에도 to_id 미도달


@dataclass(frozen=True, slots=True)
class GapFillResult:
    """
    fetch_range 결과.

    trades:
      [from_id, to_id] 범위 내 trade 만 ID 오름차순으로 정렬된 list.
      어느 source 든 ADL/insurance 등의 trade 타입이 빠져 있을 수 있으므로
      len(trades) <= (to_id - from_id + 1) 일 수 있음 (정상 케이스).

    source:
      어느 경로로 가져왔는지. 호출자가 logging 메시지/메트릭에 사용.

    attempts:
      Stage 2 에서 historical endpoint 를 호출한 횟수.
      - 0 : Stage 1 만으로 끝남 (recent_memory).
      - >=1 : Stage 2 가 동작했고, 그 중 마지막 호출 기준 attempts 번째에서 cover 됨
              (또는 PARTIAL 로 끝남).
    """

    trades: List[Dict[str, Any]]
    source: GapFillSource
    attempts: int


class GapFillFetcher:
    """
    [from_id, to_id] 범위 trade 를 두 단계로 fetch 하는 high-level helper.

    사용 예::

        fetcher = GapFillFetcher(rest_client)
        result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=120)
        for raw_trade in result.trades:
            ...  # canonical 변환 + Kafka 발행
        if result.source == GapFillSource.PARTIAL:
            logger.warning("부분 복구")
    """

    DEFAULT_BACKOFF_SCHEDULE: Tuple[float, ...] = (5.0, 10.0, 20.0, 40.0, 60.0)
    # 총 시도 횟수 = 1 + len(schedule). 누적 sleep ≈ 77s — 측정된 lag(~10s) 를
    # 결국 PARTIAL 로 분류되어 호출자가 경고 처리.

    def __init__(
        self,
        client: RestTradeClient,
        backoff_schedule: Sequence[float] = DEFAULT_BACKOFF_SCHEDULE,
    ):
        self.client = client
        self._backoff_schedule: Tuple[float, ...] = tuple(backoff_schedule)

    async def fetch_range(self, symbol: str, from_id: int, to_id: int) -> GapFillResult:
        if from_id > to_id:
            raise ValueError(f"fetch_range: from_id ({from_id}) > to_id ({to_id})")

        # ── Stage 1: recent (memory) ──
        try:
            recent = await self.client.fetch_recent_trades(
                symbol, limit=RestTradeClient.MAX_RECENT_LIMIT
            )
        except RestRateLimitError as e:
            # recent 도 rate limited 이면 historical 로 바로 fallback (auth 불필요).
            logger.warning(f"Stage1 rate limited, falling back to Stage2: {e}")
            recent = []

        cover_result = self._try_cover_with_recent(recent, from_id, to_id)
        if cover_result is not None:
            return cover_result

        # ── Stage 2: historical (DB) + retry/backoff ──
        return await self._fetch_via_historical(symbol, from_id, to_id)

    @staticmethod
    def _try_cover_with_recent(
        recent: List[Dict[str, Any]], from_id: int, to_id: int
    ) -> Optional[GapFillResult]:
        """
        recent 응답이 [from_id, to_id] 를 cover 하면 결과를 반환, 아니면 None.

        Cover 조건:
          - recent 가 비어있지 않음
          - min(recent.id) <= from_id : 응답이 from_id 영역까지 거슬러 올라옴
          - max(recent.id) >= to_id   : 응답이 to_id 영역까지 도달
        """
        if not recent:
            return None

        ids = [int(t["id"]) for t in recent]
        recent_min = min(ids)
        recent_max = max(ids)

        if recent_min > from_id or recent_max < to_id:
            return None

        in_range = sorted(
            (t for t in recent if from_id <= int(t["id"]) <= to_id),
            key=lambda t: int(t["id"]),
        )
        return GapFillResult(
            trades=in_range,
            source=GapFillSource.RECENT_MEMORY,
            attempts=0,
        )

    async def _fetch_via_historical(
        self, symbol: str, from_id: int, to_id: int
    ) -> GapFillResult:
        """
        historical endpoint 로 [from_id, to_id] 를 채우려 시도. lag 흡수를 위해
        backoff schedule 만큼 retry.
        """
        last_response: List[Dict[str, Any]] = []
        attempts = 0

        # 첫 시도는 sleep 없이, 그 후 schedule 만큼 점진적으로 대기.
        for sleep_s in (0.0, *self._backoff_schedule):
            if sleep_s > 0:
                await asyncio.sleep(sleep_s)
            attempts += 1

            try:
                resp = await self.client.fetch_trades_from_id(
                    symbol,
                    from_id=from_id,
                    limit=RestTradeClient.MAX_HISTORICAL_LIMIT,
                )
            except RestRateLimitError as e:
                # rate limit 은 backoff 더 기다리고 재시도. 마지막 시도라면 PARTIAL.
                logger.warning(
                    f"Stage2 rate limited (attempt {attempts}), backing off: {e}"
                )
                continue
            except RestApiError:
                # auth 또는 일반 API error — retry 해도 의미 없음. caller 가 처리.
                raise

            last_response = resp
            max_id = max((int(t["id"]) for t in resp), default=None)

            if max_id is not None and max_id >= to_id:
                in_range = sorted(
                    (t for t in resp if from_id <= int(t["id"]) <= to_id),
                    key=lambda t: int(t["id"]),
                )
                return GapFillResult(
                    trades=in_range,
                    source=GapFillSource.HISTORICAL_DB,
                    attempts=attempts,
                )

            logger.debug(
                f"Stage2 attempt {attempts}: max_id={max_id} < to_id={to_id}, "
                f"will retry after backoff."
            )

        # 모든 retry 후에도 to_id 미도달 → partial.
        in_range = sorted(
            (t for t in last_response if from_id <= int(t["id"]) <= to_id),
            key=lambda t: int(t["id"]),
        )
        return GapFillResult(
            trades=in_range,
            source=GapFillSource.PARTIAL,
            attempts=attempts,
        )
