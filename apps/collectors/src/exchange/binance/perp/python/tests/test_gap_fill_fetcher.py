"""
GapFillFetcher 테스트.

- 앞쪽: mock 기반 단위 테스트 — Stage1(recent) / Stage2(historical + retry/backoff)
        의 모든 분기를 deterministic 하게 커버.
- 뒤쪽: 실제 Binance REST API 통합 테스트 — Stage1 hit / Stage1 miss → Stage2
        두 production 시나리오 검증. (네트워크 + API key 필요. 환경변수로 skip 가능)

backoff_schedule 은 단위 테스트에선 0.001s 로 짧게, 통합 테스트에선 (1.0, 2.0)s
정도로 둬서 실제 lag 흡수 가능성을 남기되 너무 길어지지 않게 한다.
"""

import os
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest

from binance_perp_trade.core.gap_fill_fetcher import (
    GapFillFetcher,
    GapFillResult,
    GapFillSource,
)
from binance_perp_trade.rest.trade_client import (
    RestApiError,
    RestAuthError,
    RestRateLimitError,
    RestTradeClient,
)


def _load_binance_api_key() -> str | None:
    """
    BINANCE_API_KEY 를 다음 순서로 찾는다:
      1. os.environ (직접 export 한 경우)
      2. ENV_FILE 이 가리키는 .env 파일의 settings (pydantic-settings 경로)
    """
    direct = os.environ.get("BINANCE_API_KEY")
    if direct:
        return direct
    try:
        from binance_perp_trade.config import settings  # noqa: WPS433

        return settings.binance_api_key
    except Exception:
        return None


INTEGRATION_DISABLED = os.environ.get("SKIP_BINANCE_INTEGRATION") == "1"
BINANCE_API_KEY = _load_binance_api_key()


def _t(trade_id: int) -> Dict[str, Any]:
    """간소화된 trade row (id 만 의미 있음)."""
    return {
        "id": trade_id,
        "price": "100.0",
        "qty": "0.01",
        "quoteQty": "1.0",
        "time": 1700000000000,
        "isBuyerMaker": False,
    }


def _make_fetcher(
    *,
    recent: List[Dict[str, Any]] | Exception | None = None,
    historical_results: List[Any] | None = None,
) -> tuple[GapFillFetcher, AsyncMock, AsyncMock]:
    """
    RestTradeClient 를 mock 한 GapFillFetcher 를 만든다.

    historical_results: 호출별 응답 또는 예외. 각 element 는 list[dict] 이거나
                       raise 할 Exception 인스턴스.
    """
    client = AsyncMock(spec=RestTradeClient)

    if isinstance(recent, Exception):
        client.fetch_recent_trades.side_effect = recent
    else:
        client.fetch_recent_trades.return_value = recent or []

    if historical_results is not None:
        client.fetch_trades_from_id.side_effect = historical_results

    fetcher = GapFillFetcher(
        client,
        backoff_schedule=(0.001, 0.001, 0.001),  # 빠른 테스트용
    )
    return fetcher, client.fetch_recent_trades, client.fetch_trades_from_id


# ─── Stage 1 (recent) cover 시나리오 ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_stage1_full_cover_returns_recent_memory():
    """recent 응답이 [from, to] 를 모두 cover → RECENT_MEMORY, attempts=0."""
    fetcher, recent_mock, hist_mock = _make_fetcher(
        recent=[_t(98), _t(99), _t(100), _t(101), _t(102), _t(103)]
    )

    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)

    assert result.source == GapFillSource.RECENT_MEMORY
    assert result.attempts == 0
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]
    recent_mock.assert_awaited_once()
    hist_mock.assert_not_awaited()  # historical 호출 X


@pytest.mark.asyncio
async def test_stage1_returns_all_ids_when_response_has_no_gaps():
    """
    recent 응답 자체에 gap 이 없는 경우 (mock), [from, to] 안의 모든 ID 가
    결과에 그대로 들어가야 함. 실제 endpoint 응답에 gap 이 있는지 여부는
    fetcher 의 책임이 아니므로 별도 통합 테스트에서 다룬다.
    """
    fetcher, _, hist_mock = _make_fetcher(
        recent=[_t(99), _t(100), _t(101), _t(102), _t(103)]
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]
    hist_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_stage1_does_not_cover_when_from_id_below_recent_min():
    """recent 의 가장 오래된 ID 가 from_id 보다 크면 cover 실패 → Stage2 진입."""
    fetcher, _, hist_mock = _make_fetcher(
        recent=[_t(105), _t(106), _t(107)],  # min=105 > from_id=100
        historical_results=[[_t(100), _t(101), _t(102), _t(103), _t(104)]],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert result.attempts == 1
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]
    hist_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_stage1_empty_response_falls_back_to_stage2():
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],  # 빈 응답
        historical_results=[[_t(100), _t(101), _t(102)]],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert result.attempts == 1
    hist_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_stage1_rate_limited_falls_back_to_stage2():
    """recent 가 rate limit 이면 raise 하지 않고 Stage2 로 넘어감."""
    fetcher, _, hist_mock = _make_fetcher(
        recent=RestRateLimitError(429, "rate limited", url="x"),
        historical_results=[[_t(100), _t(101), _t(102)]],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    hist_mock.assert_awaited_once()


# ─── Stage 2 (historical + retry/backoff) 시나리오 ───────────────────────────


@pytest.mark.asyncio
async def test_stage2_first_attempt_covers():
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[[_t(100), _t(101), _t(102), _t(103)]],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert result.attempts == 1
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]


@pytest.mark.asyncio
async def test_stage2_retry_simulates_indexing_lag():
    """
    첫 호출에서 to_id 미도달 (lag 가정) → 두 번째 호출에서 도달.
    HISTORICAL_DB, attempts=2.
    """
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[
            [_t(100)],  # max=100 < to_id=102
            [_t(100), _t(101), _t(102), _t(103)],  # cover
        ],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert result.attempts == 2
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]
    assert hist_mock.await_count == 2


@pytest.mark.asyncio
async def test_stage2_all_retries_fail_returns_partial():
    """
    Stage2 모든 호출이 to_id 미도달 → PARTIAL.
    backoff_schedule=(0.001, 0.001, 0.001) → 총 4회 시도.
    PARTIAL 결과에는 마지막 응답에서 [from, to] 부분만 포함.
    """
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[
            [_t(100)],  # 모든 시도가 100까지만 도달
            [_t(100), _t(101)],
            [_t(100), _t(101)],
            [_t(100), _t(101)],
        ],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.PARTIAL
    assert result.attempts == 4  # 1 + 3 retries
    # 마지막 응답 기준 in_range filter 결과 (102 는 끝까지 못 받음)
    assert [int(t["id"]) for t in result.trades] == [100, 101]


@pytest.mark.asyncio
async def test_stage2_rate_limit_then_success():
    """첫 호출 rate limit → backoff 후 두 번째 호출 성공. attempts=2."""
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[
            RestRateLimitError(429, "rate limited", url="x"),
            [_t(100), _t(101), _t(102)],
        ],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert result.attempts == 2


@pytest.mark.asyncio
async def test_stage2_auth_error_raises_immediately():
    """AuthError 는 retry 안 하고 바로 raise — caller 가 처리."""
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[
            RestAuthError(401, "API-key invalid", url="x"),
        ],
    )
    with pytest.raises(RestAuthError):
        await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert hist_mock.await_count == 1  # retry 안 함


@pytest.mark.asyncio
async def test_stage2_generic_api_error_raises_immediately():
    fetcher, _, hist_mock = _make_fetcher(
        recent=[],
        historical_results=[
            RestApiError(400, "bad request", url="x"),
        ],
    )
    with pytest.raises(RestApiError):
        await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert hist_mock.await_count == 1


@pytest.mark.asyncio
async def test_stage2_excludes_adl_in_range():
    """
    historical 응답엔 ADL 빠져있어 [from, to] 안에서도 일부 ID 누락 가능.
    그래도 cover (max >= to_id) 만족하면 HISTORICAL_DB. restored < expected.
    """
    fetcher, _, _ = _make_fetcher(
        recent=[],
        historical_results=[
            [_t(100), _t(101), _t(103), _t(104)],  # 102 가 ADL 가정 → 빠짐
        ],
    )
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=103)
    assert result.source == GapFillSource.HISTORICAL_DB
    assert [int(t["id"]) for t in result.trades] == [100, 101, 103]
    # 호출자는 expected=4 vs restored=3 으로 ADL 의심 로깅


# ─── 입력 검증 ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_from_greater_than_to_raises_value_error():
    fetcher, _, _ = _make_fetcher(recent=[])
    with pytest.raises(ValueError):
        await fetcher.fetch_range("BTCUSDT", from_id=200, to_id=100)


@pytest.mark.asyncio
async def test_from_equals_to_single_trade():
    """from == to 인 경우 (단일 trade) 도 정상 동작."""
    fetcher, _, _ = _make_fetcher(recent=[_t(99), _t(100), _t(101)])
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=100)
    assert result.source == GapFillSource.RECENT_MEMORY
    assert [int(t["id"]) for t in result.trades] == [100]


# ─── 결과 정렬 검증 ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_result_trades_sorted_by_id_ascending():
    """응답이 정렬 안 되어 와도 결과는 ID 오름차순으로 정렬."""
    fetcher, _, _ = _make_fetcher(recent=[_t(102), _t(99), _t(100), _t(101), _t(103)])
    result = await fetcher.fetch_range("BTCUSDT", from_id=100, to_id=102)
    assert [int(t["id"]) for t in result.trades] == [100, 101, 102]


# ─── 통합 테스트 (실제 Binance API) ──────────────────────────────────────────
#
# 두 production 시나리오를 실제로 검증:
#   1) 작은 gap → Stage1 (recent_memory) 만으로 cover.  ADL/insurance 가 끼어
#      있을 수 있어 restored <= expected 는 정상.
#   2) 큰 gap (recent 1000건 너머) → Stage1 cover 불가 → Stage2 (historical_db) 발동.
#
# PARTIAL 시나리오는 production 에서 의도적으로 만들기 어렵고 (lag > backoff 또는
# from_id 자체가 ADL) 비결정적이므로 mock 으로만 커버.
#
# Skip 조건: INTEGRATION_DISABLED=1 또는 BINANCE_API_KEY 부재.


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY,
    reason="실제 Binance API 호출 — BINANCE_API_KEY 필요",
)
@pytest.mark.asyncio
async def test_real_stage1_recent_memory_hit():
    """
    실제 호출: 작은 gap 은 Stage1 (recent) 만으로 즉시 cover.

    절차:
      1) recent 로 가장 최신 ID 확보
      2) 그 직전의 [latest-15, latest-10] 범위(=확실히 1000 이내)를 from_id 로
      3) fetch_range 호출 → source=RECENT_MEMORY, attempts=0 이어야 함

    검증:
      - source == RECENT_MEMORY (Stage1 cover)
      - attempts == 0 (Stage2 호출 없음)
      - 결과 ID 들이 [from, to] 안에 있고 오름차순
      - len(restored) <= expected — ADL/insurance 가 일부 빠지면 부족할 수 있음 (정상)
      - len(restored) > 0 — 모든 ID 가 ADL 인 건 통계적으로 비현실적
    """
    client = RestTradeClient(api_key=BINANCE_API_KEY)
    await client.connect()
    try:
        symbol = "BTCUSDT"
        recent = await client.fetch_recent_trades(symbol, limit=20)
        assert recent, "최근 trade 응답이 비었음 — 네트워크/심볼 확인"
        recent.sort(key=lambda t: int(t["id"]))
        latest = int(recent[-1]["id"])

        # latest 자체는 lag 위험 회피를 위해 살짝 뒤에서 잡음
        from_id = latest - 15
        to_id = latest - 10
        expected = to_id - from_id + 1

        fetcher = GapFillFetcher(client)
        result = await fetcher.fetch_range(symbol, from_id, to_id)

        assert (
            result.source == GapFillSource.RECENT_MEMORY
        ), f"Stage1 hit 기대했지만 source={result.source}, attempts={result.attempts}"
        assert result.attempts == 0
        ids = [int(t["id"]) for t in result.trades]
        assert ids == sorted(ids), f"결과 정렬 안 됨: {ids}"
        assert all(
            from_id <= i <= to_id for i in ids
        ), f"결과에 [from, to] 범위 밖 ID: {ids}"
        assert (
            0 < len(ids) <= expected
        ), f"기대 범위 밖: expected={expected}, got={len(ids)}, ids={ids}"

        # 진단 출력: 누락이 있으면 어떤 ID가 빠졌는지 (= ADL/insurance 후보)
        if len(ids) < expected:
            present = set(ids)
            missing = sorted(set(range(from_id, to_id + 1)) - present)
            print(
                f"\n[stage1] expected={expected}, got={len(ids)}. "
                f"missing IDs (likely ADL/insurance): {missing}"
            )
    finally:
        await client.close()


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY,
    reason="실제 Binance API 호출 — BINANCE_API_KEY 필요",
)
@pytest.mark.asyncio
async def test_real_stage2_historical_db_when_gap_outside_recent():
    """
    실제 호출: recent(1000) 보다 멀리 있는 from_id 는 Stage2 fallback.

    절차:
      1) 가장 최신 ID 에서 5000 만큼 뒤의 작은 범위를 잡음 (recent endpoint 가
         절대 cover 못 하는 영역 — Stage2 강제 발동)
      2) fetch_range 호출 → Stage1 miss → Stage2 historical_db 호출
      3) 5000 정도 뒤면 색인 lag 영향이 거의 없으므로 첫 attempt 에서 cover 가 보통
      4) ADL/insurance 가 일부 있으면 restored < expected 가 될 수 있음 (정상)

    검증:
      - source ∈ {HISTORICAL_DB, PARTIAL} (PARTIAL 은 환경 의존이지만 fail은 X)
      - attempts >= 1 (Stage2 가 실제로 호출됨)
      - 결과 ID 들이 [from, to] 안에 있고 오름차순
    """
    client = RestTradeClient(api_key=BINANCE_API_KEY)
    await client.connect()
    try:
        symbol = "BTCUSDT"
        recent = await client.fetch_recent_trades(symbol, limit=10)
        assert recent
        recent.sort(key=lambda t: int(t["id"]))
        latest = int(recent[-1]["id"])

        # 5000 뒤. BTCUSDT 거래량 기준 수십 초 전 → 색인은 이미 안정됨.
        from_id = latest - 5000
        to_id = from_id + 20  # 21건 범위

        # 통합 테스트는 production 보다 짧게: 첫 시도 실패해도 빠르게 끝나도록.
        fetcher = GapFillFetcher(client, backoff_schedule=(1.0, 2.0))
        result = await fetcher.fetch_range(symbol, from_id, to_id)

        assert result.source in {
            GapFillSource.HISTORICAL_DB,
            GapFillSource.PARTIAL,
        }, f"Stage2 (historical or partial) 기대, got {result.source}"
        assert result.attempts >= 1, "Stage2 가 한 번이라도 호출돼야 함"

        ids = [int(t["id"]) for t in result.trades]
        assert ids == sorted(ids), f"결과 정렬 안 됨: {ids}"
        assert all(
            from_id <= i <= to_id for i in ids
        ), f"결과에 [from, to] 범위 밖 ID: {ids}"

        # historical_db 면 빈 응답은 안 됨 (적어도 일부 market trade 는 있어야 함).
        # 5000 뒤에 모든 ID 가 ADL 인 건 통계적으로 거의 불가능.
        if result.source == GapFillSource.HISTORICAL_DB:
            assert ids, "HISTORICAL_DB cover 인데 빈 결과 — 비정상"
    finally:
        await client.close()
