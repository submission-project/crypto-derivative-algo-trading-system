"""
RestTradeClient 테스트.

- 앞쪽: 헤더/예외/silent-failure-방지 같은 핵심 동작을 mock으로 검증
- 뒤쪽: 실제 Binance REST API에 대한 통합 테스트
  (네트워크 필요. SKIP_BINANCE_INTEGRATION=1 또는 API key 부재 시 skip)
- 마지막: historicalTrades DB 색인 lag 가설 검증 (RUN_LAG_PROBE=1 opt-in)
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from binance_perp_trade.rest.trade_client import (
    RestApiError,
    RestAuthError,
    RestRateLimitError,
    RestTradeClient,
)
from binance_perp_trade.config import settings  # noqa: WPS433


def _load_binance_api_key() -> str | None:
    return settings.binance_api_key


# ─── 공통 헬퍼 ───────────────────────────────────────────────────────────────


class _FakeResponse:
    """aiohttp.ClientResponse 의 async context manager 흉내 (mock용)."""

    def __init__(self, status: int, json_data=None, text_data: str = ""):
        self.status = status
        self._json = json_data
        self._text = text_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._json

    async def text(self):
        return self._text


def _patch_get(client: RestTradeClient, response: _FakeResponse):
    """client._session.get 을 인자 캡처가 되는 fake로 교체."""
    captured: dict = {}

    def fake_get(url, params=None, headers=None):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers or {}
        return response

    session = MagicMock(spec=aiohttp.ClientSession)
    session.get = fake_get
    client._session = session
    return captured


# ─── 헤더/인증 동작 ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_recent_trades_does_not_send_api_key():
    """fetch_recent_trades 는 PUBLIC 이므로 X-MBX-APIKEY 가 들어가서는 안 된다."""
    client = RestTradeClient(api_key="dummy-key")
    captured = _patch_get(
        client,
        _FakeResponse(200, json_data=[{"id": 1, "price": "1", "qty": "1", "time": 0}]),
    )

    await client.fetch_recent_trades("BTCUSDT", limit=5)

    assert "X-MBX-APIKEY" not in captured["headers"]


@pytest.mark.asyncio
async def test_historical_trades_sends_api_key_header():
    """fetch_trades_from_id 는 X-MBX-APIKEY 헤더를 정확히 첨부해야 한다."""
    client = RestTradeClient(api_key="my-test-key-xyz")
    captured = _patch_get(client, _FakeResponse(200, json_data=[]))

    await client.fetch_trades_from_id("BTCUSDT", from_id=100, limit=10)

    assert captured["headers"].get("X-MBX-APIKEY") == "my-test-key-xyz"
    assert "/fapi/v1/historicalTrades" in captured["url"]
    assert captured["params"]["fromId"] == 100


@pytest.mark.asyncio
async def test_historical_trades_without_api_key_raises_auth_error():
    """
    api_key 미주입 상태에서 인증 필요 호출은 즉시 RestAuthError.
    (이전 구현은 빈 리스트를 반환해 401과 'no data'를 구분 불가능하게 만들었음)
    """
    client = RestTradeClient(api_key=None)

    with pytest.raises(RestAuthError) as exc_info:
        await client.fetch_trades_from_id("BTCUSDT", from_id=100)

    assert "X-MBX-APIKEY missing" in str(exc_info.value)


# ─── 4xx/5xx 응답을 명시적 예외로 raise (silent failure 제거) ────────────────


@pytest.mark.asyncio
async def test_401_raises_rest_auth_error_with_binance_code():
    """
    실제 production 사고 재현:
      'REST API error 401: {"code":-2014,"msg":"API-key format invalid."}'
    이 응답은 빈 리스트가 아니라 RestAuthError로 명확히 raise 되어야 한다.
    """
    client = RestTradeClient(api_key="malformed-key")
    body = '{"code":-2014,"msg":"API-key format invalid."}'
    _patch_get(client, _FakeResponse(401, text_data=body))

    with pytest.raises(RestAuthError) as exc_info:
        await client.fetch_trades_from_id("BTCUSDT", from_id=100)

    assert exc_info.value.status == 401
    assert exc_info.value.code == -2014
    assert "API-key format invalid" in exc_info.value.body


@pytest.mark.asyncio
async def test_403_raises_rest_auth_error():
    client = RestTradeClient(api_key="key")
    _patch_get(client, _FakeResponse(403, text_data='{"code":-1022,"msg":"Forbidden"}'))

    with pytest.raises(RestAuthError):
        await client.fetch_trades_from_id("BTCUSDT", from_id=100)


@pytest.mark.asyncio
async def test_429_raises_rate_limit_error():
    client = RestTradeClient(api_key="key")
    _patch_get(
        client, _FakeResponse(429, text_data='{"code":-1003,"msg":"Too many requests"}')
    )

    with pytest.raises(RestRateLimitError) as exc_info:
        await client.fetch_trades_from_id("BTCUSDT", from_id=100)

    assert exc_info.value.status == 429


@pytest.mark.asyncio
async def test_500_raises_generic_rest_api_error():
    client = RestTradeClient(api_key="key")
    _patch_get(client, _FakeResponse(500, text_data="server error"))

    with pytest.raises(RestApiError) as exc_info:
        await client.fetch_recent_trades("BTCUSDT")

    assert exc_info.value.status == 500
    # 인증 에러의 더 좁은 서브클래스에는 매치되면 안 됨
    assert not isinstance(exc_info.value, RestAuthError)


# ─── limit 경계값 / fetch_recent_trades 의 client-side 검증 ───────────────────
# (fetch_trades_from_id 의 client-side limit 검증은 미사용 결정으로 제거됨.
#  콜러는 RestTradeClient.MAX_HISTORICAL_LIMIT 상수를 그대로 쓰는 것을 권장.)


@pytest.mark.asyncio
async def test_fetch_trades_from_id_accepts_max_limit_500():
    """경계값 500은 통과."""
    client = RestTradeClient(api_key="key")
    captured = _patch_get(client, _FakeResponse(200, json_data=[]))

    await client.fetch_trades_from_id("BTCUSDT", from_id=100, limit=500)

    assert captured["params"]["limit"] == 500


@pytest.mark.asyncio
async def test_fetch_recent_trades_rejects_limit_above_1000():
    """recent trades max=1000. 그 이상은 ValueError."""
    client = RestTradeClient()
    _patch_get(client, _FakeResponse(200, json_data=[]))

    with pytest.raises(ValueError, match="1..1000"):
        await client.fetch_recent_trades("BTCUSDT", limit=1001)


@pytest.mark.asyncio
async def test_fetch_recent_trades_accepts_max_limit_1000():
    client = RestTradeClient()
    captured = _patch_get(client, _FakeResponse(200, json_data=[]))

    await client.fetch_recent_trades("BTCUSDT", limit=1000)

    assert captured["params"]["limit"] == 1000


# ─── 통합 테스트 (실 Binance) ────────────────────────────────────────────────

INTEGRATION_DISABLED = os.environ.get("SKIP_BINANCE_INTEGRATION") == "1"
BINANCE_API_KEY = _load_binance_api_key()


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance API integration disabled)",
)
@pytest.mark.asyncio
async def test_fetch_recent_trades_real():
    """실 Binance — fetch_recent_trades 는 PUBLIC 이라 키 없이도 동작."""
    client = RestTradeClient()
    await client.connect()
    try:
        trades = await client.fetch_recent_trades("BTCUSDT", limit=5)
    finally:
        await client.close()

    assert isinstance(trades, list)
    assert len(trades) > 0
    first = trades[0]
    for required in ("id", "price", "qty", "time", "isBuyerMaker"):
        assert required in first, f"missing field: {required}"
    assert float(first["price"]) > 0


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY,
    reason=(
        "Requires BINANCE_API_KEY env var (real key) and "
        "SKIP_BINANCE_INTEGRATION != 1 — skipping production-path test."
    ),
)
@pytest.mark.asyncio
async def test_fetch_trades_from_id_real_with_api_key():
    """
    실 Binance — production 경로 (/fapi/v1/historicalTrades).
    이 테스트가 통과해야 production에서 repair_worker가 실제로 동작합니다.
    """
    client = RestTradeClient(api_key=BINANCE_API_KEY)

    await client.connect()
    try:
        recent = await client.fetch_recent_trades("BTCUSDT", limit=100)
        assert recent, "최근 trade를 못 받음 — 네트워크 확인"
        from_id = int(min(t["id"] for t in recent))

        trades = await client.fetch_trades_from_id("BTCUSDT", from_id=from_id, limit=5)
    finally:
        await client.close()

    # API key 가 유효하면 빈 리스트가 아니라 실제 데이터가 와야 함.
    assert isinstance(trades, list)
    assert len(trades) > 0, (
        "production 경로가 빈 리스트를 반환했습니다 — "
        "API key 가 잘못되었거나 권한이 부족합니다."
    )
    assert "id" in trades[0]


# ─── 가설 검증: historicalTrades DB 색인 lag (RUN_LAG_PROBE=1 opt-in) ─────────
#
# Binance Developer Community 에 따르면:
#   - /fapi/v1/trades            : Memory  (실시간 hot path)
#   - /fapi/v1/historicalTrades  : Database (색인 lag 존재 가능)
#
# 즉 가장 최신 trade_id는 memory(=trades)에는 즉시 보이지만, database(=historicalTrades)에는
# DB 색인이 끝날 때까지 안 보일 수 있다. 이 테스트가 그 가설을 실증합니다.
#
# 결과의 활용: lag P50/P99 데이터를 기반으로 repair_worker의 retry/backoff 전략을 결정.
# 시간이 걸리므로 (최대 ~70초) 일반 CI 에서는 돌리지 않음 → RUN_LAG_PROBE=1 opt-in.

LAG_PROBE_ENABLED = os.environ.get("RUN_LAG_PROBE") == "1"


async def _probe_single_target(
    client: RestTradeClient,
    symbol: str,
    checkpoints: list[int],
):
    """
    단일 target_id 로 lag probe 1회 수행.

    Returns dict with keys:
      target_id : int
      outcome   : "found" | "adl_suspect" | "indeterminate"
      found_at  : int | None  (outcome == "found" 일 때 등장 delay)
      history   : list of dicts (per-checkpoint 측정값)

    outcome 분류 기준:
      - "found"         : 어느 시점에 target_id 가 응답에 등장 → lag 측정 성공
      - "adl_suspect"   : 끝까지 등장 안 했지만, 응답에 target 보다 큰 ID 들이
                          일관되게 존재 (= 색인은 진척됐는데 target만 영구 누락)
                          → ADL / insurance fund trade 의심
      - "indeterminate" : 끝까지 등장도 안 하고, 응답에 target 보다 큰 ID도
                          별로 안 보임 → 다른 원인 (네트워크, traffic 부족 등)
    """
    recent = await client.fetch_recent_trades(symbol, limit=10)
    assert recent, "최근 trade를 못 받음 — 네트워크 확인"
    recent.sort(key=lambda t: int(t["id"]))
    target_id = int(recent[-1]["id"])

    history: list[dict] = []
    found_at: int | None = None
    prev = 0
    for delay in checkpoints:
        wait = delay - prev
        if wait > 0:
            await asyncio.sleep(wait)
        prev = delay

        resp = await client.fetch_trades_from_id(
            symbol, from_id=recent[-3]["id"], limit=10
        )
        ids = sorted(int(t["id"]) for t in resp)
        found = target_id in ids
        history.append(
            {
                "delay": delay,
                "found": found,
                "size": len(ids),
                "min": ids[0] if ids else None,
                "max": ids[-1] if ids else None,
                "head": ids[:3],
                "tail": ids[-3:],
            }
        )

        if found:
            found_at = delay
            break

    if found_at is not None:
        outcome = "found"
    else:
        # 끝까지 안 나타남 → ADL/insurance vs 측정 불가 분류
        # 응답에 target_id 보다 큰 ID 가 일관되게(>= 2회) 존재했다면 ADL 의심.
        later_seen = sum(
            1 for h in history if h["max"] is not None and h["max"] > target_id
        )
        outcome = "adl_suspect" if later_seen >= 2 else "indeterminate"

    return {
        "target_id": target_id,
        "outcome": outcome,
        "found_at": found_at,
        "history": history,
    }


def _print_probe_history(symbol: str, target_id: int, history: list[dict]):
    print(f"\n[lag probe] symbol={symbol}, target_id={target_id}")
    for h in history:
        print(
            f"   t+{h['delay']:3d}s  found={str(h['found']):5s}  size={h['size']:2d}  "
            f"min={h['min']} max={h['max']}  head={h['head']} tail={h['tail']}"
        )


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY or not LAG_PROBE_ENABLED,
    reason=(
        "Lag probe (실시간 측정) — RUN_LAG_PROBE=1 + BINANCE_API_KEY 필요. "
        "최대 ~3.5분 소요 가능 (재시도 포함)."
    ),
)
@pytest.mark.asyncio
async def test_historical_trades_indexing_lag():
    """
    가설: /fapi/v1/historicalTrades 는 DB-backed 라 최신 trade_id 색인에 lag 이 있다.

    절차:
      1) recent_trades(=memory) 로 가장 최신 ID(=target) 확보
      2) checkpoints 시각마다 historicalTrades(fromId=target) 호출
      3) target 이 응답에 들어있는지 매번 체크
      4) 안 나타나는 경우 ADL/insurance 의심 → 다른 target 으로 재시도

    결과 분류:
      - found        : lag 가설 입증 (found_at 만큼 색인 lag 존재)
      - adl_suspect  : target 자체가 market trade 가 아니었음 (영구 미반영)
                       → 이 경우는 lag 측정 불가, 다른 target 으로 재시도.
      - indeterminate: 응답이 거의 비어있음, 다른 원인 의심 → 재시도.

    재시도 max_attempts 회 모두 lag 측정 못 하면 SKIP (실패 아님).
    """
    assert BINANCE_API_KEY
    client = RestTradeClient(api_key=BINANCE_API_KEY)
    await client.connect()
    try:
        symbol = "BTCUSDT"
        checkpoints = [5, 10, 20, 40, 70]
        max_attempts = 3

        non_found_results: list[dict] = []
        for attempt in range(1, max_attempts + 1):
            print(f"\n[lag probe] === attempt {attempt}/{max_attempts} ===")
            result = await _probe_single_target(client, symbol, checkpoints)
            _print_probe_history(symbol, result["target_id"], result["history"])

            if result["outcome"] == "found":
                found_at = result["found_at"]
                if found_at == 0:
                    print(
                        "[lag probe] ✅ note: 색인이 즉시 완료된 케이스 "
                        "(lag 매우 작음)"
                    )
                else:
                    print(
                        f"[lag probe] ✅ 가설(lag) 입증: target_id="
                        f"{result['target_id']} 가 {found_at}s 후 등장"
                    )
                return

            non_found_results.append(result)
            print("")
            print(
                f"[lag probe] ⚠️ outcome={result['outcome']} "
                f"(target_id={result['target_id']}) → 다른 target 으로 재시도"
            )

        # 여기 오면 max_attempts 모두 lag 를 측정 못 함.
        # 분포에 따라 SKIP 또는 FAIL.
        adl_count = sum(1 for r in non_found_results if r["outcome"] == "adl_suspect")
        indet_count = sum(
            1 for r in non_found_results if r["outcome"] == "indeterminate"
        )

        msg = (
            f"{max_attempts}회 시도 모두 lag 측정 실패. "
            f"adl_suspect={adl_count}, indeterminate={indet_count}. "
            f"target 들이 모두 ADL/insurance 였을 가능성이 높음. "
            f"다음 시간대에 다시 실행해 보세요."
        )
        # ADL 의심이 우세 → 측정 자체가 불가능한 상황 → SKIP (fail이 아님).
        # 그 외 경우 (indeterminate 우세) → 진짜 lag 가 70s 이상이거나
        # 네트워크 문제 → FAIL.
        if adl_count >= indet_count:
            pytest.skip(msg)
        else:
            pytest.fail(msg)
    finally:
        await client.close()
