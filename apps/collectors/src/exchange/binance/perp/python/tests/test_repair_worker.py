"""
repair_worker 테스트 — RepairJob → REST f~l 복원 → canonical 발행 흐름

- 앞쪽: mock 기반 유닛 테스트
- 뒤쪽: 실제 Binance REST API를 호출하는 통합 테스트
  (네트워크 필요. SKIP_BINANCE_INTEGRATION=1 로 비활성화 가능)
"""

import os
from decimal import Decimal

import pytest
from unittest.mock import AsyncMock, patch

from binance_perp_trade.core.repair_job import RepairJob
from binance_perp_trade.core.normalizer import normalize_rest_trade
from binance_perp_trade.rest.trade_client import RestTradeClient
from schemas.market import TradeSource
from binance_perp_trade.config import settings  # noqa: WPS433


@pytest.mark.asyncio
async def test_repair_worker_full_restoration():
    """
    RepairJob을 받아 REST에서 f~l 범위 trade를 가져오고,
    범위 내 trade만 canonical로 발행하는지 확인
    """
    job = RepairJob(
        symbol="BTCUSDT",
        from_trade_id=100,
        to_trade_id=102,
        source_agg_trade_id=9999,
        reason="agg_trade_fallback",
    )

    # REST가 반환할 mock 데이터 (from_id=100부터 조회)
    mock_rest_response = [
        {
            "id": 100,
            "price": "70000.0",
            "qty": "0.4",
            "isBuyerMaker": False,
            "time": 1000000,
        },
        {
            "id": 101,
            "price": "70001.0",
            "qty": "0.3",
            "isBuyerMaker": True,
            "time": 1000001,
        },
        {
            "id": 102,
            "price": "70002.0",
            "qty": "0.3",
            "isBuyerMaker": False,
            "time": 1000002,
        },
        {
            "id": 103,
            "price": "70003.0",
            "qty": "0.1",
            "isBuyerMaker": True,
            "time": 1000003,
        },  # 범위 밖
    ]

    mock_rest_client = AsyncMock()
    mock_rest_client.fetch_trades_from_id.return_value = mock_rest_response

    mock_producer_canonical = AsyncMock()

    # repair worker 로직 시뮬레이션
    trades = await mock_rest_client.fetch_trades_from_id(
        symbol=job.symbol,
        from_id=job.from_trade_id,
        limit=500,
    )

    restored_count = 0
    for raw_trade in trades:
        trade_id = raw_trade["id"]
        if job.from_trade_id <= trade_id <= job.to_trade_id:
            canonical = normalize_rest_trade(
                raw_trade,
                job.symbol,
                TradeSource.REST_GAP_FILL,
                source_agg_trade_id=job.source_agg_trade_id,
            )
            await mock_producer_canonical.produce(job.symbol, canonical)
            restored_count += 1

    # 검증
    assert restored_count == 3  # 100, 101, 102만 (103 제외)
    assert mock_producer_canonical.produce.call_count == 3

    # 첫 번째 canonical trade 검증
    _, first_canonical = mock_producer_canonical.produce.call_args_list[0][0]
    assert first_canonical["trade_id"] == 100
    assert first_canonical["reconstructed_from_agg"] is True
    assert first_canonical["source_agg_trade_id"] == 9999
    assert first_canonical["verified_by_rest"] is True
    assert first_canonical["is_buyer_maker"] is False
    assert first_canonical["price"] == "70000.0"
    assert first_canonical["size"] == "0.4"
    assert first_canonical["exchange_ts"] == 1000000
    assert first_canonical["event_ts"] is None
    assert first_canonical["local_ts"] > 0
    assert first_canonical["source"] == TradeSource.REST_GAP_FILL.value


@pytest.mark.asyncio
async def test_repair_worker_partial_restoration():
    """REST에서 일부만 반환된 경우 (partial repair)"""
    job = RepairJob(
        symbol="BTCUSDT",
        from_trade_id=100,
        to_trade_id=105,
        source_agg_trade_id=9999,
        reason="agg_trade_fallback",
    )

    # REST가 100~103만 반환 (104, 105 누락)
    mock_rest_response = [
        {
            "id": 100,
            "price": "70000.0",
            "qty": "0.4",
            "isBuyerMaker": False,
            "time": 1000000,
        },
        {
            "id": 101,
            "price": "70001.0",
            "qty": "0.3",
            "isBuyerMaker": True,
            "time": 1000001,
        },
        {
            "id": 102,
            "price": "70002.0",
            "qty": "0.3",
            "isBuyerMaker": False,
            "time": 1000002,
        },
        {
            "id": 103,
            "price": "70003.0",
            "qty": "0.1",
            "isBuyerMaker": True,
            "time": 1000003,
        },
    ]

    mock_rest_client = AsyncMock()
    mock_rest_client.fetch_trades_from_id.return_value = mock_rest_response

    mock_producer_canonical = AsyncMock()

    trades = await mock_rest_client.fetch_trades_from_id(
        symbol=job.symbol,
        from_id=job.from_trade_id,
        limit=500,
    )

    restored_count = 0
    for raw_trade in trades:
        trade_id = int(raw_trade["id"])
        if job.from_trade_id <= trade_id <= job.to_trade_id:
            canonical = normalize_rest_trade(
                raw_trade,
                job.symbol,
                TradeSource.REST_GAP_FILL,
                source_agg_trade_id=job.source_agg_trade_id,
            )
            await mock_producer_canonical.produce(job.symbol, canonical)
            restored_count += 1

    expected_count = job.to_trade_id - job.from_trade_id + 1  # 6

    # Partial: 4 restored out of 6 expected
    assert restored_count == 4
    assert restored_count < expected_count
    assert mock_producer_canonical.produce.call_count == 4


# ─────────────────────────────────────────────────────────────────────────────
#  통합 테스트 — 실제 Binance REST API 사용
#  ※ 네트워크에 의존하므로 SKIP_BINANCE_INTEGRATION=1 환경 변수로 비활성화 가능
# ─────────────────────────────────────────────────────────────────────────────


def _load_binance_api_key() -> str | None:
    return settings.binance_api_key


INTEGRATION_DISABLED = os.environ.get("SKIP_BINANCE_INTEGRATION") == "1"
BINANCE_API_KEY = _load_binance_api_key()


async def _run_repair_logic(
    job: RepairJob,
    raw_trades: list[dict],
    producer: AsyncMock,
) -> list[dict]:
    """main.py의 repair_worker 핵심 로직을 그대로 시뮬레이션 (테스트 헬퍼)."""
    restored: list[dict] = []
    for raw in raw_trades:
        trade_id = raw["id"]
        if job.from_trade_id <= trade_id <= job.to_trade_id:
            canonical = normalize_rest_trade(
                raw,
                job.symbol,
                TradeSource.REST_GAP_FILL,
                source_agg_trade_id=job.source_agg_trade_id,
            )

            assert canonical["symbol"] == job.symbol
            assert canonical["exchange"] == "binance"
            assert canonical["market_type"] == "perp"
            assert canonical["source"] == TradeSource.REST_GAP_FILL.value
            assert isinstance(canonical["price"], str)
            assert isinstance(canonical["size"], str)
            assert Decimal(canonical["price"]) > 0
            assert Decimal(canonical["size"]) >= 0
            assert canonical["exchange_ts"] > 0

            await producer.produce(job.symbol, canonical)
            restored.append(canonical)
    return restored


@pytest.mark.skipif(
    INTEGRATION_DISABLED,
    reason="SKIP_BINANCE_INTEGRATION=1 (real Binance API integration disabled)",
)
@pytest.mark.asyncio
async def test_repair_worker_real_recent_trades():
    """
    실제 Binance fapi에서 최근 trade를 가져와,
    그 중 일부 구간을 RepairJob으로 정의하고 복원 흐름을 검증합니다.

    검증 포인트:
    - 실제 Binance 응답 스키마가 normalize_rest_trade와 호환되는지
    - 범위 필터(from_trade_id ~ to_trade_id)가 실데이터에서도 정확히 동작하는지
    - 복원된 canonical이 reconstructed_from_agg / verified_by_rest 메타를 갖는지
    """
    client = RestTradeClient()
    await client.connect()
    try:
        symbol = "BTCUSDT"
        recent = await client.fetch_recent_trades(symbol, limit=50)

        assert isinstance(recent, list)
        assert (
            len(recent) >= 30
        ), f"Binance에서 충분한 최근 trade를 받지 못했습니다: {len(recent)}"

        # trade_id 오름차순 정렬 (Binance는 이미 오름차순이지만 안전하게)
        recent.sort(key=lambda t: int(t["id"]))

        # 50개 중 가운데 11개([10:21])를 복원 범위로 지정
        from_id = int(recent[10]["id"])
        to_id = int(recent[20]["id"])
        expected_count = to_id - from_id + 1

        job = RepairJob(
            symbol=symbol,
            from_trade_id=from_id,
            to_trade_id=to_id,
            source_agg_trade_id=999_999,
            reason="integration_test_recent",
        )

        producer = AsyncMock()
        restored = await _run_repair_logic(job, recent, producer)

        # 1) 적어도 1건 이상 복원되어야 함
        assert len(restored) > 0
        assert producer.produce.call_count == len(restored)

        # 2) recent에는 from_id ~ to_id 사이가 빈틈없이 들어있으므로 expected_count와 동일해야 함
        assert len(restored) == expected_count, (
            f"expected={expected_count}, restored={len(restored)} "
            f"(범위 내 trade_id가 누락되었거나 정렬이 잘못되었을 수 있음)"
        )

        # 3) 모든 canonical이 범위 안에 있어야 하고, 메타 필드도 정확해야 함
        for c in restored:
            assert from_id <= c["trade_id"] <= to_id
            assert c["symbol"] == symbol
            assert c["exchange"] == "binance"
            assert c["market_type"] == "perp"
            assert c["source"] == TradeSource.REST_GAP_FILL.value
            assert c["verified_by_rest"] is True
            assert c["reconstructed_from_agg"] is True
            assert c["source_agg_trade_id"] == 999_999
            assert Decimal(c["price"]) > 0
            assert Decimal(c["size"]) >= 0
            assert c["exchange_ts"] > 0
    finally:
        await client.close()


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY,
    reason=(
        "Requires BINANCE_API_KEY (real key) and SKIP_BINANCE_INTEGRATION != 1. "
        "이 테스트가 통과해야 production repair_worker가 실제로 동작합니다."
    ),
)
@pytest.mark.asyncio
async def test_repair_worker_real_historical_trades_from_id():
    """
    프로덕션 경로 검증: fetch_trades_from_id (= /fapi/v1/historicalTrades, X-MBX-APIKEY 필수).

    Binance 동작 특성:
        /fapi/v1/historicalTrades 는 마켓 체결만 반환합니다. Insurance Fund 청산이나
        ADL 체결은 trade_id 가 발급되더라도 응답에서 제외됩니다. 따라서 임의의 from_id 로
        호출하면 응답이 from_id 보다 큰 ID부터 시작할 수 있고 (선두 gap), 응답 안에서도
        ID 사이가 띄엄띄엄할 수 있습니다.

    검증 포인트 (이 사실을 인정하는 형태로):
        1) 빈 응답이 아니다 (인증/연결 정상)
        2) 응답은 trade_id 단조 증가
        3) 모든 응답 ID >= from_id (Binance 보장)
        4) 정규화된 canonical 메타 필드가 정확하다
    """
    client = RestTradeClient(api_key=BINANCE_API_KEY)
    await client.connect()
    try:
        symbol = "BTCUSDT"

        recent = await client.fetch_recent_trades(symbol, limit=50)
        assert len(recent) >= 10, "최근 trade를 충분히 못 받음 — 네트워크 확인 필요"

        recent.sort(key=lambda t: int(t["id"]))
        from_id = int(recent[5]["id"])
        # from_id 이후의 사용 가능한 어떤 범위든 OK. ID gap을 인정함.
        to_id = from_id + 50

        job = RepairJob(
            symbol=symbol,
            from_trade_id=from_id,
            to_trade_id=to_id,
            source_agg_trade_id=999_999,
            reason="integration_test_historical",
        )

        trades = await client.fetch_trades_from_id(
            symbol=job.symbol,
            from_id=job.from_trade_id,
            limit=500,
        )

        # 1) 빈 응답이면 인증/연결 문제로 간주 (production 사고 신호)
        assert trades, (
            "production 경로 (/fapi/v1/historicalTrades)가 빈 리스트를 반환했습니다. "
            "API key 인증 또는 네트워크 문제 가능성."
        )

        # 2) 단조 증가, 3) from_id 이상 보장
        returned_ids = [int(t["id"]) for t in trades]
        assert returned_ids == sorted(returned_ids), "trade_id는 단조 증가해야 함"
        assert (
            min(returned_ids) >= from_id
        ), f"Binance가 from_id 이전의 trade를 반환함: min={min(returned_ids)}, from_id={from_id}"

        # 4) repair worker 로직 실행 — partial repair 가 발생할 수 있음 (ADL gap)
        producer = AsyncMock()
        restored = await _run_repair_logic(job, trades, producer)

        # restored 의 모든 ID가 [from_id, to_id] 안에 있고 단조 증가하는지 확인.
        # 정확한 갯수는 Binance ADL/insurance gap 때문에 보장 불가.
        prev_trade_id = -1
        prev_ts = -1
        for c in restored:
            assert from_id <= c["trade_id"] <= to_id
            assert c["reconstructed_from_agg"] is True
            assert c["verified_by_rest"] is True
            assert c["source"] == TradeSource.REST_GAP_FILL.value
            assert c["source_agg_trade_id"] == 999_999
            assert Decimal(c["price"]) > 0

            assert c["trade_id"] > prev_trade_id, "trade_id가 단조 증가해야 함"
            assert c["exchange_ts"] >= prev_ts, "exchange_ts가 비감소여야 함"
            prev_trade_id = c["trade_id"]
            prev_ts = c["exchange_ts"]
    finally:
        await client.close()


@pytest.mark.skipif(
    INTEGRATION_DISABLED or not BINANCE_API_KEY,
    reason="Requires BINANCE_API_KEY (real key) and SKIP_BINANCE_INTEGRATION != 1.",
)
@pytest.mark.asyncio
async def test_repair_worker_real_partial_when_range_exceeds_limit():
    """
    REST limit 보다 큰 범위를 요청하면 partial repair가 되는지 실데이터로 검증.

    fetch_trades_from_id(limit=N) 으로 N건만 받아오므로,
    to_id - from_id + 1 > N 이면 누락이 발생해야 합니다.
    """
    client = RestTradeClient(api_key=BINANCE_API_KEY)
    await client.connect()
    try:
        symbol = "BTCUSDT"
        recent = await client.fetch_recent_trades(symbol, limit=50)
        assert len(recent) > 0

        recent.sort(key=lambda t: int(t["id"]))
        from_id = int(recent[0]["id"])
        # 실제 limit(=5)보다 훨씬 큰 범위를 요구
        to_id = from_id + 49

        job = RepairJob(
            symbol=symbol,
            from_trade_id=from_id,
            to_trade_id=to_id,
            source_agg_trade_id=12_345,
            reason="integration_test_partial",
        )

        small_limit = 5
        trades = await client.fetch_trades_from_id(
            symbol=job.symbol,
            from_id=job.from_trade_id,
            limit=small_limit,
        )

        assert trades, "production 경로가 빈 리스트 — API key 검증 필요"

        producer = AsyncMock()
        restored = await _run_repair_logic(job, trades, producer)

        expected = job.to_trade_id - job.from_trade_id + 1  # 50
        assert (
            len(restored) <= small_limit
        ), f"limit={small_limit} 인데 {len(restored)}건이 복원됨 — REST 응답이 limit를 무시?"
        assert (
            len(restored) < expected
        ), "partial repair 가 발생해야 하는데 full로 처리됨"
        assert producer.produce.call_count == len(restored)
    finally:
        await client.close()
