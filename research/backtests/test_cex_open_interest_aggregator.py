import pytest
from urllib.parse import parse_qs, urlparse

from research.data_sources.exchanges.cex_open_interest_aggregator import (
    OpenInterestSnapshot,
    OpenInterestHistoryPoint,
    aggregate_historical_open_interest_frame,
    aggregate_open_interest_snapshots,
    _fetch_bingx,
    _fetch_bitget,
    _fetch_bybit_history,
    _fetch_htx,
    _fetch_htx_history,
    open_interest_snapshots_to_records,
)


def _snapshot(exchange: str, value: float | None, error: str | None = None) -> OpenInterestSnapshot:
    return OpenInterestSnapshot(
        exchange=exchange,
        symbol="BTCUSDT",
        open_interest_amount=None,
        open_interest_value=value,
        timestamp_ms=1_700_000_000_000,
        datetime="2023-11-14T22:13:20+00:00",
        raw={},
        error=error,
    )


def test_aggregate_open_interest_ignores_failed_exchanges() -> None:
    snapshots = [
        _snapshot("binance", 60.0),
        _snapshot("bybit", 30.0),
        _snapshot("okx", 10.0),
        _snapshot("bitget", None, "temporary API error"),
    ]

    aggregate = aggregate_open_interest_snapshots(snapshots)

    assert aggregate.total_open_interest_value == 100.0
    assert aggregate.covered_exchange_count == 3
    assert aggregate.requested_exchange_count == 4
    assert aggregate.binance_share == 0.6
    assert aggregate.hhi == pytest.approx(0.46)
    assert aggregate.errors == {"bitget": "temporary API error"}


def test_open_interest_snapshots_to_records_adds_share_column() -> None:
    snapshots = [_snapshot("binance", 75.0), _snapshot("bybit", 25.0)]

    records = open_interest_snapshots_to_records(snapshots)

    assert records[0]["exchange"] == "binance"
    assert records[0]["share"] == 0.75
    assert records[1]["share"] == 0.25


def test_bitget_fetch_uses_size_as_open_interest_amount(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, *, timeout_s: float):
        if "open-interest" in url:
            return {
                "data": {
                    "openInterestList": [
                        {
                            "symbol": "BTCUSDT",
                            "size": "35391.6725",
                        }
                    ],
                    "ts": "1700000000000",
                }
            }
        return {
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "markPrice": "64656.3",
                    "ts": "1700000000100",
                }
            ]
        }

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    snapshot = _fetch_bitget(timeout_s=1.0)

    assert snapshot.open_interest_amount == pytest.approx(35391.6725)
    assert snapshot.open_interest_value == pytest.approx(35391.6725 * 64656.3)


def test_bingx_fetch_does_not_multiply_open_interest_by_price(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, *, timeout_s: float):
        if "openInterest" in url:
            return {
                "data": {
                    "openInterest": "1111252985.3",
                    "symbol": "BTC-USDT",
                    "time": 1700000000000,
                }
            }
        return {
            "data": {
                "symbol": "BTC-USDT",
                "lastPrice": "64650.2",
                "time": 1700000000100,
            }
        }

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    snapshot = _fetch_bingx(timeout_s=1.0)

    assert snapshot.open_interest_value == pytest.approx(1_111_252_985.3)
    assert snapshot.open_interest_amount == pytest.approx(1_111_252_985.3 / 64_650.2)


def test_bybit_history_uses_reference_price_for_notional_value(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, *, timeout_s: float):
        return {
            "result": {
                "list": [
                    {"openInterest": "10.0", "timestamp": "1700000000000"},
                    {"openInterest": "12.0", "timestamp": "1700000300000"},
                ]
            }
        }

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    points = _fetch_bybit_history(
        period="5m",
        limit=2,
        start_time_ms=None,
        end_time_ms=None,
        reference_prices={1_700_000_000_000: 50_000.0, 1_700_000_300_000: 51_000.0},
        timeout_s=1.0,
    )

    assert [point.open_interest_amount for point in points] == [10.0, 12.0]
    assert [point.open_interest_value for point in points] == [500_000.0, 612_000.0]


def test_bybit_history_paginates_backwards_with_end_time(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int | None] = []

    def fake_get_json(url: str, *, timeout_s: float):
        query = parse_qs(urlparse(url).query)
        end_time = int(query["endTime"][0]) if "endTime" in query else None
        calls.append(end_time)

        if len(calls) == 1:
            rows = [
                {"openInterest": str(1000 + i), "timestamp": str(1_700_000_000_000 - i * 300_000)}
                for i in range(200)
            ]
        else:
            rows = [
                {"openInterest": "777", "timestamp": str(1_700_000_000_000 - 200 * 300_000)}
            ]
        return {"result": {"list": rows}}

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    reference_prices = {
        1_700_000_000_000 - i * 300_000: 50_000.0
        for i in range(201)
    }
    points = _fetch_bybit_history(
        period="5m",
        limit=201,
        start_time_ms=None,
        end_time_ms=1_700_000_000_000,
        reference_prices=reference_prices,
        timeout_s=1.0,
    )

    assert len(points) == 201
    assert len(calls) == 2
    assert calls[0] == 1_700_000_000_000
    assert calls[1] == 1_700_000_000_000 - 199 * 300_000 - 1
    assert points[0].timestamp_ms == 1_700_000_000_000 - 200 * 300_000
    assert points[-1].timestamp_ms == 1_700_000_000_000


def test_aggregate_historical_open_interest_frame_sums_by_timestamp() -> None:
    points = [
        OpenInterestHistoryPoint(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1_700_000_000_000,
            datetime="2023-11-14T22:13:20+00:00",
            open_interest_amount=10.0,
            open_interest_value=100.0,
            raw={},
        ),
        OpenInterestHistoryPoint(
            exchange="bybit",
            symbol="BTCUSDT",
            timestamp_ms=1_700_000_000_000,
            datetime="2023-11-14T22:13:20+00:00",
            open_interest_amount=20.0,
            open_interest_value=200.0,
            raw={},
        ),
        OpenInterestHistoryPoint(
            exchange="gate",
            symbol="BTC_USDT",
            timestamp_ms=1_700_000_300_000,
            datetime="2023-11-14T22:18:20+00:00",
            open_interest_amount=30.0,
            open_interest_value=300.0,
            raw={},
        ),
    ]

    aggregate = aggregate_historical_open_interest_frame(points, min_exchange_count=2)

    assert len(aggregate) == 1
    assert aggregate["multi_cex_oi_value"].iloc[0] == 300.0
    assert aggregate["exchange_count"].iloc[0] == 2
    assert aggregate["binance_share"].iloc[0] == pytest.approx(1 / 3)


def test_htx_snapshot_uses_value_as_notional(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, *, timeout_s: float):
        return {
            "status": "ok",
            "data": [
                {
                    "volume": 32_210_313,
                    "amount": 32_210.313,
                    "value": 2_086_113_805.5702,
                    "contract_code": "BTC-USDT",
                }
            ],
            "ts": 1_700_000_000_000,
        }

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    snapshot = _fetch_htx(timeout_s=1.0)

    assert snapshot.open_interest_amount == 32_210_313
    assert snapshot.open_interest_value == 2_086_113_805.5702
    assert snapshot.timestamp_ms == 1_700_000_000_000


def test_htx_history_parses_tick_values(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_json(url: str, *, timeout_s: float):
        return {
            "status": "ok",
            "data": {
                "tick": [
                    {"volume": 10.0, "value": 100.0, "ts": 1_700_000_000_000},
                    {"volume": 20.0, "value": 200.0, "ts": 1_700_000_300_000},
                ]
            },
        }

    monkeypatch.setattr(
        "research.data_sources.exchanges.cex_open_interest_aggregator._get_json",
        fake_get_json,
    )

    points = _fetch_htx_history(
        period="5m",
        limit=2,
        start_time_ms=None,
        end_time_ms=None,
        timeout_s=1.0,
    )

    assert [point.exchange for point in points] == ["htx", "htx"]
    assert [point.open_interest_value for point in points] == [100.0, 200.0]


def test_aggregate_historical_open_interest_frame_returns_all_exchanges() -> None:
    points = [
        OpenInterestHistoryPoint(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1_700_000_000_000,
            datetime="2023-11-14T22:13:20+00:00",
            open_interest_amount=10.0,
            open_interest_value=100.0,
            raw={},
        ),
        OpenInterestHistoryPoint(
            exchange="bybit",
            symbol="BTCUSDT",
            timestamp_ms=1_700_000_000_000,
            datetime="2023-11-14T22:13:20+00:00",
            open_interest_amount=20.0,
            open_interest_value=200.0,
            raw={},
        ),
        OpenInterestHistoryPoint(
            exchange="okx",
            symbol="BTCUSDT",
            timestamp_ms=1_700_000_000_000,
            datetime="2023-11-14T22:13:20+00:00",
            open_interest_amount=30.0,
            open_interest_value=300.0,
            raw={},
        ),
    ]

    aggregate = aggregate_historical_open_interest_frame(points, min_exchange_count=2)

    assert len(aggregate) == 1
    assert aggregate["multi_cex_oi_value"].iloc[0] == 600.0
    assert aggregate["exchange_count"].iloc[0] == 3
    assert aggregate["binance_oi_value"].iloc[0] == 100.0
    assert aggregate["binance_share"].iloc[0] == pytest.approx(100.0 / 600.0)
    assert aggregate["bybit_oi_value"].iloc[0] == 200.0
    assert aggregate["bybit_share"].iloc[0] == pytest.approx(200.0 / 600.0)
    assert aggregate["okx_oi_value"].iloc[0] == 300.0
    assert aggregate["okx_share"].iloc[0] == pytest.approx(300.0 / 600.0)
