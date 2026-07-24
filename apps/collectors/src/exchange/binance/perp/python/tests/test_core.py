"""
핵심 컴포넌트 유닛 테스트 — GapDetector, HealthMonitor, FallbackController, Normalizer, RepairJob
"""
import time
import pytest
from unittest.mock import patch

import msgspec
from binance_perp_trade.core.events import WsTradeEvent, WsAggTradeEvent
from binance_perp_trade.core.gap_detector import GapDetector
from binance_perp_trade.core.health_monitor import HealthMonitor, HealthStatus
from binance_perp_trade.core.fallback_controller import FallbackController, CollectorState
from binance_perp_trade.core.normalizer import (
    normalize_trade,
    normalize_ws_trade,
    normalize_agg_trade_event,
    normalize_rest_trade,
)
from binance_perp_trade.core.repair_job import RepairJob
from schemas.market import TradeSource


# ── GapDetector ──────────────────────────────────────────────────────────────

class TestGapDetector:
    def test_no_gap_on_sequential_ids(self):
        gd = GapDetector()
        assert gd.check("BTCUSDT", 100) == (None, None)
        assert gd.check("BTCUSDT", 101) == (None, None)
        assert gd.check("BTCUSDT", 102) == (None, None)

    def test_detects_gap(self):
        gd = GapDetector()
        gd.check("BTCUSDT", 100)
        missing_from, missing_to = gd.check("BTCUSDT", 105)
        assert missing_from == 101
        assert missing_to == 104

    def test_first_id_no_gap(self):
        gd = GapDetector()
        assert gd.check("BTCUSDT", 9999) == (None, None)

    def test_gap_frequency_tracking(self):
        gd = GapDetector()
        gd.check("BTCUSDT", 1)
        for i in range(6):
            gd.check("BTCUSDT", 10 + i * 10)
        assert gd.should_fallback("BTCUSDT") is True

    def test_reset_clears_state(self):
        gd = GapDetector()
        gd.check("BTCUSDT", 1)
        gd.check("BTCUSDT", 100)
        gd.reset("BTCUSDT")
        assert gd.check("BTCUSDT", 1) == (None, None)

    def test_reset_all(self):
        gd = GapDetector()
        gd.check("BTCUSDT", 1)
        gd.check("ETHUSDT", 1)
        gd.reset()
        assert gd.check("BTCUSDT", 1) == (None, None)
        assert gd.check("ETHUSDT", 1) == (None, None)

    def test_multi_symbol_independence(self):
        """여러 심볼의 갭이 서로 간섭하지 않는지 확인"""
        gd = GapDetector()
        gd.check("BTCUSDT", 1000)
        gd.check("ETHUSDT", 500)
        # BTCUSDT 다음 ID는 1001이어야 하는데 ETHUSDT 500이 간섭하면 안 됨
        assert gd.check("BTCUSDT", 1001) == (None, None)
        assert gd.check("ETHUSDT", 501) == (None, None)

    def test_multi_symbol_gap_detection(self):
        """한 심볼에 갭이 있어도 다른 심볼은 영향 없음"""
        gd = GapDetector()
        gd.check("BTCUSDT", 100)
        gd.check("ETHUSDT", 200)
        # BTCUSDT에 갭 발생
        missing_from, missing_to = gd.check("BTCUSDT", 110)
        assert missing_from == 101
        assert missing_to == 109
        # ETHUSDT는 정상
        assert gd.check("ETHUSDT", 201) == (None, None)
        # BTCUSDT만 fallback 판단
        assert gd.should_fallback("ETHUSDT") is False

    def test_duplicate_id_ignored(self):
        gd = GapDetector()
        gd.check("BTCUSDT", 100)
        assert gd.check("BTCUSDT", 100) == (None, None)  # 중복
        assert gd.check("BTCUSDT", 99) == (None, None)   # out-of-order


# ── WsTradeEvent / WsAggTradeEvent ────────────────────────────────────────────

class TestWsTradeEvent:
    def _make_raw(self, lag_ms: int = 0) -> dict:
        now_ms = int(time.time() * 1000)
        return {"e": "trade", "E": now_ms, "s": "BTCUSDT", "t": 1,
                "p": "70000", "q": "0.001", "T": now_ms - lag_ms, "m": False}

    def test_parse_valid(self):
        event = msgspec.convert(self._make_raw(lag_ms=10), WsTradeEvent)
        assert event.symbol == "BTCUSDT"
        assert event.trade_id == 1
        assert isinstance(event.trade_time_ms, int)
        assert event.price == "70000"
        assert event.is_buyer_maker is False

    def test_parse_missing_field_raises(self):
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert({"e": "trade"}, WsTradeEvent)

    def test_parse_wrong_type_raises(self):
        raw = self._make_raw()
        raw["T"] = "not_an_int"
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert(raw, WsTradeEvent)

    def test_parse_event_time_optional(self):
        raw = self._make_raw()
        del raw["E"]
        event = msgspec.convert(raw, WsTradeEvent)
        assert event.event_time_ms is None

    def test_frozen(self):
        event = msgspec.convert(self._make_raw(), WsTradeEvent)
        with pytest.raises(Exception):
            event.symbol = "ETHUSDT"  # type: ignore[misc]


class TestWsAggTradeEvent:
    def _make_raw(self) -> dict:
        now_ms = int(time.time() * 1000)
        return {"e": "aggTrade", "E": now_ms, "s": "BTCUSDT",
                "a": 9999, "f": 100, "l": 105,
                "p": "70000", "q": "1.0", "m": False, "T": now_ms - 3}

    def test_parse_valid(self):
        event = msgspec.convert(self._make_raw(), WsAggTradeEvent)
        assert event.agg_trade_id == 9999
        assert event.first_trade_id == 100
        assert event.last_trade_id == 105

    def test_parse_missing_field_raises(self):
        with pytest.raises(msgspec.ValidationError):
            msgspec.convert({"e": "aggTrade", "s": "BTCUSDT"}, WsAggTradeEvent)


# ── HealthMonitor ─────────────────────────────────────────────────────────────

class TestHealthMonitor:
    def _make_event(self, lag_ms: int = 0) -> WsTradeEvent:
        now_ms = int(time.time() * 1000)
        return WsTradeEvent(
            symbol="BTCUSDT", event_type="trade", trade_id=1,
            trade_time_ms=now_ms - lag_ms, event_time_ms=now_ms,
            price="70000", quantity="0.001", is_buyer_maker=False,
        )

    def test_healthy_message(self):
        hm = HealthMonitor()
        status = hm.on_message(self._make_event(lag_ms=10))
        assert status == HealthStatus.HEALTHY

    def test_degraded_on_high_lag(self):
        hm = HealthMonitor()
        status = hm.on_message(self._make_event(lag_ms=1000))
        assert status == HealthStatus.DEGRADED

    def test_failed_on_critical_lag(self):
        hm = HealthMonitor()
        status = hm.on_message(self._make_event(lag_ms=3000))
        assert status == HealthStatus.FAILED

    def test_reset(self):
        hm = HealthMonitor()
        hm.on_message(self._make_event(lag_ms=3000))
        hm.reset()
        assert hm.status == HealthStatus.HEALTHY


# ── FallbackController ────────────────────────────────────────────────────────

class TestFallbackController:
    def test_initial_state_is_primary(self):
        fc = FallbackController()
        assert fc.is_primary is True
        assert fc.state == CollectorState.TRADE_PRIMARY

    def test_trigger_fallback(self):
        fc = FallbackController()
        fc.trigger_fallback("test")
        assert fc.is_fallback is True
        assert fc.state == CollectorState.AGG_FALLBACK

    def test_double_fallback_is_idempotent(self):
        fc = FallbackController()
        fc.trigger_fallback("first")
        fc.trigger_fallback("second")
        assert fc.is_fallback is True

    def test_rest_verified_property(self):
        fc = FallbackController()
        assert fc.rest_verified is False
        fc.trigger_fallback("test")
        fc.mark_rest_verified()
        assert fc.rest_verified is True

    def test_recovery_requires_streak_and_verification(self):
        fc = FallbackController()
        fc.trigger_fallback("test")
        fc.mark_rest_verified()
        for _ in range(60):
            fc.on_healthy_trade()
        assert fc.is_fallback is True  # cooldown 미충족

    def test_recovery_after_cooldown(self):
        fc = FallbackController()
        fc.trigger_fallback("test")
        fc._fallback_since = time.time() - 31
        fc.mark_rest_verified()
        for _ in range(50):
            fc.on_healthy_trade()
        assert fc.is_primary is True


# ── Normalizer ────────────────────────────────────────────────────────────────

class TestNormalizer:
    def _make_ws_event(self) -> WsTradeEvent:
        now_ms = int(time.time() * 1000)
        return WsTradeEvent(
            symbol="BTCUSDT", event_type="trade", trade_id=12345,
            trade_time_ms=now_ms - 5, event_time_ms=now_ms,
            price="70000.5", quantity="0.001", is_buyer_maker=False,
        )

    def _make_agg_event(self) -> WsAggTradeEvent:
        now_ms = int(time.time() * 1000)
        return WsAggTradeEvent(
            symbol="BTCUSDT", event_type="aggTrade", agg_trade_id=9999,
            first_trade_id=100, last_trade_id=105,
            trade_time_ms=now_ms - 3, event_time_ms=now_ms,
            price="70000.5", quantity="1.0", is_buyer_maker=False,
        )

    def _make_ws_raw(self) -> dict:
        """레거시 normalize_trade용 raw dict"""
        now_ms = int(time.time() * 1000)
        return {"e": "trade", "E": now_ms, "T": now_ms - 5, "s": "BTCUSDT",
                "t": 12345, "p": "70000.5", "q": "0.001", "X": "MARKET", "m": False}

    def test_normalize_trade_fields(self):
        """레거시 normalize_trade — DeprecationWarning을 발생시키며 동작은 유지."""
        raw = self._make_ws_raw()
        with pytest.warns(DeprecationWarning, match="deprecated"):
            result = normalize_trade(raw, "BTCUSDT", TradeSource.UNDOCUMENTED_TRADE)
        assert result["exchange"] == "binance"
        assert result["symbol"] == "BTCUSDT"
        assert result["source"] == TradeSource.UNDOCUMENTED_TRADE.value
        assert result["verified_by_rest"] is False
        assert result["lag_ms"] >= 0

    def test_normalize_ws_trade_fields(self):
        """normalize_ws_trade: @trade 전용 정규화"""
        result = normalize_ws_trade(self._make_ws_event(), TradeSource.UNDOCUMENTED_TRADE)
        assert result["exchange"] == "binance"
        assert result["market_type"] == "perp"
        assert result["symbol"] == "BTCUSDT"
        assert result["trade_id"] == 12345
        assert result["price"] == "70000.5"  # 문자열 유지
        assert result["size"] == "0.001"     # 문자열 유지
        assert result["is_buyer_maker"] is False
        assert result["source"] == TradeSource.UNDOCUMENTED_TRADE.value
        assert result["verified_by_rest"] is False
        assert result["reconstructed_from_agg"] is False
        assert result["source_agg_trade_id"] is None
        assert result["lag_ms"] >= 0

    def test_normalize_agg_trade_event_fields(self):
        """normalize_agg_trade_event: @aggTrade 정규화 (canonical 아님, raw event용)"""
        result = normalize_agg_trade_event(self._make_agg_event(), TradeSource.AGGTRADE_EXPANDED)
        assert result["agg_trade_id"] == 9999
        assert result["first_trade_id"] == 100
        assert result["last_trade_id"] == 105
        assert result["trade_count_est"] == 6
        assert result["price"] == "70000.5"
        assert result["total_size"] == "1.0"
        assert result["expanded"] is False
        assert result["lag_ms"] >= 0

    def test_normalize_rest_trade_fields(self):
        raw = {"id": 99999, "price": "65000.0", "qty": "0.5",
               "isBuyerMaker": True, "time": int(time.time() * 1000)}
        result = normalize_rest_trade(raw, "BTCUSDT", TradeSource.REST_GAP_FILL)
        assert result["trade_id"] == 99999
        assert result["price"] == "65000.0"  # 문자열 유지
        assert result["size"] == "0.5"       # 문자열 유지
        assert result["source"] == TradeSource.REST_GAP_FILL.value
        assert result["verified_by_rest"] is True
        assert result["reconstructed_from_agg"] is False
        assert result["source_agg_trade_id"] is None
        assert result["lag_ms"] is None

    def test_normalize_rest_trade_with_agg_source(self):
        """aggTrade 복원으로 인한 REST trade"""
        raw = {"id": 100, "price": "70000.0", "qty": "0.4",
               "isBuyerMaker": False, "time": int(time.time() * 1000)}
        result = normalize_rest_trade(
            raw, "BTCUSDT", TradeSource.REST_GAP_FILL, source_agg_trade_id=9999
        )
        assert result["trade_id"] == 100
        assert result["reconstructed_from_agg"] is True
        assert result["source_agg_trade_id"] == 9999


# ── RepairJob ─────────────────────────────────────────────────────────────────

class TestRepairJob:
    def test_to_dict(self):
        job = RepairJob(
            symbol="BTCUSDT",
            from_trade_id=100,
            to_trade_id=105,
            source_agg_trade_id=9999,
            reason="agg_trade_fallback",
        )
        d = job.to_dict()
        assert d["symbol"] == "BTCUSDT"
        assert d["from_trade_id"] == 100
        assert d["to_trade_id"] == 105

    def test_from_dict(self):
        d = {
            "symbol": "ETHUSDT",
            "from_trade_id": 200,
            "to_trade_id": 210,
            "source_agg_trade_id": 5555,
            "reason": "test",
        }
        job = RepairJob.from_dict(d)
        assert job.symbol == "ETHUSDT"
        assert job.from_trade_id == 200
        assert job.to_trade_id == 210
        assert job.source_agg_trade_id == 5555

    def test_roundtrip(self):
        original = RepairJob("BTCUSDT", 100, 105, 9999, "test")
        restored = RepairJob.from_dict(original.to_dict())
        assert original == restored

    def test_frozen(self):
        job = RepairJob("BTCUSDT", 100, 105, 9999, "test")
        with pytest.raises(AttributeError):
            job.symbol = "ETHUSDT"
