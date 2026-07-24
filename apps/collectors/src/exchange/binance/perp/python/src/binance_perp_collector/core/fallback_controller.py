import time
import logging
from common.logging import setup_logger
from enum import Enum, auto

logger = setup_logger(__name__)

class CollectorState(Enum):
    TRADE_PRIMARY = auto()
    AGG_FALLBACK = auto()

class FallbackController:
    """
    Primary(@trade)와 Fallback(@aggTrade) 모드 간의 전환을 관리.
    """
    def __init__(self, recovery_cooldown_sec: float = 30.0, required_streak: int = 50):
        self._state = CollectorState.TRADE_PRIMARY
        self._fallback_since = 0.0
        self._recovery_cooldown_sec = recovery_cooldown_sec
        self._required_streak = required_streak
        self._healthy_streak = 0
        self._rest_verified = False

    @property
    def is_primary(self) -> bool:
        return self._state == CollectorState.TRADE_PRIMARY

    @property
    def is_fallback(self) -> bool:
        return self._state == CollectorState.AGG_FALLBACK

    @property
    def state(self) -> CollectorState:
        return self._state

    @property
    def rest_verified(self) -> bool:
        return self._rest_verified

    def trigger_fallback(self, reason: str):
        if self._state == CollectorState.AGG_FALLBACK:
            return
        logger.warning(f"🚨 Fallback triggered! Reason: {reason}")
        self._state = CollectorState.AGG_FALLBACK
        self._fallback_since = time.time()
        self._healthy_streak = 0
        self._rest_verified = False

    def mark_rest_verified(self):
        """REST API 갭 메꾸기가 성공적으로 완료되었음을 표시"""
        self._rest_verified = True

    def on_healthy_trade(self):
        """정상적인 trade 이벤트가 들어올 때 호출되어 복구를 시도합니다."""
        if self._state == CollectorState.TRADE_PRIMARY:
            return

        # Cooldown 체크
        if time.time() - self._fallback_since < self._recovery_cooldown_sec:
            return

        # REST 검증 체크
        if not self._rest_verified:
            return

        self._healthy_streak += 1
        if self._healthy_streak >= self._required_streak:
            self._recover_to_primary()

    def _recover_to_primary(self):
        logger.info("✅ Recovered to Primary (@trade) mode.")
        self._state = CollectorState.TRADE_PRIMARY
        self._healthy_streak = 0
        self._rest_verified = False
