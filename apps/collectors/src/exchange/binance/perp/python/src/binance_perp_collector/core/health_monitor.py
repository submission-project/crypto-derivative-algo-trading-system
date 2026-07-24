from enum import Enum, auto
from common.time import current_time_ms
from binance_perp_collector.core.events import WsTradeEvent


class HealthStatus(Enum):
    HEALTHY = auto()
    DEGRADED = auto()
    FAILED = auto()


class HealthMonitor:
    """
    WebSocket 스트림의 상태를 모니터링합니다.
    - lag (trade_time_ms 대비 로컬 수신 시간)

    payload 검증은 WsTradeEvent.parse()가 담당하므로
    여기서는 lag 계산만 합니다.
    """

    def __init__(self, critical_lag_ms: int = 2000, degraded_lag_ms: int = 500):
        self._critical_lag_ms = critical_lag_ms
        self._degraded_lag_ms = degraded_lag_ms
        self._status = HealthStatus.HEALTHY

    def on_message(self, event: WsTradeEvent) -> HealthStatus:
        lag_ms = current_time_ms() - event.trade_time_ms

        if lag_ms > self._critical_lag_ms:
            self._status = HealthStatus.FAILED
        elif lag_ms > self._degraded_lag_ms:
            self._status = HealthStatus.DEGRADED
        else:
            self._status = HealthStatus.HEALTHY

        return self._status

    @property
    def status(self) -> HealthStatus:
        return self._status

    def reset(self):
        self._status = HealthStatus.HEALTHY
