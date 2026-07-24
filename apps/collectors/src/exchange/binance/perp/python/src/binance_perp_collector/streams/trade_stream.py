import asyncio
from typing import List
import msgspec
import orjson
import websockets
from websockets.exceptions import ConnectionClosed

from common.logging import setup_logger
from messaging.producer import KafkaProducer
from binance_perp_collector.core.events import WsTradeEvent
from binance_perp_collector.core.health_monitor import HealthMonitor, HealthStatus
from binance_perp_collector.core.gap_detector import GapDetector
from binance_perp_collector.core.fallback_controller import FallbackController
from binance_perp_collector.core.normalizer import normalize_ws_trade
from schemas.market import TradeSource

logger = setup_logger(__name__)

WS_RECV_TIMEOUT = 3.0


class TradeStream:
    """Primary @trade WebSocket 스트림 처리기"""
    def __init__(self, symbols: List[str], base_ws_url: str, producer_raw: KafkaProducer, producer_canonical: KafkaProducer, health: HealthMonitor, fallback: FallbackController, gap: GapDetector):
        self.symbols = symbols
        self.url = f"{base_ws_url}/stream?streams={'/'.join(s.lower() + '@trade' for s in symbols)}"
        self.producer_raw = producer_raw
        self.producer_canonical = producer_canonical
        self.health = health
        self.fallback = fallback
        self.gap = gap
        self._stop_event = asyncio.Event()

    async def run(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    logger.info(f"🟢 Connected to Primary @trade: {'/'.join(s.lower() + '@trade' for s in self.symbols)}")
                    while not self._stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                        except asyncio.TimeoutError:
                            logger.warning("@trade receive timeout. Triggering fallback.")
                            self.fallback.trigger_fallback("@trade receive timeout")
                            continue

                        packet = orjson.loads(msg)

                        if 'data' not in packet:
                            raise ValueError(f"Error message in @trade stream: {packet}")

                        raw = packet['data']

                        # ingress boundary: dict → typed struct (C 레벨 검증)
                        try:
                            event = msgspec.convert(raw, WsTradeEvent)
                        except msgspec.ValidationError as e:
                            logger.warning(f"@trade payload validation failed: {e}. Triggering fallback.")
                            self.fallback.trigger_fallback(f"Invalid trade payload: {e}")
                            continue

                        # 1. 상태 및 갭 체크
                        status = self.health.on_message(event)
                        if status == HealthStatus.FAILED:
                            logger.warning("Trade stream health failed. Triggering fallback.")
                            self.fallback.trigger_fallback("Health monitor failed")
                            continue

                        missing_from, missing_to = self.gap.check(event.symbol, event.trade_id)
                        if self.gap.should_fallback(event.symbol):
                            self.fallback.trigger_fallback(f"Gap limit exceeded for {event.symbol}")

                        # 2. Raw 이벤트 발행 (디버깅/백업용)
                        await self.producer_raw.produce(event.symbol, raw)

                        # 3. Canonical 파이프라인으로 전송
                        # Primary 모드일 때만 canonical 발행
                        if self.fallback.is_primary:
                            canonical_trade = normalize_ws_trade(event, TradeSource.UNDOCUMENTED_TRADE)
                            await self.producer_canonical.produce(event.symbol, canonical_trade)

                        # 4. Fallback 복구 체크 (정상 메시지 수신 시)
                        if status == HealthStatus.HEALTHY:
                            self.fallback.on_healthy_trade()

            except ConnectionClosed:
                logger.warning(f"WebSocket connection closed for @trade {self.symbols}. Reconnecting in 3s...")
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Unexpected error in @trade stream: {e}")
                await asyncio.sleep(3)

    async def stop(self):
        self._stop_event.set()
