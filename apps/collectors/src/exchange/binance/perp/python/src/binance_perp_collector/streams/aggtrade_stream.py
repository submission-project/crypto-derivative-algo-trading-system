import asyncio
import msgspec
import orjson
import websockets
from websockets.exceptions import ConnectionClosed
from common.logging import setup_logger

from messaging.producer import KafkaProducer
from binance_perp_collector.core.events import WsAggTradeEvent
from binance_perp_collector.core.fallback_controller import FallbackController
from binance_perp_collector.core.repair_job import RepairJob

logger = setup_logger(__name__)


class AggTradeStream:
    """
    Fallback @aggTrade WebSocket 스트림 처리기.

    aggTrade는 개별 trade가 아닌 집계 데이터이므로 canonical에 직접 발행하지 않습니다.
    대신 fallback 상태일 때 RepairJob을 repair 토픽으로 발행하여,
    repair_worker가 REST API로 f~l 범위의 개별 trade를 복원합니다.
    """

    def __init__(
        self,
        symbols: list[str],
        base_ws_url: str,
        producer_raw: KafkaProducer,
        producer_repair: KafkaProducer,
        fallback: FallbackController,
    ):
        self.symbols = symbols
        self.url = f"{base_ws_url}/market/stream?streams={'/'.join(s.lower() + '@aggTrade' for s in symbols)}"
        self.producer_raw = producer_raw
        self.producer_repair = producer_repair
        self.fallback = fallback
        self._stop_event = asyncio.Event()

    async def run(self):
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    logger.info(
                        f"🟡 Connected to Fallback @aggTrade: {'/'.join(s.lower() + '@aggTrade' for s in self.symbols)}"
                    )
                    while not self._stop_event.is_set():
                        msg = await ws.recv()
                        packet = orjson.loads(msg)

                        if "data" not in packet:
                            logger.warning(f"Unexpected aggTrade packet: {packet}")
                            continue

                        raw = packet["data"]

                        # ingress boundary: dict → typed struct (C 레벨 검증)
                        try:
                            event = msgspec.convert(raw, WsAggTradeEvent)
                        except msgspec.ValidationError as e:
                            logger.warning(
                                f"@aggTrade payload validation failed: {e}. Skipping."
                            )
                            continue

                        # 1. Raw aggTrade 이벤트 발행 (디버깅/백업용)
                        await self.producer_raw.produce(event.symbol, raw)

                        # 2. Fallback 상태면 repair job 생성 → Redpanda 토픽으로 발행
                        if self.fallback.is_fallback:
                            job = RepairJob(
                                symbol=event.symbol,
                                from_trade_id=event.first_trade_id,
                                to_trade_id=event.last_trade_id,
                                source_agg_trade_id=event.agg_trade_id,
                                reason="agg_trade_fallback",
                            )
                            await self.producer_repair.produce(
                                event.symbol, job.to_dict()
                            )

            except ConnectionClosed:
                logger.warning(
                    f"WebSocket connection closed for @aggTrade {self.symbols}. Reconnecting in 3s..."
                )
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Unexpected error in @aggTrade stream: {e}")
                await asyncio.sleep(3)

    async def stop(self):
        self._stop_event.set()
