import asyncio
import inspect
import os
import websockets
import rust_core
from messaging.producer import KafkaProducer

from common.logging import setup_logger
from common.env import (
    ENV_KEY_REDPANDA_BROKERS,
    ENV_KEY_BINANCE_SPOT_TOPIC_TRADES,
    ENV_KEY_BINANCE_API_KEY,
    ENV_KEY_BINANCE_SPOT_SBE_WS
)

logger = setup_logger("binance_spot_sbe_collector")

def websocket_header_kwargs(headers: dict[str, str]) -> dict:
    params = inspect.signature(websockets.connect).parameters
    if "additional_headers" in params:
        return {"additional_headers": headers}
    return {"extra_headers": headers}

async def recv_ws_raw(ws) -> bytes | str:
    try:
        return await ws.recv(decode=False)
    except TypeError:
        return await ws.recv()

async def collect_sbe_trades(symbol: str, api_key: str, producer: KafkaProducer):
    stream = f"{symbol.lower()}@trade"
    url = f"{os.environ[ENV_KEY_BINANCE_SPOT_SBE_WS]}/ws/{stream}"
    headers = {"X-MBX-APIKEY": api_key}

    logger.info(f"[SBE-WS] Connecting to: {url}")
    while True:
        try:
            async with websockets.connect(
                url,
                **websocket_header_kwargs(headers),
                ping_interval=None,
                max_size=None,
                close_timeout=1,
            ) as ws:
                logger.info(f"[SBE-WS] Connected to {url}")
                
                while True:
                    frame = await recv_ws_raw(ws)
                    if isinstance(frame, str):
                        logger.warning(f"[SBE-WS] Text frame received: {frame}")
                        continue
                    
                    try:
                        # rust_core.parser.parse_binance_spot_sbe_trades_rs returns a list of dictionaries
                        parsed_entries = rust_core.parser.parse_binance_spot_sbe_trades_rs(frame)
                        
                        if parsed_entries:
                            # 비동기로 Redpanda에 전송
                            await producer.send_messages(parsed_entries)
                            
                    except Exception as e:
                        logger.error(f"[SBE-WS] Parse error: {e}")
                        continue
                        
        except asyncio.CancelledError:
            logger.info("Collector cancelled.")
            break
        except Exception as e:
            logger.error(f"[SBE-WS] Connection error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)

async def main():
    logger.info("🟢 Starting Binance Spot SBE Collector...")
    
    # 환경변수 로드
    api_key = os.getenv(ENV_KEY_BINANCE_API_KEY)
    brokers = os.getenv(ENV_KEY_REDPANDA_BROKERS) 
    topic = os.getenv(ENV_KEY_BINANCE_SPOT_TOPIC_TRADES)

    symbol = os.getenv("BINANCE_SYMBOL_BTCUSDT", "BTCUSDT")

    assert brokers and topic and api_key and producer

    producer = KafkaProducer(bootstrap_servers=brokers, topic=topic)
    await producer.connect()

    try:
        # 무한 수집 루프 실행
        await collect_sbe_trades(symbol, api_key, producer)
    finally:
        await producer.stop()
        logger.info("🔴 Binance Spot SBE Collector stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
