import asyncio
import sys
from pathlib import Path
import orjson
import websockets
import gzip

# 수집기 모듈 경로 동적 추가
exchange_root = Path("/Users/changminkim/Desktop/projects/equidice/takora-trading/apps/collectors/src/exchange")
for src in exchange_root.glob("*/perp/python/src"):
    src_text = str(src)
    if src_text not in sys.path:
        sys.path.insert(0, src_text)

from importlib import import_module

DEFAULT_OPERATIONAL_EXCHANGES = (
    "binance",
    "bybit",
    "okx",
    "bitget",
    "gate",
    "mexc",
    "kraken",
    "htx",
    "lbank",
    "bitfinex",
    "bingx",
    "kucoin",
)

async def test_exchange(exchange: str) -> None:
    print(f"\n==========================================")
    print(f"Testing WebSocket for Exchange: {exchange.upper()}")
    print(f"==========================================")
    
    module_name = f"{exchange}_perp_collector.operational"
    try:
        module = import_module(module_name)
    except ModuleNotFoundError:
        print(f"[-] 모듈 '{module_name}'을 찾을 수 없습니다. (웹소켓 연동 미구현)")
        return
        
    ws_specs = module.build_ws_specs()
    if not ws_specs:
        print(f"[-] {exchange}는 정의된 WS Spec이 없습니다. (REST 폴링만 지원하거나 검증 대기 중)")
        return
        
    for spec in ws_specs:
        print(f"[*] 연결 대상 URL: {spec.url}")
        print(f"[*] 구독 요청 메시지: {spec.subscribe_messages}")
        
        async def run_connection() -> None:
            async with websockets.connect(spec.url, ping_interval=20, ping_timeout=20) as ws:
                print("[+] 성공적으로 연결되었습니다! 구독 메시지를 전송합니다...")
                for message in spec.subscribe_messages:
                    await ws.send(orjson.dumps(message).decode())
                
                print("[*] 실시간 데이터 수신 대기 중...")
                async for raw_message in ws:
                    if isinstance(raw_message, bytes) and spec.gzip_binary:
                        try:
                            raw_message = gzip.decompress(raw_message)
                        except Exception as decompress_err:
                            print(f"[-] Gzip decompress failed: {decompress_err}")

                    if isinstance(raw_message, bytes):
                        raw_message = raw_message.decode(errors="ignore")
                    
                    print("키에몬: ", raw_message)
                    packet = orjson.loads(raw_message)
                    
                    # 1. 구독 승인(ack) 또는 메타 메시지 스킵
                    is_ack = False
                    if isinstance(packet, dict):
                        if packet.get("event") in {"subscribe", "subscribed"} or \
                           packet.get("op") in {"subscribe", "sub"} or \
                           "success" in packet or "ret_msg" in packet:
                            is_ack = True
                    
                    print("드래곤볼")
                    if is_ack:
                        print(f"[+] 구독 확인 메시지 수신: {raw_message}")
                        continue

                    print("메이슨")
                    # 2. 정규화(Normalizer) 수행 및 데이터 검증
                    normalized_events = []
                    if spec.normalizer:
                        try:
                            normalized_events = spec.normalizer(packet)
                        except Exception as norm_err:
                            print(f"[-] 정규화 수행 중 오류 발생: {norm_err}")

                    print("도라라라ㅏㄹ라ㅏㄹ")
                    # 3. 실시간 데이터 출력 후 브레이크
                    if normalized_events:
                        print(f"[+] {exchange}로부터 실시간 마켓 데이터를 수신했습니다!")
                        print(f"--- [Raw Message Snippet] ---")
                        snippet = raw_message[:500] + "..." if len(raw_message) > 500 else raw_message
                        print(snippet)
                        print("요시")
                        print(f"--- [Normalized Events] ---")
                        for idx, event in enumerate(normalized_events):
                            print(f"Event #{idx+1}: {orjson.dumps(event).decode()}")
                        break
                    else:
                        print(f"[*] 기타 수신 데이터 (스킵): {raw_message[:200]}...")
        
        try:
            # 10초 타임아웃
            await asyncio.wait_for(run_connection(), timeout=30.0)
        except asyncio.TimeoutError:
            print(f"[-] 타임아웃 (10초 경과): {exchange}로부터 데이터를 수신하지 못했습니다. (지오블락 또는 IP 차단일 수 있음)")
        except Exception as e:
            print(f"[-] {exchange} 연결 또는 데이터 수신 실패: {e}")

async def main() -> None:
    # 실행 시 특정 거래소만 명시 가능 (예: python test_ws_exchanges.py okx,bybit)
    if len(sys.argv) > 1:
        exchanges = sys.argv[1].split(",")
    else:
        exchanges = list(DEFAULT_OPERATIONAL_EXCHANGES)
        
    for exchange in exchanges:
        await test_exchange(exchange.strip().lower())

if __name__ == "__main__":
    asyncio.run(main())

