import asyncio
import time

# 방금 uv add 로 설치한 rust_core 패키지를 임포트합니다.
from rust_core import FastProxyClient

async def main():
    # 프록시 풀 설정 (실제 워커 URL로 교체)
    worker_urls = [
        f"https://takora-api-proxy-vercel-function{i}.vercel.app/api/v1/proxy"
        for i in range(1, 15)
    ]
    
    print("🚀 [apps/test] rust_core 모듈 로드 성공!")
    print("="*50)
    
    # rust_core 패키지에서 가져온 객체 초기화
    proxy = FastProxyClient(worker_urls)
    target_url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    
    print(f"📡 타겟 URL: {target_url}")
    print("⚡️ Rust 비동기 백그라운드 엔진을 통해 데이터 요청 중...")
    
    start_time = time.time()
    
    # rust_core의 메소드 실행
    response = await proxy.request_fastest(
        target_url=target_url, 
        sample_size=3
    )
    
    elapsed = time.time() - start_time
    
    print(f"⏱️ 소요 시간: {elapsed:.3f}초")
    print("✅ 결과 데이터:")
    print(response)
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main())
