import pytest
from rust_core import FastProxyClient

@pytest.mark.asyncio
async def test_fast_proxy_single_request():
    worker_urls = [
        f"https://takora-api-proxy-vercel-function{i}.vercel.app/api/v1/proxy"
        for i in range(501, 520)
    ]

    # Rust에서 구현한 FastProxyClient 인스턴스 생성
    print("\nInitializing Rust FastProxyClient...")
    proxy = FastProxyClient(worker_urls)

    # 5개의 워커에 비동기 병렬 요청
    print("Requesting Binance Trades API via 10 random workers in Rust...")
    res = await proxy.request_fastest(
        "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=1&limit=1000",
        10,
    )

    # Assertions
    assert res is not None, "Failed to get response from proxy"
    assert len(res) > 100, f"Response too short: {len(res)} chars"
    print(f"Success! Got response (length: {len(res)})")
