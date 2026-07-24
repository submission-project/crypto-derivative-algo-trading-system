import pytest
import time
from rust_core import FastProxyClient

@pytest.mark.asyncio
async def test_fast_proxy_batch_request():
    worker_urls = [
        f"https://takora-api-proxy-vercel-function{i}.vercel.app/api/v1/proxy"
        for i in range(1, 100)
    ]

    proxy = FastProxyClient(worker_urls)

    target_urls = [
        "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=1&limit=10",
        "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
        "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT",
        "https://fapi.binance.com/fapi/v1/trades?symbol=btcusdt&limit=1000",
        "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=1&limit=1000",
        "https://fapi.binance.com/fapi/v1/trades?symbol=ethusdt&limit=1000",
    ]

    print(f"\nRequesting {len(target_urls)} URLs concurrently via Rust FastProxyClient...")
    
    start_time = time.time()
    results = await proxy.request_fastest_batch(
        target_urls=target_urls,
        sample_size=10, 
    )
    elapsed = time.time() - start_time

    print(f"All requests completed in {elapsed:.3f} seconds.")
    
    # Assertions
    assert len(results) == len(target_urls), f"Expected {len(target_urls)} results, got {len(results)}"
    
    success_count = 0
    for url, response in results.items():
        if response:
            success_count += 1
            assert len(response) > 0, f"Response for {url} is empty"
    
    print(f"Success count: {success_count}/{len(target_urls)}")
    assert success_count > 0, "No requests succeeded"
