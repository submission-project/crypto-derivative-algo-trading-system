use rust_core::network::fast_proxy::FastProxyClient;

#[tokio::test]
async fn test_fast_proxy_binance() {
    let worker_urls: Vec<String> = (1..=100)
        .map(|i| format!("https://takora-api-proxy-vercel-function{}.vercel.app/api/v1/proxy", i))
        .collect();

    let proxy = FastProxyClient::new(worker_urls);

    // Python이 개입하지 않는 순수 Rust 비동기 함수 호출
    let res = proxy
        .request_fastest_rs("https://fapi.binance.com/fapi/v1/trades?symbol=btcusdt&limit=1000", 5, None, None, None)
        .await;

    assert!(
        res.is_some(),
        "FastProxyClient failed to return a response from Vercel workers"
    );

    let body = res.unwrap();
    // println!("Fastest response body (first 200 chars): {}", &body.chars().take(200).collect::<String>());

    assert!(
        body.starts_with('[') || body.starts_with('{'),
        "Response body does not look like JSON"
    );
    
    let urls: Vec<String> = vec![
        "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=1&limit=10".to_string(),
        "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT".to_string(),
        "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT".to_string(),
        "https://fapi.binance.com/fapi/v1/trades?symbol=btcusdt&limit=1000".to_string(),
        "https://api.coinmarketcap.com/data-api/v3/exchange/listing?exType=1&limit=1000".to_string(),
        "https://fapi.binance.com/fapi/v1/trades?symbol=ethusdt&limit=1000".to_string(),
    ];

    let batch_res = proxy
        .request_fastest_batch_rs(urls, 5, None, None)
        .await;

    assert!(
        !batch_res.is_empty(),
        "FastProxyClient batch request failed to return results"
    );

    for (url, response) in batch_res {
        assert!(response.is_some(), "Failed to get response for {}", url);
        let body_str = response.unwrap();
        println!("Response for {}: {}", url, &body_str.chars().take(100).collect::<String>());
        assert!(
            body_str.starts_with('[') || body_str.starts_with('{'),
            "Response body does not look like JSON"
        );
    }
}
