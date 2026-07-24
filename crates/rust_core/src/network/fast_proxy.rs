use futures::stream::{FuturesUnordered, StreamExt}; // Python의 async, await랑 비슷
use pyo3::prelude::*; // 파이썬과 연동하기 위해 필요한 모듈
use rand::seq::SliceRandom; // Python의 random.sample랑 비슷
use reqwest::{Client, StatusCode}; // Python의 requests랑 비슷
use serde::Serialize; // Rust 구조체를 JSON으로 바꾸기 위해 사용
use std::time::Duration; // Python의 time.sleep랑 비슷

use std::collections::HashMap;

#[derive(Serialize)]
struct ProxyPayload<'a> {
    url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<&'a HashMap<String, String>>,
}

#[pyclass] // 이 구조체를 파이썬에서 사용할 수 있도록
#[derive(Clone)] // 인스턴스를 복제할 수 있도록 (자료형에 추가해줌)
pub struct FastProxyClient {
    worker_urls: Vec<String>,
    client: Client,
}

// 1. 순수 Rust 구현부 (파이썬 종속성 없음, 순수 Rust 테스트 가능)
impl FastProxyClient {
    pub async fn request_fastest_rs(
        &self,
        target_url: &str,
        sample_size: usize,
        method: Option<String>,
        body: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> Option<String> {
        let sampled_urls: Vec<String> = {
            let mut rng = rand::thread_rng();
            self.worker_urls
                .choose_multiple(&mut rng, sample_size)
                .cloned()
                .collect()
        };

        let mut tasks = FuturesUnordered::new();

        for url in sampled_urls {
            let client_c = self.client.clone();
            let url_c = url.clone();
            let target_c = target_url.to_string();
            let method_c = method.clone();
            let body_c = body.clone();
            let headers_c = headers.clone();

            tasks.push(async move {
                let payload = ProxyPayload {
                    url: &target_c,
                    method: method_c.as_deref(),
                    body: body_c.as_ref(),
                    headers: headers_c.as_ref(),
                };
                let res = client_c.post(&url_c).json(&payload).send().await.ok()?;
                if res.status() == StatusCode::OK {
                    res.text().await.ok()
                } else {
                    None
                }
            });
        }

        while let Some(res) = tasks.next().await {
            if let Some(data) = res {
                return Some(data);
            }
        }
        None
    }

    pub async fn request_fastest_batch_rs(
        &self,
        target_urls: Vec<String>,
        sample_size: usize,
        method: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> HashMap<String, Option<String>> {
        let mut tasks = Vec::new();

        for target_url in target_urls {
            let method_c = method.clone();
            let headers_c = headers.clone();
            let proxy_clone = self.clone();
            
            tasks.push(async move {
                let res = proxy_clone.request_fastest_rs(&target_url, sample_size, method_c, None, headers_c).await;
                (target_url, res)
            });
        }

        let results = futures::future::join_all(tasks).await;
        results.into_iter().collect()
    }
}

// 2. 파이썬 바인딩 구현부
#[pymethods]
impl FastProxyClient {
    #[new]
    pub fn new(worker_urls: Vec<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(1))
            .pool_max_idle_per_host(20)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            worker_urls,
            client,
        }
    }

    /// 파이썬 비동기 런타임(asyncio)과 통합
    #[pyo3(signature = (target_url, sample_size, method=None, body=None, headers=None))]
    #[pyo3(name = "request_fastest")]
    pub fn request_fastest_py<'a>(
        &self,
        py: Python<'a>,
        target_url: String,
        sample_size: usize,
        method: Option<String>,
        body: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        // 백그라운드 스레드로 넘기기 위해 인스턴스를 Clone 합니다.
        let proxy_clone = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match proxy_clone.request_fastest_rs(&target_url, sample_size, method, body, headers).await {
                Some(data) => Ok(Some(data)),
                None => Ok(None::<String>),
            }
        })
    }

    /// 여러 타겟 URL에 대해 동시에 프록시 요청을 보냄
    #[pyo3(signature = (target_urls, sample_size, method=None, headers=None))]
    #[pyo3(name = "request_fastest_batch")]
    pub fn request_fastest_batch_py<'a>(
        &self,
        py: Python<'a>,
        target_urls: Vec<String>,
        sample_size: usize,
        method: Option<String>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let proxy_clone = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = proxy_clone.request_fastest_batch_rs(target_urls, sample_size, method, headers).await;
            Ok(results)
        })
    }
}