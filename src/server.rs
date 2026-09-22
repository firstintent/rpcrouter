use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::error;

use crate::{
    admin::AdminState,
    forward::{Forwarder, all_endpoints_exhausted},
    guard::{self, BearerAuth, IpRateLimiter},
    metrics::Metrics,
    registry::Registry,
};

#[derive(Clone)]
pub struct AppState {
    registry: Arc<Registry>,
    forwarder: Arc<Forwarder>,
    metrics: Arc<Metrics>,
    batch_limit: usize,
    metrics_enabled: bool,
    max_body_bytes: usize,
    concurrency: Arc<Semaphore>,
    rate_limiter: Option<Arc<IpRateLimiter>>,
    metrics_auth: Option<BearerAuth>,
    admin: Option<AdminState>,
}

impl AppState {
    pub fn new(registry: Arc<Registry>, forwarder: Arc<Forwarder>, batch_limit: usize) -> Self {
        let metrics = forwarder.metrics();
        Self {
            registry,
            forwarder,
            metrics,
            batch_limit,
            metrics_enabled: true,
            max_body_bytes: crate::config::DEFAULT_MAX_BODY_BYTES,
            concurrency: Arc::new(Semaphore::new(
                crate::config::DEFAULT_MAX_CONCURRENT_REQUESTS,
            )),
            rate_limiter: None,
            metrics_auth: None,
            admin: None,
        }
    }

    pub fn with_metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// 应用服务器加固参数（请求体上限、并发上限、每 IP 限速、/metrics 鉴权）。
    pub fn with_hardening(
        mut self,
        max_body_bytes: usize,
        max_concurrent_requests: usize,
        per_ip_rate_limit: Option<(u64, u64)>,
        metrics_auth_token: Option<String>,
    ) -> Self {
        self.max_body_bytes = max_body_bytes;
        self.concurrency = Arc::new(Semaphore::new(max_concurrent_requests));
        if let Some((rps, burst)) = per_ip_rate_limit {
            let limiter = Arc::new(IpRateLimiter::new(rps, burst));
            limiter.spawn_housekeeping();
            self.rate_limiter = Some(limiter);
        }
        self.metrics_auth = metrics_auth_token.map(|token| BearerAuth::new(&token));
        self
    }

    pub fn with_admin(mut self, admin: AdminState) -> Self {
        self.admin = Some(admin);
        self
    }
}

pub fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/healthz", get(healthz))
        .route("/chains", get(chains))
        .route("/rpc/{chain_id}", post(rpc));

    // /metrics 支持可选的 bearer token 鉴权。
    if let Some(auth) = state.metrics_auth.clone() {
        router = router.route("/metrics", get(metrics).layer(auth));
    } else {
        router = router.route("/metrics", get(metrics));
    }

    let admin = state.admin.clone();
    let mut router = router.with_state(state);
    if let Some(admin) = admin {
        router = router.merge(crate::admin::router(admin));
    }
    router
}

/// 构建带入口防护层（请求体上限/并发背压/每 IP 限速）的服务。
///
/// 防护层必须包在 Router 外层（Router::layer 需要 Sync，而防护层不是），
/// 故用 tower::ServiceBuilder 直接包裹 Router 服务。
pub fn guarded_service_from_state(
    state: AppState,
) -> tower::util::BoxCloneService<
    axum::http::Request<axum::body::Body>,
    Response,
    std::convert::Infallible,
> {
    let guard_metrics = Arc::clone(&state.metrics);
    let concurrency = Arc::clone(&state.concurrency);
    let rate_limiter = state.rate_limiter.clone();
    let max_body_bytes = state.max_body_bytes;
    let router = router(state);
    tower::ServiceBuilder::new()
        .layer_fn(move |inner| {
            guard::guarded_service(
                inner,
                max_body_bytes,
                Arc::clone(&concurrency),
                Arc::clone(&guard_metrics),
                rate_limiter.clone(),
            )
        })
        .service(router)
}

async fn healthz() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn chains(State(state): State<AppState>) -> Json<Value> {
    Json(json!({"chains": state.registry.summaries().await}))
}

async fn metrics(State(state): State<AppState>) -> Response {
    if !state.metrics_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.metrics.encode(&state.registry).await {
        Ok(body) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(error) => {
            error!(error = %error, "metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn rpc(Path(chain_id): Path<u64>, State(state): State<AppState>, body: Bytes) -> Response {
    // 路由层解析链一次（batch 也只做一次）。
    // 语义边界：未知链 404 / 禁用链 403 / 0 端点链 503，均为入口拒绝，
    // 计入 ingress_rejected，不计 user_visible_errors。
    let request: Box<RawValue> = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return Json(jsonrpc_error(Value::Null, -32700, "Parse error")).into_response(),
    };
    let request_is_batch = request.get().trim_start().starts_with('[');
    let ingress_error = |status: StatusCode, error: Value| {
        if request_is_batch
            && let Ok(batch) = serde_json::from_str::<Vec<Box<RawValue>>>(request.get())
        {
            return (
                status,
                Json(Value::Array(
                    batch
                        .iter()
                        .map(|item| {
                            let id = request_id(item);
                            let mut item_error = error.clone();
                            item_error["id"] = id;
                            item_error
                        })
                        .collect(),
                )),
            )
                .into_response();
        }
        (status, Json(error)).into_response()
    };
    let resolved = state.registry.resolve_for_request(chain_id).await;
    let Some(chain_state) = resolved else {
        // 未知链 → 404。
        state.metrics.record_ingress_rejected("unknown_chain");
        return ingress_error(
            StatusCode::NOT_FOUND,
            jsonrpc_error(
                Value::Null,
                -32000,
                &format!("rpcrouter: unknown chain id {chain_id}"),
            ),
        );
    };
    if chain_state.state_label() == crate::registry::ChainStateLabel::Disabled {
        state.metrics.record_ingress_rejected("chain_disabled");
        return ingress_error(
            StatusCode::FORBIDDEN,
            jsonrpc_error(
                Value::Null,
                -32000,
                &format!("rpcrouter: chain {chain_id} is disabled"),
            ),
        );
    }
    // 0 端点链 → 503。配了付费兜底时仍放行，由转发层在公共池为空后试一次。
    let endpoint_count = state.registry.endpoint_count(chain_id).await;
    if endpoint_count == 0 && !state.registry.fallback_configured(chain_id) {
        state.metrics.record_ingress_rejected("no_endpoints");
        return ingress_error(
            StatusCode::SERVICE_UNAVAILABLE,
            jsonrpc_error(
                Value::Null,
                -32000,
                &format!("rpcrouter: chain {chain_id} has no public endpoints"),
            ),
        );
    }

    match request.get().trim_start().as_bytes().first() {
        Some(b'{') => Json(state.forwarder.execute_raw(chain_id, &request).await).into_response(),
        Some(b'[') => {
            let requests: Vec<Box<RawValue>> = match serde_json::from_str(request.get()) {
                Ok(requests) => requests,
                Err(_) => {
                    return Json(jsonrpc_error(Value::Null, -32700, "Parse error")).into_response();
                }
            };
            if requests.is_empty() {
                return Json(jsonrpc_error(
                    Value::Null,
                    -32600,
                    "Invalid Request: batch must not be empty",
                ))
                .into_response();
            }
            if requests.len() > state.batch_limit {
                return Json(jsonrpc_error(
                    Value::Null,
                    -32600,
                    &format!(
                        "Invalid Request: batch exceeds limit of {}",
                        state.batch_limit
                    ),
                ))
                .into_response();
            }
            Json(execute_batch(chain_id, &state, requests).await).into_response()
        }
        _ => Json(jsonrpc_error(Value::Null, -32600, "Invalid Request")).into_response(),
    }
}

async fn execute_batch(chain_id: u64, state: &AppState, requests: Vec<Box<RawValue>>) -> Value {
    // 解析链一次（batch 中共享），在调用方已解析。
    let mut results = vec![None; requests.len()];
    let request_ids: Vec<_> = requests.iter().map(|request| request_id(request)).collect();
    let mut tasks = JoinSet::new();

    for (index, request) in requests.into_iter().enumerate() {
        if !request.get().trim_start().starts_with('{') {
            results[index] = Some(jsonrpc_error(Value::Null, -32600, "Invalid Request"));
            continue;
        }
        let forwarder = Arc::clone(&state.forwarder);
        tasks.spawn(async move { (index, forwarder.execute_raw(chain_id, &request).await) });
    }

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok((index, response)) => results[index] = Some(response),
            Err(join_error) => error!(error = %join_error, "batch forwarding task failed"),
        }
    }

    Value::Array(
        results
            .into_iter()
            .enumerate()
            .map(|(index, response)| {
                response.unwrap_or_else(|| {
                    all_endpoints_exhausted(chain_id, request_ids[index].clone())
                })
            })
            .collect(),
    )
}

fn request_id(request: &RawValue) -> Value {
    #[derive(Deserialize)]
    struct IdOnly {
        #[serde(default)]
        id: Value,
    }

    serde_json::from_str::<IdOnly>(request.get()).map_or(Value::Null, |request| request.id)
}

fn jsonrpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        body::{Body, to_bytes},
        extract::{Path, State},
        http::{Request, StatusCode, header::CONTENT_TYPE},
        response::{IntoResponse, Response},
    };
    use tower::ServiceExt;

    use crate::{
        chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
        config::{ChainOverride, Config, DiscoveryConfig, UpstreamConfig},
    };

    use super::*;

    #[derive(Clone)]
    struct MockState(Arc<AtomicUsize>);

    async fn mock_upstream(
        Path(behavior): Path<String>,
        State(state): State<MockState>,
        body: Bytes,
    ) -> Response {
        state.0.fetch_add(1, Ordering::SeqCst);
        match behavior.as_str() {
            "limited" => StatusCode::TOO_MANY_REQUESTS.into_response(),
            "failure" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            "html" => (
                StatusCode::OK,
                [(CONTENT_TYPE, "text/html")],
                "<html>not an RPC response</html>",
            )
                .into_response(),
            "slow" => {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Json(json!({"jsonrpc":"2.0", "id":1, "result":"too late"})).into_response()
            }
            "rpc" => {
                let request: Value = serde_json::from_slice(&body).expect("single JSON request");
                assert!(request.is_object(), "batch must be split before forwarding");
                let method = request["method"].as_str().unwrap_or_default();
                if method == "fail_method" {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                match method {
                    "slow_method" => tokio::time::sleep(Duration::from_millis(30)).await,
                    "medium_method" => tokio::time::sleep(Duration::from_millis(15)).await,
                    _ => {}
                }
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": request.get("id").cloned().unwrap_or(Value::Null),
                    "result": method
                }))
                .into_response()
            }
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn spawn_mock() -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/{behavior}", post(mock_upstream))
            .with_state(MockState(Arc::clone(&calls)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local mock");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock");
        });
        (format!("http://{address}"), calls)
    }

    fn test_config(timeout_ms: u64) -> Config {
        Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: UpstreamConfig {
                request_timeout_ms: timeout_ms,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 4,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        }
    }

    async fn test_app(urls: &[String], timeout_ms: u64) -> Router {
        let config = test_config(timeout_ms);
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test Chain".to_owned(),
                    endpoints: urls.to_vec(),
                }],
            })
            .await;
        for endpoint in registry.all_endpoints(1).await {
            let latency = if endpoint.url().ends_with("/failure") {
                Duration::from_millis(1)
            } else {
                Duration::from_millis(10)
            };
            endpoint.record_success(tokio::time::Instant::now(), latency, true);
            endpoint.record_success(tokio::time::Instant::now(), latency, true);
        }
        let forwarder = Arc::new(
            Forwarder::new(Arc::clone(&registry), &config).expect("create test forwarder"),
        );
        router(AppState::new(
            registry,
            forwarder,
            config.server.batch_limit,
        ))
    }

    async fn post_json(app: Router, path: &str, value: Value) -> Value {
        let response = app
            .oneshot(
                Request::post(path)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(value.to_string()))
                    .expect("request"),
            )
            .await
            .expect("router response");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    #[tokio::test]
    async fn retries_a_different_endpoint_after_failure() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(&[format!("{base}/failure"), format!("{base}/rpc")], 100).await;
        let response = post_json(
            app,
            "/rpc/1",
            json!({"jsonrpc":"2.0", "id":9, "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(response["id"], 9);
        assert_eq!(response["result"], "eth_blockNumber");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn returns_gateway_error_after_all_failure_signals() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(
            &[
                format!("{base}/limited"),
                format!("{base}/failure"),
                format!("{base}/html"),
                format!("{base}/slow"),
            ],
            20,
        )
        .await;
        let response = post_json(
            app,
            "/rpc/1",
            json!({"jsonrpc":"2.0", "id":"request-a", "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(response["id"], "request-a");
        assert_eq!(response["error"]["code"], -32000);
        assert_eq!(
            response["error"]["message"],
            "rpcrouter: all upstream endpoints exhausted for chain 1"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn splits_batch_and_restores_original_order() {
        let (base, calls) = spawn_mock().await;
        let app = test_app(&[format!("{base}/rpc")], 100).await;
        let response = post_json(
            app,
            "/rpc/1",
            json!([
                {"jsonrpc":"2.0", "id":1, "method":"slow_method", "params":[]},
                {"jsonrpc":"2.0", "id":2, "method":"fast_method", "params":[]},
                {"jsonrpc":"2.0", "id":3, "method":"medium_method", "params":[]}
            ]),
        )
        .await;
        let responses = response.as_array().expect("batch response");
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["id"], 2);
        assert_eq!(responses[2]["id"], 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn mixed_batch_attributes_probation_failure_as_cold_start() {
        let (base, _) = spawn_mock().await;
        let config = test_config(100);
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Cold Batch".to_owned(),
                    endpoints: vec![format!("{base}/rpc")],
                }],
            })
            .await;
        let forwarder =
            Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("create forwarder"));
        let metrics = forwarder.metrics();
        let app = router(AppState::new(
            Arc::clone(&registry),
            forwarder,
            config.server.batch_limit,
        ));
        let response = post_json(
            app,
            "/rpc/1",
            json!([
                {"jsonrpc":"2.0", "id":1, "method":"ok_method", "params":[]},
                {"jsonrpc":"2.0", "id":2, "method":"fail_method", "params":[]}
            ]),
        )
        .await;
        let items = response.as_array().expect("batch");
        assert_eq!(items[0]["result"], "ok_method");
        assert_eq!(items[1]["error"]["data"]["reason"], "cold_start");
        assert_eq!(registry.user_visible_errors(), 0);
        let encoded = metrics.encode(&registry).await.expect("metrics");
        assert!(encoded.contains("rpcrouter_cold_start_failures_total{chain_id=\"1\"} 1"));
    }

    #[tokio::test]
    async fn exposes_health_and_chain_pool_summary() {
        let (base, _) = spawn_mock().await;
        let app = test_app(&[format!("{base}/rpc")], 100).await;
        let health = app
            .clone()
            .oneshot(
                Request::get("/healthz")
                    .body(Body::empty())
                    .expect("health request"),
            )
            .await
            .expect("health response");
        assert_eq!(health.status(), StatusCode::OK);

        let chains = app
            .clone()
            .oneshot(
                Request::get("/chains")
                    .body(Body::empty())
                    .expect("chains request"),
            )
            .await
            .expect("chains response");
        let body = to_bytes(chains.into_body(), usize::MAX)
            .await
            .expect("chains body");
        let value: Value = serde_json::from_slice(&body).expect("chains JSON");
        assert_eq!(value["chains"][0]["chainId"], 1);
        assert_eq!(value["chains"][0]["active"], 1);

        let metrics = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers()[CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics body");
        let body = String::from_utf8(body.to_vec()).expect("metrics text");
        assert!(body.contains("rpcrouter_endpoint_state"));
    }

    // ── acceptance c: 语义边界 ──

    fn v2_test_config() -> Config {
        Config {
            chains: vec![1], // pinned
            discovery: DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            upstream: UpstreamConfig {
                request_timeout_ms: 100,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 2,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        }
    }

    async fn v2_test_app_with_catalog(catalog: Catalog) -> Router {
        let config = v2_test_config();
        let registry = Arc::new(Registry::new(&config));
        registry.set_catalog(Arc::new(catalog)).await;
        // 预激活 pinned 链。
        let _ = registry.resolve_for_request(1).await;
        let forwarder = Arc::new(
            Forwarder::new(Arc::clone(&registry), &config).expect("create test forwarder"),
        );
        router(AppState::new(
            registry,
            forwarder,
            config.server.batch_limit,
        ))
    }

    async fn post_json_status(app: Router, path: &str, value: Value) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                Request::post(path)
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(value.to_string()))
                    .expect("request"),
            )
            .await
            .expect("router response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body)
    }

    #[tokio::test]
    async fn unknown_chain_returns_404_with_jsonrpc_error() {
        let catalog = Catalog {
            chains: vec![],
            by_id: std::collections::HashMap::new(),
        };
        let app = v2_test_app_with_catalog(catalog).await;

        let (status, body) = post_json_status(
            app,
            "/rpc/999999",
            json!({"jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], -32000);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown chain")
        );
    }

    #[tokio::test]
    async fn zero_endpoint_chain_returns_503() {
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 127,
                name: "Empty".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![], // 0 端点
            }],
            by_id: std::collections::HashMap::from([(127, 0)]),
        };
        let app = v2_test_app_with_catalog(catalog).await;

        let (status, body) = post_json_status(
            app,
            "/rpc/127",
            json!({"jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["code"], -32000);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("no public endpoints")
        );
    }

    #[tokio::test]
    async fn empty_public_pool_uses_configured_fallback() {
        #[derive(Clone)]
        struct Hits(Arc<AtomicUsize>);
        async fn ok_rpc(State(hits): State<Hits>, body: Bytes) -> Response {
            hits.0.fetch_add(1, Ordering::SeqCst);
            let id = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|value| value.get("id").cloned())
                .unwrap_or(Value::Null);
            Json(json!({"jsonrpc":"2.0","id":id,"result":"0x10"})).into_response()
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/", post(ok_rpc))
            .with_state(Hits(Arc::clone(&hits)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fallback");
        let address = listener.local_addr().expect("fallback address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fallback");
        });

        let mut config = v2_test_config();
        config.chain_overrides.push(ChainOverride {
            chain_id: 127,
            fallback_url: Some(format!("http://{address}/")),
            ..ChainOverride::default()
        });
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 127,
                name: "Empty".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![],
            }],
            by_id: std::collections::HashMap::from([(127, 0)]),
        };
        let registry = Arc::new(Registry::new(&config));
        registry.set_catalog(Arc::new(catalog)).await;
        let forwarder =
            Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("forwarder"));
        let app = router(AppState::new(
            registry,
            forwarder,
            config.server.batch_limit,
        ));
        let (status, body) = post_json_status(
            app,
            "/rpc/127",
            json!({"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"], "0x10");
        assert_eq!(body["id"], 7);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deny_chain_returns_403() {
        let config = Config {
            chains: vec![],
            discovery: DiscoveryConfig {
                enabled: true,
                deny: vec![13],
                ..Default::default()
            },
            upstream: UpstreamConfig {
                request_timeout_ms: 100,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 2,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        };
        let registry = Arc::new(Registry::new(&config));
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 13,
                name: "Blocked".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![CatalogEndpoint {
                    url: "https://rpc.blocked.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: std::collections::HashMap::from([(13, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;
        let forwarder = Arc::new(
            Forwarder::new(Arc::clone(&registry), &config).expect("create test forwarder"),
        );
        let app = router(AppState::new(
            registry,
            forwarder,
            config.server.batch_limit,
        ));

        let (status, body) = post_json_status(
            app,
            "/rpc/13",
            json!({ "jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[] }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["code"], -32000);
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("disabled")
        );
    }

    #[tokio::test]
    async fn semantic_boundary_ingress_rejected_not_user_visible_errors() {
        // 验证：未知链/0端点/deny 错误计入 ingress_rejected，不计入 user_visible_errors。
        let config = Config {
            chains: vec![],
            discovery: DiscoveryConfig {
                enabled: true,
                deny: vec![13],
                ..Default::default()
            },
            upstream: UpstreamConfig {
                request_timeout_ms: 100,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 2,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        };
        let registry = Arc::new(Registry::new(&config));
        let catalog = Catalog {
            chains: vec![
                CatalogChain {
                    chain_id: 13,
                    name: "Blocked".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: "https://rpc.blocked.example".to_owned(),
                        tracking: None,
                    }],
                },
                CatalogChain {
                    chain_id: 127,
                    name: "Empty".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![],
                },
            ],
            by_id: std::collections::HashMap::from([(13, 0), (127, 1)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;
        let forwarder = Arc::new(
            Forwarder::new(Arc::clone(&registry), &config).expect("create test forwarder"),
        );
        let metrics = forwarder.metrics();
        let app = router(AppState::new(
            Arc::clone(&registry),
            forwarder,
            config.server.batch_limit,
        ));

        let uve_before = registry.user_visible_errors();

        // 未知链。
        let _ = post_json_status(
            app.clone(),
            "/rpc/999999",
            json!({ "jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[] }),
        )
        .await;
        // 0 端点链。
        let _ = post_json_status(
            app.clone(),
            "/rpc/127",
            json!({ "jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[] }),
        )
        .await;
        // deny 链。
        let _ = post_json_status(
            app.clone(),
            "/rpc/13",
            json!({ "jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[] }),
        )
        .await;

        // user_visible_errors 不增。
        assert_eq!(registry.user_visible_errors(), uve_before);

        // ingress_rejected 被记录（通过 metrics 编码验证）。
        let encoded = metrics.encode(&registry).await.expect("encode");
        assert!(encoded.contains("rpcrouter_ingress_rejected_total{reason=\"unknown_chain\"}"));
        assert!(encoded.contains("rpcrouter_ingress_rejected_total{reason=\"no_endpoints\"}"));
        assert!(encoded.contains("rpcrouter_ingress_rejected_total{reason=\"chain_disabled\"}"));
    }

    #[tokio::test]
    async fn dormant_chain_cold_start_serves_and_becomes_hot() {
        let (base, calls) = spawn_mock().await;
        let config = Config {
            chains: vec![],
            discovery: DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            upstream: UpstreamConfig {
                request_timeout_ms: 100,
                slow_threshold_ms: 50,
                deadline_ms: 500,
                max_attempts: 2,
                default_rps: 100,
                default_concurrency: 8,
            },
            ..Config::default()
        };
        let registry = Arc::new(Registry::new(&config));
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 777,
                    name: "Dynamic".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: format!("{base}/rpc"),
                        tracking: None,
                    }],
                }],
                by_id: std::collections::HashMap::from([(777, 0)]),
            }))
            .await;
        assert_eq!(registry.chain_counts().await.dormant, 1);
        let forwarder =
            Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("forwarder"));
        let app = router(AppState::new(
            Arc::clone(&registry),
            forwarder,
            config.server.batch_limit,
        ));
        let (status, body) = post_json_status(
            app,
            "/rpc/777",
            json!({"jsonrpc":"2.0", "id":1, "method":"eth_blockNumber", "params":[]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["result"], "eth_blockNumber");
        assert_eq!(registry.chain_counts().await.hot, 1);
        assert_eq!(registry.user_visible_errors(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn batch_semantic_rejection_resolves_chain_once() {
        let config = Config {
            chains: vec![],
            discovery: DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Arc::new(Registry::new(&config));
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 127,
                    name: "Empty".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![],
                }],
                by_id: std::collections::HashMap::from([(127, 0)]),
            }))
            .await;
        let forwarder =
            Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("forwarder"));
        let metrics = forwarder.metrics();
        let app = router(AppState::new(
            registry.clone(),
            forwarder,
            config.server.batch_limit,
        ));
        let (status, body) = post_json_status(app.clone(), "/rpc/999", json!([{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}])).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.as_array().map(Vec::len), Some(2));
        let (status, body) = post_json_status(
            app,
            "/rpc/127",
            json!([{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}]),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let encoded = metrics.encode(&registry).await.expect("metrics");
        assert!(encoded.contains("reason=\"unknown_chain\"} 1"));
        assert!(encoded.contains("reason=\"no_endpoints\"} 1"));
        assert_eq!(registry.user_visible_errors(), 0);
    }
}
