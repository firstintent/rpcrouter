use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
};
use rpcrouter::{
    admin::AdminState,
    chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
    config::Config,
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    probe::ProbeManager,
    registry::{EndpointState, Registry},
    server::{AppState, router as app_router},
    state::{
        BootstrapState, ChainOverrideState, EndpointOverrideState, HealthSnapshot, MemoryStore,
        Overrides, StateExport, StateRuntimeSnapshot, StateStore,
    },
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, time::Instant};
use tower::ServiceExt;

struct UnavailableStore;

#[async_trait]
impl StateStore for UnavailableStore {
    async fn bootstrap(&self) -> anyhow::Result<BootstrapState> {
        anyhow::bail!("unavailable")
    }
    async fn set_catalog(&self, _: &Value) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn load_overrides(&self) -> anyhow::Result<Overrides> {
        anyhow::bail!("unavailable")
    }
    async fn put_chain_override(&self, _: u64, _: &ChainOverrideState) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn delete_chain_override(&self, _: u64) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn put_endpoint_override(
        &self,
        _: &str,
        _: &EndpointOverrideState,
    ) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn delete_endpoint_override(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn flush_health(&self, _: &[HealthSnapshot]) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn load_health(&self) -> anyhow::Result<Vec<HealthSnapshot>> {
        anyhow::bail!("unavailable")
    }
    async fn set_hot_chains(&self, _: &[(u64, u64)]) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn append_audit(&self, _: &str, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn export(&self) -> anyhow::Result<StateExport> {
        anyhow::bail!("unavailable")
    }
    async fn import(&self, _: &StateExport) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn reset(&self) -> anyhow::Result<()> {
        anyhow::bail!("unavailable")
    }
    async fn health(&self) -> bool {
        false
    }
}

async fn mock() -> (String, MockController) {
    let c = MockController::new(MockBehavior::default());
    let served = c.clone();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(l, mock_router(served)).await.unwrap();
    });
    (format!("http://{a}/"), c)
}

async fn app(
    token: Option<&str>,
    enabled: bool,
) -> (Router, Arc<Registry>, Arc<MemoryStore>, MockController) {
    let (url, controller) = mock().await;
    let mut cfg = Config {
        chains: Vec::new(),
        ..Config::default()
    };
    cfg.admin.auth_token = token.map(str::to_owned);
    cfg.admin.enabled = enabled;
    let registry = Arc::new(Registry::new(&cfg));
    registry
        .set_catalog(Arc::new(Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "One".into(),
                short_name: Some("one".into()),
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: Some("active".into()),
                tvl: None,
                endpoints: vec![CatalogEndpoint {
                    url: url.clone(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        }))
        .await;
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "One".into(),
                endpoints: vec![url.clone()],
            }],
        })
        .await;
    registry.resolve_for_request(1).await.unwrap();
    let endpoint = registry.endpoint(1, &url).await.unwrap();
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &cfg).unwrap());
    let store = Arc::new(MemoryStore::new());
    store.bootstrap().await.unwrap();
    let state_runtime = StateRuntimeSnapshot::new("memory", "test", "test-1");
    {
        let mut snapshot = state_runtime.write().await;
        snapshot.up = true;
        snapshot.last_flush_unix = 101;
        snapshot.last_flush_result = "success".into();
        snapshot.last_flush_duration_ms = 12;
        snapshot.dirty_endpoints = 3;
        snapshot.last_ping_unix = 102;
    }
    let admin = AdminState {
        registry: Arc::clone(&registry),
        forwarder: Arc::clone(&forwarder),
        metrics: forwarder.metrics(),
        store: store.clone(),
        chainlist: None,
        probe: Some(Arc::new(ProbeManager::new(registry.clone(), &cfg).unwrap())),
        config: cfg.clone(),
        started: std::time::Instant::now(),
        state_runtime,
        auto_enable: None,
        public_cache: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let service = app_router(AppState::new(registry.clone(), forwarder, 10).with_admin(admin));
    (service, registry, store, controller)
}

async fn json_body(r: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn admin_auth_matrix_and_read_only_contract() {
    let (service, _, _, _) = app(None, true).await;
    let r = service
        .clone()
        .oneshot(
            Request::get("/admin/api/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = service
        .clone()
        .oneshot(
            Request::post("/admin/api/cache/clear")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let (service, _, _, _) = app(Some("secret"), true).await;
    let r = service
        .clone()
        .oneshot(
            Request::get("/admin/api/chains")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let r = service
        .clone()
        .oneshot(
            Request::get("/admin/api/chains")
                .header(AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let body = json_body(r).await;
    assert!(body["items"][0].get("chainId").is_some());
    let (disabled, _, _, _) = app(None, false).await;
    let r = disabled
        .oneshot(
            Request::get("/admin/api/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn state_metadata_uses_runtime_snapshot_without_store_calls() {
    let (service, _, store, _) = app(None, true).await;
    let before = store.call_count();
    let response = service
        .oneshot(
            Request::get("/admin/api/state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["backend"], "memory");
    assert_eq!(body["namespace"], "test");
    assert_eq!(body["instanceId"], "test-1");
    assert_eq!(body["lastFlushUnix"], 101);
    assert_eq!(body["lastFlushResult"], "success");
    assert_eq!(body["lastFlushDurationMs"], 12);
    assert_eq!(body["dirtyEndpoints"], 3);
    assert_eq!(body["lastPingUnix"], 102);
    assert_eq!(store.call_count(), before);
}

#[tokio::test]
async fn endpoint_controls_persist_then_change_runtime() {
    let (app, registry, store, _) = app(Some("secret"), true).await;
    let body = json!({"url": registry.endpoint(1, registry.all_endpoints(1).await[0].url()).await.unwrap().url()});
    let r = app
        .clone()
        .oneshot(
            Request::post("/admin/api/chains/1/endpoints/disable")
                .header(AUTHORIZATION, "Bearer secret")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert!(registry.candidates(1).await.endpoints.is_empty());
    assert!(store.audit_count() > 0);
}

#[tokio::test]
async fn unavailable_store_returns_503_without_memory_change() {
    let (_, registry, _, _) = app(Some("secret"), true).await;
    let mut cfg = Config {
        chains: vec![1],
        ..Config::default()
    };
    cfg.admin.auth_token = Some("secret".into());
    let f = Arc::new(Forwarder::new(registry.clone(), &cfg).unwrap());
    let admin = AdminState {
        registry: registry.clone(),
        forwarder: f.clone(),
        metrics: f.metrics(),
        store: Arc::new(UnavailableStore),
        chainlist: None,
        probe: None,
        config: cfg,
        started: std::time::Instant::now(),
        state_runtime: StateRuntimeSnapshot::new("memory", "test", "test-1"),
        auto_enable: None,
        public_cache: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let service = app_router(AppState::new(registry.clone(), f, 10).with_admin(admin));
    let r = service
        .oneshot(
            Request::post("/admin/api/chains/1/disable")
                .header(AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_ne!(
        registry.resolve_for_request(1).await.unwrap().state_label(),
        rpcrouter::registry::ChainStateLabel::Disabled
    );
}

#[tokio::test]
async fn override_export_import_reset_round_trip() {
    let (service, registry, store, _) = app(Some("secret"), true).await;
    let request = |method: &str, uri: &str, body: Value| {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, "Bearer secret")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let r = service
        .clone()
        .oneshot(request(
            "PUT",
            "/admin/api/chains/1/settings",
            json!({"tipTtlMs":777,"confirmationDepth":3}),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(registry.chain_settings(1).2, 777);
    let exported = service
        .clone()
        .oneshot(
            Request::get("/admin/api/state/export")
                .header(AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mut export = json_body(exported).await;
    let endpoint_url = registry.all_endpoints(1).await[0].url().to_owned();
    export["health"] = json!([{"chainId":1,"url":endpoint_url,"state":"cooling","coolingUntilUnix":rpcrouter::registry::unix_seconds()+60,"strikes":3,"latencyEwmaUs":0,"lag":0}]);
    let r = service
        .clone()
        .oneshot(request(
            "POST",
            "/admin/api/state/reset",
            json!({"confirm":true}),
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_ne!(registry.chain_settings(1).2, 777);
    let r = service
        .clone()
        .oneshot(request("POST", "/admin/api/state/import", export))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(registry.chain_settings(1).2, 777);
    assert!(matches!(
        registry.all_endpoints(1).await[0].state(Instant::now()),
        EndpointState::Cooling { .. }
    ));
    assert!(store.audit_count() >= 1);
}

#[tokio::test]
async fn static_spa_fallback_and_disabled_admin() {
    let dir = std::env::temp_dir().join(format!("rpcrouter-admin-{}", std::process::id()));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("index.html"), "hello")
        .await
        .unwrap();
    tokio::fs::create_dir_all(dir.join("assets")).await.unwrap();
    tokio::fs::write(dir.join("assets/app.js"), "ok")
        .await
        .unwrap();
    let (url, _) = mock().await;
    let mut cfg = Config {
        chains: vec![1],
        ..Config::default()
    };
    cfg.admin.static_dir = Some(dir.clone());
    let registry = Arc::new(Registry::new(&cfg));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "One".into(),
                endpoints: vec![url],
            }],
        })
        .await;
    let f = Arc::new(Forwarder::new(Arc::clone(&registry), &cfg).unwrap());
    let store = Arc::new(MemoryStore::new());
    let admin = AdminState {
        registry: registry.clone(),
        forwarder: f.clone(),
        metrics: f.metrics(),
        store,
        chainlist: None,
        probe: None,
        config: cfg,
        started: std::time::Instant::now(),
        state_runtime: StateRuntimeSnapshot::new("memory", "test", "test-1"),
        auto_enable: None,
        public_cache: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let service = app_router(AppState::new(registry, f, 10).with_admin(admin));
    let r = service
        .clone()
        .oneshot(
            Request::get("/dashboard/chains/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(to_bytes(r.into_body(), usize::MAX).await.unwrap(), "hello");
    for path in [
        "/dashboard//etc/passwd",
        "/dashboard///etc/passwd",
        "/dashboard/%2e%2e/etc/passwd",
        "/dashboard/assets/missing.js",
    ] {
        let response = service
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    let response = service
        .oneshot(
            Request::get("/dashboard/assets/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "text/javascript; charset=utf-8"
    );
    tokio::fs::remove_dir_all(dir).await.unwrap();
}

#[tokio::test]
async fn invalid_limits_and_settings_are_rejected_before_persistence() {
    let (service, _, store, _) = app(Some("secret"), true).await;
    let before = store.call_count();
    for (uri, body) in [
        (
            "/admin/api/chains/1/endpoints/limits",
            json!({"url":"http://127.0.0.1/","rps":0}),
        ),
        ("/admin/api/chains/1/settings", json!({"tipTtlMs":1})),
        ("/admin/api/chains/1/settings", json!({"unknown":1})),
    ] {
        let method = if uri.ends_with("settings") {
            "PUT"
        } else {
            "POST"
        };
        let response = service
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(AUTHORIZATION, "Bearer secret")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(store.call_count(), before);
}

#[tokio::test]
async fn read_only_admin_endpoints_do_not_call_store() {
    let (service, _, store, _) = app(None, true).await;
    let before = store.call_count();
    for _ in 0..100 {
        for uri in [
            "/admin/api/overview",
            "/admin/api/chains",
            "/admin/api/state",
        ] {
            assert_eq!(
                service
                    .clone()
                    .oneshot(Request::get(uri).body(Body::empty()).unwrap())
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
    }
    assert_eq!(store.call_count(), before);
}

#[tokio::test]
async fn chains_listing_does_not_increase_metrics_cardinality() {
    let (service, _, _, _) = app(None, true).await;
    let metrics = |service: Router| async move {
        let response = service
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .split(|byte| *byte == b'\n')
            .count()
    };
    let before = metrics(service.clone()).await;
    assert_eq!(
        service
            .clone()
            .oneshot(
                Request::get("/admin/api/chains?state=all")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(metrics(service).await, before);
}

#[tokio::test]
async fn settings_null_deletes_override_and_pin_uses_two_store_calls() {
    let (service, registry, store, _) = app(Some("secret"), true).await;
    let request = |method: &str, uri: &str, value: Value| {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, "Bearer secret")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .unwrap()
    };
    assert_eq!(
        service
            .clone()
            .oneshot(request(
                "PUT",
                "/admin/api/chains/1/settings",
                json!({"tipTtlMs":777})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        service
            .clone()
            .oneshot(request(
                "PUT",
                "/admin/api/chains/1/settings",
                json!({"tipTtlMs":null})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_ne!(registry.chain_settings(1).2, 777);
    let before = store.call_count();
    assert_eq!(
        service
            .oneshot(request("POST", "/admin/api/chains/1/pin", json!({})))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(store.call_count() - before <= 2);
}
