use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::AUTHORIZATION},
};
use rpcrouter::{
    admin::{AdminState, PublicCache},
    chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
    config::Config,
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    registry::{EndpointState, Registry},
    server::{AppState, router as app_router},
    state::{MemoryStore, StateRuntimeSnapshot, StateStore},
};
use serde_json::Value;
use tokio::{net::TcpListener, time::Instant};
use tower::ServiceExt;

async fn mock() -> (String, MockController) {
    let controller = MockController::new(MockBehavior::default());
    let served = controller.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock_router(served)).await.unwrap();
    });
    (format!("http://{address}/private-upstream"), controller)
}

fn static_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rpcrouter-w8-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(
        dir.join("index.html"),
        "<!doctype html><script src=\"/dashboard/assets/app.js\"></script>",
    )
    .unwrap();
    std::fs::write(dir.join("assets/app.js"), "console.log('ok')").unwrap();
    dir
}

async fn app(
    token: Option<&str>,
    public_site: bool,
    static_dir: Option<PathBuf>,
) -> (
    Router,
    PathBuf,
    Arc<Registry>,
    Arc<tokio::sync::Mutex<Option<PublicCache>>>,
) {
    let (url, _controller) = mock().await;
    let mut config = Config {
        chains: Vec::new(),
        ..Config::default()
    };
    config.admin.auth_token = token.map(str::to_owned);
    config.admin.public_site = public_site;
    config.admin.static_dir = static_dir.clone();
    let registry = Arc::new(Registry::new(&config));
    registry
        .set_catalog(Arc::new(Catalog {
            chains: vec![
                CatalogChain {
                    chain_id: 1,
                    name: "Ethereum".into(),
                    short_name: Some("eth".into()),
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: Some("ETH".into()),
                    explorer_url: Some("https://explorer.example/".into()),
                    status: Some("active".into()),
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: url.clone(),
                        tracking: None,
                    }],
                },
                CatalogChain {
                    chain_id: 2,
                    name: "Test Chain".into(),
                    short_name: Some("test".into()),
                    chain: None,
                    slug: None,
                    is_testnet: true,
                    native_symbol: Some("TST".into()),
                    explorer_url: None,
                    status: Some("active".into()),
                    tvl: None,
                    endpoints: Vec::new(),
                },
            ],
            by_id: HashMap::from([(1, 0), (2, 1)]),
        }))
        .await;
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Ethereum".into(),
                endpoints: vec![url.clone()],
            }],
        })
        .await;
    registry.resolve_for_request(1).await.unwrap();
    let endpoint = registry.endpoint(1, &url).await.unwrap();
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    endpoint.record_success(Instant::now(), Duration::from_millis(1), true);
    assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
    let forwarder = Arc::new(Forwarder::new(registry.clone(), &config).unwrap());
    let store = Arc::new(MemoryStore::new());
    store.bootstrap().await.unwrap();
    let public_cache = Arc::new(tokio::sync::Mutex::new(None));
    let admin = AdminState {
        registry: registry.clone(),
        forwarder: forwarder.clone(),
        metrics: forwarder.metrics(),
        store,
        chainlist: None,
        probe: None,
        config,
        started: std::time::Instant::now(),
        state_runtime: StateRuntimeSnapshot::new("memory", "test", "test-1"),
        auto_enable: None,
        public_cache: Arc::clone(&public_cache),
    };
    (
        app_router(AppState::new(registry.clone(), forwarder, 10).with_admin(admin)),
        static_dir.unwrap_or_default(),
        registry,
        public_cache,
    )
}

async fn response(service: &Router, path: &str, auth: Option<&str>) -> axum::response::Response {
    let mut request = Request::get(path);
    if let Some(auth) = auth {
        request = request.header(AUTHORIZATION, auth);
    }
    service
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn public_api_ignores_admin_authorization_and_sets_cache_header() {
    let (service, _, _, _) = app(Some("secret"), true, None).await;
    for path in [
        "/api/public/overview",
        "/api/public/chains",
        "/api/public/chains/1",
    ] {
        for auth in [None, Some("Bearer wrong")] {
            let response = response(&service, path, auth).await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()["cache-control"], "public, max-age=5");
        }
    }
}

#[tokio::test]
async fn public_overview_and_list_values_sort_and_validate_queries() {
    let (service, _, _, _) = app(None, true, None).await;
    let overview = json_body(response(&service, "/api/public/overview", None).await).await;
    assert_eq!(overview["chains"]["catalog"], 2);
    assert_eq!(overview["chains"]["serving"], 1);
    assert_eq!(overview["endpoints"]["active"], 1);
    let list = json_body(response(&service, "/api/public/chains?sort=priority", None).await).await;
    assert_eq!(list["items"][0]["chainId"], 1);
    assert!(list["items"].as_array().unwrap().len() <= 200);
    assert_eq!(response(&service, "/api/public/chains?q=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", None).await.status(), StatusCode::BAD_REQUEST);
    let limited = json_body(response(&service, "/api/public/chains?limit=1000", None).await).await;
    assert!(limited["items"].as_array().unwrap().len() <= 200);
}

#[tokio::test]
async fn public_large_catalog_is_memoized_and_bounded() {
    let (service, _, registry, public_cache) = app(None, true, None).await;
    let mut catalog = (*registry.catalog().await.unwrap()).clone();
    for chain_id in 3..=3000 {
        catalog.by_id.insert(chain_id, catalog.chains.len());
        catalog.chains.push(CatalogChain {
            chain_id,
            name: format!("Chain {chain_id}"),
            short_name: None,
            chain: None,
            slug: None,
            is_testnet: false,
            native_symbol: None,
            explorer_url: Some("javascript:alert(1)".into()),
            status: Some("active".into()),
            tvl: None,
            endpoints: Vec::new(),
        });
    }
    registry.set_catalog(Arc::new(catalog)).await;
    let started = std::time::Instant::now();
    assert_eq!(
        response(&service, "/api/public/overview", None)
            .await
            .status(),
        StatusCode::OK
    );
    let first_built_at = public_cache.lock().await.as_ref().unwrap().built_at;
    for _ in 0..19 {
        assert_eq!(
            response(&service, "/api/public/overview", None)
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            response(&service, "/api/public/chains?limit=200", None)
                .await
                .status(),
            StatusCode::OK
        );
    }
    assert_eq!(
        response(&service, "/api/public/chains?limit=200", None)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        public_cache.lock().await.as_ref().unwrap().built_at,
        first_built_at
    );
    let elapsed = started.elapsed();
    eprintln!("3000-chain public memo regression: {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(3),
        "large public catalog took {elapsed:?}"
    );
    let overview = json_body(response(&service, "/api/public/overview", None).await).await;
    assert_eq!(overview["chains"]["catalog"], 3000);
}

#[tokio::test]
async fn public_rows_are_redacted_and_disabled_chains_are_hidden() {
    let (service, _, _, _) = app(Some("secret"), true, None).await;
    let body = json_body(response(&service, "/api/public/chains", None).await).await;
    let serialized = body.to_string();
    for forbidden in [
        "endpointRows",
        "settings",
        "userVisibleErrorsTotal",
        "private-upstream",
    ] {
        assert!(!serialized.contains(forbidden), "leaked {forbidden}");
    }
    assert_eq!(body["items"][0]["nativeSymbol"], "ETH");
    let keys = body["items"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        keys,
        [
            "active",
            "cacheHitsTotal",
            "cacheLookupsTotal",
            "catalogEndpoints",
            "chainId",
            "endpoints",
            "explorerUrl",
            "head",
            "ingressTotal",
            "isTestnet",
            "name",
            "nativeSymbol",
            "shortName",
            "state",
            "status",
        ]
        .into_iter()
        .collect()
    );
    let disabled = service
        .clone()
        .oneshot(
            Request::post("/admin/api/chains/1/disable")
                .header(AUTHORIZATION, "Bearer secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    let body = json_body(response(&service, "/api/public/chains", None).await).await;
    assert!(
        body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["chainId"] != 1)
    );
    assert_eq!(
        response(&service, "/api/public/chains/1", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        response(&service, "/api/public/chains/999", None)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn public_spa_routes_are_safe_and_obey_configuration() {
    let dir = static_dir();
    let (service, cleanup, _, _) = app(None, true, Some(dir.clone())).await;
    for path in ["/", "/chain/1", "/chain/1/"] {
        let response = response(&service, path, None).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert!(
            response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("/dashboard/assets/app.js"));
    }
    for path in ["/chain/../x", "/%2e%2e"] {
        assert_eq!(
            response(&service, path, None).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    let (disabled, _, _, _) = app(None, false, Some(dir)).await;
    for path in [
        "/",
        "/chain/1",
        "/chain/1/",
        "/api/public/overview",
        "/api/public/chains",
        "/api/public/chains/1",
    ] {
        assert_eq!(
            response(&disabled, path, None).await.status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        response(&disabled, "/dashboard/", None).await.status(),
        StatusCode::OK
    );
    let (no_static, _, _, _) = app(None, true, None).await;
    assert_eq!(
        response(&no_static, "/", None).await.status(),
        StatusCode::NOT_FOUND
    );
    std::fs::remove_dir_all(cleanup).unwrap();
}

#[tokio::test]
async fn legacy_and_admin_routes_keep_their_contracts() {
    let (service, _, _, _) = app(Some("secret"), true, None).await;
    assert_eq!(
        response(&service, "/chains", None).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        response(&service, "/admin/api/overview", None)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(&service, "/admin/api/overview", Some("Bearer secret"))
            .await
            .status(),
        StatusCode::OK
    );
}

#[test]
fn public_site_config_defaults_on_and_accepts_explicit_off() {
    let default = Config::from_toml("chains = [1]").unwrap();
    assert!(default.admin.public_site);
    let disabled = Config::from_toml("chains = [1]\n[admin]\npublic_site = false").unwrap();
    assert!(!disabled.admin.public_site);
}
