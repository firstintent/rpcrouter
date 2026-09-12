//! W9 接口层验收：公共接口两档状态与默认过滤、Admin 自动开启字段、关指标开关后功能完整。
use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::AUTHORIZATION},
};
use rpcrouter::{
    admin::AdminState,
    autoenable::AutoEnableManager,
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

async fn mock() -> String {
    let controller = MockController::new(MockBehavior::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, mock_router(controller))
            .await
            .unwrap();
    });
    format!("http://{address}/private-upstream")
}

fn chain(chain_id: u64, name: &str, testnet: bool, urls: Vec<String>) -> CatalogChain {
    CatalogChain {
        chain_id,
        name: name.to_owned(),
        short_name: Some(name.to_ascii_lowercase()),
        chain: None,
        slug: None,
        is_testnet: testnet,
        native_symbol: None,
        explorer_url: None,
        status: Some("active".to_owned()),
        tvl: None,
        endpoints: urls
            .into_iter()
            .map(|url| CatalogEndpoint {
                url,
                tracking: None,
            })
            .collect(),
    }
}

/// 目录里三条链：1 已开启且有活跃端点，7 是 dormant（只在搜索里可见），9 被禁用。
async fn app(metrics_enabled: bool, with_manager: bool) -> (Router, Arc<Registry>) {
    let url = mock().await;
    let mut config = Config {
        chains: Vec::new(),
        ..Config::default()
    };
    config.metrics_enabled = metrics_enabled;
    let registry = Arc::new(Registry::new(&config));
    registry
        .set_catalog(Arc::new(Catalog {
            chains: vec![
                chain(1, "Ethereum", false, vec![url.clone()]),
                chain(7, "Dormant Chain", false, vec![url.clone()]),
                chain(9, "Blocked Chain", false, vec![url.clone()]),
            ],
            by_id: HashMap::from([(1, 0), (7, 1), (9, 2)]),
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
    registry.set_disabled(9, true).await;

    let forwarder = Arc::new(Forwarder::new(registry.clone(), &config).unwrap());
    let store = Arc::new(MemoryStore::new());
    store.bootstrap().await.unwrap();
    let auto_enable = with_manager.then(|| {
        Arc::new(
            AutoEnableManager::new(
                Arc::clone(&registry),
                Arc::clone(&store) as Arc<dyn StateStore>,
                &config,
            )
            .unwrap(),
        )
    });
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
        auto_enable,
        public_cache: Arc::new(tokio::sync::Mutex::new(None)),
    };
    (
        app_router(AppState::new(registry.clone(), forwarder, 10).with_admin(admin)),
        registry,
    )
}

async fn get(service: &Router, path: &str) -> axum::response::Response {
    service
        .clone()
        .oneshot(
            Request::get(path)
                .header(AUTHORIZATION, "Bearer ignored")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn json(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

fn ids(body: &Value) -> Vec<u64> {
    body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["chainId"].as_u64().unwrap())
        .collect()
}

#[tokio::test]
async fn public_list_defaults_to_enabled_chains_and_search_opens_catalog() {
    let (service, _) = app(true, true).await;

    let default_body = json(get(&service, "/api/public/chains").await).await;
    assert_eq!(
        ids(&default_body),
        vec![1],
        "缺省只列已开启且有活跃端点的链"
    );
    assert_eq!(default_body["items"][0]["state"], "available");

    let scoped = json(get(&service, "/api/public/chains?scope=all").await).await;
    let mut all = ids(&scoped);
    all.sort_unstable();
    assert_eq!(all, vec![1, 7], "scope=all 展开全目录但仍不含 disabled 链");
    let dormant = scoped["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["chainId"] == 7)
        .unwrap()
        .clone();
    assert_eq!(dormant["state"], "unverified");

    let searched = json(get(&service, "/api/public/chains?q=dormant").await).await;
    assert_eq!(ids(&searched), vec![7], "搜索能命中未开启的链");

    let blocked = json(get(&service, "/api/public/chains?q=blocked").await).await;
    assert!(ids(&blocked).is_empty(), "disabled 链搜索也不可见");
}

#[tokio::test]
async fn public_detail_covers_both_tiers_and_hides_disabled() {
    let (service, _) = app(true, true).await;
    for (id, expected) in [(1u64, "available"), (7, "unverified")] {
        let response = get(&service, &format!("/api/public/chains/{id}")).await;
        assert_eq!(response.status(), StatusCode::OK, "chain {id}");
        assert_eq!(json(response).await["state"], expected);
    }
    assert_eq!(
        get(&service, "/api/public/chains/9").await.status(),
        StatusCode::NOT_FOUND
    );
    let overview = json(get(&service, "/api/public/overview").await).await;
    assert_eq!(overview["chains"]["available"], 1);
    // 内部生命周期词不出现在公共链表里。
    let body = json(get(&service, "/api/public/chains?scope=all").await).await;
    for row in body["items"].as_array().unwrap() {
        assert!(
            row["state"] == "available" || row["state"] == "unverified",
            "unexpected public state {}",
            row["state"]
        );
    }
}

#[tokio::test]
async fn admin_exposes_pin_source_and_auto_enable_block() {
    let (service, registry) = app(true, true).await;
    registry.set_auto_pinned(7, true).await;

    let chains = json(get(&service, "/admin/api/chains").await).await;
    let row = chains["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["chainId"] == 7)
        .unwrap()
        .clone();
    assert_eq!(row["pinSource"], "auto");

    let overview = json(get(&service, "/admin/api/overview").await).await;
    let auto = &overview["autoEnable"];
    assert_eq!(auto["enabled"], true);
    assert_eq!(auto["chains"], 1);
    assert!(auto["promotionsTotal"].is_number());
    assert!(auto["capped"].is_boolean());
}

#[tokio::test]
async fn auto_enable_works_with_metrics_disabled() {
    let (service, registry) = app(false, true).await;
    registry.set_auto_pinned(7, true).await;

    let overview = json(get(&service, "/admin/api/overview").await).await;
    assert_eq!(overview["autoEnable"]["enabled"], true);
    assert_eq!(overview["autoEnable"]["chains"], 1);

    let chains = json(get(&service, "/admin/api/chains").await).await;
    let row = chains["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["chainId"] == 7)
        .unwrap()
        .clone();
    assert_eq!(row["pinSource"], "auto");

    let public = json(get(&service, "/api/public/chains?scope=all").await).await;
    assert!(!public["items"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn overview_reports_disabled_auto_enable() {
    let (service, _) = app(true, false).await;
    let overview = json(get(&service, "/admin/api/overview").await).await;
    assert_eq!(overview["autoEnable"]["enabled"], false);
    assert_eq!(overview["autoEnable"]["chains"], 0);
}
