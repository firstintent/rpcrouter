//! W9 自动开启：端到端晋级、人工墓碑、上限、存储不可写与并发预算。
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{Json, Router, extract::State, routing::post};
use rpcrouter::{
    autoenable::AutoEnableManager,
    chainlist::{Catalog, CatalogChain, CatalogEndpoint},
    config::Config,
    registry::Registry,
    state::{
        AutoChainState, BootstrapState, ChainOverrideState, EndpointOverrideState, HealthSnapshot,
        MemoryStore, StateExport, StateStore,
    },
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// 记录并发峰值的假上游：只回 eth_chainId / eth_blockNumber。
#[derive(Clone)]
struct Upstream {
    chain_id: u64,
    head: u64,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

async fn handle(State(up): State<Upstream>, body: String) -> Json<Value> {
    let current = up.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    up.peak.fetch_max(current, Ordering::SeqCst);
    tokio::time::sleep(up.delay).await;
    let method = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v.get("method").and_then(|m| m.as_str()).map(str::to_owned))
        .unwrap_or_default();
    let result = match method.as_str() {
        "eth_chainId" => format!("0x{:x}", up.chain_id),
        _ => format!("0x{:x}", up.head),
    };
    up.in_flight.fetch_sub(1, Ordering::SeqCst);
    Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
}

async fn spawn_upstream(chain_id: u64, head: u64, delay_ms: u64) -> (String, Arc<AtomicUsize>) {
    spawn_upstream_shared(
        chain_id,
        head,
        delay_ms,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    )
    .await
}

/// 共享在飞/峰值计数器，用于测量**跨端点的全局并发**。
async fn spawn_upstream_shared(
    chain_id: u64,
    head: u64,
    delay_ms: u64,
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
) -> (String, Arc<AtomicUsize>) {
    let state = Upstream {
        chain_id,
        head,
        in_flight,
        peak: Arc::clone(&peak),
        delay: Duration::from_millis(delay_ms),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");
    let app = Router::new().route("/", post(handle)).with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{address}/"), peak)
}

fn catalog(chains: Vec<(u64, Vec<String>)>) -> Catalog {
    let mut by_id = HashMap::new();
    let mut out = Vec::new();
    for (index, (chain_id, urls)) in chains.into_iter().enumerate() {
        by_id.insert(chain_id, index);
        out.push(CatalogChain {
            chain_id,
            name: format!("Chain {chain_id}"),
            short_name: None,
            chain: None,
            slug: None,
            is_testnet: false,
            native_symbol: None,
            explorer_url: None,
            status: Some("active".to_owned()),
            tvl: None,
            endpoints: urls
                .into_iter()
                .map(|url| CatalogEndpoint {
                    url,
                    tracking: Some("none".to_owned()),
                })
                .collect(),
        });
    }
    Catalog { chains: out, by_id }
}

fn config(min_endpoints: usize, max_chains: usize, probe_concurrency: usize) -> Config {
    let mut config = Config::from_toml("chains = []\n[discovery]\nenabled = true").expect("config");
    config.discovery.auto_enable.min_endpoints = min_endpoints;
    config.discovery.auto_enable.max_chains = max_chains;
    config.discovery.auto_enable.probe_concurrency = probe_concurrency;
    config.discovery.auto_enable.promote_after_rounds = 2;
    config.discovery.auto_enable.min_active_endpoints = 2;
    config.probe.request_timeout_ms = 2000;
    config
}

async fn registry_with(catalog_value: Catalog, config: &Config) -> Arc<Registry> {
    let registry = Arc::new(Registry::new(config));
    registry.set_catalog(Arc::new(catalog_value)).await;
    registry
}

/// 只读状态存储：writable() = false，且写入直接报错。
struct ReadOnlyStore(MemoryStore);

#[async_trait]
impl StateStore for ReadOnlyStore {
    async fn bootstrap(&self) -> anyhow::Result<BootstrapState> {
        self.0.bootstrap().await
    }
    async fn set_catalog(&self, catalog: &Value) -> anyhow::Result<()> {
        self.0.set_catalog(catalog).await
    }
    async fn load_overrides(&self) -> anyhow::Result<rpcrouter::state::Overrides> {
        self.0.load_overrides().await
    }
    async fn put_chain_override(
        &self,
        chain_id: u64,
        value: &ChainOverrideState,
    ) -> anyhow::Result<()> {
        self.0.put_chain_override(chain_id, value).await
    }
    async fn delete_chain_override(&self, chain_id: u64) -> anyhow::Result<()> {
        self.0.delete_chain_override(chain_id).await
    }
    async fn put_endpoint_override(
        &self,
        key: &str,
        value: &EndpointOverrideState,
    ) -> anyhow::Result<()> {
        self.0.put_endpoint_override(key, value).await
    }
    async fn delete_endpoint_override(&self, key: &str) -> anyhow::Result<()> {
        self.0.delete_endpoint_override(key).await
    }
    async fn flush_health(&self, batch: &[HealthSnapshot]) -> anyhow::Result<()> {
        self.0.flush_health(batch).await
    }
    async fn load_health(&self) -> anyhow::Result<Vec<HealthSnapshot>> {
        self.0.load_health().await
    }
    async fn set_hot_chains(&self, chains: &[(u64, u64)]) -> anyhow::Result<()> {
        self.0.set_hot_chains(chains).await
    }
    async fn put_auto_chain(&self, _id: u64, _value: &AutoChainState) -> anyhow::Result<()> {
        anyhow::bail!("read-only store")
    }
    async fn append_audit(&self, what: &str, target: &str) -> anyhow::Result<()> {
        self.0.append_audit(what, target).await
    }
    async fn export(&self) -> anyhow::Result<StateExport> {
        self.0.export().await
    }
    async fn import(&self, value: &StateExport) -> anyhow::Result<()> {
        self.0.import(value).await
    }
    async fn reset(&self) -> anyhow::Result<()> {
        self.0.reset().await
    }
    async fn health(&self) -> bool {
        true
    }
    async fn writable(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn promotes_only_after_consecutive_qualified_rounds() {
    let (a, _) = spawn_upstream(9001, 100, 0).await;
    let (b, _) = spawn_upstream(9001, 100, 0).await;
    let config = config(2, 10, 8);
    let registry = registry_with(catalog(vec![(9001, vec![a, b])]), &config).await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)
        .expect("manager");

    manager.run_once().await;
    assert!(
        registry.auto_chain_ids().is_empty(),
        "一轮合格不应晋级（promote_after_rounds=2）"
    );
    manager.run_once().await;
    assert_eq!(registry.auto_chain_ids(), vec![9001]);
    assert_eq!(registry.pin_source(9001), Some("auto"));
    let persisted = store.load_auto_chains().await.expect("load");
    assert!(persisted.contains_key(&9001), "晋级必须落盘");
    let status = manager.status().await;
    assert_eq!(status.chains, 1);
    assert_eq!(status.promotions_total, 1);
}

#[tokio::test]
async fn wrong_chain_id_and_thin_pool_never_qualify() {
    // 一个端点链号不匹配，另一个正常：合格端点只有 1 个，达不到 min_active_endpoints=2。
    let (good, _) = spawn_upstream(9002, 50, 0).await;
    let (liar, _) = spawn_upstream(1234, 50, 0).await;
    let config = config(2, 10, 8);
    let registry = registry_with(catalog(vec![(9002, vec![good, liar])]), &config).await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), store, &config).expect("manager");

    manager.run_once().await;
    manager.run_once().await;
    manager.run_once().await;
    assert!(registry.auto_chain_ids().is_empty());
    let progress = manager.candidate_progress(9002).await.expect("progress");
    assert_eq!(progress.rounds, 0);
    assert!(!progress.last_qualified);
}

#[tokio::test]
async fn manual_tombstone_is_never_auto_enabled() {
    let (a, _) = spawn_upstream(9003, 10, 0).await;
    let (b, _) = spawn_upstream(9003, 10, 0).await;
    let config = config(2, 10, 8);
    let registry = registry_with(catalog(vec![(9003, vec![a, b])]), &config).await;
    registry
        .apply_override(
            9003,
            ChainOverrideState {
                pinned: Some(false),
                ..Default::default()
            },
        )
        .await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)
        .expect("manager");

    for _ in 0..3 {
        manager.run_once().await;
    }
    assert!(
        registry.auto_chain_ids().is_empty(),
        "取消开启的链不得被自动加回"
    );
    assert!(store.load_auto_chains().await.expect("load").is_empty());

    // 预热同样跳过墓碑链。
    let mut persisted = std::collections::BTreeMap::new();
    persisted.insert(9003, AutoChainState::default());
    assert_eq!(manager.preheat(&persisted).await, 0);
    assert!(registry.auto_chain_ids().is_empty());
}

#[tokio::test]
async fn promotion_paused_when_store_not_writable() {
    let (a, _) = spawn_upstream(9004, 10, 0).await;
    let (b, _) = spawn_upstream(9004, 10, 0).await;
    let config = config(2, 10, 8);
    let registry = registry_with(catalog(vec![(9004, vec![a, b])]), &config).await;
    let store: Arc<dyn StateStore> = Arc::new(ReadOnlyStore(MemoryStore::default()));
    let manager = AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)
        .expect("manager");

    manager.run_once().await;
    manager.run_once().await;
    assert!(
        registry.auto_chain_ids().is_empty(),
        "存储不可写时不得只在内存里晋级"
    );
    assert_eq!(manager.status().await.pending, 1);
}

#[tokio::test]
async fn cap_stops_new_promotions_without_evicting() {
    let (a1, _) = spawn_upstream(9005, 10, 0).await;
    let (a2, _) = spawn_upstream(9005, 10, 0).await;
    let (b1, _) = spawn_upstream(9006, 10, 0).await;
    let (b2, _) = spawn_upstream(9006, 10, 0).await;
    let config = config(2, 1, 8);
    let registry = registry_with(
        catalog(vec![(9005, vec![a1, a2]), (9006, vec![b1, b2])]),
        &config,
    )
    .await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)
        .expect("manager");

    for _ in 0..3 {
        manager.run_once().await;
    }
    let enabled = registry.auto_chain_ids();
    assert_eq!(enabled.len(), 1, "max_chains=1 时只应有一条链被开启");
    let status = manager.status().await;
    assert!(status.capped);
    assert!(status.pending >= 1);

    // 已开启的链不因上限被淘汰。
    for _ in 0..2 {
        manager.run_once().await;
    }
    assert_eq!(registry.auto_chain_ids(), enabled);
}

#[tokio::test]
async fn global_probe_concurrency_is_bounded() {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut chains = Vec::new();
    for chain_id in 9101..9105u64 {
        let (a, _) =
            spawn_upstream_shared(chain_id, 10, 60, Arc::clone(&in_flight), Arc::clone(&peak))
                .await;
        let (b, _) =
            spawn_upstream_shared(chain_id, 10, 60, Arc::clone(&in_flight), Arc::clone(&peak))
                .await;
        chains.push((chain_id, vec![a, b]));
    }
    let config = config(2, 10, 2);
    let registry = registry_with(catalog(chains), &config).await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), store, &config).expect("manager");

    manager.run_once().await;
    let observed = peak.load(Ordering::SeqCst);
    assert!(observed > 0, "探测应真实发生");
    assert!(
        observed <= 2,
        "跨链探测并发不得超过 probe_concurrency，实测峰值 {observed}"
    );
}

#[tokio::test]
async fn enabled_chain_survives_dead_endpoints_and_restart() {
    let (a, _) = spawn_upstream(9007, 10, 0).await;
    let (b, _) = spawn_upstream(9007, 10, 0).await;
    let config = config(2, 10, 8);
    let registry = registry_with(catalog(vec![(9007, vec![a, b])]), &config).await;
    let store: Arc<dyn StateStore> = Arc::new(MemoryStore::default());
    let manager = AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)
        .expect("manager");
    manager.run_once().await;
    manager.run_once().await;
    assert_eq!(registry.auto_chain_ids(), vec![9007]);

    // 目录里该链的端点全部消失（等价端点全死）：只增不减，集合不变。
    registry
        .set_catalog(Arc::new(catalog(vec![(9007, vec![])])))
        .await;
    for _ in 0..3 {
        manager.run_once().await;
    }
    assert_eq!(registry.auto_chain_ids(), vec![9007]);

    // 重启：新 Registry + 新 manager 共享同一状态存储，预热后集合完整恢复。
    let restarted = registry_with(catalog(vec![(9007, vec![])]), &config).await;
    let manager2 = AutoEnableManager::new(Arc::clone(&restarted), Arc::clone(&store), &config)
        .expect("manager");
    let boot = store.bootstrap().await.expect("bootstrap");
    assert_eq!(manager2.preheat(&boot.auto_chains).await, 1);
    assert_eq!(restarted.auto_chain_ids(), vec![9007]);
}
