//! 自动开启候选评估器：独立于 Registry 的轻量探测逻辑。
use crate::{
    chainlist::{Catalog, CatalogEndpoint},
    config::Config,
    registry::Registry,
    signals::{ResponseClassification, classify_response},
    state::{AutoChainState, StateStore},
};
use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{sleep, timeout},
};
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct AutoEnableConfig {
    pub min_endpoints: usize,
    pub max_candidates: usize,
    pub probe_batch: usize,
    pub max_endpoints_per_chain: usize,
    pub min_active_endpoints: usize,
    pub head_tolerance_blocks: u64,
    pub probe_concurrency: usize,
    pub request_timeout: Duration,
    pub promote_after_rounds: usize,
    pub max_chains: usize,
}
impl Default for AutoEnableConfig {
    fn default() -> Self {
        Self {
            min_endpoints: 5,
            max_candidates: 512,
            probe_batch: 32,
            max_endpoints_per_chain: 8,
            min_active_endpoints: 2,
            head_tolerance_blocks: 64,
            probe_concurrency: 8,
            request_timeout: Duration::from_millis(2000),
            promote_after_rounds: 2,
            max_chains: 400,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    pub chain_id: u64,
    pub score: i64,
    pub endpoints: Vec<CatalogEndpoint>,
}

/// 根据目录元数据构建候选池。墓碑、测试网、已开启和配置 pinned 链会被跳过。
pub fn build_candidates(
    catalog: &Catalog,
    cfg: &AutoEnableConfig,
    deny: &HashSet<u64>,
    auto: &HashSet<u64>,
    config_pinned: &HashSet<u64>,
    tombstones: &HashSet<u64>,
) -> Vec<Candidate> {
    let mut out: Vec<Candidate> = catalog
        .chains
        .iter()
        .filter_map(|c| {
            if c.is_testnet
                || deny.contains(&c.chain_id)
                || auto.contains(&c.chain_id)
                || config_pinned.contains(&c.chain_id)
                || tombstones.contains(&c.chain_id)
            {
                return None;
            }
            let mut seen = HashSet::new();
            let mut eps = Vec::new();
            for e in &c.endpoints {
                // 目录端点在 chainlist 解析阶段已过滤为公开 https，这里只做去重。
                if seen.insert(e.url.clone()) {
                    eps.push(e.clone());
                }
            }
            if eps.len() < cfg.min_endpoints {
                return None;
            }
            let mut score = eps.len() as i64;
            score += eps
                .iter()
                .filter(|e| e.tracking.as_deref() == Some("none"))
                .count() as i64
                * 2;
            if c.status.as_deref() == Some("active") {
                score += 2;
            }
            if c.tvl.unwrap_or(0.0) > 0.0 {
                score += 1;
            }
            Some(Candidate {
                chain_id: c.chain_id,
                score,
                endpoints: eps,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| a.chain_id.cmp(&b.chain_id))
    });
    out.truncate(cfg.max_candidates);
    out
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProbeRound {
    pub chain_id: u64,
    pub active_endpoints: usize,
    pub heads: Vec<u64>,
    pub qualified: bool,
    pub failures: usize,
}

pub async fn probe_candidate(
    client: &Client,
    candidate: &Candidate,
    cfg: &AutoEnableConfig,
    slow_threshold: Duration,
) -> ProbeRound {
    probe_candidate_with_semaphore(
        client,
        candidate,
        cfg,
        slow_threshold,
        Arc::new(tokio::sync::Semaphore::new(cfg.probe_concurrency.max(1))),
    )
    .await
}

pub async fn probe_candidate_with_semaphore(
    client: &Client,
    candidate: &Candidate,
    cfg: &AutoEnableConfig,
    slow_threshold: Duration,
    sem: Arc<tokio::sync::Semaphore>,
) -> ProbeRound {
    let mut eps = candidate.endpoints.clone();
    eps.sort_by_key(|e| {
        if e.tracking.as_deref() == Some("none") {
            0
        } else {
            1
        }
    });
    eps.truncate(cfg.max_endpoints_per_chain);
    let mut joins = tokio::task::JoinSet::new();
    for ep in eps {
        let c = client.clone();
        let sem = sem.clone();
        let timeout_d = cfg.request_timeout;
        let cid = candidate.chain_id;
        joins.spawn(async move {
            let _p = sem.acquire_owned().await.ok()?;
            probe_endpoint(&c, &ep.url, cid, timeout_d, slow_threshold).await
        });
    }
    let mut heads = Vec::new();
    let mut failures = 0;
    while let Some(r) = joins.join_next().await {
        match r.ok().flatten() {
            Some(h) => heads.push(h),
            None => failures += 1,
        }
    }
    let qualified = heads.len() >= cfg.min_active_endpoints
        && heads
            .iter()
            .max()
            .map(|max| {
                heads
                    .iter()
                    .filter(|h| *h + cfg.head_tolerance_blocks >= *max)
                    .count()
                    >= 2
            })
            .unwrap_or(false);
    ProbeRound {
        chain_id: candidate.chain_id,
        active_endpoints: heads.len(),
        heads,
        qualified,
        failures,
    }
}

async fn probe_endpoint(
    client: &Client,
    url: &str,
    chain_id: u64,
    timeout_d: Duration,
    slow: Duration,
) -> Option<u64> {
    let req_id = json!(1);
    let call = |method: &'static str| async move {
        client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(json!({"jsonrpc":"2.0","id":1,"method":method,"params":[]}).to_string())
            .send()
            .await
    };
    let r1 = timeout(timeout_d, call("eth_chainId")).await.ok()?.ok()?;
    let h1 = r1.headers().clone();
    let r1_status = r1.status();
    let b1 = r1.bytes().await.ok()?;
    let c1 = classify_response(r1_status, &h1, &b1, Duration::ZERO, &req_id, slow);
    let v1 = match c1 {
        ResponseClassification::Valid(v) | ResponseClassification::Degraded { response: v, .. } => {
            v
        }
        _ => return None,
    };
    let got = v1
        .get("result")?
        .as_str()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())?;
    if got != chain_id {
        return None;
    }
    let r2 = timeout(timeout_d, call("eth_blockNumber"))
        .await
        .ok()?
        .ok()?;
    let h2 = r2.headers().clone();
    let r2_status = r2.status();
    let b2 = r2.bytes().await.ok()?;
    let c2 = classify_response(r2_status, &h2, &b2, Duration::ZERO, &req_id, slow);
    let v2 = match c2 {
        ResponseClassification::Valid(v) | ResponseClassification::Degraded { response: v, .. } => {
            v
        }
        _ => return None,
    };
    let s = v2.get("result")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

/// 单条候选链的评估进度（供 Admin API 展示）。
#[derive(Clone, Debug, Default)]
pub struct CandidateProgress {
    /// 连续合格轮次。
    pub rounds: u32,
    pub last_qualified: bool,
    pub last_error: Option<String>,
}

/// 自动开启整体状态（供 Admin API 展示）。
#[derive(Clone, Debug, Default)]
pub struct AutoEnableStatus {
    pub enabled: bool,
    pub chains: usize,
    pub candidates: usize,
    /// 已连续合格但因上限或状态存储不可写而未加入的链数。
    pub pending: usize,
    pub capped: bool,
    pub last_scan_at: u64,
    pub promotions_total: u64,
}

#[derive(Default)]
struct ScanState {
    cursor: usize,
    progress: HashMap<u64, CandidateProgress>,
    candidates: usize,
    pending: usize,
    capped: bool,
    last_scan_at: u64,
    promotions_total: u64,
}

/// 自动开启后台任务：轮转分批探测候选链，连续合格轮次达标后晋级为常驻开启。
///
/// 只增不减：本任务永远不会移除已开启的链；移除只来自人工覆写（pinned=false / disabled=true）。
pub struct AutoEnableManager {
    registry: Arc<Registry>,
    store: Arc<dyn StateStore>,
    client: Client,
    cfg: AutoEnableConfig,
    deny: HashSet<u64>,
    config_pinned: HashSet<u64>,
    slow_threshold: Duration,
    interval: Duration,
    semaphore: Arc<Semaphore>,
    state: Mutex<ScanState>,
}

impl AutoEnableManager {
    pub fn new(
        registry: Arc<Registry>,
        store: Arc<dyn StateStore>,
        config: &Config,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self::with_client(registry, store, config, client))
    }

    pub fn with_client(
        registry: Arc<Registry>,
        store: Arc<dyn StateStore>,
        config: &Config,
        client: Client,
    ) -> Self {
        let raw = &config.discovery.auto_enable;
        let cfg = AutoEnableConfig {
            min_endpoints: raw.min_endpoints,
            max_candidates: raw.max_candidates,
            probe_batch: raw.probe_batch,
            max_endpoints_per_chain: raw.max_endpoints_per_chain,
            min_active_endpoints: raw.min_active_endpoints,
            head_tolerance_blocks: raw.head_tolerance_blocks,
            probe_concurrency: raw.probe_concurrency,
            request_timeout: Duration::from_millis(config.probe.request_timeout_ms),
            promote_after_rounds: raw.promote_after_rounds,
            max_chains: raw.max_chains,
        };
        Self {
            registry,
            store,
            client,
            deny: config.discovery.deny.iter().copied().collect(),
            config_pinned: config.chains.iter().copied().collect(),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            interval: Duration::from_secs(raw.candidate_interval_seconds.max(1)),
            semaphore: Arc::new(Semaphore::new(cfg.probe_concurrency.max(1))),
            cfg,
            state: Mutex::new(ScanState::default()),
        }
    }

    /// 启动预热：恢复持久化的自动开启集合并 materialize（带人工墓碑的条目跳过）。
    pub async fn preheat(&self, auto_chains: &BTreeMap<u64, AutoChainState>) -> usize {
        let ids = auto_chains
            .keys()
            .copied()
            .filter(|id| {
                let override_value = self.registry.runtime_chain_override(*id);
                override_value.pinned != Some(false) && override_value.disabled != Some(true)
            })
            .collect::<Vec<_>>();
        self.registry.restore_auto_pinned(ids.iter().copied());
        for id in &ids {
            let _ = self.registry.resolve_for_request(*id).await;
        }
        if !ids.is_empty() {
            info!(chains = ids.len(), "restored auto-enabled chains");
        }
        ids.len()
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.run_once().await;
            sleep(self.interval).await;
        }
    }

    /// 跑一轮：重建候选池 → 取一批探测 → 更新连续轮次 → 达标者晋级。
    pub async fn run_once(&self) {
        let Some(catalog) = self.registry.catalog().await else {
            return;
        };
        let auto = self
            .registry
            .auto_chain_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        let tombstones = self.registry.tombstoned_chain_ids();
        let candidates = build_candidates(
            &catalog,
            &self.cfg,
            &self.deny,
            &auto,
            &self.config_pinned,
            &tombstones,
        );
        let total = candidates.len();
        let live = candidates
            .iter()
            .map(|c| c.chain_id)
            .collect::<HashSet<_>>();
        let batch = {
            let mut state = self.state.lock().await;
            state.candidates = total;
            state.progress.retain(|id, _| live.contains(id));
            if total == 0 {
                state.cursor = 0;
                state.pending = 0;
                state.capped = auto.len() >= self.cfg.max_chains;
                state.last_scan_at = crate::registry::unix_seconds();
                Vec::new()
            } else {
                let start = state.cursor % total;
                let take = self.cfg.probe_batch.min(total);
                let batch = candidates
                    .iter()
                    .cycle()
                    .skip(start)
                    .take(take)
                    .cloned()
                    .collect::<Vec<_>>();
                state.cursor = (start + take) % total;
                batch
            }
        };
        if batch.is_empty() {
            return;
        }

        // 状态存储不可写时本轮不晋级：只增不减要求集合必须先落盘，否则重启会回退。
        let writable = self.store.writable().await;
        let mut rounds = tokio::task::JoinSet::new();
        for candidate in batch {
            let client = self.client.clone();
            let cfg = self.cfg.clone();
            let slow = self.slow_threshold;
            let semaphore = Arc::clone(&self.semaphore);
            rounds.spawn(async move {
                let round =
                    probe_candidate_with_semaphore(&client, &candidate, &cfg, slow, semaphore)
                        .await;
                (candidate, round)
            });
        }

        let mut pending = 0usize;
        let mut capped = false;
        while let Some(joined) = rounds.join_next().await {
            let Ok((candidate, round)) = joined else {
                continue;
            };
            let ready = {
                let mut state = self.state.lock().await;
                let entry = state.progress.entry(candidate.chain_id).or_default();
                if round.qualified {
                    entry.rounds = entry.rounds.saturating_add(1);
                    entry.last_qualified = true;
                    entry.last_error = None;
                } else {
                    entry.rounds = 0;
                    entry.last_qualified = false;
                    entry.last_error = Some(format!(
                        "active={} failures={}",
                        round.active_endpoints, round.failures
                    ));
                }
                entry.rounds as usize >= self.cfg.promote_after_rounds
            };
            if !ready {
                continue;
            }
            if self.registry.auto_chain_ids().len() >= self.cfg.max_chains {
                pending += 1;
                capped = true;
                continue;
            }
            if !writable {
                pending += 1;
                continue;
            }
            if self.promote(&candidate, &round).await {
                let mut state = self.state.lock().await;
                state.promotions_total = state.promotions_total.saturating_add(1);
            } else {
                pending += 1;
            }
        }

        if capped {
            warn!(
                max_chains = self.cfg.max_chains,
                pending, "auto-enable capped: no new chains will be added"
            );
        }
        if !writable {
            warn!("auto-enable promotions paused: state store is not writable");
        }
        let mut state = self.state.lock().await;
        state.pending = pending;
        state.capped = capped || self.registry.auto_chain_ids().len() >= self.cfg.max_chains;
        state.last_scan_at = crate::registry::unix_seconds();
    }

    /// 晋级：先写状态存储，落盘成功才进内存并 materialize。
    async fn promote(&self, candidate: &Candidate, round: &ProbeRound) -> bool {
        let value = AutoChainState {
            enabled_at: crate::registry::unix_seconds(),
            endpoints: candidate.endpoints.len() as u32,
            active_seen: round.active_endpoints as u32,
            head: round.heads.iter().copied().max().unwrap_or_default(),
        };
        if let Err(error) = self.store.put_auto_chain(candidate.chain_id, &value).await {
            warn!(
                chain_id = candidate.chain_id,
                %error,
                "auto-enable promotion deferred: state store write failed"
            );
            return false;
        }
        if !self
            .registry
            .set_auto_pinned(candidate.chain_id, true)
            .await
        {
            return false;
        }
        let _ = self.registry.resolve_for_request(candidate.chain_id).await;
        info!(
            chain_id = candidate.chain_id,
            active_endpoints = round.active_endpoints,
            "chain auto-enabled"
        );
        true
    }

    pub async fn status(&self) -> AutoEnableStatus {
        let state = self.state.lock().await;
        AutoEnableStatus {
            enabled: true,
            chains: self.registry.auto_chain_ids().len(),
            candidates: state.candidates,
            pending: state.pending,
            capped: state.capped,
            last_scan_at: state.last_scan_at,
            promotions_total: state.promotions_total,
        }
    }

    pub async fn candidate_progress(&self, chain_id: u64) -> Option<CandidateProgress> {
        self.state.lock().await.progress.get(&chain_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainlist::{Catalog, CatalogChain, CatalogEndpoint};
    use std::collections::HashMap;
    #[test]
    fn candidate_filters_and_scores() {
        let c = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "x".into(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: Some("active".into()),
                tvl: Some(1.0),
                endpoints: (0..5)
                    .map(|i| CatalogEndpoint {
                        url: format!("https://e{i}"),
                        tracking: Some("none".into()),
                    })
                    .collect(),
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        let v = build_candidates(
            &c,
            &AutoEnableConfig::default(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(v.len(), 1);
        assert!(v[0].score > 5);
    }
}
