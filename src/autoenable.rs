//! 自动开启候选评估器：独立于 Registry 的轻量探测逻辑。
use crate::{
    chainlist::{Catalog, CatalogChain, CatalogEndpoint},
    signals::{ResponseClassification, classify_response},
};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::timeout;

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
                if e.url.starts_with("https://") && seen.insert(e.url.clone()) {
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
    let mut eps = candidate.endpoints.clone();
    eps.sort_by_key(|e| {
        if e.tracking.as_deref() == Some("none") {
            0
        } else {
            1
        }
    });
    eps.truncate(cfg.max_endpoints_per_chain);
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.probe_concurrency.max(1)));
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
    let call = |method: &str| async {
        client
            .post(url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":[]}))
            .send()
            .await
    };
    let r1 = timeout(timeout_d, call("eth_chainId")).await.ok()?.ok()?;
    let b1 = r1.bytes().await.ok()?;
    let c1 = classify_response(
        StatusCode::OK,
        &r1.headers(),
        &b1,
        Duration::ZERO,
        &req_id,
        slow,
    );
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
    let b2 = r2.bytes().await.ok()?;
    let c2 = classify_response(
        StatusCode::OK,
        &r2.headers(),
        &b2,
        Duration::ZERO,
        &req_id,
        slow,
    );
    let v2 = match c2 {
        ResponseClassification::Valid(v) | ResponseClassification::Degraded { response: v, .. } => {
            v
        }
        _ => return None,
    };
    let s = v2.get("result")?.as_str()?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
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
