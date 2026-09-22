use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use reqwest::{Client, header::CONTENT_TYPE};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::time::{Instant, timeout};
use tracing::debug;

use crate::{
    cache::{CacheLookup, CachedResponse, ResponseCache},
    classify::Classifier,
    config::Config,
    hedge::HedgeGate,
    metrics::Metrics,
    registry::{Endpoint, EndpointLease, EndpointState, PoolKind, Registry},
    signals::{FailureSignal, FaultKind, ResponseClassification, classify_response},
};

const MAX_UPSTREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct Forwarder {
    registry: Arc<Registry>,
    client: Client,
    classifier: Classifier,
    cache: ResponseCache,
    metrics: Arc<Metrics>,
    hedge_gate: HedgeGate,
    hedge_delay: Duration,
    hedge_minimum_active: usize,
    request_timeout: Duration,
    slow_threshold: Duration,
    deadline: Duration,
    max_attempts: usize,
}

impl Forwarder {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let metrics = Arc::new(Metrics::new().context("failed to create metrics registry")?);
        Self::with_metrics(registry, config, metrics)
    }

    pub fn with_metrics(
        registry: Arc<Registry>,
        config: &Config,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build upstream HTTP client")?;
        Ok(Self {
            registry,
            client,
            classifier: Classifier::new(config),
            cache: ResponseCache::new(config),
            metrics,
            hedge_gate: HedgeGate::new(config),
            hedge_delay: Duration::from_millis(config.hedging.delay_ms),
            hedge_minimum_active: config.hedging.min_active_endpoints,
            request_timeout: Duration::from_millis(config.upstream.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            deadline: Duration::from_millis(config.upstream.deadline_ms),
            max_attempts: config.upstream.max_attempts,
        })
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn apply_state_overrides(&self, overrides: &crate::state::Overrides) {
        self.classifier.apply_overrides(overrides);
        for chain_id in overrides.chains.keys() {
            let settings = self.registry.chain_settings(*chain_id);
            self.classifier.set_chain_settings(
                *chain_id,
                Some(settings.1),
                Some(settings.2.min(settings.0)),
            );
        }
    }

    pub fn apply_chain_settings(
        &self,
        chain_id: u64,
        confirmation_depth: Option<u64>,
        tip_ttl_ms: Option<u64>,
    ) {
        self.classifier
            .set_chain_settings(chain_id, confirmation_depth, tip_ttl_ms);
    }

    pub fn cache(&self) -> &ResponseCache {
        &self.cache
    }

    pub fn hedge_counts(&self) -> (u64, u64) {
        self.hedge_gate.counts()
    }

    pub async fn execute(&self, chain_id: u64, request: Value) -> Value {
        let serialized = match serde_json::to_string(&request) {
            Ok(serialized) => serialized,
            Err(error) => {
                let request_id = request.get("id").cloned().unwrap_or(Value::Null);
                debug!(chain_id, error = %error, "JSON-RPC request serialization failed");
                self.metrics.record_ingress(chain_id);
                return self.exhausted(chain_id, request_id, PoolKind::Active);
            }
        };
        let raw = match RawValue::from_string(serialized) {
            Ok(raw) => raw,
            Err(error) => {
                let request_id = request.get("id").cloned().unwrap_or(Value::Null);
                debug!(chain_id, error = %error, "JSON-RPC RawValue conversion failed");
                self.metrics.record_ingress(chain_id);
                return self.exhausted(chain_id, request_id, PoolKind::Active);
            }
        };
        self.execute_raw(chain_id, &raw).await
    }

    pub async fn execute_raw(&self, chain_id: u64, request: &RawValue) -> Value {
        self.metrics.record_ingress(chain_id);

        let started = Instant::now();
        let metadata: RawRequestMetadata<'_> = match serde_json::from_str(request.get()) {
            Ok(metadata) => metadata,
            Err(error) => {
                debug!(chain_id, error = %error, "JSON-RPC metadata parsing failed");
                let response = self.exhausted(chain_id, Value::Null, PoolKind::Active);
                self.metrics.record_latency(chain_id, started.elapsed());
                return response;
            }
        };
        let request_id = metadata.id;
        let read_only = metadata
            .method
            .is_some_and(|method| self.classifier.is_read_only(method));
        let cache_plan = metadata.method.and_then(|method| {
            self.classifier.cache_plan(
                chain_id,
                method,
                metadata.params,
                self.registry.head(chain_id),
            )
        });

        let response = if let Some(plan) = cache_plan {
            match self.cache.lookup(plan).await {
                CacheLookup::Hit(cached) => {
                    self.metrics.record_cache_lookup(chain_id, true);
                    self.metrics.record_failover_depth(chain_id, 0);
                    cached.with_id(request_id.clone())
                }
                CacheLookup::Leader(leader) => {
                    self.metrics.record_cache_lookup(chain_id, false);
                    self.metrics.record_cache_miss_role(chain_id, false);
                    let response = self
                        .execute_uncached(
                            chain_id,
                            request.get().as_bytes(),
                            &request_id,
                            read_only,
                        )
                        .await;
                    if let Some(cached) = CachedResponse::from_plan_success(&response, plan) {
                        self.cache.insert(plan, Arc::clone(&cached)).await;
                        leader.complete_success(cached);
                    } else {
                        leader.complete_failure();
                    }
                    response
                }
                CacheLookup::Follower(follower) => {
                    self.metrics.record_cache_lookup(chain_id, false);
                    if let Some(cached) = follower.wait().await {
                        self.metrics.record_cache_miss_role(chain_id, true);
                        self.metrics.record_failover_depth(chain_id, 0);
                        cached.with_id(request_id.clone())
                    } else {
                        self.metrics.record_cache_miss_role(chain_id, false);
                        let response = self
                            .execute_uncached(
                                chain_id,
                                request.get().as_bytes(),
                                &request_id,
                                read_only,
                            )
                            .await;
                        if let Some(cached) = CachedResponse::from_plan_success(&response, plan) {
                            self.cache.insert(plan, cached).await;
                        }
                        response
                    }
                }
            }
        } else {
            self.execute_uncached(chain_id, request.get().as_bytes(), &request_id, read_only)
                .await
        };
        self.metrics.record_latency(chain_id, started.elapsed());
        response
    }

    async fn execute_uncached(
        &self,
        chain_id: u64,
        body: &[u8],
        request_id: &Value,
        read_only: bool,
    ) -> Value {
        let deadline_at = Instant::now() + self.deadline;
        // 给兜底留出一次请求的时间，避免公共池把 deadline 吃完后付费节点根本没机会上场。
        let public_deadline =
            self.public_deadline(deadline_at, self.registry.fallback_configured(chain_id));
        let mut candidate_set = self.registry.candidates(chain_id).await;
        if candidate_set.is_empty() {
            let _ = self.registry.resolve_for_request(chain_id).await;
            candidate_set = self.registry.candidates(chain_id).await;
        }
        let pool_kind = candidate_set.kind;
        let candidates = candidate_set.endpoints;
        let mut next_candidate = 0;
        let mut started_attempts = 0;
        let mut failures = 0;

        let hedge_eligible = read_only
            && self.hedge_gate.enabled()
            && candidates.len() >= 2
            && self
                .registry
                .healthy_for_hedging(chain_id, self.hedge_minimum_active)
                .await;
        if hedge_eligible {
            let primary_endpoint = Arc::clone(&candidates[0]);
            next_candidate = 1;
            if let Some(primary_lease) = primary_endpoint.try_acquire() {
                started_attempts += 1;
                self.hedge_gate.record_primary();
                self.metrics
                    .record_upstream(chain_id, &primary_endpoint.log_label());
                let mut primary = Box::pin(self.perform_attempt(
                    primary_endpoint,
                    primary_lease,
                    body,
                    request_id,
                    public_deadline,
                ));
                tokio::select! {
                    completion = &mut primary => {
                        match self.apply_completion(chain_id, completion, started_attempts, failures) {
                            Ok(response) => return response,
                            Err(()) => failures += 1,
                        }
                    }
                    () = tokio::time::sleep(self.hedge_delay) => {
                        if started_attempts < self.max_attempts
                            && self.registry
                                .healthy_for_hedging(chain_id, self.hedge_minimum_active)
                                .await
                        {
                            let hedge_endpoint = Arc::clone(&candidates[1]);
                            next_candidate = 2;
                            if let Some(hedge_lease) = hedge_endpoint.try_acquire() {
                                if self.hedge_gate.try_acquire() {
                                    started_attempts += 1;
                                    self.metrics.record_upstream(chain_id, &hedge_endpoint.log_label());
                                    self.metrics.record_hedge(chain_id);
                                    let mut hedge = Box::pin(self.perform_attempt(
                                        hedge_endpoint,
                                        hedge_lease,
                                        body,
                                        request_id,
                                        public_deadline,
                                    ));
                                    let (first, second) = tokio::select! {
                                        completion = &mut primary => (completion, hedge),
                                        completion = &mut hedge => (completion, primary),
                                    };
                                    match self.apply_completion(chain_id, first, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                    let second = second.await;
                                    match self.apply_completion(chain_id, second, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                } else {
                                    drop(hedge_lease);
                                    let completion = primary.await;
                                    match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                        Ok(response) => return response,
                                        Err(()) => failures += 1,
                                    }
                                }
                            } else {
                                let completion = primary.await;
                                match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                    Ok(response) => return response,
                                    Err(()) => failures += 1,
                                }
                            }
                        } else {
                            let completion = primary.await;
                            match self.apply_completion(chain_id, completion, started_attempts, failures) {
                                Ok(response) => return response,
                                Err(()) => failures += 1,
                            }
                        }
                    }
                }
            }
        }

        while next_candidate < candidates.len() && started_attempts < self.max_attempts {
            let endpoint = Arc::clone(&candidates[next_candidate]);
            next_candidate += 1;
            if Instant::now() >= public_deadline {
                break;
            }
            let Some(lease) = endpoint.try_acquire() else {
                continue;
            };
            started_attempts += 1;
            self.hedge_gate.record_primary();
            self.metrics
                .record_upstream(chain_id, &endpoint.log_label());
            let completion = self
                .perform_attempt(endpoint, lease, body, request_id, public_deadline)
                .await;
            match self.apply_completion(chain_id, completion, started_attempts, failures) {
                Ok(response) => return response,
                Err(()) => failures += 1,
            }
        }

        match self
            .try_fallback(chain_id, body, request_id, deadline_at, failures)
            .await
        {
            FallbackOutcome::Success(response) => return response,
            FallbackOutcome::Failed => failures += 1,
            FallbackOutcome::Skipped => {}
        }

        self.metrics.record_failover_depth(chain_id, failures);
        self.exhausted(chain_id, request_id.clone(), pool_kind)
    }

    /// 公共池没有给出可用响应时，对配置的付费节点再试一次。
    async fn try_fallback(
        &self,
        chain_id: u64,
        body: &[u8],
        request_id: &Value,
        deadline_at: Instant,
        failures: usize,
    ) -> FallbackOutcome {
        if !self.registry.fallback_configured(chain_id) {
            return FallbackOutcome::Skipped;
        }
        if Instant::now() >= deadline_at {
            self.metrics.record_fallback_skipped(chain_id, "deadline");
            return FallbackOutcome::Skipped;
        }
        let Some(endpoint) = self.registry.fallback_endpoint(chain_id).await else {
            self.metrics
                .record_fallback_skipped(chain_id, "unavailable");
            return FallbackOutcome::Skipped;
        };
        if matches!(
            endpoint.state(Instant::now()),
            EndpointState::Cooling { .. }
        ) {
            self.metrics.record_fallback_skipped(chain_id, "cooling");
            return FallbackOutcome::Skipped;
        }
        let Some(lease) = endpoint.try_acquire() else {
            self.metrics.record_fallback_skipped(chain_id, "no_token");
            return FallbackOutcome::Skipped;
        };
        self.metrics.record_fallback_attempt(chain_id);
        self.metrics
            .record_upstream(chain_id, &endpoint.log_label());
        let completion = self
            .perform_attempt(endpoint, lease, body, request_id, deadline_at)
            .await;
        self.metrics
            .record_fallback_latency(chain_id, completion.latency);
        match self.apply_completion(
            chain_id,
            completion,
            self.max_attempts.saturating_add(1),
            failures,
        ) {
            Ok(response) => {
                self.metrics.record_fallback_success(chain_id);
                FallbackOutcome::Success(response)
            }
            Err(()) => {
                self.metrics.record_fallback_failure(chain_id);
                FallbackOutcome::Failed
            }
        }
    }

    fn public_deadline(&self, deadline_at: Instant, reserve_for_fallback: bool) -> Instant {
        if !reserve_for_fallback {
            return deadline_at;
        }
        let reserve = self.request_timeout.min(self.deadline / 2);
        if reserve.is_zero() {
            deadline_at
        } else {
            deadline_at.checked_sub(reserve).unwrap_or(deadline_at)
        }
    }

    async fn perform_attempt(
        &self,
        endpoint: Arc<Endpoint>,
        _lease: EndpointLease,
        body: &[u8],
        request_id: &Value,
        expires_at: Instant,
    ) -> AttemptCompletion {
        let started = Instant::now();
        let remaining = expires_at.saturating_duration_since(started);
        let result = if remaining.is_zero() {
            AttemptResult::Failure(FailureSignal::new(FaultKind::Timeout))
        } else {
            match timeout(
                self.request_timeout.min(remaining),
                self.send_attempt(endpoint.url(), body, request_id, started),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => AttemptResult::Failure(FailureSignal::new(FaultKind::Timeout)),
            }
        };
        let finished = Instant::now();
        AttemptCompletion {
            endpoint,
            result,
            finished,
            latency: finished.saturating_duration_since(started),
        }
    }

    fn apply_completion(
        &self,
        chain_id: u64,
        completion: AttemptCompletion,
        attempt: usize,
        failures: usize,
    ) -> std::result::Result<Value, ()> {
        match completion.result {
            AttemptResult::Valid(response) => {
                completion
                    .endpoint
                    .record_success(completion.finished, completion.latency, false);
                self.metrics.record_failover_depth(chain_id, failures);
                Ok(response)
            }
            AttemptResult::Degraded { response, fault } => {
                completion
                    .endpoint
                    .record_degraded(completion.finished, completion.latency, fault);
                self.metrics.record_failover_depth(chain_id, failures);
                Ok(response)
            }
            AttemptResult::Failure(signal) => {
                completion
                    .endpoint
                    .record_failure(completion.finished, signal.clone());
                debug!(
                    chain_id,
                    endpoint = completion.endpoint.log_label(),
                    attempt,
                    fault = ?signal.kind,
                    "upstream attempt failed"
                );
                Err(())
            }
        }
    }

    async fn send_attempt(
        &self,
        url: &str,
        body: &[u8],
        request_id: &Value,
        started: Instant,
    ) -> AttemptResult {
        let mut response = match self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_vec())
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                return AttemptResult::Failure(FailureSignal::new(FaultKind::Transport));
            }
        };
        let status = response.status();
        let headers = response.headers().clone();
        let mut response_body = Vec::new();
        loop {
            let chunk = match response.chunk().await {
                Ok(chunk) => chunk,
                Err(_) => {
                    return AttemptResult::Failure(FailureSignal::new(FaultKind::Transport));
                }
            };
            let Some(chunk) = chunk else {
                break;
            };
            if response_body.len().saturating_add(chunk.len()) > MAX_UPSTREAM_RESPONSE_BYTES {
                return AttemptResult::Failure(FailureSignal::new(FaultKind::InvalidResponse));
            }
            response_body.extend_from_slice(&chunk);
        }
        match classify_response(
            status,
            &headers,
            &response_body,
            started.elapsed(),
            request_id,
            self.slow_threshold,
        ) {
            ResponseClassification::Valid(response) => AttemptResult::Valid(response),
            ResponseClassification::Degraded { response, fault } => {
                AttemptResult::Degraded { response, fault }
            }
            ResponseClassification::Failure(signal) => AttemptResult::Failure(signal),
        }
    }

    fn exhausted(&self, chain_id: u64, request_id: Value, pool_kind: PoolKind) -> Value {
        if pool_kind == PoolKind::Active {
            self.registry.record_user_visible_error();
            self.metrics.record_user_visible_error(chain_id);
        } else {
            self.metrics.record_cold_start_failure(chain_id);
        }
        all_endpoints_exhausted_for_pool(chain_id, request_id, pool_kind)
    }
}

#[derive(Deserialize)]
struct RawRequestMetadata<'a> {
    #[serde(default)]
    method: Option<&'a str>,
    #[serde(default, borrow)]
    params: Option<&'a RawValue>,
    #[serde(default)]
    id: Value,
}

enum FallbackOutcome {
    Success(Value),
    Failed,
    Skipped,
}

enum AttemptResult {
    Valid(Value),
    Degraded { response: Value, fault: FaultKind },
    Failure(FailureSignal),
}

struct AttemptCompletion {
    endpoint: Arc<Endpoint>,
    result: AttemptResult,
    finished: Instant,
    latency: Duration,
}

pub fn all_endpoints_exhausted(chain_id: u64, request_id: Value) -> Value {
    all_endpoints_exhausted_for_pool(chain_id, request_id, PoolKind::Active)
}

fn all_endpoints_exhausted_for_pool(
    chain_id: u64,
    request_id: Value,
    pool_kind: PoolKind,
) -> Value {
    let mut error = json!({
        "code": -32000,
        "message": format!("rpcrouter: all upstream endpoints exhausted for chain {chain_id}")
    });
    if pool_kind != PoolKind::Active {
        error["data"] = json!({"reason": "cold_start"});
    }
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": error
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use axum::{
        Json, Router,
        body::Bytes,
        extract::State,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::post,
    };
    use serde_json::json;
    use tokio::time::Instant;

    use crate::{
        chainlist::{ChainEndpoints, ChainlistSnapshot},
        config::{ChainOverride, Config, DiscoveryConfig, HedgingConfig, UpstreamConfig},
        registry::Registry,
    };

    use super::*;

    #[derive(Clone)]
    struct Upstream {
        hits: Arc<AtomicU64>,
        fail: bool,
    }

    async fn handle(State(upstream): State<Upstream>, body: Bytes) -> Response {
        upstream.hits.fetch_add(1, Ordering::Relaxed);
        let id = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|value| value.get("id").cloned())
            .unwrap_or(Value::Null);
        if upstream.fail {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Json(json!({"jsonrpc":"2.0","id":id,"result":"0x10"})).into_response()
    }

    async fn spawn_upstream(fail: bool) -> (String, Arc<AtomicU64>) {
        let hits = Arc::new(AtomicU64::new(0));
        let app = Router::new()
            .route("/", post(handle))
            .route("/secret-token", post(handle))
            .with_state(Upstream {
                hits: Arc::clone(&hits),
                fail,
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let address = listener.local_addr().expect("upstream address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve upstream");
        });
        (format!("http://{address}/"), hits)
    }

    fn config_with_fallback(fallback: Option<String>) -> Config {
        let mut config = Config {
            chains: vec![1],
            discovery: DiscoveryConfig {
                enabled: false,
                ..DiscoveryConfig::default()
            },
            upstream: UpstreamConfig {
                request_timeout_ms: 500,
                slow_threshold_ms: 400,
                deadline_ms: 2_000,
                max_attempts: 2,
                default_rps: 100,
                default_concurrency: 8,
            },
            hedging: HedgingConfig {
                enabled: false,
                ..HedgingConfig::default()
            },
            ..Config::default()
        };
        if let Some(url) = fallback {
            config.chain_overrides.push(ChainOverride {
                chain_id: 1,
                fallback_url: Some(url),
                ..ChainOverride::default()
            });
        }
        config
    }

    async fn registry_with(config: &Config, pool: &[String]) -> Arc<Registry> {
        let registry = Arc::new(Registry::new(config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Fallback Test".to_owned(),
                    endpoints: pool.to_vec(),
                }],
            })
            .await;
        registry
    }

    fn activate(registry_endpoint: &crate::registry::Endpoint) {
        let now = Instant::now();
        registry_endpoint.record_success(now, std::time::Duration::from_millis(1), true);
        registry_endpoint.record_success(now, std::time::Duration::from_millis(1), true);
    }

    fn request() -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"fallback_uncached","params":[]})
    }

    #[tokio::test]
    async fn fallback_runs_only_after_the_public_pool_fails() {
        let (pool_url, pool_hits) = spawn_upstream(true).await;
        let (fallback_base, fallback_hits) = spawn_upstream(false).await;
        let fallback_url = format!("{fallback_base}secret-token");
        let config = config_with_fallback(Some(fallback_url.clone()));
        let registry = registry_with(&config, std::slice::from_ref(&pool_url)).await;
        let pool = registry
            .endpoint(1, &pool_url)
            .await
            .expect("pool endpoint");
        activate(&pool);
        let fallback = registry
            .fallback_endpoint(1)
            .await
            .expect("fallback endpoint");
        assert!(fallback.is_fallback());
        assert!(!fallback.log_label().contains("secret-token"));
        let candidates = registry.candidates(1).await;
        assert!(candidates.iter().all(|endpoint| !endpoint.is_fallback()));
        assert_eq!(candidates.len(), 1);
        let targets = registry.probe_targets(1).await;
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().any(|endpoint| endpoint.is_fallback()));

        let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");
        let response = forwarder.execute(1, request()).await;
        assert_eq!(response["result"], "0x10");
        assert_eq!(pool_hits.load(Ordering::Relaxed), 1);
        assert_eq!(fallback_hits.load(Ordering::Relaxed), 1);
        assert_eq!(registry.user_visible_errors(), 0);
        let encoded = forwarder
            .metrics()
            .encode(&registry)
            .await
            .expect("metrics");
        assert!(encoded.contains("rpcrouter_fallback_successes_total"));
        assert!(!encoded.contains("secret-token"));
    }

    #[tokio::test]
    async fn healthy_pool_does_not_call_fallback() {
        let (pool_url, pool_hits) = spawn_upstream(false).await;
        let (fallback_base, fallback_hits) = spawn_upstream(false).await;
        let fallback_url = format!("{fallback_base}secret-token");
        let config = config_with_fallback(Some(fallback_url));
        let registry = registry_with(&config, std::slice::from_ref(&pool_url)).await;
        activate(&registry.endpoint(1, &pool_url).await.expect("pool"));
        let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");
        let response = forwarder.execute(1, request()).await;
        assert_eq!(response["result"], "0x10");
        assert_eq!(pool_hits.load(Ordering::Relaxed), 1);
        assert_eq!(fallback_hits.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exhausted_fallback_counts_a_user_visible_error() {
        let (pool_url, _) = spawn_upstream(true).await;
        let (fallback_base, fallback_hits) = spawn_upstream(true).await;
        let fallback_url = format!("{fallback_base}secret-token");
        let config = config_with_fallback(Some(fallback_url));
        let registry = registry_with(&config, std::slice::from_ref(&pool_url)).await;
        activate(&registry.endpoint(1, &pool_url).await.expect("pool"));
        let forwarder = Forwarder::new(registry.clone(), &config).expect("forwarder");
        let response = forwarder.execute(1, request()).await;
        assert_eq!(response["error"]["code"], -32000);
        assert_eq!(fallback_hits.load(Ordering::Relaxed), 1);
        assert_eq!(registry.user_visible_errors(), 1);
    }

    #[tokio::test]
    async fn fallback_serves_when_the_public_pool_is_empty() {
        let (fallback_base, fallback_hits) = spawn_upstream(false).await;
        let fallback_url = format!("{fallback_base}secret-token");
        let config = config_with_fallback(Some(fallback_url));
        let registry = registry_with(&config, &[]).await;
        let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");
        let response = forwarder.execute(1, request()).await;
        assert_eq!(response["result"], "0x10");
        assert_eq!(fallback_hits.load(Ordering::Relaxed), 1);
        assert_eq!(registry.user_visible_errors(), 0);
    }
}
