use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dashmap::DashMap;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry as PrometheusRegistry, TextEncoder,
};

use crate::registry::{EndpointStatsSnapshot, Registry};

pub struct Metrics {
    registry: PrometheusRegistry,
    ingress: IntCounterVec,
    ingress_rejected: IntCounterVec,
    in_flight: IntGauge,
    cache_lookups: IntCounterVec,
    cache_hits: IntCounterVec,
    cache_hit_ratio: GaugeVec,
    cache_misses: IntCounterVec,
    coalesced: IntCounterVec,
    coalesce_ratio: GaugeVec,
    upstream: IntCounterVec,
    user_visible_errors: IntCounterVec,
    cold_start_failures: IntCounterVec,
    latency: HistogramVec,
    failover_depth: HistogramVec,
    hedge_attempts: IntCounterVec,
    hedge_ratio: GaugeVec,
    endpoint_requests: IntCounterVec,
    endpoint_rate_limited: IntCounterVec,
    endpoint_cooling_events: IntCounterVec,
    endpoint_state: IntGaugeVec,
    known_endpoints: Mutex<HashMap<(String, String), EndpointStatsSnapshot>>,
    hedge_totals: DashMap<u64, Arc<HedgeTotals>>,
    // ── v2 新指标 ──
    chain_state: IntGaugeVec,
    chain_pinned: IntGaugeVec,
    catalog_chains: IntGauge,
    catalog_endpoints: IntGauge,
    catalog_records_skipped: IntCounter,
    probe_queue_depth: IntGauge,
    auto_enable_chains: IntGauge,
    auto_enable_candidates: IntGauge,
    auto_enable_promotions: IntCounter,
    auto_enable_probe_failures: IntCounter,
    probe_in_flight: IntGauge,
    chainlist_last_refresh: IntGauge,
    chainlist_refresh_total: IntCounterVec,
    chain_activations: IntCounter,
    chain_demotions: IntCounterVec,
    background_task_restarts: IntCounterVec,
    state_store_up: IntGauge,
    state_flush_total: IntCounterVec,
    state_flush_duration: HistogramVec,
    state_dirty_endpoints: IntGauge,
    v2_totals: Mutex<V2Totals>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ChainMetricsSnapshot {
    pub ingress: u64,
    pub cache_lookups: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub coalesced: u64,
    pub upstream: u64,
    pub user_visible_errors: u64,
    pub cold_start_failures: u64,
    pub hedges: u64,
}

#[derive(Default)]
struct HedgeTotals {
    upstream: AtomicU64,
    hedges: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct V2Totals {
    activations: u64,
    demotions_idle: u64,
    demotions_lru: u64,
    demotions_admin: u64,
}

impl Metrics {
    pub fn new() -> prometheus::Result<Self> {
        let registry = PrometheusRegistry::new();
        let ingress = IntCounterVec::new(
            Opts::new(
                "rpcrouter_chain_ingress_requests_total",
                "Ingress JSON-RPC requests; use rate() for QPS.",
            ),
            &["chain_id"],
        )?;
        let ingress_rejected = IntCounterVec::new(
            Opts::new(
                "rpcrouter_ingress_rejected_total",
                "Requests rejected at the ingress guard before forwarding. Not part of user_visible_errors.",
            ),
            &["reason"],
        )?;
        let in_flight = IntGauge::new(
            "rpcrouter_in_flight_requests",
            "Number of ingress requests currently in flight (before guard passes).",
        )?;
        let cache_lookups = IntCounterVec::new(
            Opts::new(
                "rpcrouter_cache_lookups_total",
                "Cacheable request lookups.",
            ),
            &["chain_id"],
        )?;
        let cache_hits = IntCounterVec::new(
            Opts::new("rpcrouter_cache_hits_total", "Response cache hits."),
            &["chain_id"],
        )?;
        let cache_hit_ratio = GaugeVec::new(
            Opts::new(
                "rpcrouter_cache_hit_ratio",
                "Cache hits divided by cacheable lookups.",
            ),
            &["chain_id"],
        )?;
        let cache_misses = IntCounterVec::new(
            Opts::new(
                "rpcrouter_cache_misses_total",
                "Cache misses entering singleflight.",
            ),
            &["chain_id"],
        )?;
        let coalesced = IntCounterVec::new(
            Opts::new(
                "rpcrouter_coalesced_requests_total",
                "Cache misses served by an in-flight leader.",
            ),
            &["chain_id"],
        )?;
        let coalesce_ratio = GaugeVec::new(
            Opts::new(
                "rpcrouter_coalesce_ratio",
                "Coalesced followers divided by cache misses.",
            ),
            &["chain_id"],
        )?;
        let upstream = IntCounterVec::new(
            Opts::new(
                "rpcrouter_chain_upstream_requests_total",
                "Data-plane upstream requests; use rate() for QPS.",
            ),
            &["chain_id", "endpoint"],
        )?;
        let user_visible_errors = IntCounterVec::new(
            Opts::new(
                "rpcrouter_user_visible_errors_total",
                "Requests with an active upstream promise that exhaust all endpoints.",
            ),
            &["chain_id"],
        )?;
        let cold_start_failures = IntCounterVec::new(
            Opts::new(
                "rpcrouter_cold_start_failures_total",
                "Requests exhausting a chain without any active endpoint at ingress.",
            ),
            &["chain_id"],
        )?;
        let latency = HistogramVec::new(
            HistogramOpts::new(
                "rpcrouter_request_latency_seconds",
                "End-to-end JSON-RPC request latency.",
            )
            .buckets(vec![
                0.000_1, 0.000_25, 0.000_5, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
                0.5, 1.0, 2.5, 5.0, 15.0,
            ]),
            &["chain_id"],
        )?;
        let failover_depth = HistogramVec::new(
            HistogramOpts::new(
                "rpcrouter_failover_depth",
                "Number of failed upstream attempts before completion.",
            )
            .buckets(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
            &["chain_id"],
        )?;
        let hedge_attempts = IntCounterVec::new(
            Opts::new(
                "rpcrouter_hedge_attempts_total",
                "Secondary hedge requests.",
            ),
            &["chain_id"],
        )?;
        let hedge_ratio = GaugeVec::new(
            Opts::new(
                "rpcrouter_hedge_ratio",
                "Hedge requests divided by data-plane upstream requests.",
            ),
            &["chain_id"],
        )?;
        let endpoint_requests = IntCounterVec::new(
            Opts::new(
                "rpcrouter_endpoint_requests_total",
                "All endpoint requests including health probes; use rate() for QPS.",
            ),
            &["chain_id", "endpoint"],
        )?;
        let endpoint_rate_limited = IntCounterVec::new(
            Opts::new(
                "rpcrouter_endpoint_rate_limited_total",
                "Rate-limit responses observed for an endpoint.",
            ),
            &["chain_id", "endpoint"],
        )?;
        let endpoint_cooling_events = IntCounterVec::new(
            Opts::new(
                "rpcrouter_endpoint_cooling_events_total",
                "Cooling transitions observed for an endpoint.",
            ),
            &["chain_id", "endpoint"],
        )?;
        let endpoint_state = IntGaugeVec::new(
            Opts::new(
                "rpcrouter_endpoint_state",
                "Endpoint state represented as a one-hot gauge.",
            ),
            &["chain_id", "endpoint", "state"],
        )?;

        // ── v2 新指标 ──
        let chain_state = IntGaugeVec::new(
            Opts::new("rpcrouter_chains", "Number of chains by lifecycle state."),
            &["state"],
        )?;
        let chain_pinned = IntGaugeVec::new(
            Opts::new(
                "rpcrouter_chain_pinned",
                "1 if the chain is pinned, 0 otherwise.",
            ),
            &["chain_id"],
        )?;
        let catalog_chains = IntGauge::new(
            "rpcrouter_catalog_chains",
            "Number of chains in the catalog.",
        )?;
        let catalog_endpoints = IntGauge::new(
            "rpcrouter_catalog_endpoints",
            "Number of endpoints in the catalog.",
        )?;
        let catalog_records_skipped = IntCounter::new(
            "rpcrouter_catalog_records_skipped_total",
            "Number of malformed chainlist records skipped during tolerant parsing.",
        )?;
        let probe_queue_depth = IntGauge::new(
            "rpcrouter_probe_queue_depth",
            "Number of probe tasks waiting in the queue.",
        )?;
        // 自动开启：全局标量，不带 chain_id 标签（基数不随链数增长）。
        let auto_enable_chains = IntGauge::new(
            "rpcrouter_auto_enable_chains",
            "Number of chains currently auto-enabled.",
        )?;
        let auto_enable_candidates = IntGauge::new(
            "rpcrouter_auto_enable_candidates",
            "Number of chains in the auto-enable candidate pool.",
        )?;
        let auto_enable_promotions = IntCounter::new(
            "rpcrouter_auto_enable_promotions_total",
            "Number of chains promoted to auto-enabled.",
        )?;
        let auto_enable_probe_failures = IntCounter::new(
            "rpcrouter_auto_enable_probe_failures_total",
            "Number of failed candidate endpoint probes.",
        )?;
        let probe_in_flight = IntGauge::new(
            "rpcrouter_probe_in_flight",
            "Number of probes currently in flight.",
        )?;
        let chainlist_last_refresh = IntGauge::new(
            "rpcrouter_chainlist_last_refresh_timestamp_seconds",
            "Unix timestamp of the last successful chainlist refresh.",
        )?;
        let chainlist_refresh_total = IntCounterVec::new(
            Opts::new(
                "rpcrouter_chainlist_refresh_total",
                "Chainlist refresh attempts by source.",
            ),
            &["source"],
        )?;
        let chain_activations = IntCounter::new(
            "rpcrouter_chain_activations_total",
            "Number of chain activations (dormant → hot).",
        )?;
        let chain_demotions = IntCounterVec::new(
            Opts::new(
                "rpcrouter_chain_demotions_total",
                "Number of chain demotions by reason.",
            ),
            &["reason"],
        )?;
        let background_task_restarts = IntCounterVec::new(
            Opts::new(
                "rpcrouter_background_task_restarts_total",
                "Background task restarts after panic or unexpected exit.",
            ),
            &["task"],
        )?;
        let state_store_up = IntGauge::new(
            "rpcrouter_state_store_up",
            "1 when the configured persistent state store is reachable.",
        )?;
        let state_flush_total = IntCounterVec::new(
            Opts::new(
                "rpcrouter_state_flush_total",
                "State write-behind flushes by result.",
            ),
            &["result"],
        )?;
        let state_flush_duration = HistogramVec::new(
            HistogramOpts::new(
                "rpcrouter_state_flush_duration_seconds",
                "State write-behind flush duration.",
            ),
            &[],
        )?;
        let state_dirty_endpoints = IntGauge::new(
            "rpcrouter_state_dirty_endpoints",
            "Endpoints waiting for state write-behind.",
        )?;

        for collector in [
            Box::new(ingress.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(ingress_rejected.clone()),
            Box::new(in_flight.clone()),
            Box::new(cache_lookups.clone()),
            Box::new(cache_hits.clone()),
            Box::new(cache_hit_ratio.clone()),
            Box::new(cache_misses.clone()),
            Box::new(coalesced.clone()),
            Box::new(coalesce_ratio.clone()),
            Box::new(upstream.clone()),
            Box::new(user_visible_errors.clone()),
            Box::new(cold_start_failures.clone()),
            Box::new(latency.clone()),
            Box::new(failover_depth.clone()),
            Box::new(hedge_attempts.clone()),
            Box::new(hedge_ratio.clone()),
            Box::new(endpoint_requests.clone()),
            Box::new(endpoint_rate_limited.clone()),
            Box::new(endpoint_cooling_events.clone()),
            Box::new(endpoint_state.clone()),
            Box::new(chain_state.clone()),
            Box::new(chain_pinned.clone()),
            Box::new(catalog_chains.clone()),
            Box::new(catalog_endpoints.clone()),
            Box::new(catalog_records_skipped.clone()),
            Box::new(probe_queue_depth.clone()),
            Box::new(auto_enable_chains.clone()),
            Box::new(auto_enable_candidates.clone()),
            Box::new(auto_enable_promotions.clone()),
            Box::new(auto_enable_probe_failures.clone()),
            Box::new(probe_in_flight.clone()),
            Box::new(chainlist_last_refresh.clone()),
            Box::new(chainlist_refresh_total.clone()),
            Box::new(chain_activations.clone()),
            Box::new(chain_demotions.clone()),
            Box::new(background_task_restarts.clone()),
            Box::new(state_store_up.clone()),
            Box::new(state_flush_total.clone()),
            Box::new(state_flush_duration.clone()),
            Box::new(state_dirty_endpoints.clone()),
        ] {
            registry.register(collector)?;
        }

        Ok(Self {
            registry,
            ingress,
            ingress_rejected,
            in_flight,
            cache_lookups,
            cache_hits,
            cache_hit_ratio,
            cache_misses,
            coalesced,
            coalesce_ratio,
            upstream,
            user_visible_errors,
            cold_start_failures,
            latency,
            failover_depth,
            hedge_attempts,
            hedge_ratio,
            endpoint_requests,
            endpoint_rate_limited,
            endpoint_cooling_events,
            endpoint_state,
            known_endpoints: Mutex::new(HashMap::new()),
            hedge_totals: DashMap::new(),
            chain_state,
            chain_pinned,
            catalog_chains,
            catalog_endpoints,
            catalog_records_skipped,
            probe_queue_depth,
            auto_enable_chains,
            auto_enable_candidates,
            auto_enable_promotions,
            auto_enable_probe_failures,
            probe_in_flight,
            chainlist_last_refresh,
            chainlist_refresh_total,
            chain_activations,
            chain_demotions,
            background_task_restarts,
            state_store_up,
            state_flush_total,
            state_flush_duration,
            state_dirty_endpoints,
            v2_totals: Mutex::new(V2Totals::default()),
        })
    }

    pub fn record_ingress(&self, chain_id: u64) {
        self.ingress
            .with_label_values(&[&chain_id.to_string()])
            .inc();
    }

    pub fn record_ingress_rejected(&self, reason: &str) {
        self.ingress_rejected.with_label_values(&[reason]).inc();
    }

    pub fn in_flight_inc(&self) {
        self.in_flight.inc();
    }

    pub fn in_flight_dec(&self) {
        self.in_flight.dec();
    }

    pub fn record_cache_lookup(&self, chain_id: u64, hit: bool) {
        let chain = chain_id.to_string();
        let lookups = self.cache_lookups.with_label_values(&[&chain]);
        lookups.inc();
        let hits = self.cache_hits.with_label_values(&[&chain]);
        if hit {
            hits.inc();
        }
        self.cache_hit_ratio
            .with_label_values(&[&chain])
            .set(hits.get() as f64 / lookups.get() as f64);
    }

    pub fn record_cache_miss_role(&self, chain_id: u64, coalesced: bool) {
        let chain = chain_id.to_string();
        let misses = self.cache_misses.with_label_values(&[&chain]);
        misses.inc();
        let followers = self.coalesced.with_label_values(&[&chain]);
        if coalesced {
            followers.inc();
        }
        self.coalesce_ratio
            .with_label_values(&[&chain])
            .set(followers.get() as f64 / misses.get() as f64);
    }

    pub fn record_upstream(&self, chain_id: u64, endpoint: &str) {
        let chain = chain_id.to_string();
        self.upstream.with_label_values(&[&chain, endpoint]).inc();
        let totals = self.hedge_totals(chain_id);
        totals.upstream.fetch_add(1, Ordering::Relaxed);
        self.update_hedge_ratio(&chain, &totals);
    }

    pub fn record_hedge(&self, chain_id: u64) {
        let chain = chain_id.to_string();
        self.hedge_attempts.with_label_values(&[&chain]).inc();
        let totals = self.hedge_totals(chain_id);
        totals.hedges.fetch_add(1, Ordering::Relaxed);
        self.update_hedge_ratio(&chain, &totals);
    }

    fn update_hedge_ratio(&self, chain: &str, totals: &HedgeTotals) {
        let denominator = totals.upstream.load(Ordering::Relaxed);
        let hedges = totals.hedges.load(Ordering::Relaxed);
        self.hedge_ratio
            .with_label_values(&[chain])
            .set(if denominator == 0 {
                0.0
            } else {
                hedges as f64 / denominator as f64
            });
    }

    fn hedge_totals(&self, chain_id: u64) -> Arc<HedgeTotals> {
        self.hedge_totals
            .entry(chain_id)
            .or_insert_with(|| Arc::new(HedgeTotals::default()))
            .clone()
    }

    pub fn record_user_visible_error(&self, chain_id: u64) {
        self.user_visible_errors
            .with_label_values(&[&chain_id.to_string()])
            .inc();
    }

    pub fn record_cold_start_failure(&self, chain_id: u64) {
        self.cold_start_failures
            .with_label_values(&[&chain_id.to_string()])
            .inc();
    }

    pub fn record_latency(&self, chain_id: u64, latency: Duration) {
        self.latency
            .with_label_values(&[&chain_id.to_string()])
            .observe(latency.as_secs_f64());
    }

    pub fn record_failover_depth(&self, chain_id: u64, depth: usize) {
        self.failover_depth
            .with_label_values(&[&chain_id.to_string()])
            .observe(depth as f64);
    }

    // ── v2 新方法 ──

    pub fn set_chain_state_counts(&self, pinned: u64, hot: u64, dormant: u64, disabled: u64) {
        self.chain_state
            .with_label_values(&["pinned"])
            .set(pinned as i64);
        self.chain_state.with_label_values(&["hot"]).set(hot as i64);
        self.chain_state
            .with_label_values(&["dormant"])
            .set(dormant as i64);
        self.chain_state
            .with_label_values(&["disabled"])
            .set(disabled as i64);
    }

    pub fn set_chain_pinned(&self, chain_id: u64, pinned: bool) {
        let chain = chain_id.to_string();
        self.chain_pinned
            .with_label_values(&[&chain])
            .set(i64::from(pinned));
    }

    pub fn set_catalog_counts(&self, chains: u64, endpoints: u64) {
        self.catalog_chains.set(chains as i64);
        self.catalog_endpoints.set(endpoints as i64);
    }

    pub fn record_catalog_records_skipped(&self, count: usize) {
        self.catalog_records_skipped.inc_by(count as u64);
    }

    pub fn set_auto_enable_gauges(&self, chains: u64, candidates: u64) {
        self.auto_enable_chains.set(chains as i64);
        self.auto_enable_candidates.set(candidates as i64);
    }

    pub fn record_auto_enable_promotion(&self) {
        self.auto_enable_promotions.inc();
    }

    pub fn record_auto_enable_probe_failures(&self, count: u64) {
        self.auto_enable_probe_failures.inc_by(count);
    }

    pub fn auto_enable_promotions_total(&self) -> u64 {
        self.auto_enable_promotions.get()
    }

    pub fn set_probe_queue_depth(&self, depth: u64) {
        self.probe_queue_depth.set(depth as i64);
    }

    pub fn set_probe_in_flight(&self, in_flight: u64) {
        self.probe_in_flight.set(in_flight as i64);
    }

    pub fn set_chainlist_last_refresh(&self, ts: u64) {
        self.chainlist_last_refresh.set(ts as i64);
    }

    pub fn record_chainlist_refresh(&self, source: &str) {
        self.chainlist_refresh_total
            .with_label_values(&[source])
            .inc();
    }

    pub fn record_chain_activation(&self) {
        self.chain_activations.inc();
    }

    pub fn record_chain_demotion(&self, reason: &str) {
        self.chain_demotions.with_label_values(&[reason]).inc();
    }

    pub fn record_background_task_restart(&self, task: &str) {
        self.background_task_restarts
            .with_label_values(&[task])
            .inc();
    }
    pub fn background_task_restarts(&self, task: &str) -> u64 {
        self.background_task_restarts
            .with_label_values(&[task])
            .get()
    }
    pub fn set_state_store_up(&self, up: bool) {
        self.state_store_up.set(i64::from(up));
    }
    pub fn record_state_flush(&self, result: &str, duration: Duration) {
        self.state_flush_total.with_label_values(&[result]).inc();
        self.state_flush_duration
            .with_label_values(&[] as &[&str])
            .observe(duration.as_secs_f64());
    }
    pub fn set_state_dirty_endpoints(&self, count: usize) {
        self.state_dirty_endpoints.set(count as i64);
    }

    pub fn chain_snapshot(&self, chain_id: u64) -> ChainMetricsSnapshot {
        self.chain_snapshots().remove(&chain_id).unwrap_or_default()
    }

    /// 一次 gather 聚合所有链的计数，避免逐链对每个 CounterVec 重复 collect。
    pub fn chain_snapshots(&self) -> HashMap<u64, ChainMetricsSnapshot> {
        let mut snapshots = HashMap::<u64, ChainMetricsSnapshot>::new();
        for family in self.registry.gather() {
            let field = match family.name() {
                "rpcrouter_chain_ingress_requests_total" => 0,
                "rpcrouter_cache_lookups_total" => 1,
                "rpcrouter_cache_hits_total" => 2,
                "rpcrouter_cache_misses_total" => 3,
                "rpcrouter_coalesced_requests_total" => 4,
                "rpcrouter_user_visible_errors_total" => 5,
                "rpcrouter_cold_start_failures_total" => 6,
                _ => continue,
            };
            for metric in family.get_metric() {
                let Some(chain_id) = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "chain_id")
                    .and_then(|label| label.value().parse::<u64>().ok())
                else {
                    continue;
                };
                let value = metric.get_counter().value() as u64;
                let snapshot = snapshots.entry(chain_id).or_default();
                match field {
                    0 => snapshot.ingress = value,
                    1 => snapshot.cache_lookups = value,
                    2 => snapshot.cache_hits = value,
                    3 => snapshot.cache_misses = value,
                    4 => snapshot.coalesced = value,
                    5 => snapshot.user_visible_errors = value,
                    6 => snapshot.cold_start_failures = value,
                    _ => unreachable!(),
                }
            }
        }
        for entry in &self.hedge_totals {
            let snapshot = snapshots.entry(*entry.key()).or_default();
            let totals = entry.value();
            snapshot.upstream = totals.upstream.load(Ordering::Relaxed);
            snapshot.hedges = totals.hedges.load(Ordering::Relaxed);
        }
        snapshots
    }

    pub fn in_flight(&self) -> i64 {
        self.in_flight.get()
    }

    pub fn ingress_rejected_total(&self) -> u64 {
        [
            "unknown_chain",
            "chain_disabled",
            "no_endpoints",
            "overload",
            "body_too_large",
            "rate_limited",
        ]
        .iter()
        .map(|reason| self.ingress_rejected.with_label_values(&[*reason]).get())
        .sum()
    }

    pub async fn encode(&self, rpc_registry: &Registry) -> prometheus::Result<String> {
        self.sync_endpoints(rpc_registry).await;
        self.sync_v2_gauges(rpc_registry).await;
        let families = self.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }

    async fn sync_v2_gauges(&self, rpc_registry: &Registry) {
        let counts = rpc_registry.chain_counts().await;
        self.set_chain_state_counts(counts.pinned, counts.hot, counts.dormant, counts.disabled);
        self.set_catalog_counts(
            rpc_registry.catalog_chain_count(),
            rpc_registry.catalog_endpoint_count(),
        );
        self.set_probe_queue_depth(rpc_registry.probe_queue_depth.load(Ordering::Relaxed));
        self.set_probe_in_flight(rpc_registry.probe_in_flight.load(Ordering::Relaxed));
        self.set_chainlist_last_refresh(rpc_registry.chainlist_last_refresh());
        for (chain_id, pinned) in rpc_registry.materialized_chain_pinned() {
            self.set_chain_pinned(chain_id, pinned);
        }

        let activations = rpc_registry.chain_activations();
        let (demotions_idle, demotions_lru, demotions_admin) = rpc_registry.chain_demotions();
        let mut totals = lock(&self.v2_totals);
        self.chain_activations
            .inc_by(activations.saturating_sub(totals.activations));
        for (reason, current, previous) in [
            ("idle", demotions_idle, totals.demotions_idle),
            ("lru", demotions_lru, totals.demotions_lru),
            ("admin", demotions_admin, totals.demotions_admin),
        ] {
            self.chain_demotions
                .with_label_values(&[reason])
                .inc_by(current.saturating_sub(previous));
        }
        *totals = V2Totals {
            activations,
            demotions_idle,
            demotions_lru,
            demotions_admin,
        };
    }

    async fn sync_endpoints(&self, rpc_registry: &Registry) {
        let snapshots = rpc_registry.endpoint_metric_snapshots().await;
        let current_keys: HashSet<_> = snapshots
            .iter()
            .map(|snapshot| (snapshot.chain_id.to_string(), snapshot.url.clone()))
            .collect();
        let mut known = lock(&self.known_endpoints);
        for (chain, endpoint) in known.keys().filter(|key| !current_keys.contains(*key)) {
            let _ = self
                .endpoint_requests
                .remove_label_values(&[chain, endpoint]);
            let _ = self
                .endpoint_rate_limited
                .remove_label_values(&[chain, endpoint]);
            let _ = self
                .endpoint_cooling_events
                .remove_label_values(&[chain, endpoint]);
            for state in ["active", "cooling", "probation"] {
                let _ = self
                    .endpoint_state
                    .remove_label_values(&[chain, endpoint, state]);
            }
        }
        let mut current = HashMap::with_capacity(snapshots.len());
        for snapshot in snapshots {
            let chain = snapshot.chain_id.to_string();
            let key = (chain.clone(), snapshot.url.clone());
            let previous = known.get(&key).copied().unwrap_or_default();
            self.endpoint_requests
                .with_label_values(&[&chain, &snapshot.url])
                .inc_by(
                    snapshot
                        .stats
                        .outbound_requests
                        .saturating_sub(previous.outbound_requests),
                );
            self.endpoint_rate_limited
                .with_label_values(&[&chain, &snapshot.url])
                .inc_by(
                    snapshot
                        .stats
                        .rate_limited
                        .saturating_sub(previous.rate_limited),
                );
            self.endpoint_cooling_events
                .with_label_values(&[&chain, &snapshot.url])
                .inc_by(
                    snapshot
                        .stats
                        .cooling_events
                        .saturating_sub(previous.cooling_events),
                );
            for state in ["active", "cooling", "probation"] {
                self.endpoint_state
                    .with_label_values(&[&chain, &snapshot.url, state])
                    .set(i64::from(state == snapshot.state));
            }
            current.insert(key, snapshot.stats);
        }
        *known = current;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
        config::{Config, DiscoveryConfig},
    };

    use super::*;

    #[tokio::test]
    async fn encodes_chain_cache_and_endpoint_metrics() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let rpc_registry = Arc::new(Registry::new(&config));
        rpc_registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test".to_owned(),
                    endpoints: vec!["http://upstream".to_owned()],
                }],
            })
            .await;
        let metrics = Metrics::new().expect("metrics");
        metrics.record_ingress(1);
        metrics.record_cache_lookup(1, true);
        metrics.record_cache_miss_role(1, true);
        metrics.record_upstream(1, "http://upstream");
        metrics.record_latency(1, Duration::from_millis(2));
        metrics.record_catalog_records_skipped(1);
        let encoded = metrics.encode(&rpc_registry).await.expect("encode");
        assert!(encoded.contains("rpcrouter_chain_ingress_requests_total{chain_id=\"1\"} 1"));
        assert!(encoded.contains("rpcrouter_cache_hit_ratio{chain_id=\"1\"} 1"));
        assert!(encoded.contains("rpcrouter_endpoint_state"));
        assert!(encoded.contains("endpoint=\"http://upstream\""));
        assert!(encoded.contains("rpcrouter_chain_pinned{chain_id=\"1\"} 1"));
        assert!(encoded.contains("rpcrouter_catalog_records_skipped_total 1"));
    }

    #[tokio::test]
    async fn syncs_lifecycle_counter_deltas() {
        let config = Config {
            chains: vec![],
            discovery: DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 42,
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
                        url: "http://upstream".to_owned(),
                        tracking: None,
                    }],
                }],
                by_id: HashMap::from([(42, 0)]),
            }))
            .await;
        registry.resolve_for_request(42).await.expect("activate");
        registry.demote(42, "lru").await;
        let metrics = Metrics::new().expect("metrics");
        let encoded = metrics.encode(&registry).await.expect("encode");
        assert!(encoded.contains("rpcrouter_chain_activations_total 1"));
        assert!(encoded.contains("rpcrouter_chain_demotions_total{reason=\"lru\"} 1"));
        assert!(encoded.contains("rpcrouter_chains{state=\"dormant\"} 1"));
    }

    #[test]
    fn gathers_all_chain_snapshots_in_one_batch() {
        let metrics = Metrics::new().expect("metrics");
        metrics.record_ingress(1);
        metrics.record_cache_lookup(1, true);
        metrics.record_upstream(1, "http://one");
        metrics.record_ingress(2);
        metrics.record_user_visible_error(2);
        let snapshots = metrics.chain_snapshots();
        assert_eq!(snapshots[&1].ingress, 1);
        assert_eq!(snapshots[&1].cache_hits, 1);
        assert_eq!(snapshots[&1].upstream, 1);
        assert_eq!(snapshots[&2].ingress, 1);
        assert_eq!(snapshots[&2].user_visible_errors, 1);
    }
}
