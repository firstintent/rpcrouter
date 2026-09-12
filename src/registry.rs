use std::ops::Deref;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroU32,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use rand::Rng;
use serde::Serialize;
use tokio::{
    sync::{OwnedSemaphorePermit, RwLock, Semaphore, broadcast},
    time::{Duration, Instant},
};

use crate::{
    chainlist::{Catalog, ChainEndpoints, ChainlistSnapshot},
    config::Config,
    signals::{FailureSignal, FaultKind},
    state::{ChainOverrideState, EndpointOverrideState, HealthSnapshot, Overrides},
};

const ERROR_WINDOW: Duration = Duration::from_secs(60);
const STRIKE_DECAY_INTERVAL: Duration = Duration::from_secs(15 * 60);
const FAULTS_BEFORE_COOLING: u8 = 3;
const PROBATION_PASSES_REQUIRED: u8 = 2;

fn valid_chain_override(value: &ChainOverrideState) -> bool {
    value
        .block_time_ms
        .is_none_or(|v| (100..=600_000).contains(&v))
        && value
            .confirmation_depth
            .is_none_or(|v| (1..=100_000).contains(&v))
        && value.tip_ttl_ms.is_none_or(|v| (100..=60_000).contains(&v))
        && value.max_block_lag.is_none_or(|v| v <= 10_000)
}

// ── 端点状态机（v1 不变） ──

#[derive(Clone, Debug, PartialEq)]
pub enum EndpointState {
    Active,
    Cooling { until: Instant, strikes: u32 },
    Probation { passes: u8 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolKind {
    Active,
    Probation,
    Empty,
}

pub struct CandidateSet {
    pub endpoints: Vec<Arc<Endpoint>>,
    pub kind: PoolKind,
}

struct ActivationLockCleanup<'a> {
    locks: &'a DashMap<u64, Arc<tokio::sync::Mutex<()>>>,
    chain_id: u64,
}

impl Drop for ActivationLockCleanup<'_> {
    fn drop(&mut self) {
        let _ = self
            .locks
            .remove_if(&self.chain_id, |_, lock| Arc::strong_count(lock) <= 2);
    }
}

impl Deref for CandidateSet {
    type Target = [Arc<Endpoint>];

    fn deref(&self) -> &Self::Target {
        &self.endpoints
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HealthStatus {
    Active,
    Cooling { until: Instant },
    Probation { passes: u8 },
}

struct HealthState {
    status: HealthStatus,
    strikes: u32,
    healthy_since: Option<Instant>,
    consecutive_faults: u8,
    last_fault: Option<Instant>,
}

impl HealthState {
    fn probation() -> Self {
        Self {
            status: HealthStatus::Probation { passes: 0 },
            strikes: 0,
            healthy_since: None,
            consecutive_faults: 0,
            last_fault: None,
        }
    }

    fn decay_strikes(&mut self, now: Instant) {
        if !matches!(self.status, HealthStatus::Active) {
            return;
        }
        let Some(healthy_since) = self.healthy_since else {
            return;
        };
        let elapsed = now.saturating_duration_since(healthy_since);
        let steps = elapsed.as_secs() / STRIKE_DECAY_INTERVAL.as_secs();
        if steps == 0 {
            return;
        }
        self.strikes = self
            .strikes
            .saturating_sub(steps.min(u64::from(u32::MAX)) as u32);
        self.healthy_since = Some(now);
    }
}

#[derive(Default)]
struct OutcomeWindow {
    entries: VecDeque<(Instant, bool)>,
}

impl OutcomeWindow {
    fn record(&mut self, now: Instant, failed: bool) {
        self.prune(now);
        self.entries.push_back((now, failed));
    }

    fn error_rate(&mut self, now: Instant) -> f64 {
        self.prune(now);
        if self.entries.is_empty() {
            return 0.0;
        }
        let failures = self.entries.iter().filter(|(_, failed)| *failed).count();
        failures as f64 / self.entries.len() as f64
    }

    fn prune(&mut self, now: Instant) {
        while self
            .entries
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > ERROR_WINDOW)
        {
            self.entries.pop_front();
        }
    }
}

#[derive(Default)]
struct EndpointStats {
    outbound_requests: AtomicU64,
    failures: AtomicU64,
    rate_limited: AtomicU64,
    cooling_events: AtomicU64,
    probe_successes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointStatsSnapshot {
    pub outbound_requests: u64,
    pub failures: u64,
    pub rate_limited: u64,
    pub cooling_events: u64,
    pub probe_successes: u64,
}

impl EndpointStats {
    fn snapshot(&self) -> EndpointStatsSnapshot {
        EndpointStatsSnapshot {
            outbound_requests: self.outbound_requests.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            cooling_events: self.cooling_events.load(Ordering::Relaxed),
            probe_successes: self.probe_successes.load(Ordering::Relaxed),
        }
    }
}

pub struct Endpoint {
    url: String,
    rps: u32,
    concurrency: usize,
    bucket: DefaultDirectRateLimiter,
    inflight: Arc<Semaphore>,
    last_seen: AtomicU64,
    health: Mutex<HealthState>,
    outcomes: Mutex<OutcomeWindow>,
    latency_ewma_micros: AtomicU64,
    lag: AtomicU64,
    height_observation: Mutex<Option<(u64, Instant)>>,
    stats: EndpointStats,
    /// 同一端点同一时刻至多一个在飞探针（防止 kick 与周期调度重复入队）。
    probing: AtomicBool,
    dirty: AtomicBool,
}

impl Endpoint {
    fn new(url: String, rps: u32, concurrency: usize, now: u64) -> Self {
        let rps = rps.clamp(1, 100);
        let concurrency = concurrency.clamp(1, 64);
        let quota = Quota::per_second(NonZeroU32::new(rps).expect("validated nonzero RPS"))
            .allow_burst(NonZeroU32::new(rps).expect("validated nonzero RPS"));
        Self {
            url,
            rps,
            concurrency,
            bucket: RateLimiter::direct(quota),
            inflight: Arc::new(Semaphore::new(concurrency)),
            last_seen: AtomicU64::new(now),
            health: Mutex::new(HealthState::probation()),
            outcomes: Mutex::new(OutcomeWindow::default()),
            latency_ewma_micros: AtomicU64::new(0),
            lag: AtomicU64::new(0),
            height_observation: Mutex::new(None),
            stats: EndpointStats::default(),
            probing: AtomicBool::new(false),
            dirty: AtomicBool::new(true),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn rps(&self) -> u32 {
        self.rps
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn state(&self, now: Instant) -> EndpointState {
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        match health.status {
            HealthStatus::Active => EndpointState::Active,
            HealthStatus::Cooling { until } => EndpointState::Cooling {
                until,
                strikes: health.strikes,
            },
            HealthStatus::Probation { passes } => EndpointState::Probation { passes },
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(lock(&self.health).status, HealthStatus::Active)
    }

    fn is_probation(&self) -> bool {
        matches!(lock(&self.health).status, HealthStatus::Probation { .. })
    }

    pub fn begin_probe(&self, now: Instant) -> bool {
        // 同一端点同一时刻至多一个在飞探针。
        if self.probing.swap(true, Ordering::AcqRel) {
            return false;
        }
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        match health.status {
            HealthStatus::Cooling { until } if now < until => {
                self.probing.store(false, Ordering::Release);
                false
            }
            HealthStatus::Cooling { .. } => {
                health.status = HealthStatus::Probation { passes: 0 };
                health.consecutive_faults = 0;
                health.last_fault = None;
                true
            }
            HealthStatus::Active | HealthStatus::Probation { .. } => true,
        }
    }

    /// 标记探针结束（探测者必须在探针完成/跳过后调用，释放 per-endpoint 探针位）。
    pub fn end_probe(&self) {
        self.probing.store(false, Ordering::Release);
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<EndpointLease> {
        if matches!(lock(&self.health).status, HealthStatus::Cooling { .. }) {
            return None;
        }
        self.acquire_outbound()
    }

    pub fn try_acquire_probe(self: &Arc<Self>) -> Option<EndpointLease> {
        self.acquire_outbound()
    }

    fn acquire_outbound(self: &Arc<Self>) -> Option<EndpointLease> {
        let permit = Arc::clone(&self.inflight).try_acquire_owned().ok()?;
        if self.bucket.check().is_err() {
            drop(permit);
            return None;
        }
        self.stats.outbound_requests.fetch_add(1, Ordering::Relaxed);
        Some(EndpointLease {
            endpoint: Arc::clone(self),
            _permit: permit,
        })
    }

    pub fn record_success(&self, now: Instant, latency: Duration, probe: bool) {
        self.dirty.store(true, Ordering::Release);
        self.update_latency(latency);
        lock(&self.outcomes).record(now, false);
        if probe {
            self.stats.probe_successes.fetch_add(1, Ordering::Relaxed);
        }
        let mut health = lock(&self.health);
        health.decay_strikes(now);
        health.consecutive_faults = 0;
        health.last_fault = None;
        if let HealthStatus::Probation { passes } = health.status {
            let passes = passes.saturating_add(1);
            health.status = if passes >= PROBATION_PASSES_REQUIRED {
                health.healthy_since = Some(now);
                HealthStatus::Active
            } else {
                HealthStatus::Probation { passes }
            };
        }
    }

    pub fn record_degraded(&self, now: Instant, latency: Duration, kind: FaultKind) {
        self.update_latency(latency);
        self.record_failure(now, FailureSignal::new(kind));
    }

    pub fn record_failure(&self, now: Instant, signal: FailureSignal) {
        self.dirty.store(true, Ordering::Release);
        self.stats.failures.fetch_add(1, Ordering::Relaxed);
        if signal.kind == FaultKind::RateLimited {
            self.stats.rate_limited.fetch_add(1, Ordering::Relaxed);
        }
        lock(&self.outcomes).record(now, true);

        let mut health = lock(&self.health);
        health.decay_strikes(now);
        if let HealthStatus::Cooling { until } = health.status {
            if let Some(retry_after) = signal.retry_after {
                health.status = HealthStatus::Cooling {
                    until: until.max(now + retry_after),
                };
            }
            return;
        }

        if health
            .last_fault
            .is_none_or(|last_fault| now.saturating_duration_since(last_fault) > ERROR_WINDOW)
        {
            health.consecutive_faults = 0;
        }
        health.last_fault = Some(now);
        health.consecutive_faults = health.consecutive_faults.saturating_add(1);
        if matches!(health.status, HealthStatus::Active) {
            health.healthy_since = Some(now);
        }
        let should_cool = signal.kind.requires_immediate_cooling()
            || matches!(health.status, HealthStatus::Probation { .. })
            || health.consecutive_faults >= FAULTS_BEFORE_COOLING;
        if !should_cool {
            return;
        }

        health.strikes = health.strikes.saturating_add(1);
        health.healthy_since = None;
        health.consecutive_faults = 0;
        health.last_fault = None;
        let policy_delay = cooldown_for_strikes(health.strikes);
        let cooldown = signal
            .retry_after
            .map_or(policy_delay, |retry_after| retry_after.max(policy_delay));
        health.status = HealthStatus::Cooling {
            until: now + cooldown,
        };
        self.stats.cooling_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn score(&self, now: Instant) -> f64 {
        let latency_ms = match self.latency_ewma_micros.load(Ordering::Relaxed) {
            0 => 1_000.0,
            micros => micros as f64 / 1_000.0,
        };
        let error_penalty = lock(&self.outcomes).error_rate(now) * 5_000.0;
        let lag_penalty = self.lag.load(Ordering::Relaxed) as f64 * 250.0;
        let used_slots = self
            .concurrency
            .saturating_sub(self.inflight.available_permits());
        let concurrency_penalty = used_slots as f64 / self.concurrency as f64 * 500.0;
        latency_ms + error_penalty + lag_penalty + concurrency_penalty
    }

    pub fn latency_ewma_micros(&self) -> u64 {
        self.latency_ewma_micros.load(Ordering::Relaxed)
    }

    pub fn lag(&self) -> u64 {
        self.lag.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> EndpointStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn health_snapshot(&self, chain_id: u64) -> HealthSnapshot {
        let now_unix = unix_seconds();
        let state = self.state(Instant::now());
        let (state_name, cooling_until_unix, strikes) = match state {
            EndpointState::Active => ("active".to_owned(), None, 0),
            EndpointState::Probation { .. } => ("probation".to_owned(), None, 0),
            EndpointState::Cooling { until, strikes } => {
                let secs = until.saturating_duration_since(Instant::now()).as_secs();
                (
                    "cooling".to_owned(),
                    Some(now_unix.saturating_add(secs)),
                    strikes,
                )
            }
        };
        HealthSnapshot {
            chain_id,
            url: self.url.clone(),
            state: state_name,
            cooling_until_unix,
            strikes,
            latency_ewma_us: self.latency_ewma_micros(),
            lag: self.lag(),
        }
    }

    pub fn restore_health(&self, snapshot: &HealthSnapshot) {
        let mut health = lock(&self.health);
        health.strikes = snapshot.strikes;
        health.status = if snapshot.state == "cooling" {
            let remaining = snapshot
                .cooling_until_unix
                .unwrap_or(0)
                .saturating_sub(unix_seconds());
            if remaining > 0 {
                HealthStatus::Cooling {
                    until: Instant::now() + Duration::from_secs(remaining),
                }
            } else {
                HealthStatus::Probation { passes: 0 }
            }
        } else {
            HealthStatus::Probation { passes: 0 }
        };
        self.latency_ewma_micros
            .store(snapshot.latency_ewma_us, Ordering::Relaxed);
        self.lag.store(snapshot.lag, Ordering::Relaxed);
        self.dirty.store(false, Ordering::Release);
    }

    pub fn reset_health(&self) {
        let mut health = lock(&self.health);
        health.status = HealthStatus::Probation { passes: 0 };
        health.strikes = 0;
        health.consecutive_faults = 0;
        health.last_fault = None;
        health.healthy_since = None;
        self.dirty.store(true, Ordering::Release);
    }

    pub fn cool_for(&self, duration: Duration) {
        let mut health = lock(&self.health);
        health.status = HealthStatus::Cooling {
            until: Instant::now() + duration,
        };
        health.strikes = health.strikes.saturating_add(1);
        health.healthy_since = None;
        self.dirty.store(true, Ordering::Release);
    }

    fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }
    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn update_latency(&self, latency: Duration) {
        let sample = latency.as_micros().min(u128::from(u64::MAX)) as u64;
        let mut previous = self.latency_ewma_micros.load(Ordering::Relaxed);
        loop {
            let updated = if previous == 0 {
                sample
            } else {
                previous.saturating_mul(4).saturating_add(sample) / 5
            };
            match self.latency_ewma_micros.compare_exchange_weak(
                previous,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => previous = actual,
            }
        }
    }

    fn observe_height(&self, height: u64, now: Instant) {
        *lock(&self.height_observation) = Some((height, now));
        self.dirty.store(true, Ordering::Release);
    }

    fn recent_height(&self, now: Instant, freshness: Duration) -> Option<u64> {
        lock(&self.height_observation).and_then(|(height, observed_at)| {
            (now.saturating_duration_since(observed_at) <= freshness).then_some(height)
        })
    }
}

pub struct EndpointLease {
    endpoint: Arc<Endpoint>,
    _permit: OwnedSemaphorePermit,
}

impl EndpointLease {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

// ── 链状态（v2：增加生命周期字段） ──

/// 链的可见状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ChainStateLabel {
    Pinned,
    Hot,
    Dormant,
    Disabled,
}

pub struct ChainState {
    chain_id: u64,
    name: RwLock<String>,
    endpoints: RwLock<Vec<Arc<Endpoint>>>,
    rejected: RwLock<HashSet<String>>,
    head: AtomicU64,
    /// pinned 链永不降级，启动即 materialize。
    pinned: AtomicBool,
    /// disabled 链拒绝流量（403）。
    disabled: AtomicBool,
    /// 最后一次入口请求的 Unix 秒时间戳（粗粒度，仅秒值变化时写入）。
    last_ingress: AtomicU64,
}

impl ChainState {
    fn new(chain_id: u64, name: String, pinned: bool) -> Self {
        Self {
            chain_id,
            name: RwLock::new(name),
            endpoints: RwLock::new(Vec::new()),
            rejected: RwLock::new(HashSet::new()),
            head: AtomicU64::new(0),
            pinned: AtomicBool::new(pinned),
            disabled: AtomicBool::new(false),
            last_ingress: AtomicU64::new(0),
        }
    }

    pub fn state_label(&self) -> ChainStateLabel {
        if self.disabled.load(Ordering::Relaxed) {
            ChainStateLabel::Disabled
        } else if self.pinned.load(Ordering::Relaxed) {
            ChainStateLabel::Pinned
        } else if self.last_ingress.load(Ordering::Relaxed) > 0 {
            ChainStateLabel::Hot
        } else {
            ChainStateLabel::Dormant
        }
    }
}

// ── Registry（v2） ──

/// 激活通知：新链被 materialize 时发送 chain_id。
pub type ActivationKick = broadcast::Sender<u64>;

pub struct Registry {
    chains: DashMap<u64, Arc<ChainState>>,
    activation_locks: DashMap<u64, Arc<tokio::sync::Mutex<()>>>,
    runtime_pinned: DashMap<u64, bool>,
    auto_pinned: DashMap<u64, bool>,
    runtime_chain_overrides: DashMap<u64, ChainOverrideState>,
    runtime_endpoint_overrides: DashMap<String, EndpointOverrideState>,
    restored_health: DashMap<String, HealthSnapshot>,
    catalog: RwLock<Option<Arc<Catalog>>>,
    config: Config,
    user_visible_errors: AtomicU64,
    /// 链激活计数器。
    chain_activations: AtomicU64,
    /// 链降级计数器，按 reason。
    chain_demotions_idle: AtomicU64,
    chain_demotions_lru: AtomicU64,
    chain_demotions_admin: AtomicU64,
    /// 激活通知通道（探针调度器订阅）。
    activation_tx: broadcast::Sender<u64>,
    /// v2 指标快照：目录链/端点数量（由 set_catalog 更新）。
    catalog_chains_count: AtomicU64,
    catalog_endpoints_count: AtomicU64,
    /// v2 指标快照：chainlist 最近刷新时间戳（由 main 刷新循环更新）。
    chainlist_last_refresh_unix: AtomicU64,
    /// v2 指标快照：chainlist 最近刷新来源（由 main 刷新循环更新）。
    chainlist_refresh_source: Mutex<String>,
    /// v2 指标快照：探针在飞/队列深度（由 ProbeManager 共享更新）。
    pub probe_in_flight: Arc<AtomicU64>,
    pub probe_queue_depth: Arc<AtomicU64>,
}

impl Registry {
    pub fn new(config: &Config) -> Self {
        let (activation_tx, _) = broadcast::channel(256);
        Self {
            chains: DashMap::new(),
            activation_locks: DashMap::new(),
            runtime_pinned: DashMap::new(),
            auto_pinned: DashMap::new(),
            runtime_chain_overrides: DashMap::new(),
            runtime_endpoint_overrides: DashMap::new(),
            restored_health: DashMap::new(),
            catalog: RwLock::new(None),
            config: config.clone(),
            user_visible_errors: AtomicU64::new(0),
            chain_activations: AtomicU64::new(0),
            chain_demotions_idle: AtomicU64::new(0),
            chain_demotions_lru: AtomicU64::new(0),
            chain_demotions_admin: AtomicU64::new(0),
            activation_tx,
            catalog_chains_count: AtomicU64::new(0),
            catalog_endpoints_count: AtomicU64::new(0),
            chainlist_last_refresh_unix: AtomicU64::new(0),
            chainlist_refresh_source: Mutex::new(String::new()),
            probe_in_flight: Arc::new(AtomicU64::new(0)),
            probe_queue_depth: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 设置目录（每次 chainlist 刷新整体替换）。
    pub async fn set_catalog(&self, catalog: Arc<Catalog>) {
        let chain_count = catalog.chains.len() as u64;
        let endpoint_count: u64 = catalog
            .chains
            .iter()
            .map(|ch| ch.endpoints.len() as u64)
            .sum();
        self.catalog_chains_count
            .store(chain_count, Ordering::Relaxed);
        self.catalog_endpoints_count
            .store(endpoint_count, Ordering::Relaxed);
        *self.catalog.write().await = Some(catalog);
    }

    /// 获取当前目录（只读）。
    pub async fn catalog(&self) -> Option<Arc<Catalog>> {
        self.catalog.read().await.clone()
    }

    /// 激活通知通道（探针订阅）。
    pub fn activation_channel(&self) -> broadcast::Sender<u64> {
        self.activation_tx.clone()
    }

    /// 将持久化运行时覆写叠加到内存。请求路径只读取这些并发 map，不访问 StateStore。
    pub async fn apply_overrides(&self, overrides: &Overrides) {
        self.runtime_chain_overrides.clear();
        self.runtime_endpoint_overrides.clear();
        self.runtime_pinned.clear();
        for entry in &self.chains {
            entry
                .value()
                .pinned
                .store(self.is_pinned_chain(*entry.key()), Ordering::Relaxed);
            entry.value().disabled.store(false, Ordering::Relaxed);
        }
        for (id, value) in &overrides.chains {
            if !valid_chain_override(value) {
                tracing::warn!(chain_id = *id, "invalid runtime chain override ignored");
                continue;
            }
            self.runtime_chain_overrides.insert(*id, value.clone());
            if let Some(pinned) = value.pinned {
                self.runtime_pinned.insert(*id, pinned);
            }
            if let Some(state) = self.chain(*id) {
                if let Some(pinned) = value.pinned {
                    state.pinned.store(pinned, Ordering::Relaxed);
                }
                if let Some(disabled) = value.disabled {
                    state.disabled.store(disabled, Ordering::Relaxed);
                }
            }
        }
        for (key, value) in &overrides.endpoints {
            if value.rps.is_some_and(|v| !(1..=100).contains(&v))
                || value.concurrency.is_some_and(|v| !(1..=64).contains(&v))
            {
                tracing::warn!(override_key=%key, "invalid runtime endpoint override ignored");
                continue;
            }
            self.runtime_endpoint_overrides
                .insert(key.clone(), value.clone());
            self.set_endpoint_override(
                key.split(':')
                    .next()
                    .and_then(|id| id.parse().ok())
                    .unwrap_or_default(),
                value.clone(),
            )
            .await;
        }
    }

    /// 从状态存储恢复自动开启集合。仅用于启动阶段，不触及请求热路径。
    pub fn restore_auto_pinned<I>(&self, chain_ids: I)
    where
        I: IntoIterator<Item = u64>,
    {
        self.auto_pinned.clear();
        for id in chain_ids {
            self.auto_pinned.insert(id, true);
        }
    }

    pub async fn apply_override(&self, chain_id: u64, value: ChainOverrideState) {
        self.runtime_chain_overrides.insert(chain_id, value.clone());
        if let Some(pinned) = value.pinned {
            self.runtime_pinned.insert(chain_id, pinned);
        }
        if let Some(state) = self.chain(chain_id) {
            if let Some(pinned) = value.pinned {
                state.pinned.store(pinned, Ordering::Relaxed);
            }
            if let Some(disabled) = value.disabled {
                state.disabled.store(disabled, Ordering::Relaxed);
            }
        }
    }

    pub fn runtime_chain_override(&self, chain_id: u64) -> ChainOverrideState {
        self.runtime_chain_overrides
            .get(&chain_id)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    pub fn runtime_overrides(&self) -> Overrides {
        Overrides {
            chains: self
                .runtime_chain_overrides
                .iter()
                .map(|v| (*v.key(), v.value().clone()))
                .collect(),
            endpoints: self
                .runtime_endpoint_overrides
                .iter()
                .map(|v| (v.key().clone(), v.value().clone()))
                .collect(),
        }
    }

    pub fn runtime_endpoint_override(
        &self,
        chain_id: u64,
        url: &str,
    ) -> Option<EndpointOverrideState> {
        self.runtime_endpoint_overrides
            .get(&crate::state::endpoint_key(chain_id, url))
            .map(|v| v.clone())
    }

    pub async fn endpoint_known(&self, chain_id: u64, url: &str) -> bool {
        if self.endpoint(chain_id, url).await.is_some()
            || self.runtime_endpoint_override(chain_id, url).is_some()
        {
            return true;
        }
        if self
            .catalog
            .read()
            .await
            .as_ref()
            .and_then(|catalog| catalog.lookup(chain_id))
            .is_some_and(|chain| chain.endpoints.iter().any(|endpoint| endpoint.url == url))
        {
            return true;
        }
        self.config
            .chain_override(chain_id)
            .is_some_and(|chain| chain.extra_endpoints.iter().any(|endpoint| endpoint == url))
    }

    // ── 热路径：resolve_for_request ──

    fn is_pinned_chain(&self, chain_id: u64) -> bool {
        if self.config.chains.contains(&chain_id) {
            return true;
        }
        if let Some(v) = self.runtime_pinned.get(&chain_id) {
            return *v;
        }
        self.auto_pinned.get(&chain_id).is_some_and(|v| *v)
    }

    /// 热路径：解析 chain_id 并触达 ChainState。
    /// DashMap get + 原子读；dormant 时才走慢路径 materialize。
    /// 返回 `None` 表示未知链（不在目录也不在 pinned）。
    pub async fn resolve_for_request(&self, chain_id: u64) -> Option<Arc<ChainState>> {
        // 路由层用这个做语义判定：未知链 / 0端点 / disabled。
        // 先查 DashMap（已 materialized 的链）。
        if let Some(state) = self.chain(chain_id) {
            let label = state.state_label();
            // disabled 链直接返回，由 server 层返回 403。
            if label == ChainStateLabel::Disabled {
                return Some(state);
            }
            // dormant 链（已降级，端点运行态已丢弃）需要重新 materialize。
            if label == ChainStateLabel::Dormant {
                // Dormant 状态由锁内慢路径统一替换，避免并发请求删除新建状态。
            } else {
                // pinned / hot：更新 last_ingress 并返回。
                let now_sec = unix_seconds();
                let last = state.last_ingress.load(Ordering::Relaxed);
                if now_sec != last {
                    state.last_ingress.store(now_sec, Ordering::Relaxed);
                }
                return Some(state);
            }
        }

        // 未知链不得污染 activation_locks：先完成目录/配置判定，再建立锁。
        let pinned = self.is_pinned_chain(chain_id);
        let denied = self.config.discovery.deny.contains(&chain_id);
        let catalog = self.catalog.read().await;
        let catalog_entry = catalog.as_ref().and_then(|c| c.lookup(chain_id));
        if !denied
            && ((!self.config.discovery.enabled && !pinned) || (catalog_entry.is_none() && !pinned))
        {
            return None;
        }
        drop(catalog);

        // 串行化同一链的首次 materialize，避免重复激活/kick。
        let activation_lock = self
            .activation_locks
            .entry(chain_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _cleanup = ActivationLockCleanup {
            locks: &self.activation_locks,
            chain_id,
        };
        let _activation_guard = activation_lock.lock().await;
        if let Some(state) = self.chain(chain_id) {
            if state.state_label() != ChainStateLabel::Dormant {
                let now_sec = unix_seconds();
                if now_sec != state.last_ingress.load(Ordering::Relaxed) {
                    state.last_ingress.store(now_sec, Ordering::Relaxed);
                }
                return Some(state);
            }
            self.chains.remove(&chain_id);
        }

        // 未 materialized → 查目录。
        let catalog = self.catalog.read().await;
        let catalog_entry = catalog.as_ref().and_then(|c| c.lookup(chain_id));

        // discovery disabled 等价 v1：目录只用于 pinned 链，其他链视为未知。
        if !denied
            && ((!self.config.discovery.enabled && !pinned) || (catalog_entry.is_none() && !pinned))
        {
            return None;
        }

        // discovery.deny 链：只建 disabled 空状态（不 materialize 端点、不发激活 kick，
        // 避免对被禁链做任何外呼/探针）。
        if self.config.discovery.deny.contains(&chain_id) {
            let name = catalog_entry
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("Chain {chain_id}"));
            drop(catalog);
            let state = Arc::new(ChainState::new(chain_id, name, pinned));
            state.disabled.store(true, Ordering::Relaxed);
            self.chains.insert(chain_id, Arc::clone(&state));
            return Some(state);
        }

        // 慢路径：materialize。
        drop(catalog);
        let state = self.materialize(chain_id, pinned).await?;

        Some(state)
    }

    #[cfg(test)]
    fn activation_lock_count(&self) -> usize {
        self.activation_locks.len()
    }

    /// 慢路径：materialize 链（从 Catalog 取端点 + 配置叠加）。
    async fn materialize(&self, chain_id: u64, pinned: bool) -> Option<Arc<ChainState>> {
        let catalog = self.catalog.read().await;
        let catalog_entry = catalog.as_ref().and_then(|c| c.lookup(chain_id));

        let name = catalog_entry
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("Chain {chain_id}"));
        let state = Arc::new(ChainState::new(chain_id, name, pinned));

        // 构建端点池：目录端点 + extra_endpoints - disabled_endpoints - rejected。
        let mut desired_urls: Vec<String> = Vec::new();
        if let Some(entry) = catalog_entry {
            desired_urls.extend(entry.endpoints.iter().map(|e| e.url.clone()));
        }
        if let Some(chain_override) = self.config.chain_override(chain_id) {
            desired_urls.extend(chain_override.extra_endpoints.iter().cloned());
        }
        desired_urls.extend(
            self.runtime_endpoint_overrides
                .iter()
                .filter(|entry| {
                    entry.key().starts_with(&format!("{chain_id}:"))
                        && entry.value().disabled != Some(true)
                })
                .map(|entry| entry.value().url.clone()),
        );

        let runtime = self
            .runtime_chain_overrides
            .get(&chain_id)
            .map(|v| v.clone());
        let chain_override = self.config.chain_override(chain_id);
        let disabled: HashSet<&str> = chain_override
            .into_iter()
            .flat_map(|chain| chain.disabled_endpoints.iter().map(String::as_str))
            .collect();

        let now = unix_seconds();
        let mut seen = HashSet::new();
        let mut endpoints = Vec::new();
        for url in desired_urls {
            if disabled.contains(url.as_str()) || !seen.insert(url.clone()) {
                continue;
            }
            let (mut rps, mut concurrency) = self.config.endpoint_limits(chain_id, &url);
            if let Some(endpoint) = self
                .runtime_endpoint_overrides
                .get(&crate::state::endpoint_key(chain_id, &url))
            {
                if let Some(v) = endpoint.rps {
                    rps = v;
                }
                if let Some(v) = endpoint.concurrency {
                    concurrency = v;
                }
                if endpoint.disabled == Some(true) {
                    continue;
                }
            }
            endpoints.push(Arc::new(Endpoint::new(url, rps, concurrency, now)));
        }
        *state.endpoints.write().await = endpoints;
        for endpoint in state.endpoints.read().await.iter() {
            if let Some(snapshot) = self
                .restored_health
                .get(&crate::state::endpoint_key(chain_id, endpoint.url()))
            {
                endpoint.restore_health(&snapshot);
            }
        }
        if let Some(value) = runtime
            && let Some(disabled) = value.disabled
        {
            state.disabled.store(disabled, Ordering::Relaxed);
        }

        // 插入 DashMap。
        self.chains.insert(chain_id, Arc::clone(&state));
        self.chain_activations.fetch_add(1, Ordering::Relaxed);

        // 更新 last_ingress。
        state.last_ingress.store(now, Ordering::Relaxed);

        // 激活路径同步执行软上限检查，避免短时间冷启动扫过整份目录。
        let hot_count = self
            .chains
            .iter()
            .filter(|entry| entry.value().state_label() == ChainStateLabel::Hot)
            .count();
        let mut kick_probes = true;
        if hot_count > self.config.discovery.max_hot_chains {
            let candidate = self
                .chains
                .iter()
                .filter_map(|entry| {
                    let value = entry.value();
                    let last = value.last_ingress.load(Ordering::Relaxed);
                    (!value.pinned.load(Ordering::Relaxed)
                        && *entry.key() != chain_id
                        && now.saturating_sub(last) >= 5)
                        .then_some((*entry.key(), last))
                })
                .min_by_key(|(_, last)| *last);
            kick_probes = if let Some((evict_id, last)) = candidate {
                self.demote_if_unchanged(evict_id, "lru", last).await
            } else {
                false
            };
        }

        // 通知探针调度器。
        if kick_probes {
            let _ = self.activation_tx.send(chain_id);
        }

        Some(state)
    }

    // ── 兼容旧接口：apply_snapshot ──

    /// 刷新时复用同 URL 的端点对象，避免丢失健康与出站限流状态。
    /// 新端点从 Probation 起步，消失端点宽限 24h。
    pub async fn apply_snapshot(&self, snapshot: &ChainlistSnapshot) {
        let source_by_chain: HashMap<_, _> = snapshot
            .chains
            .iter()
            .map(|chain| (chain.chain_id, chain))
            .collect();
        let now = unix_seconds();

        // pinned 链始终合并；动态发现模式下，已 materialized 的 hot 链也必须在刷新时
        // 保留健康运行态并合并新增/消失端点。dormant 链保持零成本，不创建端点对象。
        let mut materialized_ids: HashSet<u64> = self.config.chains.iter().copied().collect();
        materialized_ids.extend(
            self.runtime_pinned
                .iter()
                .filter_map(|entry| (*entry.value()).then_some(*entry.key())),
        );
        materialized_ids.extend(
            self.auto_pinned
                .iter()
                .filter_map(|entry| (*entry.value()).then_some(*entry.key())),
        );
        materialized_ids.extend(self.chains.iter().filter_map(|entry| {
            matches!(
                entry.value().state_label(),
                ChainStateLabel::Pinned | ChainStateLabel::Hot
            )
            .then_some(*entry.key())
        }));

        for chain_id in materialized_ids {
            let source = source_by_chain.get(&chain_id).copied();
            let name = source
                .map(|chain| chain.name.clone())
                .unwrap_or_else(|| format!("Chain {chain_id}"));
            let pinned = self.is_pinned_chain(chain_id);
            let state = self
                .chains
                .entry(chain_id)
                .or_insert_with(|| Arc::new(ChainState::new(chain_id, name.clone(), pinned)))
                .clone();
            state.pinned.store(pinned, Ordering::Relaxed);
            if source.is_some() {
                *state.name.write().await = name;
            }
            if self.config.discovery.deny.contains(&chain_id) {
                state.disabled.store(true, Ordering::Relaxed);
                *state.endpoints.write().await = Vec::new();
                continue;
            }
            self.merge_chain(&state, source, now).await;
        }
    }

    async fn merge_chain(
        &self,
        state: &Arc<ChainState>,
        source: Option<&ChainEndpoints>,
        now: u64,
    ) {
        let previous = state.endpoints.read().await.clone();
        let previous_by_url: HashMap<_, _> = previous
            .iter()
            .map(|endpoint| (endpoint.url.clone(), Arc::clone(endpoint)))
            .collect();
        let rejected = state.rejected.read().await.clone();
        let chain_override = self.config.chain_override(state.chain_id);
        let disabled: HashSet<&str> = chain_override
            .into_iter()
            .flat_map(|chain| chain.disabled_endpoints.iter().map(String::as_str))
            .collect();
        let mut desired = Vec::new();
        if let Some(source) = source {
            desired.extend(source.endpoints.iter().cloned());
        }
        if let Some(chain) = chain_override {
            desired.extend(chain.extra_endpoints.iter().cloned());
        }
        desired.extend(
            self.runtime_endpoint_overrides
                .iter()
                .filter(|entry| {
                    entry.key().starts_with(&format!("{}:", state.chain_id))
                        && entry.value().disabled != Some(true)
                })
                .map(|entry| entry.value().url.clone()),
        );

        let mut present = HashSet::new();
        let mut merged = Vec::new();
        for url in desired {
            if disabled.contains(url.as_str())
                || rejected.contains(&url)
                || !present.insert(url.clone())
            {
                continue;
            }
            let runtime = self
                .runtime_endpoint_overrides
                .get(&crate::state::endpoint_key(state.chain_id, &url))
                .map(|entry| entry.clone());
            if runtime
                .as_ref()
                .is_some_and(|entry| entry.disabled == Some(true))
            {
                continue;
            }
            if let Some(endpoint) = previous_by_url.get(&url) {
                let (configured_rps, configured_concurrency) =
                    self.config.endpoint_limits(state.chain_id, &url);
                let desired_rps = runtime
                    .as_ref()
                    .and_then(|entry| entry.rps)
                    .unwrap_or(configured_rps);
                let desired_concurrency = runtime
                    .as_ref()
                    .and_then(|entry| entry.concurrency)
                    .unwrap_or(configured_concurrency);
                if endpoint.rps() == desired_rps && endpoint.concurrency() == desired_concurrency {
                    endpoint.last_seen.store(now, Ordering::Relaxed);
                    merged.push(Arc::clone(endpoint));
                } else {
                    merged.push(Arc::new(Endpoint::new(
                        url,
                        desired_rps,
                        desired_concurrency,
                        now,
                    )));
                }
            } else {
                let (rps, concurrency) = self.config.endpoint_limits(state.chain_id, &url);
                merged.push(Arc::new(Endpoint::new(
                    url,
                    runtime.as_ref().and_then(|entry| entry.rps).unwrap_or(rps),
                    runtime
                        .as_ref()
                        .and_then(|entry| entry.concurrency)
                        .unwrap_or(concurrency),
                    now,
                )));
            }
        }

        for endpoint in previous {
            let age = now.saturating_sub(endpoint.last_seen.load(Ordering::Relaxed));
            if !present.contains(endpoint.url())
                && !disabled.contains(endpoint.url())
                && !rejected.contains(endpoint.url())
                && age < self.config.chainlist.stale_grace_seconds
            {
                present.insert(endpoint.url.clone());
                merged.push(endpoint);
            }
        }
        *state.endpoints.write().await = merged;
        for endpoint in state.endpoints.read().await.iter() {
            if let Some(snapshot) = self
                .restored_health
                .get(&crate::state::endpoint_key(state.chain_id, endpoint.url()))
            {
                endpoint.restore_health(&snapshot);
            }
        }
    }

    // ── 生命周期控制 ──

    /// demote：将 hot 链降级为 dormant（丢弃端点运行态）。
    pub async fn demote(&self, chain_id: u64, reason: &str) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        let expected_last = state.last_ingress.load(Ordering::Relaxed);
        self.demote_if_unchanged(chain_id, reason, expected_last)
            .await
    }

    async fn demote_if_unchanged(&self, chain_id: u64, reason: &str, expected_last: u64) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        if state.pinned.load(Ordering::Relaxed) {
            return false; // pinned 不降级
        }
        // 先标记为 dormant（清 last_ingress）。
        if state
            .last_ingress
            .compare_exchange(expected_last, 0, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        // 丢弃端点运行态。
        *state.endpoints.write().await = Vec::new();
        match reason {
            "idle" => self.chain_demotions_idle.fetch_add(1, Ordering::Relaxed),
            "lru" => self.chain_demotions_lru.fetch_add(1, Ordering::Relaxed),
            _ => self.chain_demotions_admin.fetch_add(1, Ordering::Relaxed),
        };
        true
    }

    /// 设置 disabled 状态。
    pub async fn set_disabled(&self, chain_id: u64, disabled: bool) -> bool {
        let state = match self.chain(chain_id) {
            Some(state) => state,
            None => match self.resolve_for_request(chain_id).await {
                Some(state) => state,
                None => return false,
            },
        };
        state.disabled.store(disabled, Ordering::Relaxed);
        true
    }

    /// 设置 pinned 状态。
    pub async fn set_pinned(&self, chain_id: u64, pinned: bool) -> bool {
        let state = match self.chain(chain_id) {
            Some(state) => state,
            None => match self.resolve_for_request(chain_id).await {
                Some(state) => state,
                None => return false,
            },
        };
        self.runtime_pinned.insert(chain_id, pinned);
        state.pinned.store(pinned, Ordering::Relaxed);
        if !pinned {
            state.last_ingress.store(unix_seconds(), Ordering::Relaxed);
        }
        true
    }

    pub async fn set_auto_pinned(&self, chain_id: u64, enabled: bool) -> bool {
        if enabled {
            let ov = self.runtime_chain_override(chain_id);
            if ov.pinned == Some(false) || ov.disabled == Some(true) {
                return false;
            }
        }
        if enabled {
            self.auto_pinned.insert(chain_id, true);
        } else {
            self.auto_pinned.remove(&chain_id);
        }
        if let Some(state) = self.chain(chain_id) {
            state
                .pinned
                .store(self.is_pinned_chain(chain_id), Ordering::Relaxed);
        }
        enabled
    }

    /// 人工墓碑：被显式 unpin 或 disable 的链，自动开启规则永远跳过。
    pub fn tombstoned_chain_ids(&self) -> std::collections::HashSet<u64> {
        self.runtime_chain_overrides
            .iter()
            .filter(|entry| {
                entry.value().pinned == Some(false) || entry.value().disabled == Some(true)
            })
            .map(|entry| *entry.key())
            .collect()
    }

    pub fn auto_chain_ids(&self) -> Vec<u64> {
        self.auto_pinned
            .iter()
            .filter_map(|e| (*e.value()).then_some(*e.key()))
            .collect()
    }

    pub fn pin_source(&self, chain_id: u64) -> Option<&'static str> {
        if self.config.chains.contains(&chain_id) {
            Some("config")
        } else if self
            .runtime_pinned
            .get(&chain_id)
            .is_some_and(|value| *value)
        {
            Some("manual")
        } else if self.auto_pinned.contains_key(&chain_id) {
            Some("auto")
        } else {
            None
        }
    }

    /// 已 materialized 的热链 id 列表（pinned + hot）。
    pub fn hot_chain_ids(&self) -> Vec<u64> {
        self.chains
            .iter()
            .filter(|entry| {
                let state = entry.value();
                let label = state.state_label();
                matches!(label, ChainStateLabel::Pinned | ChainStateLabel::Hot)
            })
            .map(|entry| *entry.key())
            .collect()
    }

    pub fn hot_chain_snapshot(&self) -> Vec<(u64, u64)> {
        self.chains
            .iter()
            .filter_map(|entry| {
                let last = entry.value().last_ingress.load(Ordering::Relaxed);
                (last > 0).then_some((*entry.key(), last))
            })
            .collect()
    }

    pub fn hot_chain_timestamps(&self) -> Vec<(u64, u64)> {
        self.chains
            .iter()
            .filter_map(|entry| {
                let state = entry.value();
                matches!(
                    state.state_label(),
                    ChainStateLabel::Pinned | ChainStateLabel::Hot
                )
                .then_some((*entry.key(), state.last_ingress.load(Ordering::Relaxed)))
            })
            .collect()
    }

    pub async fn activate_restored_hot(&self, chains: &[(u64, u64)]) {
        let now = unix_seconds();
        for (chain_id, last) in chains {
            if now.saturating_sub(*last) <= self.config.discovery.idle_seconds
                && let Some(state) = self.resolve_for_request(*chain_id).await
            {
                state.last_ingress.store(*last, Ordering::Relaxed);
            }
        }
    }

    pub async fn restore_health(&self, snapshots: &[HealthSnapshot]) {
        for snapshot in snapshots {
            self.restored_health.insert(
                crate::state::endpoint_key(snapshot.chain_id, &snapshot.url),
                snapshot.clone(),
            );
            if let Some(endpoint) = self
                .all_endpoints(snapshot.chain_id)
                .await
                .into_iter()
                .find(|e| e.url() == snapshot.url)
            {
                endpoint.restore_health(snapshot);
            }
        }
    }

    pub async fn take_dirty_health(&self, limit: usize) -> Vec<HealthSnapshot> {
        let mut out = Vec::new();
        for entry in &self.hot_chain_ids() {
            for endpoint in self.all_endpoints(*entry).await {
                if out.len() >= limit {
                    return out;
                }
                if endpoint.take_dirty() {
                    out.push(endpoint.health_snapshot(*entry));
                }
            }
        }
        out
    }

    pub async fn restore_dirty_health(&self, snapshots: &[HealthSnapshot]) {
        for snapshot in snapshots {
            if let Some(endpoint) = self
                .all_endpoints(snapshot.chain_id)
                .await
                .into_iter()
                .find(|e| e.url() == snapshot.url)
            {
                endpoint.mark_dirty();
            }
        }
    }

    pub async fn dirty_endpoint_count(&self) -> usize {
        let mut count = 0;
        for chain_id in self.hot_chain_ids() {
            count += self
                .all_endpoints(chain_id)
                .await
                .into_iter()
                .filter(|e| e.dirty.load(Ordering::Acquire))
                .count();
        }
        count
    }

    /// housekeeping：idle 降级 + LRU 淘汰。
    pub async fn housekeeping(&self) {
        self.housekeeping_at(unix_seconds()).await;
    }

    async fn housekeeping_at(&self, now_sec: u64) {
        let idle_seconds = self.config.discovery.idle_seconds;
        let max_hot = self.config.discovery.max_hot_chains;

        // 收集 hot 链（非 pinned、非 disabled）。
        let mut hot_entries: Vec<(u64, u64)> = self
            .chains
            .iter()
            .filter(|entry| {
                let state = entry.value();
                !state.pinned.load(Ordering::Relaxed)
                    && !state.disabled.load(Ordering::Relaxed)
                    && state.last_ingress.load(Ordering::Relaxed) > 0
            })
            .map(|entry| {
                let last = entry.value().last_ingress.load(Ordering::Relaxed);
                (*entry.key(), last)
            })
            .collect();

        // idle 降级：last_ingress 超过 idle_seconds 的链。
        for (chain_id, last) in &hot_entries {
            if now_sec.saturating_sub(*last) > idle_seconds {
                self.demote_if_unchanged(*chain_id, "idle", *last).await;
            }
        }

        // 重新收集（idle 降级后）。
        hot_entries.retain(|(chain_id, _)| {
            self.chains
                .get(chain_id)
                .is_some_and(|state| state.last_ingress.load(Ordering::Relaxed) > 0)
        });

        // LRU 淘汰：超过 max_hot 时按 last_ingress 升序淘汰最久未用的。
        if hot_entries.len() > max_hot {
            hot_entries.sort_by_key(|(_, last)| *last);
            let mut remaining = hot_entries.len();
            for (chain_id, last) in hot_entries {
                if remaining <= max_hot {
                    break;
                }
                if now_sec.saturating_sub(last) < 5 {
                    continue;
                }
                if self.demote_if_unchanged(chain_id, "lru", last).await {
                    remaining -= 1;
                }
            }
        }
    }

    // ── 查询接口 ──

    /// 优先对 Active 集执行 P2C；冷启动无 Active 时回退到 Probation 集。
    pub async fn candidates(&self, chain_id: u64) -> CandidateSet {
        let endpoints = self.all_endpoints(chain_id).await;
        let mut pool: Vec<_> = endpoints
            .iter()
            .filter(|endpoint| endpoint.is_active())
            .cloned()
            .collect();
        let kind = if !pool.is_empty() {
            PoolKind::Active
        } else {
            pool = endpoints
                .into_iter()
                .filter(|endpoint| endpoint.is_probation())
                .collect();
            if pool.is_empty() {
                PoolKind::Empty
            } else {
                PoolKind::Probation
            }
        };
        let now = Instant::now();
        let mut ordered = Vec::with_capacity(pool.len());
        let mut rng = rand::rng();
        while pool.len() > 1 {
            let first = rng.random_range(0..pool.len());
            let mut second = rng.random_range(0..pool.len() - 1);
            if second >= first {
                second += 1;
            }
            let selected = if pool[first].score(now) <= pool[second].score(now) {
                first
            } else {
                second
            };
            ordered.push(pool.swap_remove(selected));
        }
        ordered.extend(pool);
        CandidateSet {
            endpoints: ordered,
            kind,
        }
    }

    pub async fn all_endpoints(&self, chain_id: u64) -> Vec<Arc<Endpoint>> {
        let Some(state) = self.chain(chain_id) else {
            return Vec::new();
        };
        state.endpoints.read().await.clone()
    }

    /// 端点数量（materialized 链）。
    pub async fn endpoint_count(&self, chain_id: u64) -> usize {
        let Some(state) = self.chain(chain_id) else {
            return 0;
        };
        state.endpoints.read().await.len()
    }

    /// 链是否为 disabled。
    pub fn is_disabled(&self, chain_id: u64) -> bool {
        self.chain(chain_id)
            .is_some_and(|state| state.disabled.load(Ordering::Relaxed))
    }

    /// 链是否在目录中。
    pub async fn chain_in_catalog(&self, chain_id: u64) -> bool {
        self.catalog
            .read()
            .await
            .as_ref()
            .is_some_and(|c| c.by_id.contains_key(&chain_id))
    }

    pub async fn healthy_for_hedging(&self, chain_id: u64, minimum_active: usize) -> bool {
        let endpoints = self.all_endpoints(chain_id).await;
        let active = endpoints
            .iter()
            .filter(|endpoint| endpoint.is_active())
            .count();
        active >= minimum_active && active.saturating_mul(2) >= endpoints.len()
    }

    pub async fn endpoint(&self, chain_id: u64, url: &str) -> Option<Arc<Endpoint>> {
        self.all_endpoints(chain_id)
            .await
            .into_iter()
            .find(|endpoint| endpoint.url() == url)
    }

    pub async fn remove_endpoint(&self, chain_id: u64, url: &str) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        state.rejected.write().await.insert(url.to_owned());
        let mut endpoints = state.endpoints.write().await;
        let previous_len = endpoints.len();
        endpoints.retain(|endpoint| endpoint.url() != url);
        endpoints.len() != previous_len
    }

    pub async fn set_endpoint_override(&self, chain_id: u64, value: EndpointOverrideState) -> bool {
        let key = crate::state::endpoint_key(chain_id, &value.url);
        self.runtime_endpoint_overrides.insert(key, value.clone());
        if let Some(state) = self.chain(chain_id) {
            let mut endpoints = state.endpoints.write().await;
            if value.disabled == Some(true) {
                endpoints.retain(|e| e.url() != value.url);
            } else if let Some(existing) = endpoints.iter_mut().find(|e| e.url() == value.url) {
                if value.rps.is_some() || value.concurrency.is_some() {
                    let (rps, concurrency) = self.config.endpoint_limits(chain_id, &value.url);
                    let replacement = Arc::new(Endpoint::new(
                        value.url.clone(),
                        value.rps.unwrap_or(rps),
                        value.concurrency.unwrap_or(concurrency),
                        unix_seconds(),
                    ));
                    *existing = replacement;
                }
            } else {
                let (rps, concurrency) = self.config.endpoint_limits(chain_id, &value.url);
                endpoints.push(Arc::new(Endpoint::new(
                    value.url.clone(),
                    value.rps.unwrap_or(rps),
                    value.concurrency.unwrap_or(concurrency),
                    unix_seconds(),
                )));
            }
        }
        true
    }

    pub async fn add_runtime_endpoint(&self, chain_id: u64, url: String) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        let (rps, concurrency) = self.config.endpoint_limits(chain_id, &url);
        let mut endpoints = state.endpoints.write().await;
        if endpoints.iter().any(|e| e.url() == url) {
            return true;
        }
        endpoints.push(Arc::new(Endpoint::new(
            url,
            rps,
            concurrency,
            unix_seconds(),
        )));
        true
    }

    pub async fn remove_runtime_endpoint(&self, chain_id: u64, url: &str) -> bool {
        let Some(state) = self.chain(chain_id) else {
            return false;
        };
        let key = crate::state::endpoint_key(chain_id, url);
        if self.runtime_endpoint_overrides.remove(&key).is_none() {
            return false;
        }
        let mut endpoints = state.endpoints.write().await;
        let old = endpoints.len();
        endpoints.retain(|e| e.url() != url);
        old != endpoints.len()
    }

    pub async fn runtime_endpoint_exists(&self, chain_id: u64, url: &str) -> bool {
        if !self
            .runtime_endpoint_overrides
            .contains_key(&crate::state::endpoint_key(chain_id, url))
        {
            return false;
        }
        if self
            .config
            .chain_override(chain_id)
            .is_some_and(|value| value.extra_endpoints.iter().any(|item| item == url))
        {
            return false;
        }
        !self
            .catalog
            .read()
            .await
            .as_ref()
            .and_then(|catalog| catalog.lookup(chain_id))
            .is_some_and(|chain| chain.endpoints.iter().any(|item| item.url == url))
    }

    pub async fn record_probe_height(
        &self,
        chain_id: u64,
        endpoint: &Arc<Endpoint>,
        height: u64,
        now: Instant,
    ) {
        endpoint.observe_height(height, now);
        let Some(state) = self.chain(chain_id) else {
            return;
        };
        let freshness =
            Duration::from_secs(self.config.probe.max_interval_seconds.saturating_mul(2));
        let endpoints = state.endpoints.read().await.clone();
        let heights: Vec<_> = endpoints
            .iter()
            .filter_map(|item| item.recent_height(now, freshness))
            .collect();
        let Some(head) = trimmed_max(&heights) else {
            return;
        };
        state.head.store(head, Ordering::Relaxed);
        for item in &endpoints {
            if let Some(item_height) = item.recent_height(now, freshness) {
                item.lag
                    .store(head.abs_diff(item_height), Ordering::Relaxed);
                item.dirty.store(true, Ordering::Release);
            }
        }
        if head.abs_diff(height) > self.runtime_lag_threshold(chain_id) {
            endpoint.record_failure(now, FailureSignal::new(FaultKind::Lagging));
        }
    }

    pub fn head(&self, chain_id: u64) -> u64 {
        self.chain(chain_id)
            .map_or(0, |state| state.head.load(Ordering::Relaxed))
    }

    pub fn record_user_visible_error(&self) {
        self.user_visible_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn user_visible_errors(&self) -> u64 {
        self.user_visible_errors.load(Ordering::Relaxed)
    }

    pub fn chain_activations(&self) -> u64 {
        self.chain_activations.load(Ordering::Relaxed)
    }

    pub fn chain_demotions(&self) -> (u64, u64, u64) {
        (
            self.chain_demotions_idle.load(Ordering::Relaxed),
            self.chain_demotions_lru.load(Ordering::Relaxed),
            self.chain_demotions_admin.load(Ordering::Relaxed),
        )
    }

    pub fn materialized_chain_pinned(&self) -> Vec<(u64, bool)> {
        self.chains
            .iter()
            .map(|entry| (*entry.key(), entry.value().pinned.load(Ordering::Relaxed)))
            .collect()
    }

    /// v2 指标：目录链数量。
    pub fn catalog_chain_count(&self) -> u64 {
        self.catalog_chains_count.load(Ordering::Relaxed)
    }

    /// v2 指标：目录端点总数。
    pub fn catalog_endpoint_count(&self) -> u64 {
        self.catalog_endpoints_count.load(Ordering::Relaxed)
    }

    /// v2 指标：chainlist 最近刷新时间戳。
    pub fn chainlist_last_refresh(&self) -> u64 {
        self.chainlist_last_refresh_unix.load(Ordering::Relaxed)
    }

    /// v2 指标：设置 chainlist 刷新信息（由 main 刷新循环调用）。
    pub fn record_chainlist_refresh(&self, unix_ts: u64, source: &str) {
        self.chainlist_last_refresh_unix
            .store(unix_ts, Ordering::Relaxed);
        *lock(&self.chainlist_refresh_source) = source.to_owned();
    }

    /// v2 指标：chainlist 最近刷新来源。
    pub fn chainlist_refresh_source_str(&self) -> String {
        lock(&self.chainlist_refresh_source).clone()
    }

    pub fn chain_settings(&self, chain_id: u64) -> (u64, u64, u64, u64, &'static str) {
        if let Some(v) = self.runtime_chain_overrides.get(&chain_id) {
            return (
                v.block_time_ms
                    .unwrap_or_else(|| self.config.block_time_ms(chain_id)),
                v.confirmation_depth
                    .unwrap_or_else(|| self.config.confirmation_depth(chain_id)),
                v.tip_ttl_ms
                    .unwrap_or_else(|| self.config.tip_ttl_ms(chain_id)),
                v.max_block_lag
                    .unwrap_or_else(|| self.config.lag_threshold(chain_id)),
                "runtime",
            );
        }
        let source = if self.config.chain_override(chain_id).is_some() {
            "config"
        } else {
            "default"
        };
        (
            self.config.block_time_ms(chain_id),
            self.config.confirmation_depth(chain_id),
            self.config.tip_ttl_ms(chain_id),
            self.config.lag_threshold(chain_id),
            source,
        )
    }

    fn runtime_lag_threshold(&self, chain_id: u64) -> u64 {
        self.runtime_chain_overrides
            .get(&chain_id)
            .and_then(|value| value.max_block_lag)
            .unwrap_or_else(|| self.config.lag_threshold(chain_id))
    }

    pub fn chain_last_ingress(&self, chain_id: u64) -> u64 {
        self.chain(chain_id)
            .map_or(0, |state| state.last_ingress.load(Ordering::Relaxed))
    }

    // ── summaries（v2：增加 state 字段） ──

    pub async fn summaries(&self) -> Vec<ChainSummary> {
        let mut states: Vec<_> = self
            .chains
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        states.sort_by_key(|state| state.chain_id);
        let now = Instant::now();

        let mut summaries = Vec::with_capacity(states.len());
        for state in states {
            let name = state.name.read().await.clone();
            let endpoints = state.endpoints.read().await;
            let mut active = 0;
            let mut cooling = 0;
            let mut probation = 0;
            for endpoint in endpoints.iter() {
                match endpoint.state(now) {
                    EndpointState::Active => active += 1,
                    EndpointState::Cooling { .. } => cooling += 1,
                    EndpointState::Probation { .. } => probation += 1,
                }
            }
            summaries.push(ChainSummary {
                chain_id: state.chain_id,
                name,
                endpoints: endpoints.len(),
                active,
                cooling,
                probation,
                head: state.head.load(Ordering::Relaxed),
                state: state.state_label(),
            });
        }
        summaries
    }

    /// 链数量统计（按状态）。
    pub async fn chain_counts(&self) -> ChainCounts {
        let mut pinned = 0u64;
        let mut hot = 0u64;
        let mut dormant = 0u64;
        let mut disabled = 0u64;
        for entry in self.chains.iter() {
            match entry.value().state_label() {
                ChainStateLabel::Pinned => pinned += 1,
                ChainStateLabel::Hot => hot += 1,
                ChainStateLabel::Dormant => dormant += 1,
                ChainStateLabel::Disabled => disabled += 1,
            }
        }
        let catalog = self.catalog.read().await;
        let catalog_count = catalog.as_ref().map_or(0, |c| c.chains.len() as u64);
        if let Some(catalog) = catalog.as_ref() {
            for chain in &catalog.chains {
                if self.chains.contains_key(&chain.chain_id) {
                    continue;
                }
                if !self.config.discovery.enabled && !self.config.chains.contains(&chain.chain_id) {
                    continue;
                }
                if self.config.discovery.deny.contains(&chain.chain_id) {
                    disabled += 1;
                } else {
                    dormant += 1;
                }
            }
        }
        ChainCounts {
            catalog: catalog_count,
            pinned,
            hot,
            dormant,
            disabled,
        }
    }

    pub async fn endpoint_metric_snapshots(&self) -> Vec<EndpointMetricSnapshot> {
        let mut states: Vec<_> = self
            .chains
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        states.sort_by_key(|state| state.chain_id);
        let now = Instant::now();
        let mut snapshots = Vec::new();
        for state in states {
            for endpoint in state.endpoints.read().await.iter() {
                let state_name = match endpoint.state(now) {
                    EndpointState::Active => "active",
                    EndpointState::Cooling { .. } => "cooling",
                    EndpointState::Probation { .. } => "probation",
                };
                snapshots.push(EndpointMetricSnapshot {
                    chain_id: state.chain_id,
                    url: endpoint.url().to_owned(),
                    state: state_name,
                    stats: endpoint.stats(),
                });
            }
        }
        snapshots
    }

    fn chain(&self, chain_id: u64) -> Option<Arc<ChainState>> {
        self.chains
            .get(&chain_id)
            .map(|state| Arc::clone(state.value()))
    }
}

// ── 汇总类型 ──

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainSummary {
    pub chain_id: u64,
    pub name: String,
    pub endpoints: usize,
    pub active: usize,
    pub cooling: usize,
    pub probation: usize,
    pub head: u64,
    /// v2: 链生命周期状态。
    pub state: ChainStateLabel,
}

#[derive(Clone, Debug, Default)]
pub struct ChainCounts {
    pub catalog: u64,
    pub pinned: u64,
    pub hot: u64,
    pub dormant: u64,
    pub disabled: u64,
}

#[derive(Clone, Debug)]
pub struct EndpointMetricSnapshot {
    pub chain_id: u64,
    pub url: String,
    pub state: &'static str,
    pub stats: EndpointStatsSnapshot,
}

// ── 工具函数 ──

pub fn cooldown_for_strikes(strikes: u32) -> Duration {
    let seconds = match strikes {
        0 | 1 => 30,
        2 => 60,
        3 => 5 * 60,
        4 => 15 * 60,
        5 => 30 * 60,
        _ => 60 * 60,
    };
    Duration::from_secs(seconds)
}

pub fn trimmed_max(heights: &[u64]) -> Option<u64> {
    if heights.is_empty() {
        return None;
    }
    let mut heights = heights.to_vec();
    heights.sort_unstable();
    let trim = if heights.len() >= 3 {
        (heights.len() / 10).max(1)
    } else {
        0
    };
    heights.get(heights.len() - 1 - trim).copied()
}

pub fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use crate::chainlist::{CatalogChain, CatalogEndpoint};

    use super::*;

    fn snapshot(urls: &[&str]) -> ChainlistSnapshot {
        ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Ethereum Mainnet".to_owned(),
                endpoints: urls.iter().map(ToString::to_string).collect(),
            }],
        }
    }

    fn activate(endpoint: &Endpoint, now: Instant, latency_ms: u64) {
        endpoint.record_success(now, Duration::from_millis(latency_ms), true);
        endpoint.record_success(now, Duration::from_millis(latency_ms), true);
        assert_eq!(endpoint.state(now), EndpointState::Active);
    }

    #[test]
    fn exponential_cooldown_and_retry_after_are_enforced() {
        let endpoint = Endpoint::new("http://limited".to_owned(), 15, 8, 0);
        let mut now = Instant::now();
        activate(&endpoint, now, 10);
        for (strike, expected) in [30, 60, 300, 900, 1_800, 3_600].into_iter().enumerate() {
            let retry_after = (strike == 0).then_some(Duration::from_secs(45));
            endpoint.record_failure(
                now,
                FailureSignal {
                    kind: FaultKind::RateLimited,
                    retry_after,
                },
            );
            let expected = if strike == 0 { 45 } else { expected };
            let EndpointState::Cooling { until, strikes } = endpoint.state(now) else {
                panic!("endpoint must be cooling");
            };
            assert_eq!(strikes, strike as u32 + 1);
            assert_eq!(
                until.saturating_duration_since(now),
                Duration::from_secs(expected)
            );
            now = until;
            assert!(endpoint.begin_probe(now));
            endpoint.record_success(now, Duration::from_millis(10), true);
            endpoint.record_success(now, Duration::from_millis(10), true);
            endpoint.end_probe();
        }
    }

    #[test]
    fn strikes_decay_after_quiet_period() {
        let endpoint = Endpoint::new("http://limited".to_owned(), 15, 8, 0);
        let start = Instant::now();
        activate(&endpoint, start, 10);
        endpoint.record_failure(start, FailureSignal::new(FaultKind::RateLimited));
        let recovered = start + Duration::from_secs(30);
        assert!(endpoint.begin_probe(recovered));
        endpoint.record_success(recovered, Duration::from_millis(10), true);
        endpoint.record_success(recovered, Duration::from_millis(10), true);
        endpoint.end_probe();
        let later = recovered + STRIKE_DECAY_INTERVAL + Duration::from_secs(1);
        endpoint.record_failure(later, FailureSignal::new(FaultKind::RateLimited));
        let EndpointState::Cooling { until, strikes } = endpoint.state(later) else {
            panic!("endpoint must be cooling");
        };
        assert_eq!(strikes, 1);
        assert_eq!(
            until.saturating_duration_since(later),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn ordinary_faults_cool_after_threshold() {
        let endpoint = Endpoint::new("http://flaky".to_owned(), 15, 8, 0);
        let now = Instant::now();
        activate(&endpoint, now, 10);
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        assert_eq!(endpoint.state(now), EndpointState::Active);
        endpoint.record_failure(now, FailureSignal::new(FaultKind::ServerError));
        assert!(matches!(endpoint.state(now), EndpointState::Cooling { .. }));
    }

    #[test]
    fn ordinary_fault_threshold_resets_outside_error_window() {
        let endpoint = Endpoint::new("http://flaky".to_owned(), 15, 8, 0);
        let start = Instant::now();
        activate(&endpoint, start, 10);
        endpoint.record_failure(start, FailureSignal::new(FaultKind::ServerError));
        endpoint.record_failure(start, FailureSignal::new(FaultKind::ServerError));
        let later = start + ERROR_WINDOW + Duration::from_secs(1);
        endpoint.record_failure(later, FailureSignal::new(FaultKind::ServerError));
        assert_eq!(endpoint.state(later), EndpointState::Active);
        endpoint.record_failure(later, FailureSignal::new(FaultKind::ServerError));
        endpoint.record_failure(later, FailureSignal::new(FaultKind::ServerError));
        assert!(matches!(
            endpoint.state(later),
            EndpointState::Cooling { .. }
        ));
    }

    #[tokio::test]
    async fn p2c_prefers_lower_scored_active_endpoint() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://slow", "http://fast"]))
            .await;
        let now = Instant::now();
        let slow = registry.endpoint(1, "http://slow").await.expect("slow");
        let fast = registry.endpoint(1, "http://fast").await.expect("fast");
        activate(&slow, now, 500);
        activate(&fast, now, 10);
        assert_eq!(registry.candidates(1).await[0].url(), "http://fast");

        registry
            .apply_snapshot(&snapshot(&["http://slow", "http://fast"]))
            .await;
        assert!(Arc::ptr_eq(
            &fast,
            &registry
                .endpoint(1, "http://fast")
                .await
                .expect("refreshed")
        ));
    }

    #[tokio::test]
    async fn enforces_endpoint_concurrency_and_rate_tokens() {
        let config = Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: crate::config::UpstreamConfig {
                default_rps: 1,
                default_concurrency: 1,
                ..crate::config::UpstreamConfig::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://limited"]))
            .await;
        let endpoint = registry
            .endpoint(1, "http://limited")
            .await
            .expect("endpoint");
        activate(&endpoint, Instant::now(), 10);

        let lease = endpoint.try_acquire().expect("first request is allowed");
        assert!(endpoint.try_acquire().is_none(), "concurrency slot is held");
        drop(lease);
        assert!(endpoint.try_acquire().is_none(), "rate token was consumed");
    }

    #[tokio::test]
    async fn removes_rejected_endpoint_across_refresh() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry.apply_snapshot(&snapshot(&["http://wrong"])).await;
        assert!(registry.remove_endpoint(1, "http://wrong").await);
        registry.apply_snapshot(&snapshot(&["http://wrong"])).await;
        assert!(registry.all_endpoints(1).await.is_empty());
    }

    #[tokio::test]
    async fn refresh_merges_materialized_dynamic_chain() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
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
                        url: "http://old".to_owned(),
                        tracking: None,
                    }],
                }],
                by_id: HashMap::from([(42, 0)]),
            }))
            .await;
        registry.resolve_for_request(42).await.expect("activate");
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 42,
                    name: "Dynamic refreshed".to_owned(),
                    endpoints: vec!["http://new".to_owned()],
                }],
            })
            .await;
        assert!(registry.endpoint(42, "http://new").await.is_some());
        assert_eq!(registry.chain_activations(), 1);
    }

    #[tokio::test]
    async fn tracks_trimmed_head_and_penalizes_lag_or_high_outliers() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .apply_snapshot(&snapshot(&["http://a", "http://b", "http://outlier"]))
            .await;
        let now = Instant::now();
        let a = registry.endpoint(1, "http://a").await.expect("a");
        let b = registry.endpoint(1, "http://b").await.expect("b");
        let outlier = registry
            .endpoint(1, "http://outlier")
            .await
            .expect("outlier");
        for endpoint in [&a, &b, &outlier] {
            activate(endpoint, now, 10);
        }
        registry.record_probe_height(1, &a, 100, now).await;
        registry.record_probe_height(1, &b, 101, now).await;
        registry.record_probe_height(1, &outlier, 10_000, now).await;
        assert_eq!(registry.head(1), 101);
        assert_eq!(a.lag(), 1);
        assert_eq!(b.lag(), 0);
        assert_eq!(outlier.lag(), 9_899);
        assert!(outlier.score(now) > a.score(now));
    }

    #[test]
    fn computes_trimmed_max_without_high_outlier() {
        assert_eq!(trimmed_max(&[]), None);
        assert_eq!(trimmed_max(&[10]), Some(10));
        assert_eq!(trimmed_max(&[10, 11]), Some(11));
        assert_eq!(trimmed_max(&[100, 101, 10_000]), Some(101));
        assert_eq!(trimmed_max(&[98, 99, 100, 101, 10_000]), Some(101));
    }

    // ── v2 生命周期测试 ──

    #[tokio::test]
    async fn resolve_for_request_returns_none_for_unknown_chain() {
        let config = Config {
            chains: vec![1],
            ..Config::default()
        };
        let registry = Registry::new(&config);
        // 没有 catalog，也没有 pinned 999999。
        assert!(registry.resolve_for_request(999999).await.is_none());
    }

    #[tokio::test]
    async fn unknown_chain_ids_do_not_accumulate_activation_locks() {
        let registry = Registry::new(&Config::default());
        for chain_id in 10_000..20_000 {
            assert!(registry.resolve_for_request(chain_id).await.is_none());
        }
        assert_eq!(registry.activation_lock_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_activation_materializes_once_and_returns_one_state() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Arc::new(Registry::new(&config));
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 42,
                    name: "Concurrent".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: "http://concurrent".to_owned(),
                        tracking: None,
                    }],
                }],
                by_id: HashMap::from([(42, 0)]),
            }))
            .await;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let registry = Arc::clone(&registry);
            tasks.spawn(async move { registry.resolve_for_request(42).await.expect("state") });
        }
        let first = tasks.join_next().await.expect("task").expect("join");
        while let Some(result) = tasks.join_next().await {
            assert!(Arc::ptr_eq(&first, &result.expect("join")));
        }
        assert_eq!(registry.chain_activations(), 1);
        assert_eq!(registry.activation_lock_count(), 0);
    }

    #[tokio::test]
    async fn stale_demotion_sample_cannot_clear_recent_ingress() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 77,
                    name: "Race".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: "http://race".to_owned(),
                        tracking: None,
                    }],
                }],
                by_id: HashMap::from([(77, 0)]),
            }))
            .await;
        let state = registry.resolve_for_request(77).await.expect("state");
        let sampled = unix_seconds().saturating_sub(10);
        state.last_ingress.store(sampled, Ordering::Relaxed);
        registry
            .resolve_for_request(77)
            .await
            .expect("recent ingress");
        assert!(!registry.demote_if_unchanged(77, "lru", sampled).await);
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
        assert_eq!(registry.user_visible_errors(), 0);
    }

    #[tokio::test]
    async fn runtime_pin_state_survives_snapshot_refresh_and_unpin_stays_hot() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        registry
            .set_catalog(Arc::new(Catalog {
                chains: vec![CatalogChain {
                    chain_id: 1,
                    name: "Runtime".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: "http://runtime".to_owned(),
                        tracking: None,
                    }],
                }],
                by_id: HashMap::from([(1, 0)]),
            }))
            .await;
        let state = registry.resolve_for_request(1).await.expect("state");
        registry
            .apply_snapshot(&snapshot(&["http://runtime"]))
            .await;
        assert!(registry.set_pinned(1, true).await);
        registry
            .apply_snapshot(&snapshot(&["http://runtime"]))
            .await;
        assert_eq!(state.state_label(), ChainStateLabel::Pinned);
        assert!(registry.set_pinned(1, false).await);
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
        registry
            .apply_snapshot(&snapshot(&["http://runtime"]))
            .await;
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
    }

    #[tokio::test]
    async fn discovery_disabled_only_serves_pinned_chains() {
        let config = Config {
            chains: vec![1],
            discovery: crate::config::DiscoveryConfig {
                enabled: false,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![
                CatalogChain {
                    chain_id: 1,
                    name: "Pinned".to_owned(),
                    short_name: None,
                    chain: None,
                    slug: None,
                    is_testnet: false,
                    native_symbol: None,
                    explorer_url: None,
                    status: None,
                    tvl: None,
                    endpoints: vec![CatalogEndpoint {
                        url: "https://pinned.example".to_owned(),
                        tracking: None,
                    }],
                },
                CatalogChain {
                    chain_id: 2,
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
                        url: "https://dynamic.example".to_owned(),
                        tracking: None,
                    }],
                },
            ],
            by_id: HashMap::from([(1, 0), (2, 1)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;
        assert!(registry.resolve_for_request(1).await.is_some());
        assert!(registry.resolve_for_request(2).await.is_none());
    }

    #[tokio::test]
    async fn resolve_for_request_materializes_dormant_chain() {
        let config = Config {
            chains: vec![1],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        // 设置 catalog（含 chain 1）。
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "Test".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        let state = registry.resolve_for_request(1).await.expect("chain 1");
        assert_eq!(state.state_label(), ChainStateLabel::Pinned); // pinned because in config.chains
        assert_eq!(registry.all_endpoints(1).await.len(), 1);
        assert_eq!(registry.chain_activations(), 1);
    }

    // ── acceptance b: lifecycle ──

    #[tokio::test]
    async fn dormant_chain_activates_on_first_request_and_becomes_hot() {
        // 使用 discovery 模式（chains 为空），链在 catalog 中但未 pinned → dormant。
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 42,
                name: "DynamicChain".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.dynamic.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(42, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        // 激活前：不在 hot_chain_ids 中。
        assert!(registry.hot_chain_ids().is_empty());

        // 首次请求触发 materialize。
        let state = registry.resolve_for_request(42).await.expect("chain 42");
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
        assert_eq!(registry.all_endpoints(42).await.len(), 1);
        assert_eq!(registry.chain_activations(), 1);

        // 激活后：在 hot_chain_ids 中。
        assert!(registry.hot_chain_ids().contains(&42));

        // 再次请求复用已有 state。
        let state2 = registry
            .resolve_for_request(42)
            .await
            .expect("chain 42 again");
        assert!(Arc::ptr_eq(&state, &state2));
        assert_eq!(registry.chain_activations(), 1); // 不重复计数。
    }

    #[tokio::test]
    async fn idle_chain_is_demoted_to_dormant() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                idle_seconds: 1, // 1 秒后降级
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 7,
                name: "FastIdle".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.fast.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(7, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        // 激活链。
        let _ = registry.resolve_for_request(7).await.expect("chain 7");
        assert_eq!(registry.all_endpoints(7).await.len(), 1);
        assert_eq!(registry.chain_counts().await.hot, 1);

        let last_ingress = registry
            .chain(7)
            .expect("chain state")
            .last_ingress
            .load(Ordering::Relaxed);
        registry.housekeeping_at(last_ingress + 2).await;

        // 验证已降级。
        let counts = registry.chain_counts().await;
        assert_eq!(counts.hot, 0);
        assert_eq!(counts.dormant, 1);
        assert_eq!(registry.all_endpoints(7).await.len(), 0); // 端点运行态丢弃。
        let (idle, lru, admin) = registry.chain_demotions();
        assert_eq!(idle, 1);
        assert_eq!(lru, 0);
        assert_eq!(admin, 0);
    }

    #[tokio::test]
    async fn pinned_chain_is_never_demoted_by_idle_or_lru() {
        let config = Config {
            chains: vec![1], // pinned
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                idle_seconds: 1,
                max_hot_chains: 0, // 容不下任何 hot 链
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "Pinned".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.pinned.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        let _ = registry.resolve_for_request(1).await.expect("chain 1");
        assert_eq!(registry.chain_counts().await.pinned, 1);

        let last_ingress = registry
            .chain(1)
            .expect("chain state")
            .last_ingress
            .load(Ordering::Relaxed);
        registry.housekeeping_at(last_ingress + 2).await;

        // pinned 不降级。
        let counts = registry.chain_counts().await;
        assert_eq!(counts.pinned, 1);
        assert_eq!(counts.dormant, 0);
        assert_eq!(registry.all_endpoints(1).await.len(), 1);
    }

    #[tokio::test]
    async fn demote_clears_endpoints_and_chain_can_reactivate() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                max_hot_chains: 256,
                idle_seconds: 86_400,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);

        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "Test".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc1.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        let _ = registry.resolve_for_request(1).await.expect("chain 1");
        assert_eq!(registry.all_endpoints(1).await.len(), 1);

        // 手动 demote（模拟 LRU）。
        assert!(registry.demote(1, "lru").await);
        assert_eq!(registry.all_endpoints(1).await.len(), 0);
        assert_eq!(registry.chain_counts().await.dormant, 1);

        // 重新激活。
        let state = registry
            .resolve_for_request(1)
            .await
            .expect("chain 1 re-activate");
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
        assert_eq!(registry.all_endpoints(1).await.len(), 1);
    }

    #[tokio::test]
    async fn housekeeping_lru_evicts_when_over_max_hot_chains() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                max_hot_chains: 2,
                idle_seconds: 86_400, // 极大，不会 idle 降级
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);

        // 在目录中放入 3 条链。
        let mut chains = Vec::new();
        let mut ids = HashMap::new();
        for i in 1..=3u64 {
            ids.insert(i, chains.len());
            chains.push(CatalogChain {
                chain_id: i,
                name: format!("Chain{i}"),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: format!("https://rpc{i}.example"),
                    tracking: None,
                }],
            });
        }
        registry
            .set_catalog(Arc::new(Catalog { chains, by_id: ids }))
            .await;

        // 激活全部 3 条链。
        let _ = registry.resolve_for_request(1).await.expect("chain 1");
        let _ = registry.resolve_for_request(2).await.expect("chain 2");
        let _ = registry.resolve_for_request(3).await.expect("chain 3");
        assert_eq!(registry.chain_counts().await.hot, 3);

        let base = unix_seconds();
        registry
            .chain(1)
            .expect("chain 1")
            .last_ingress
            .store(base, Ordering::Relaxed);
        registry
            .chain(2)
            .expect("chain 2")
            .last_ingress
            .store(base + 1, Ordering::Relaxed);
        registry
            .chain(3)
            .expect("chain 3")
            .last_ingress
            .store(base + 2, Ordering::Relaxed);

        // housekeeping: max_hot_chains=2，应淘汰 1 条链。
        registry.housekeeping_at(base + 7).await;

        let counts = registry.chain_counts().await;
        assert_eq!(counts.hot, 2);
        assert_eq!(counts.dormant, 1);

        // 恰好 1 条链被淘汰（端点清空）。
        let mut evicted = Vec::new();
        for id in [1u64, 2, 3] {
            if registry.all_endpoints(id).await.is_empty() {
                evicted.push(id);
            }
        }
        assert_eq!(evicted.len(), 1, "exactly one chain should be evicted");
        assert_eq!(
            evicted,
            vec![1],
            "least recently used chain must be evicted"
        );

        let (idle, lru, _) = registry.chain_demotions();
        assert_eq!(idle, 0);
        assert_eq!(lru, 1);

        // 被淘汰的链可重新激活。
        let evicted_id = evicted[0];
        let state = registry
            .resolve_for_request(evicted_id)
            .await
            .expect("chain re-activate");
        assert_eq!(state.state_label(), ChainStateLabel::Hot);
        assert_eq!(registry.all_endpoints(evicted_id).await.len(), 1);
    }

    #[tokio::test]
    async fn demoted_chain_can_be_re_activated() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                idle_seconds: 1,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 99,
                name: "Phantom".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.phantom.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(99, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        // 激活。
        let s1 = registry.resolve_for_request(99).await.expect("chain 99");
        assert_eq!(s1.state_label(), ChainStateLabel::Hot);

        let last_ingress = s1.last_ingress.load(Ordering::Relaxed);
        registry.housekeeping_at(last_ingress + 2).await;
        assert_eq!(registry.chain_counts().await.hot, 0);

        // 重新激活。
        let s2 = registry
            .resolve_for_request(99)
            .await
            .expect("chain 99 again");
        assert_eq!(s2.state_label(), ChainStateLabel::Hot);
        assert_eq!(registry.all_endpoints(99).await.len(), 1);
        // 激活计数为 2（两次 materialize）。
        assert_eq!(registry.chain_activations(), 2);
    }

    #[tokio::test]
    async fn deny_chain_is_marked_disabled_on_materialize() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                deny: vec![13],
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
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
                endpoints: vec![crate::chainlist::CatalogEndpoint {
                    url: "https://rpc.blocked.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(13, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        let state = registry.resolve_for_request(13).await.expect("chain 13");
        assert_eq!(state.state_label(), ChainStateLabel::Disabled);
    }

    #[tokio::test]
    async fn chainstate_label_reflects_current_state() {
        let config = Config {
            chains: vec![100],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..Config::default()
        };
        let registry = Registry::new(&config);
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 100,
                name: "LabelTest".to_owned(),
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
            by_id: HashMap::from([(100, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;

        // pinned。
        let state = registry.resolve_for_request(100).await.expect("chain 100");
        assert_eq!(state.state_label(), ChainStateLabel::Pinned);

        // unpin → hot。
        registry.set_pinned(100, false).await;
        assert_eq!(state.state_label(), ChainStateLabel::Hot);

        // disable。
        registry.set_disabled(100, true).await;
        assert_eq!(state.state_label(), ChainStateLabel::Disabled);

        // re-enable → hot。
        registry.set_disabled(100, false).await;
        assert_eq!(state.state_label(), ChainStateLabel::Hot);

        // demote → dormant。
        assert!(registry.demote(100, "admin").await);
        assert_eq!(state.state_label(), ChainStateLabel::Dormant);

        // pinned 不降级。
        registry.set_pinned(100, true).await;
        assert_eq!(state.state_label(), ChainStateLabel::Pinned);
        assert!(!registry.demote(100, "idle").await);
        assert_eq!(state.state_label(), ChainStateLabel::Pinned);
    }
}
