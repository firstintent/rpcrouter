use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use rand::Rng;
use reqwest::{Client, StatusCode, header::CONTENT_TYPE, header::HeaderMap};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::{Instant, sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::{
    config::Config,
    registry::{ArchiveStatus, Endpoint, EndpointState, Registry, unix_seconds},
    signals::{
        ArchiveClass, FailureSignal, FaultKind, ResponseClassification, TraceClass, TraceVerdict,
        classify_archive_response, classify_response, classify_trace_response,
        internal_transaction_count, resolve_archive_class, transaction_hash_for_trace,
    },
};

const ARCHIVE_BALANCE_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
/// 全节点一定还留着的近期深度。再近的区块 trace 失败，就不能拿来代表「只能查近期」。
const ARCHIVE_RECENT_DEPTH: u64 = 32;
/// 超出默认全节点状态窗口的深度。geth 现行默认大约保留 9 万块。
const ARCHIVE_TRACE_LOOKBACK: u64 = 100_000;
/// 目标块没有交易时，最多再往前看几个块，避免打到空块就放弃。
const ARCHIVE_BLOCK_WALK: u64 = 3;

const PROBE_BODY_LIMIT: usize = 1024 * 1024;
const SCHEDULER_TICK: Duration = Duration::from_millis(250);

type ProbeQueueRx = mpsc::Receiver<(u64, Arc<Endpoint>)>;

struct ProbeGuard<'a>(&'a Endpoint);

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        self.0.end_probe();
    }
}

struct InFlightGuard<'a>(&'a AtomicU64);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Posted {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
    elapsed: Duration,
}

struct ArchiveSample {
    class: ArchiveClass,
    latency: Option<Duration>,
}

struct Traced {
    class: TraceClass,
    latency: Option<Duration>,
}

enum FoundTx {
    Hash(String),
    Pruned { latency: Duration },
    Missing,
}

struct TraceSample {
    verdict: TraceVerdict,
    latency: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeOutcome {
    Passed,
    Failed(FaultKind),
    RemovedWrongChain { actual: u64 },
    Skipped,
}

pub struct ProbeManager {
    registry: Arc<Registry>,
    client: Client,
    schedules: Mutex<HashMap<(u64, String), Instant>>,
    min_interval: Duration,
    max_interval: Duration,
    request_timeout: Duration,
    slow_threshold: Duration,
    /// 有界工作池：due 端点入队，worker 消费。
    queue_tx: mpsc::Sender<(u64, Arc<Endpoint>)>,
    /// 工作池接收端（在 start_workers 中取出）。
    queue_rx: Arc<Mutex<ProbeQueueRx>>,
    /// 激活 kick 通道。
    kick_rx: Mutex<tokio::sync::broadcast::Receiver<u64>>,
    /// 在飞探针计数。
    in_flight: Arc<AtomicU64>,
    /// 队列深度（近似，由 channel 长度估算）。
    queue_depth: Arc<AtomicU64>,
    queued: StdMutex<HashSet<(u64, String)>>,
    /// 工作池并发数。
    max_concurrency: usize,
    archive_enabled: bool,
    archive_interval: Duration,
    archive_min_head: u64,
    metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl ProbeManager {
    pub fn new(registry: Arc<Registry>, config: &Config) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("rpcrouter/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build probe HTTP client")?;
        let (queue_tx, queue_rx) = mpsc::channel(4096);
        let kick_rx = registry.activation_channel().subscribe();
        // 共享 Registry 的原子计数器，使 metrics encode 能读到最新值。
        let in_flight = Arc::clone(&registry.probe_in_flight);
        let queue_depth = Arc::clone(&registry.probe_queue_depth);
        Ok(Self {
            registry,
            client,
            schedules: Mutex::new(HashMap::new()),
            min_interval: Duration::from_secs(config.probe.min_interval_seconds),
            max_interval: Duration::from_secs(config.probe.max_interval_seconds),
            request_timeout: Duration::from_millis(config.probe.request_timeout_ms),
            slow_threshold: Duration::from_millis(config.upstream.slow_threshold_ms),
            queue_tx,
            queue_rx: Arc::new(Mutex::new(queue_rx)),
            kick_rx: Mutex::new(kick_rx),
            in_flight,
            queue_depth,
            queued: StdMutex::new(HashSet::new()),
            max_concurrency: config.probe.max_concurrency,
            archive_enabled: config.probe.archive_enabled,
            archive_interval: Duration::from_secs(config.probe.archive_interval_seconds),
            archive_min_head: config.probe.archive_min_head,
            metrics: None,
        })
    }

    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            self.schedule_due_probes().await;
            self.process_kicks().await;
            sleep(SCHEDULER_TICK).await;
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    fn enqueue(&self, chain_id: u64, endpoint: Arc<Endpoint>) {
        let key = (chain_id, endpoint.url().to_owned());
        let mut queued = self
            .queued
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !queued.insert(key.clone()) {
            return;
        }
        if self.queue_tx.try_send((chain_id, endpoint)).is_ok() {
            self.queue_depth.fetch_add(1, Ordering::Relaxed);
        } else {
            queued.remove(&key);
        }
    }

    fn mark_received(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn complete(&self, chain_id: u64, endpoint: &Endpoint) {
        let mut queued = self
            .queued
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queued.remove(&(chain_id, endpoint.url().to_owned()));
    }

    async fn schedule_after_completion(&self, chain_id: u64, endpoint: &Endpoint) {
        self.schedules.lock().await.insert(
            (chain_id, endpoint.url().to_owned()),
            Instant::now() + self.jittered_interval(),
        );
    }

    async fn schedule_due_probes(self: &Arc<Self>) {
        let now = Instant::now();
        let hot_chains = self.registry.hot_chain_ids();

        let mut listed = Vec::new();
        for chain_id in hot_chains {
            listed.extend(
                self.registry
                    .probe_targets(chain_id)
                    .await
                    .into_iter()
                    .map(|endpoint| (chain_id, endpoint)),
            );
        }

        let present: HashSet<_> = listed
            .iter()
            .map(|(chain_id, endpoint)| (*chain_id, endpoint.url().to_owned()))
            .collect();
        let mut due = Vec::new();
        {
            let mut schedules = self.schedules.lock().await;
            schedules.retain(|key, _| present.contains(key));
            for (chain_id, endpoint) in listed {
                let key = (chain_id, endpoint.url().to_owned());
                let next = schedules.entry(key).or_insert(now);
                if let EndpointState::Cooling { until, .. } = endpoint.state(now)
                    && now < until
                {
                    *next = until;
                    continue;
                }
                if now >= *next {
                    *next = now + self.jittered_interval();
                    due.push((chain_id, endpoint));
                }
            }
        }

        for (chain_id, endpoint) in due {
            self.enqueue(chain_id, endpoint);
        }
    }

    /// 处理激活 kick：收到 kick 后立即将该链全部端点入队。
    async fn process_kicks(self: &Arc<Self>) {
        let mut rx = self.kick_rx.lock().await;
        loop {
            let chain_id = match rx.try_recv() {
                Ok(chain_id) => chain_id,
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            };
            let now = Instant::now();
            let endpoints = self.registry.probe_targets(chain_id).await;
            let mut schedules = self.schedules.lock().await;
            for endpoint in endpoints {
                let key = (chain_id, endpoint.url().to_owned());
                if let EndpointState::Cooling { until, .. } = endpoint.state(now)
                    && now < until
                {
                    schedules.insert(key, until);
                    continue;
                }
                let should_kick = schedules.get(&key).is_none_or(|next| *next <= now);
                schedules.insert(key, now + self.jittered_interval());
                if should_kick {
                    self.enqueue(chain_id, endpoint);
                }
            }
        }
    }

    pub async fn probe_endpoint(&self, chain_id: u64, endpoint: Arc<Endpoint>) -> ProbeOutcome {
        self.probe_endpoint_at(chain_id, endpoint, Instant::now())
            .await
    }

    /// 显式时钟入口用于无需真实等待冷却窗口的确定性测试。
    pub async fn probe_endpoint_at(
        &self,
        chain_id: u64,
        endpoint: Arc<Endpoint>,
        now: Instant,
    ) -> ProbeOutcome {
        if !endpoint.begin_probe(now) {
            return ProbeOutcome::Skipped;
        }
        let _probe_guard = ProbeGuard(&endpoint);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let _in_flight_guard = InFlightGuard(&self.in_flight);
        let started = Instant::now();

        let chain_id_response = match self
            .rpc_call(&endpoint, "eth_chainId", json!("rpcrouter-probe-chain"))
            .await
        {
            Ok(response) => response,
            Err(None) => return ProbeOutcome::Skipped,
            Err(Some(signal)) => {
                endpoint.record_failure(now + started.elapsed(), signal.clone());
                return ProbeOutcome::Failed(signal.kind);
            }
        };
        let Some(actual_chain_id) = parse_hex_result(&chain_id_response) else {
            let signal = FailureSignal::new(FaultKind::InvalidResponse);
            endpoint.record_failure(now + started.elapsed(), signal.clone());
            return ProbeOutcome::Failed(signal.kind);
        };
        if actual_chain_id != chain_id {
            self.registry
                .remove_endpoint(chain_id, endpoint.url())
                .await;
            warn!(
                expected_chain_id = chain_id,
                actual_chain_id,
                endpoint = endpoint.log_label(),
                "probe removed endpoint with mismatched chain ID"
            );
            return ProbeOutcome::RemovedWrongChain {
                actual: actual_chain_id,
            };
        }

        let block_response = match self
            .rpc_call(&endpoint, "eth_blockNumber", json!("rpcrouter-probe-block"))
            .await
        {
            Ok(response) => response,
            Err(None) => return ProbeOutcome::Skipped,
            Err(Some(signal)) => {
                endpoint.record_failure(now + started.elapsed(), signal.clone());
                return ProbeOutcome::Failed(signal.kind);
            }
        };
        let Some(height) = parse_hex_result(&block_response) else {
            let signal = FailureSignal::new(FaultKind::InvalidResponse);
            endpoint.record_failure(now + started.elapsed(), signal.clone());
            return ProbeOutcome::Failed(signal.kind);
        };

        let latency = started.elapsed();
        let finished = now + latency;
        endpoint.record_success(finished, latency, true);
        self.registry
            .record_probe_height(chain_id, &endpoint, height, finished)
            .await;
        // 落后摘除已经说明这轮不健康，不再追加一次历史状态读取。
        if !matches!(endpoint.state(finished), EndpointState::Cooling { .. }) {
            self.maybe_probe_archive(chain_id, &endpoint, height).await;
        }
        debug!(
            chain_id,
            endpoint = endpoint.log_label(),
            height,
            archive = endpoint.archive_status().as_str(),
            "probe passed"
        );
        ProbeOutcome::Passed
    }

    /// 链足够老时做两步归档探测，结论和历史读取的时延单独记下。
    /// 不改存活探针的通过与否，也不因为「不是归档」摘除端点。
    ///
    /// 1. 区块 1 的 `eth_getBalance`：没有 debug 接口时靠它判断创世状态还在不在。
    /// 2. 解析内部交易：在近期区块和大约 10 万块之前各找一笔真实交易，
    ///    用 `debug_traceTransaction` + `callTracer` 拆出内部调用。
    ///    近处能解析、远处被裁掉，就是默认全节点；两处都能解析才算归档。
    ///    节点没开 debug 时，这一步不参与结论。
    async fn maybe_probe_archive(&self, chain_id: u64, endpoint: &Arc<Endpoint>, height: u64) {
        if !self.archive_enabled
            || !endpoint.archive_check_due(unix_seconds(), self.archive_interval)
        {
            return;
        }
        let now_unix = unix_seconds();
        if height < self.archive_min_head {
            endpoint.touch_archive_check(now_unix);
            return;
        }
        let Some(balance) = self.probe_archive_balance(endpoint).await else {
            return;
        };
        let trace = self.probe_archive_trace(endpoint, height).await;
        let class = resolve_archive_class(balance.class, trace.verdict);
        let latency = match trace.verdict {
            TraceVerdict::Archive | TraceVerdict::FullNode => trace.latency.or(balance.latency),
            TraceVerdict::Unavailable | TraceVerdict::Inconclusive => balance.latency,
        };
        match (class, latency) {
            (ArchiveClass::Yes | ArchiveClass::No, Some(latency)) => {
                let status = if class == ArchiveClass::Yes {
                    ArchiveStatus::Yes
                } else {
                    ArchiveStatus::No
                };
                endpoint.record_archive(status, latency, now_unix);
            }
            _ => endpoint.touch_archive_check(now_unix),
        }
        let result = match (class, trace.verdict) {
            (ArchiveClass::Yes, _) => "yes",
            (_, TraceVerdict::FullNode) => "full_node",
            (ArchiveClass::No, _) => "no",
            _ => "unknown",
        };
        if let Some(metrics) = &self.metrics {
            metrics.record_archive_probe(chain_id, result);
            if class != ArchiveClass::Inconclusive
                && let Some(latency) = latency
            {
                metrics.record_archive_probe_latency(chain_id, latency);
            }
        }
        debug!(
            chain_id,
            endpoint = endpoint.log_label(),
            archive = endpoint.archive_status().as_str(),
            trace = ?trace.verdict,
            "archive probe finished"
        );
    }

    async fn probe_archive_balance(&self, endpoint: &Arc<Endpoint>) -> Option<ArchiveSample> {
        let request_id = json!("rpcrouter-probe-archive");
        let params = json!([ARCHIVE_BALANCE_ADDRESS, "0x1"]);
        let posted = match self
            .post_json(endpoint, "eth_getBalance", &params, &request_id)
            .await
        {
            Ok(posted) => posted,
            // 没借到探针配额时不推进周期，下一轮存活探针再试。
            Err(None) => return None,
            Err(Some(_)) => {
                return Some(ArchiveSample {
                    class: ArchiveClass::Inconclusive,
                    latency: None,
                });
            }
        };
        let class =
            classify_archive_response(posted.status, &posted.headers, &posted.body, &request_id);
        Some(ArchiveSample {
            class,
            latency: (class != ArchiveClass::Inconclusive).then_some(posted.elapsed),
        })
    }

    async fn probe_archive_trace(&self, endpoint: &Arc<Endpoint>, height: u64) -> TraceSample {
        let recent = height.saturating_sub(ARCHIVE_RECENT_DEPTH).max(1);
        let far = height.saturating_sub(ARCHIVE_TRACE_LOOKBACK).max(1);
        if recent <= far {
            return TraceSample {
                verdict: TraceVerdict::Inconclusive,
                latency: None,
            };
        }
        let recent = match self
            .post_trace(endpoint, recent, json!("rpcrouter-probe-trace-recent"))
            .await
        {
            Ok(class) => class,
            Err(_) => {
                return TraceSample {
                    verdict: TraceVerdict::Inconclusive,
                    latency: None,
                };
            }
        };
        if recent.class != TraceClass::Ok {
            let verdict = if recent.class == TraceClass::Unsupported {
                TraceVerdict::Unavailable
            } else {
                // 近期就 trace 不了，不能当成「全节点只能查近期」。
                TraceVerdict::Inconclusive
            };
            return TraceSample {
                verdict,
                latency: None,
            };
        }
        let far = match self
            .post_trace(endpoint, far, json!("rpcrouter-probe-trace-far"))
            .await
        {
            Ok(class) => class,
            Err(_) => {
                return TraceSample {
                    verdict: TraceVerdict::Inconclusive,
                    latency: None,
                };
            }
        };
        let verdict = match far.class {
            TraceClass::Ok => TraceVerdict::Archive,
            TraceClass::Pruned => TraceVerdict::FullNode,
            TraceClass::Unsupported | TraceClass::Inconclusive => TraceVerdict::Inconclusive,
        };
        TraceSample {
            verdict,
            latency: matches!(far.class, TraceClass::Ok | TraceClass::Pruned)
                .then_some(far.latency)
                .flatten(),
        }
    }

    async fn post_trace(
        &self,
        endpoint: &Arc<Endpoint>,
        block: u64,
        request_id: Value,
    ) -> std::result::Result<Traced, Option<()>> {
        let label = match request_id.as_str() {
            Some(label) => label.to_owned(),
            None => "rpcrouter-probe-trace".to_owned(),
        };
        let hash = match self.find_transaction(endpoint, block, &label).await? {
            FoundTx::Hash(hash) => hash,
            FoundTx::Pruned { latency } => {
                return Ok(Traced {
                    class: TraceClass::Pruned,
                    latency: Some(latency),
                });
            }
            FoundTx::Missing => {
                return Ok(Traced {
                    class: TraceClass::Inconclusive,
                    latency: None,
                });
            }
        };
        let trace_id = json!(format!("{label}-tx"));
        let posted = self
            .post_json(
                endpoint,
                "debug_traceTransaction",
                &json!([hash, {"tracer": "callTracer"}]),
                &trace_id,
            )
            .await
            .map_err(|error| error.map(|_| ()))?;
        let class =
            classify_trace_response(posted.status, &posted.headers, &posted.body, &trace_id);
        if class == TraceClass::Ok {
            let parsed = rpc_result(&posted.body);
            let Some(count) = parsed.as_ref().and_then(internal_transaction_count) else {
                return Ok(Traced {
                    class: TraceClass::Inconclusive,
                    latency: Some(posted.elapsed),
                });
            };
            debug!(
                endpoint = endpoint.log_label(),
                block,
                internal_transactions = count,
                "parsed internal transactions"
            );
        }
        Ok(Traced {
            class,
            latency: Some(posted.elapsed),
        })
    }

    /// 从目标块往前找一笔交易。空块换前一个块，最多再看 `ARCHIVE_BLOCK_WALK` 个。
    async fn find_transaction(
        &self,
        endpoint: &Arc<Endpoint>,
        start: u64,
        label: &str,
    ) -> std::result::Result<FoundTx, Option<()>> {
        let mut block_number = start;
        for step in 0..=ARCHIVE_BLOCK_WALK {
            let request_id = json!(format!("{label}-block-{step}"));
            let posted = self
                .post_json(
                    endpoint,
                    "eth_getBlockByNumber",
                    &json!([format!("0x{block_number:x}"), true]),
                    &request_id,
                )
                .await
                .map_err(|error| error.map(|_| ()))?;
            let class =
                classify_trace_response(posted.status, &posted.headers, &posted.body, &request_id);
            match class {
                TraceClass::Pruned => {
                    return Ok(FoundTx::Pruned {
                        latency: posted.elapsed,
                    });
                }
                TraceClass::Unsupported | TraceClass::Inconclusive => {
                    let missing_block =
                        rpc_result(&posted.body).is_none_or(|value| value.is_null());
                    if class == TraceClass::Unsupported
                        || !missing_block
                        || block_number <= 1
                        || step == ARCHIVE_BLOCK_WALK
                    {
                        return Ok(FoundTx::Missing);
                    }
                }
                TraceClass::Ok => {
                    if let Some(hash) = rpc_result(&posted.body)
                        .as_ref()
                        .and_then(transaction_hash_for_trace)
                        .map(str::to_owned)
                    {
                        return Ok(FoundTx::Hash(hash));
                    }
                    if block_number <= 1 || step == ARCHIVE_BLOCK_WALK {
                        return Ok(FoundTx::Missing);
                    }
                }
            }
            block_number = block_number.saturating_sub(1);
        }
        Ok(FoundTx::Missing)
    }

    async fn rpc_call(
        &self,
        endpoint: &Arc<Endpoint>,
        method: &str,
        request_id: Value,
    ) -> std::result::Result<Value, Option<FailureSignal>> {
        let posted = self
            .post_json(endpoint, method, &json!([]), &request_id)
            .await?;
        match classify_response(
            posted.status,
            &posted.headers,
            &posted.body,
            posted.elapsed,
            &request_id,
            self.slow_threshold,
        ) {
            ResponseClassification::Valid(value) => Ok(value),
            ResponseClassification::Degraded { fault, .. } => Err(Some(FailureSignal::new(fault))),
            ResponseClassification::Failure(signal) => Err(Some(signal)),
        }
    }

    async fn post_json(
        &self,
        endpoint: &Arc<Endpoint>,
        method: &str,
        params: &Value,
        request_id: &Value,
    ) -> std::result::Result<Posted, Option<FailureSignal>> {
        let Some(lease) = endpoint.try_acquire_probe() else {
            return Err(None);
        };
        let request = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });
        let request_body = serde_json::to_vec(&request)
            .map_err(|_| Some(FailureSignal::new(FaultKind::InvalidResponse)))?;
        let started = Instant::now();
        let response = timeout(
            self.request_timeout,
            self.client
                .post(lease.endpoint().url())
                .header(CONTENT_TYPE, "application/json")
                .body(request_body)
                .send(),
        )
        .await;
        let mut response = match response {
            Err(_) => return Err(Some(FailureSignal::new(FaultKind::Timeout))),
            Ok(Err(_)) => return Err(Some(FailureSignal::new(FaultKind::Transport))),
            Ok(Ok(response)) => response,
        };
        let status = response.status();
        let headers = response.headers().clone();
        let mut body = Vec::new();
        loop {
            let chunk = match response.chunk().await {
                Ok(chunk) => chunk,
                Err(_) => return Err(Some(FailureSignal::new(FaultKind::Transport))),
            };
            let Some(chunk) = chunk else {
                break;
            };
            if body.len().saturating_add(chunk.len()) > PROBE_BODY_LIMIT {
                return Err(Some(FailureSignal::new(FaultKind::InvalidResponse)));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Posted {
            status,
            headers,
            body,
            elapsed: started.elapsed(),
        })
    }

    pub fn jittered_interval(&self) -> Duration {
        let min_millis = self.min_interval.as_millis().min(u128::from(u64::MAX)) as u64;
        let max_millis = self.max_interval.as_millis().min(u128::from(u64::MAX)) as u64;
        Duration::from_millis(rand::rng().random_range(min_millis..=max_millis))
    }
}

/// 有界工作池：最多 N 个探针任务同时运行；不会为每个排队项创建等待信号量的任务。
async fn worker_pool(manager: Arc<ProbeManager>, rx: Arc<Mutex<ProbeQueueRx>>) {
    let mut active = JoinSet::new();
    loop {
        while active.len() >= manager.max_concurrency {
            let _ = active.join_next().await;
        }
        tokio::select! {
            item = async { rx.lock().await.recv().await } => {
                let Some((chain_id, endpoint)) = item else { break };
                manager.mark_received();
                let manager = Arc::clone(&manager);
                active.spawn(async move {
                    struct CompletionGuard {
                        manager: Arc<ProbeManager>,
                        chain_id: u64,
                        endpoint: Arc<Endpoint>,
                    }
                    impl Drop for CompletionGuard {
                        fn drop(&mut self) {
                            self.manager.complete(self.chain_id, &self.endpoint);
                        }
                    }
                    let _guard = CompletionGuard {
                        manager: Arc::clone(&manager),
                        chain_id,
                        endpoint: Arc::clone(&endpoint),
                    };
                    manager.probe_endpoint(chain_id, Arc::clone(&endpoint)).await;
                    manager.schedule_after_completion(chain_id, &endpoint).await;
                });
            }
            joined = active.join_next(), if !active.is_empty() => {
                let _ = joined;
            }
        }
    }
    while active.join_next().await.is_some() {}
}

fn rpc_result(body: &[u8]) -> Option<Value> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("result").cloned())
}

fn parse_hex_result(response: &Value) -> Option<u64> {
    let value = response.get("result")?.as_str()?;
    let digits = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))?;
    u64::from_str_radix(digits, 16).ok()
}

pub fn spawn(manager: Arc<ProbeManager>) {
    let metrics = Arc::new(crate::metrics::Metrics::new().expect("probe metrics"));
    spawn_supervised(manager, metrics);
}

pub fn spawn_supervised(manager: Arc<ProbeManager>, metrics: Arc<crate::metrics::Metrics>) {
    let worker_rx = Arc::clone(&manager.queue_rx);
    let worker_manager = Arc::clone(&manager);
    crate::supervisor::spawn("probe-worker", Arc::clone(&metrics), move || {
        let manager = Arc::clone(&worker_manager);
        let rx = Arc::clone(&worker_rx);
        async move { worker_pool(manager, rx).await }
    });
    crate::supervisor::spawn("probe-scheduler", metrics, move || {
        let manager = Arc::clone(&manager);
        async move {
            info!("health probe scheduler started");
            manager.run().await
        }
    });
}

#[cfg(test)]
mod tests {
    use axum::{Json, Router, body::Bytes, extract::State, routing::post};

    use crate::{
        chainlist::{Catalog, CatalogChain, CatalogEndpoint, ChainEndpoints, ChainlistSnapshot},
        config::{ProbeConfig, UpstreamConfig},
    };

    use super::*;

    #[derive(Clone)]
    struct MockState {
        chain_id: u64,
        height: u64,
    }

    async fn mock_rpc(State(state): State<MockState>, body: Bytes) -> Json<Value> {
        let request: Value = serde_json::from_slice(&body).expect("probe request");
        let result = match request["method"].as_str().expect("probe method") {
            "eth_chainId" => format!("0x{:x}", state.chain_id),
            "eth_blockNumber" => format!("0x{:x}", state.height),
            method => panic!("unexpected method {method}"),
        };
        Json(json!({"jsonrpc":"2.0", "id":request["id"], "result":result}))
    }

    async fn mock_url(chain_id: u64, height: u64) -> String {
        let app = Router::new()
            .route("/", post(mock_rpc))
            .with_state(MockState { chain_id, height });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock");
        });
        format!("http://{address}/")
    }

    fn test_config() -> Config {
        Config {
            chains: vec![1],
            chain_overrides: Vec::new(),
            upstream: UpstreamConfig {
                slow_threshold_ms: 100,
                default_rps: 100,
                ..UpstreamConfig::default()
            },
            probe: ProbeConfig {
                max_concurrency: 2,
                request_timeout_ms: 100,
                archive_enabled: false,
                ..ProbeConfig::default()
            },
            ..Config::default()
        }
    }

    async fn setup(url: &str) -> (Arc<Registry>, ProbeManager, Arc<Endpoint>) {
        let config = test_config();
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test".to_owned(),
                    endpoints: vec![url.to_owned()],
                }],
            })
            .await;
        let endpoint = registry.endpoint(1, url).await.expect("endpoint");
        let manager = ProbeManager::new(Arc::clone(&registry), &config).expect("probe manager");
        (registry, manager, endpoint)
    }

    #[tokio::test]
    async fn two_probe_passes_activate_endpoint_and_track_head() {
        let url = mock_url(1, 1234).await;
        let (registry, manager, endpoint) = setup(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(
            endpoint.state(Instant::now()),
            EndpointState::Probation { passes: 1 }
        );
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
        assert_eq!(registry.head(1), 1234);
    }

    #[tokio::test]
    async fn wrong_chain_id_is_removed() {
        let url = mock_url(143, 1234).await;
        let (registry, manager, endpoint) = setup(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, endpoint).await,
            ProbeOutcome::RemovedWrongChain { actual: 143 }
        );
        assert!(registry.all_endpoints(1).await.is_empty());
    }

    #[tokio::test]
    async fn jitter_is_uniformly_bounded() {
        let config = test_config();
        let manager =
            ProbeManager::new(Arc::new(Registry::new(&config)), &config).expect("probe manager");
        let mut distinct = HashSet::new();
        for _ in 0..100 {
            let interval = manager.jittered_interval();
            assert!(interval >= Duration::from_secs(15));
            assert!(interval <= Duration::from_secs(30));
            distinct.insert(interval);
        }
        assert!(distinct.len() > 1);
    }

    #[tokio::test]
    async fn dormant_chain_not_in_hot_chain_ids() {
        let config = Config {
            chains: vec![],
            discovery: crate::config::DiscoveryConfig {
                enabled: true,
                ..Default::default()
            },
            ..test_config()
        };
        let registry = Arc::new(Registry::new(&config));
        // 设置 catalog 含 chain 1（dormant，因为没有 pinned）。
        let catalog = Catalog {
            chains: vec![CatalogChain {
                chain_id: 1,
                name: "DormantChain".to_owned(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: None,
                status: None,
                tvl: None,
                endpoints: vec![CatalogEndpoint {
                    url: "https://rpc.example".to_owned(),
                    tracking: None,
                }],
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        registry.set_catalog(Arc::new(catalog)).await;
        // dormant 链不在 hot_chain_ids 中。
        assert!(registry.hot_chain_ids().is_empty());
    }

    #[derive(Clone, Copy)]
    enum DebugMode {
        /// 远处 trace 与余额结论一致；近处总是成功。
        Mirror,
        /// 没开 debug 命名空间。
        Unsupported,
        /// 余额假装有创世状态，但远处 trace 被裁掉。
        FullNode,
    }

    #[derive(Clone)]
    struct ArchiveMock {
        chain_id: u64,
        height: u64,
        archive: bool,
        debug: DebugMode,
        balance_calls: Arc<AtomicU64>,
        trace_calls: Arc<AtomicU64>,
    }

    fn hex_param(request: &Value, index: usize) -> u64 {
        request["params"][index]
            .as_str()
            .and_then(|value| value.strip_prefix("0x"))
            .and_then(|digits| u64::from_str_radix(digits, 16).ok())
            .unwrap_or(0)
    }

    async fn archive_rpc(State(state): State<ArchiveMock>, body: Bytes) -> Json<Value> {
        let request: Value = serde_json::from_slice(&body).expect("probe request");
        let method = request["method"].as_str().unwrap_or("");
        let result = match method {
            "eth_chainId" => json!(format!("0x{:x}", state.chain_id)),
            "eth_blockNumber" => json!(format!("0x{:x}", state.height)),
            "eth_getBalance" => {
                state.balance_calls.fetch_add(1, Ordering::Relaxed);
                if state.archive {
                    json!("0x0")
                } else {
                    return Json(json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {"code": -32000, "message": "missing trie node 0xabc"}
                    }));
                }
            }
            "eth_getBlockByNumber" => {
                let block = hex_param(&request, 0);
                let recent = block + 64 >= state.height;
                let far_ok = matches!(state.debug, DebugMode::Mirror) && state.archive;
                if !recent
                    && !far_ok
                    && !matches!(state.debug, DebugMode::FullNode | DebugMode::Unsupported)
                {
                    return Json(json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {"code": -32000, "message": "missing trie node 0xabc"}
                    }));
                }
                json!({
                    "number": format!("0x{block:x}"),
                    "transactions": [{
                        "hash": format!("0x{block:x}"),
                        "input": "0xa9059cbb"
                    }]
                })
            }
            "debug_traceTransaction" => {
                state.trace_calls.fetch_add(1, Ordering::Relaxed);
                if matches!(state.debug, DebugMode::Unsupported) {
                    return Json(json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {
                            "code": -32601,
                            "message": "the method debug_traceTransaction does not exist/is not available"
                        }
                    }));
                }
                let hash = request["params"][0].as_str().unwrap_or("0x0");
                let block = u64::from_str_radix(hash.trim_start_matches("0x"), 16).unwrap_or(0);
                let recent = block + 64 >= state.height;
                let far_ok = matches!(state.debug, DebugMode::Mirror) && state.archive;
                if recent || far_ok {
                    json!({
                        "type": "CALL",
                        "from": "0x0000000000000000000000000000000000000001",
                        "to": "0x0000000000000000000000000000000000000002",
                        "calls": [{
                            "type": "CALL",
                            "from": "0x0000000000000000000000000000000000000002",
                            "to": "0x0000000000000000000000000000000000000003"
                        }]
                    })
                } else {
                    return Json(json!({
                        "jsonrpc": "2.0",
                        "id": request["id"],
                        "error": {"code": -32000, "message": "missing trie node 0xabc"}
                    }));
                }
            }
            other => panic!("unexpected method {other}"),
        };
        Json(json!({"jsonrpc":"2.0", "id": request["id"], "result": result}))
    }

    async fn spawn_archive_mock(height: u64, archive: bool) -> (String, Arc<AtomicU64>) {
        spawn_archive_mock_with(height, archive, DebugMode::Mirror).await
    }

    async fn spawn_archive_mock_with(
        height: u64,
        archive: bool,
        debug: DebugMode,
    ) -> (String, Arc<AtomicU64>) {
        let balance_calls = Arc::new(AtomicU64::new(0));
        let app = Router::new()
            .route("/", post(archive_rpc))
            .with_state(ArchiveMock {
                chain_id: 1,
                height,
                archive,
                debug,
                balance_calls: Arc::clone(&balance_calls),
                trace_calls: Arc::new(AtomicU64::new(0)),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind archive mock");
        let address = listener.local_addr().expect("archive mock address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve archive mock");
        });
        (format!("http://{address}/"), balance_calls)
    }

    fn archive_config() -> Config {
        Config {
            probe: ProbeConfig {
                max_concurrency: 2,
                request_timeout_ms: 1_000,
                archive_enabled: true,
                archive_interval_seconds: 600,
                archive_min_head: 1_000,
                ..ProbeConfig::default()
            },
            upstream: UpstreamConfig {
                slow_threshold_ms: 4_000,
                default_rps: 100,
                ..UpstreamConfig::default()
            },
            ..test_config()
        }
    }

    async fn probe_once(url: &str) -> (ProbeManager, Arc<Endpoint>, Arc<Registry>) {
        let config = archive_config();
        let registry = Arc::new(Registry::new(&config));
        registry
            .apply_snapshot(&ChainlistSnapshot {
                chains: vec![ChainEndpoints {
                    chain_id: 1,
                    name: "Test".to_owned(),
                    endpoints: vec![url.to_owned()],
                }],
            })
            .await;
        let endpoint = registry.endpoint(1, url).await.expect("endpoint");
        let manager = ProbeManager::new(Arc::clone(&registry), &config).expect("probe manager");
        (manager, endpoint, registry)
    }

    #[tokio::test]
    async fn archive_probe_records_yes_and_does_not_repeat_within_the_interval() {
        let (url, calls) = spawn_archive_mock(200_000, true).await;
        let (manager, endpoint, _) = probe_once(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.archive_status(), ArchiveStatus::Yes);
        assert!(endpoint.archive_latency_ewma_micros() > 0);
        assert_eq!(endpoint.stats().failures, 0);
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(endpoint.state(Instant::now()), EndpointState::Active);
    }

    #[tokio::test]
    async fn pruned_history_is_not_archive_and_stays_healthy() {
        let (url, calls) = spawn_archive_mock(200_000, false).await;
        let (manager, endpoint, _) = probe_once(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, Arc::clone(&endpoint)).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.archive_status(), ArchiveStatus::No);
        assert!(endpoint.archive_latency_ewma_micros() > 0);
        assert_eq!(endpoint.stats().failures, 0);
        assert_eq!(
            endpoint.state(Instant::now()),
            EndpointState::Probation { passes: 1 }
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn young_chain_stays_unknown_without_a_historical_read() {
        let (url, calls) = spawn_archive_mock(10, true).await;
        let (manager, endpoint, _) = probe_once(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, endpoint.clone()).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.archive_status(), ArchiveStatus::Unknown);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn recent_trace_with_pruned_history_is_a_full_node_even_if_balance_lies() {
        let (url, _) = spawn_archive_mock_with(200_000, true, DebugMode::FullNode).await;
        let (manager, endpoint, _) = probe_once(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, endpoint.clone()).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.archive_status(), ArchiveStatus::No);
        assert_eq!(endpoint.stats().failures, 0);
    }

    #[tokio::test]
    async fn missing_debug_namespace_falls_back_to_balance() {
        let (url, _) = spawn_archive_mock_with(200_000, true, DebugMode::Unsupported).await;
        let (manager, endpoint, _) = probe_once(&url).await;
        assert_eq!(
            manager.probe_endpoint(1, endpoint.clone()).await,
            ProbeOutcome::Passed
        );
        assert_eq!(endpoint.archive_status(), ArchiveStatus::Yes);
        assert_eq!(endpoint.stats().failures, 0);
    }
}
