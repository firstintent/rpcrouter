use std::{
    env,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rpcrouter::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::{CacheConfig, Config, HedgingConfig, ProbeConfig, UpstreamConfig},
    forward::Forwarder,
    metrics::ChainMetricsSnapshot,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    probe::{ProbeManager, spawn_supervised as spawn_probes},
    registry::{Endpoint, EndpointState, Registry},
    server::{AppState, router},
};
use serde::Serialize;
use serde_json::json;
use tokio::{net::TcpListener, task::JoinSet, time::Instant};

#[derive(Clone, Copy)]
struct Options {
    qps: u64,
    duration_seconds: u64,
    concurrency: usize,
    storm: bool,
}

#[derive(Serialize)]
struct TimelineEvent {
    at_seconds: f64,
    event: String,
}

#[derive(Serialize)]
struct EndpointReport {
    name: &'static str,
    total_requests: u64,
    max_requests_per_second: u64,
    configured_rps: u32,
    limit_breached: bool,
}

#[derive(Serialize)]
struct LoadReport {
    requested_qps: u64,
    duration_seconds: u64,
    scheduled_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    achieved_qps: f64,
    p50_ms: f64,
    p99_ms: f64,
    cache_hit_percent: f64,
    coalesce_percent: f64,
    combined_hit_coalesce_percent: f64,
    upstream_data_requests: u64,
    user_visible_errors: u64,
    cooling_events: u64,
    final_storm_endpoint_state: String,
    endpoints: Vec<EndpointReport>,
    timeline: Vec<TimelineEvent>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = parse_options()?;
    let configured_rps = 15;
    let (storm_url, storm) = spawn_mock().await?;
    let (healthy_url, healthy) = spawn_mock().await?;
    let config = load_config(configured_rps);
    let registry = Arc::new(Registry::new(&config));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Load Test".to_owned(),
                endpoints: vec![storm_url.clone(), healthy_url.clone()],
            }],
        })
        .await;
    let storm_endpoint = registry
        .endpoint(1, &storm_url)
        .await
        .context("missing storm endpoint")?;
    let healthy_endpoint = registry
        .endpoint(1, &healthy_url)
        .await
        .context("missing healthy endpoint")?;
    activate(&storm_endpoint, Duration::from_millis(1));
    activate(&healthy_endpoint, Duration::from_millis(50));

    let forwarder = Arc::new(Forwarder::new(Arc::clone(&registry), &config)?);
    let metrics = forwarder.metrics();
    let app = router(AppState::new(
        Arc::clone(&registry),
        Arc::clone(&forwarder),
        config.server.batch_limit,
    ));
    let gateway_listener = TcpListener::bind("127.0.0.1:0").await?;
    let gateway_address = gateway_listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(gateway_listener, app)
            .await
            .expect("serve load-test gateway");
    });
    let gateway_url = format!("http://{gateway_address}/rpc/1");
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(options.concurrency)
        .build()?;

    send_request(&client, &gateway_url, block_request()).await?;
    forwarder.cache().sync().await;
    storm.reset_request_count();
    healthy.reset_request_count();
    let baseline = metrics.chain_snapshot(1);

    let probes = Arc::new(
        ProbeManager::new(Arc::clone(&registry), &config)?.with_metrics(Arc::clone(&metrics)),
    );
    spawn_probes(probes, Arc::clone(&metrics));
    let timeline = Arc::new(Mutex::new(Vec::new()));
    let started = Instant::now();
    record_event(&timeline, started, "load_started");
    let monitor = spawn_state_monitor(Arc::clone(&storm_endpoint), Arc::clone(&timeline), started);
    let injection = options.storm.then(|| {
        spawn_storm(
            storm.clone(),
            Arc::clone(&timeline),
            started,
            Arc::clone(&forwarder),
        )
    });

    let total = options.qps.saturating_mul(options.duration_seconds);
    let next = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let mut workers = JoinSet::new();
    for _ in 0..options.concurrency {
        let client = client.clone();
        let gateway_url = gateway_url.clone();
        let next = Arc::clone(&next);
        let failures = Arc::clone(&failures);
        workers.spawn(async move {
            run_worker(
                client,
                gateway_url,
                next,
                failures,
                started,
                options.qps,
                total,
            )
            .await
        });
    }
    let mut latencies = Vec::with_capacity(total.min(usize::MAX as u64) as usize);
    while let Some(worker) = workers.join_next().await {
        latencies.extend(worker.context("load worker panicked")?);
    }
    let load_elapsed = started.elapsed();
    if let Some(injection) = injection {
        injection.await.context("storm task panicked")??;
    }
    monitor.abort();
    let _ = monitor.await;
    record_event(&timeline, started, "load_finished");

    latencies.sort_unstable();
    let failed = failures.load(Ordering::Relaxed);
    let successful = total.saturating_sub(failed);
    let current = metrics.chain_snapshot(1);
    let delta = subtract_metrics(current, baseline);
    let storm_stats = storm_endpoint.stats();
    let storm_max_qps = storm.max_requests_per_second();
    let healthy_max_qps = healthy.max_requests_per_second();
    let report = LoadReport {
        requested_qps: options.qps,
        duration_seconds: options.duration_seconds,
        scheduled_requests: total,
        successful_requests: successful,
        failed_requests: failed,
        achieved_qps: successful as f64 / load_elapsed.as_secs_f64(),
        p50_ms: percentile_millis(&latencies, 0.50),
        p99_ms: percentile_millis(&latencies, 0.99),
        cache_hit_percent: percent(delta.cache_hits, delta.cache_lookups),
        coalesce_percent: percent(delta.coalesced, delta.cache_misses),
        combined_hit_coalesce_percent: percent(
            delta.cache_hits.saturating_add(delta.coalesced),
            delta.ingress,
        ),
        upstream_data_requests: delta.upstream,
        user_visible_errors: delta.user_visible_errors,
        cooling_events: storm_stats.cooling_events,
        final_storm_endpoint_state: state_name(storm_endpoint.state(Instant::now())),
        endpoints: vec![
            EndpointReport {
                name: "storm",
                total_requests: storm.request_count(),
                max_requests_per_second: storm_max_qps,
                configured_rps,
                limit_breached: storm_max_qps > u64::from(configured_rps),
            },
            EndpointReport {
                name: "healthy",
                total_requests: healthy.request_count(),
                max_requests_per_second: healthy_max_qps,
                configured_rps,
                limit_breached: healthy_max_qps > u64::from(configured_rps),
            },
        ],
        timeline: take_events(&timeline),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);

    if failed != 0
        || report.combined_hit_coalesce_percent < 98.0
        || report.p99_ms > 50.0
        || report.user_visible_errors != 0
        || report
            .endpoints
            .iter()
            .any(|endpoint| endpoint.limit_breached)
        || (options.storm && report.final_storm_endpoint_state != "active")
    {
        bail!("load-test acceptance criteria failed");
    }
    Ok(())
}

fn load_config(rps: u32) -> Config {
    Config {
        chains: vec![1],
        chain_overrides: Vec::new(),
        upstream: UpstreamConfig {
            request_timeout_ms: 5_000,
            slow_threshold_ms: 4_000,
            deadline_ms: 15_000,
            max_attempts: 4,
            default_rps: rps,
            default_concurrency: 8,
        },
        probe: ProbeConfig {
            min_interval_seconds: 15,
            max_interval_seconds: 15,
            ..ProbeConfig::default()
        },
        cache: CacheConfig {
            max_bytes: 128 * 1024 * 1024,
            immutable_ttl_seconds: 3_600,
        },
        hedging: HedgingConfig {
            enabled: false,
            ..HedgingConfig::default()
        },
        ..Config::default()
    }
}

async fn spawn_mock() -> Result<(String, MockController)> {
    let controller = MockController::new(MockBehavior {
        delay_ms: 5,
        ..MockBehavior::default()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = mock_router(controller.clone());
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve load-test mock");
    });
    Ok((format!("http://{address}/"), controller))
}

fn activate(endpoint: &Endpoint, latency: Duration) {
    let now = Instant::now();
    endpoint.record_success(now, latency, true);
    endpoint.record_success(now, latency, true);
}

fn block_request() -> &'static str {
    r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#
}

async fn send_request(client: &reqwest::Client, url: &str, body: &str) -> Result<()> {
    let response = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body.to_owned())
        .send()
        .await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() || !bytes.windows(8).any(|window| window == b"\"result\"") {
        bail!(
            "gateway request failed with HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    Ok(())
}

async fn run_worker(
    client: reqwest::Client,
    url: String,
    next: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    started: Instant,
    qps: u64,
    total: u64,
) -> Vec<u64> {
    let mut latencies = Vec::new();
    loop {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= total {
            break;
        }
        let scheduled_nanos = u128::from(index)
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(qps))
            .unwrap_or(0)
            .min(u128::from(u64::MAX)) as u64;
        tokio::time::sleep_until(started + Duration::from_nanos(scheduled_nanos)).await;
        let request_started = Instant::now();
        let request_result = send_request(&client, &url, block_request()).await;
        let succeeded = request_result.is_ok();
        latencies.push(
            request_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
        if !succeeded {
            let failure = failures.fetch_add(1, Ordering::Relaxed);
            if failure < 10
                && let Err(error) = request_result
            {
                eprintln!("load request failed: {error:#}");
            }
        }
    }
    latencies
}

fn spawn_storm(
    controller: MockController,
    timeline: Arc<Mutex<Vec<TimelineEvent>>>,
    started: Instant,
    forwarder: Arc<Forwarder>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        tokio::time::sleep_until(started + Duration::from_secs(10)).await;
        controller.set_rate_limit_after(Some(0));
        record_event(&timeline, started, "429_storm_enabled");
        let response = forwarder
            .execute(
                1,
                json!({
                    "jsonrpc":"2.0",
                    "id":"storm",
                    "method":"loadtest_uncached",
                    "params":[]
                }),
            )
            .await;
        if response.get("result").is_none() {
            bail!("429 injection request failed: {response}");
        }
        tokio::time::sleep_until(started + Duration::from_secs(20)).await;
        controller.set_rate_limit_after(None);
        record_event(&timeline, started, "429_storm_disabled");
        Ok(())
    })
}

fn spawn_state_monitor(
    endpoint: Arc<Endpoint>,
    timeline: Arc<Mutex<Vec<TimelineEvent>>>,
    started: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut previous = String::new();
        loop {
            let current = state_name(endpoint.state(Instant::now()));
            if current != previous {
                record_event(&timeline, started, format!("storm_endpoint_{current}"));
                previous = current;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

fn state_name(state: EndpointState) -> String {
    match state {
        EndpointState::Active => "active".to_owned(),
        EndpointState::Cooling { strikes, .. } => format!("cooling_strike_{strikes}"),
        EndpointState::Probation { passes } => format!("probation_pass_{passes}"),
    }
}

fn record_event(timeline: &Mutex<Vec<TimelineEvent>>, started: Instant, event: impl Into<String>) {
    lock(timeline).push(TimelineEvent {
        at_seconds: started.elapsed().as_secs_f64(),
        event: event.into(),
    });
}

fn take_events(timeline: &Mutex<Vec<TimelineEvent>>) -> Vec<TimelineEvent> {
    std::mem::take(&mut *lock(timeline))
}

fn subtract_metrics(
    current: ChainMetricsSnapshot,
    baseline: ChainMetricsSnapshot,
) -> ChainMetricsSnapshot {
    ChainMetricsSnapshot {
        ingress: current.ingress.saturating_sub(baseline.ingress),
        cache_lookups: current.cache_lookups.saturating_sub(baseline.cache_lookups),
        cache_hits: current.cache_hits.saturating_sub(baseline.cache_hits),
        cache_misses: current.cache_misses.saturating_sub(baseline.cache_misses),
        coalesced: current.coalesced.saturating_sub(baseline.coalesced),
        upstream: current.upstream.saturating_sub(baseline.upstream),
        user_visible_errors: current
            .user_visible_errors
            .saturating_sub(baseline.user_visible_errors),
        cold_start_failures: current
            .cold_start_failures
            .saturating_sub(baseline.cold_start_failures),
        hedges: current.hedges.saturating_sub(baseline.hedges),
    }
}

fn percentile_millis(sorted_micros: &[u64], percentile: f64) -> f64 {
    if sorted_micros.is_empty() {
        return 0.0;
    }
    let index = ((sorted_micros.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted_micros.len() - 1);
    sorted_micros[index] as f64 / 1_000.0
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64 * 100.0
    }
}

fn parse_options() -> Result<Options> {
    let mut options = Options {
        qps: 10_000,
        duration_seconds: 60,
        concurrency: 64,
        storm: true,
    };
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--qps" => options.qps = next(&mut arguments, &argument)?.parse()?,
            "--duration" => options.duration_seconds = next(&mut arguments, &argument)?.parse()?,
            "--concurrency" => options.concurrency = next(&mut arguments, &argument)?.parse()?,
            "--no-storm" => options.storm = false,
            unknown => bail!("unknown argument {unknown}"),
        }
    }
    if options.qps == 0 || options.duration_seconds == 0 || options.concurrency == 0 {
        bail!("qps, duration, and concurrency must be nonzero");
    }
    Ok(options)
}

fn next(arguments: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    arguments
        .next()
        .with_context(|| format!("missing value for {option}"))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_percentiles_and_metric_deltas() {
        assert_eq!(percentile_millis(&[100, 200, 300, 400], 0.99), 0.4);
        assert_eq!(percent(98, 100), 98.0);
        let delta = subtract_metrics(
            ChainMetricsSnapshot {
                ingress: 10,
                cache_hits: 9,
                ..ChainMetricsSnapshot::default()
            },
            ChainMetricsSnapshot {
                ingress: 2,
                cache_hits: 1,
                ..ChainMetricsSnapshot::default()
            },
        );
        assert_eq!(delta.ingress, 8);
        assert_eq!(delta.cache_hits, 8);
    }
}
