use std::{sync::Arc, time::Duration};

use rpcrouter::{
    chainlist::{ChainEndpoints, ChainlistSnapshot},
    config::{Config, ProbeConfig, UpstreamConfig},
    forward::Forwarder,
    mock_upstream::{MockBehavior, MockController, router as mock_router},
    probe::{ProbeManager, ProbeOutcome},
    registry::{Endpoint, EndpointState, Registry},
    signals::{FailureSignal, FaultKind},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinSet, time::Instant};

async fn spawn_mock(behavior: MockBehavior) -> (String, MockController) {
    let controller = MockController::new(behavior);
    let app = mock_router(controller.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock upstream");
    let address = listener.local_addr().expect("mock address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve mock upstream");
    });
    (format!("http://{address}/"), controller)
}

fn phase2_config() -> Config {
    Config {
        chains: vec![1],
        chain_overrides: Vec::new(),
        upstream: UpstreamConfig {
            request_timeout_ms: 500,
            slow_threshold_ms: 400,
            deadline_ms: 2_000,
            max_attempts: 4,
            default_rps: 100,
            default_concurrency: 64,
        },
        probe: ProbeConfig {
            request_timeout_ms: 500,
            ..ProbeConfig::default()
        },
        ..Config::default()
    }
}

async fn setup_registry(config: &Config, urls: &[String]) -> Arc<Registry> {
    let registry = Arc::new(Registry::new(config));
    registry
        .apply_snapshot(&ChainlistSnapshot {
            chains: vec![ChainEndpoints {
                chain_id: 1,
                name: "Integration Test".to_owned(),
                endpoints: urls.to_vec(),
            }],
        })
        .await;
    registry
}

fn activate(endpoint: &Endpoint, latency: Duration) {
    let now = Instant::now();
    endpoint.record_success(now, latency, true);
    endpoint.record_success(now, latency, true);
    assert_eq!(endpoint.state(now), EndpointState::Active);
}

fn request(id: u64) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "method":"eth_blockNumber", "params":[]})
}

fn uncached_request(id: u64) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "method":"phase2_uncached", "params":[]})
}

#[tokio::test]
async fn cold_start_probation_endpoint_serves_immediately() {
    let (url, upstream) = spawn_mock(MockBehavior::default()).await;
    let config = phase2_config();
    let registry = setup_registry(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("cold endpoint");
    assert_eq!(
        endpoint.state(Instant::now()),
        EndpointState::Probation { passes: 0 }
    );
    let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");

    let response = forwarder.execute(1, uncached_request(1)).await;

    assert_eq!(response["result"], "0x1");
    assert_eq!(upstream.request_count(), 1);
    assert_eq!(registry.user_visible_errors(), 0);
    assert_eq!(
        endpoint.state(Instant::now()),
        EndpointState::Probation { passes: 1 }
    );
}

#[tokio::test]
async fn cooling_and_empty_pools_still_return_gateway_error() {
    let (url, upstream) = spawn_mock(MockBehavior::default()).await;
    let config = phase2_config();
    let cooling_registry = setup_registry(&config, std::slice::from_ref(&url)).await;
    let endpoint = cooling_registry
        .endpoint(1, &url)
        .await
        .expect("cooling endpoint");
    endpoint.record_failure(Instant::now(), FailureSignal::new(FaultKind::RateLimited));
    assert!(matches!(
        endpoint.state(Instant::now()),
        EndpointState::Cooling { .. }
    ));
    let cooling_forwarder =
        Forwarder::new(Arc::clone(&cooling_registry), &config).expect("cooling forwarder");

    let cooling_response = cooling_forwarder.execute(1, uncached_request(2)).await;
    assert_eq!(cooling_response["error"]["code"], -32000);
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(cooling_response["error"]["data"]["reason"], "cold_start");
    assert_eq!(cooling_registry.user_visible_errors(), 0);
    let cooling_metrics = cooling_forwarder
        .metrics()
        .encode(&cooling_registry)
        .await
        .expect("metrics");
    assert!(cooling_metrics.contains("rpcrouter_cold_start_failures_total{chain_id=\"1\"} 1"));

    let empty_registry = setup_registry(&config, &[]).await;
    let empty_forwarder =
        Forwarder::new(Arc::clone(&empty_registry), &config).expect("empty forwarder");
    let empty_response = empty_forwarder.execute(1, uncached_request(3)).await;
    assert_eq!(empty_response["error"]["code"], -32000);
    assert_eq!(empty_registry.user_visible_errors(), 0);
}

#[tokio::test]
async fn active_pool_failure_is_user_visible_not_cold_start() {
    let (url, _upstream) = spawn_mock(MockBehavior {
        status_5xx: Some(503),
        ..MockBehavior::default()
    })
    .await;
    let config = phase2_config();
    let registry = setup_registry(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("endpoint");
    activate(&endpoint, Duration::from_millis(1));
    let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");
    let response = forwarder.execute(1, uncached_request(4)).await;
    assert_eq!(response["error"]["code"], -32000);
    assert!(response["error"].get("data").is_none());
    assert_eq!(registry.user_visible_errors(), 1);
    let metrics = forwarder
        .metrics()
        .encode(&registry)
        .await
        .expect("metrics");
    assert!(metrics.contains("rpcrouter_user_visible_errors_total{chain_id=\"1\"} 1"));
    assert!(!metrics.contains("rpcrouter_cold_start_failures_total{chain_id=\"1\"} 1"));
}

#[tokio::test]
async fn persistent_429_cools_stops_traffic_and_recovers_via_probe() {
    let (url, controller) = spawn_mock(MockBehavior {
        rate_limit_after: Some(0),
        retry_after_seconds: Some(45),
        ..MockBehavior::default()
    })
    .await;
    let config = phase2_config();
    let registry = setup_registry(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("limited endpoint");
    activate(&endpoint, Duration::from_millis(1));
    let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");

    let before_first = Instant::now();
    assert_eq!(
        forwarder.execute(1, uncached_request(1)).await["error"]["code"],
        -32000
    );
    let EndpointState::Cooling {
        until: first_until,
        strikes: first_strikes,
    } = endpoint.state(Instant::now())
    else {
        panic!("429 endpoint must cool");
    };
    assert_eq!(first_strikes, 1);
    let first_delay = first_until.saturating_duration_since(before_first);
    assert!(first_delay >= Duration::from_secs(45));
    assert!(first_delay < Duration::from_secs(46));

    let calls_while_cooling = controller.request_count();
    let _ = forwarder.execute(1, uncached_request(2)).await;
    assert_eq!(controller.request_count(), calls_while_cooling);

    controller.set_rate_limit_after(None);
    let probes = ProbeManager::new(Arc::clone(&registry), &config).expect("probe manager");
    assert_eq!(
        probes
            .probe_endpoint_at(1, Arc::clone(&endpoint), first_until)
            .await,
        ProbeOutcome::Passed
    );
    assert_eq!(
        endpoint.state(first_until),
        EndpointState::Probation { passes: 1 }
    );
    let second_probe_at = first_until + Duration::from_millis(10);
    assert_eq!(
        probes
            .probe_endpoint_at(1, Arc::clone(&endpoint), second_probe_at)
            .await,
        ProbeOutcome::Passed
    );
    assert_eq!(endpoint.state(second_probe_at), EndpointState::Active);
    assert!(
        forwarder
            .execute(1, uncached_request(3))
            .await
            .get("result")
            .is_some()
    );

    controller.set_rate_limit_after(Some(0));
    let before_second = Instant::now();
    let _ = forwarder.execute(1, uncached_request(4)).await;
    let EndpointState::Cooling {
        until: second_until,
        strikes: second_strikes,
    } = endpoint.state(Instant::now())
    else {
        panic!("second 429 must cool endpoint");
    };
    assert_eq!(second_strikes, 2);
    let second_delay = second_until.saturating_duration_since(before_second);
    assert!(second_delay >= Duration::from_secs(60));
    assert!(second_delay < Duration::from_secs(61));
}

#[tokio::test]
async fn rate_limit_storm_is_transparent_while_healthy_endpoint_exists() {
    let (limited_url, limited) = spawn_mock(MockBehavior {
        rate_limit_after: Some(0),
        ..MockBehavior::default()
    })
    .await;
    let (healthy_url, healthy) = spawn_mock(MockBehavior::default()).await;
    let config = phase2_config();
    let registry = setup_registry(&config, &[limited_url.clone(), healthy_url.clone()]).await;
    let limited_endpoint = registry
        .endpoint(1, &limited_url)
        .await
        .expect("limited endpoint");
    let healthy_endpoint = registry
        .endpoint(1, &healthy_url)
        .await
        .expect("healthy endpoint");
    activate(&limited_endpoint, Duration::from_millis(1));
    activate(&healthy_endpoint, Duration::from_millis(50));
    let forwarder =
        Arc::new(Forwarder::new(Arc::clone(&registry), &config).expect("storm forwarder"));

    let mut tasks = JoinSet::new();
    for id in 0..32 {
        let forwarder = Arc::clone(&forwarder);
        tasks.spawn(async move { forwarder.execute(1, request(id)).await });
    }
    while let Some(result) = tasks.join_next().await {
        let response = result.expect("request task");
        assert!(response.get("result").is_some(), "{response}");
        assert!(response.get("error").is_none(), "{response}");
    }

    assert!(limited.request_count() > 0);
    assert!(healthy.request_count() > 0);
    assert!(matches!(
        limited_endpoint.state(Instant::now()),
        EndpointState::Cooling { .. }
    ));
    assert_eq!(registry.user_visible_errors(), 0);
}

#[tokio::test]
async fn execution_reverted_passes_through_without_failover_or_penalty() {
    let (revert_url, reverted) = spawn_mock(MockBehavior {
        execution_reverted: true,
        ..MockBehavior::default()
    })
    .await;
    let (healthy_url, healthy) = spawn_mock(MockBehavior::default()).await;
    let config = phase2_config();
    let registry = setup_registry(&config, &[revert_url.clone(), healthy_url.clone()]).await;
    let revert_endpoint = registry
        .endpoint(1, &revert_url)
        .await
        .expect("revert endpoint");
    let healthy_endpoint = registry
        .endpoint(1, &healthy_url)
        .await
        .expect("healthy endpoint");
    activate(&revert_endpoint, Duration::from_millis(1));
    activate(&healthy_endpoint, Duration::from_millis(50));
    let forwarder = Forwarder::new(Arc::clone(&registry), &config).expect("forwarder");

    let response = forwarder
        .execute(
            1,
            json!({"jsonrpc":"2.0", "id":7, "method":"eth_call", "params":[]}),
        )
        .await;
    assert_eq!(response["error"]["code"], 3);
    assert_eq!(
        response["error"]["message"],
        "execution reverted: mock rejection"
    );
    assert_eq!(reverted.request_count(), 1);
    assert_eq!(healthy.request_count(), 0);
    assert_eq!(revert_endpoint.stats().failures, 0);
    assert_eq!(revert_endpoint.state(Instant::now()), EndpointState::Active);
}

#[tokio::test]
async fn wrong_chain_id_probe_removes_endpoint_from_pool() {
    let (url, _) = spawn_mock(MockBehavior {
        chain_id: 143,
        ..MockBehavior::default()
    })
    .await;
    let config = phase2_config();
    let registry = setup_registry(&config, std::slice::from_ref(&url)).await;
    let endpoint = registry.endpoint(1, &url).await.expect("wrong endpoint");
    let probes = ProbeManager::new(Arc::clone(&registry), &config).expect("probe manager");
    assert_eq!(
        probes.probe_endpoint(1, endpoint).await,
        ProbeOutcome::RemovedWrongChain { actual: 143 }
    );
    assert!(registry.all_endpoints(1).await.is_empty());
    assert!(registry.candidates(1).await.is_empty());
}
