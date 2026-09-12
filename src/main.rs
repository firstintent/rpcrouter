use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use axum::ServiceExt;
use rpcrouter::{
    admin::AdminState,
    autoenable::AutoEnableManager,
    chainlist::{ChainlistLoader, catalog_document},
    config::Config,
    forward::Forwarder,
    probe::{ProbeManager, spawn_supervised as spawn_probes},
    registry::{Registry, unix_seconds},
    server::{AppState, guarded_service_from_state},
    state::{
        FileStore, MemoryStore, RedisStore, ResilientStore, StateRuntimeSnapshot, StateStore,
        instance_id,
    },
    supervisor,
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rpcrouter=info")),
        )
        .init();

    let config_path = env::var_os("RPCROUTER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.toml"));
    let mut config = Config::load(&config_path)?;
    config.apply_env_overrides()?;
    if env::args().any(|arg| arg == "--reset-state") {
        config.state.reset = true;
    }
    if let Some(dir) = &config.admin.static_dir
        && !dir.is_dir()
    {
        tracing::warn!(path = %dir.display(), "admin static directory does not exist");
    }
    info!(path = %config_path.display(), listen = %config.listen, "configuration loaded");

    let mut resilient = None;
    let store: Arc<dyn StateStore> = match config.state.backend.as_str() {
        "memory" => Arc::new(MemoryStore::new()),
        "file" => Arc::new(FileStore::open(&config.state.file_path).await?),
        "redis" if !config.state.required => {
            let value = Arc::new(
                ResilientStore::open(
                    &config.state.redis_url,
                    &config.state.namespace,
                    config.state.health_ttl_seconds,
                    &config.state.file_path,
                )
                .await?,
            );
            resilient = Some(Arc::clone(&value));
            value
        }
        "redis" => match tokio::time::timeout(
            Duration::from_secs(2),
            RedisStore::connect_with_ttl(
                &config.state.redis_url,
                &config.state.namespace,
                config.state.health_ttl_seconds,
            ),
        )
        .await
        {
            Ok(Ok(redis)) => Arc::new(redis),
            Ok(Err(error)) => bail!("required state store unavailable: {error}"),
            Err(_) => bail!("required state store unavailable: connection timed out"),
        },
        _ => unreachable!(),
    };
    if config.state.reset {
        tracing::warn!("state reset requested at startup");
        store.reset().await.context("failed to reset state store")?;
    }
    let boot = match store.bootstrap().await {
        Ok(value) => value,
        Err(error) if !config.state.required => {
            tracing::warn!(error=%error,"state schema could not be loaded; resetting optional state");
            store.reset().await?;
            store.bootstrap().await?
        }
        Err(error) => return Err(error.context("required state bootstrap failed")),
    };
    let chainlist = Arc::new(ChainlistLoader::new(&config)?);
    let registry = Arc::new(Registry::new(&config));
    let initial = chainlist
        .load_with_store_catalog(boot.catalog.as_ref())
        .await?;
    info!(source = ?initial.source, chains = initial.catalog.chains.len(), "chainlist loaded");
    registry.set_catalog(initial.catalog).await;
    if boot.catalog.is_none()
        || matches!(initial.source, rpcrouter::chainlist::RefreshSource::Network)
    {
        let catalog = registry.catalog().await.expect("catalog loaded");
        let refresh_state = chainlist.refresh_state().await;
        store
            .set_catalog_metadata(
                &catalog_document(catalog.as_ref()),
                refresh_state.etag.as_deref(),
                unix_seconds(),
            )
            .await?;
    }
    registry.apply_overrides(&boot.overrides).await;
    registry.apply_snapshot(&initial.snapshot).await;
    if config.state.restore_hot {
        registry.activate_restored_hot(&boot.hot_chains).await;
    }
    registry.restore_health(&boot.health).await;
    let initial_is_fresh = matches!(
        initial.source,
        rpcrouter::chainlist::RefreshSource::Network
            | rpcrouter::chainlist::RefreshSource::NotModified
    );
    if initial_is_fresh {
        registry.record_chainlist_refresh(unix_seconds(), initial.source.label());
    }
    let forwarder_value = Forwarder::new(Arc::clone(&registry), &config)?;
    forwarder_value.apply_state_overrides(&boot.overrides);
    let forwarder = Arc::new(forwarder_value);
    let metrics = forwarder.metrics();
    let state_runtime = StateRuntimeSnapshot::new(
        store.backend_name(),
        config.state.namespace.clone(),
        instance_id(),
    );
    if let Some(resilient) = resilient {
        spawn_state_reconnect(
            resilient,
            Arc::clone(&registry),
            Arc::clone(&forwarder),
            Arc::clone(&metrics),
        );
    }
    spawn_state_ping(
        Arc::clone(&store),
        Arc::clone(&metrics),
        Arc::clone(&state_runtime),
    );
    let probes = Arc::new(ProbeManager::new(Arc::clone(&registry), &config)?);
    spawn_probes(Arc::clone(&probes), Arc::clone(&metrics));
    // 自动开启：恢复持久化集合并预热，然后起后台评估任务（关闭时两者都不做）。
    let auto_enable = if config.discovery.auto_enable.enabled {
        let manager = Arc::new(
            AutoEnableManager::new(Arc::clone(&registry), Arc::clone(&store), &config)?
                .with_metrics(Arc::clone(&metrics)),
        );
        manager.preheat(&boot.auto_chains).await;
        let task = Arc::clone(&manager);
        supervisor::spawn("auto-enable", Arc::clone(&metrics), move || {
            let task = Arc::clone(&task);
            async move { task.run().await }
        });
        Some(manager)
    } else {
        None
    };
    let initial_up = store.health().await;
    metrics.set_state_store_up(initial_up);
    state_runtime.write().await.up = initial_up;
    state_runtime.write().await.writable = store.writable().await;
    state_runtime.write().await.last_ping_unix = unix_seconds();
    // 启动 housekeeping 后台任务（每 30s 一次）。
    spawn_housekeeping(Arc::clone(&registry), Arc::clone(&metrics));
    metrics.record_chainlist_refresh(initial.source.label());
    if initial.rejected_network_snapshot {
        metrics.record_chainlist_refresh("rejected");
    }
    metrics.record_catalog_records_skipped(initial.records_skipped);
    spawn_chainlist_refresh(
        Arc::clone(&chainlist),
        Arc::clone(&registry),
        Arc::clone(&store),
        Duration::from_secs(config.chainlist.refresh_seconds),
        Arc::clone(&metrics),
    );
    spawn_state_flush(
        Arc::clone(&registry),
        Arc::clone(&store),
        Arc::clone(&metrics),
        Duration::from_millis(config.state.flush_interval_ms),
        Arc::clone(&state_runtime),
    );
    let per_ip = if config.server.per_ip_rate_limit.enabled {
        Some((
            config.server.per_ip_rate_limit.requests_per_second,
            config.server.per_ip_rate_limit.burst,
        ))
    } else {
        None
    };
    let admin_state = AdminState {
        registry: Arc::clone(&registry),
        forwarder: Arc::clone(&forwarder),
        metrics: Arc::clone(&metrics),
        store: Arc::clone(&store),
        chainlist: Some(Arc::clone(&chainlist)),
        probe: Some(Arc::clone(&probes)),
        config: config.clone(),
        started: std::time::Instant::now(),
        state_runtime,
        public_cache: Arc::new(tokio::sync::Mutex::new(None)),
        auto_enable: auto_enable.clone(),
    };
    let app = guarded_service_from_state(
        AppState::new(registry, forwarder, config.server.batch_limit)
            .with_admin(admin_state)
            .with_metrics_enabled(config.metrics_enabled)
            .with_hardening(
                config.server.max_body_bytes,
                config.server.max_concurrent_requests,
                per_ip,
                config.server.metrics_auth_token.clone(),
            ),
    );

    // 用 connect-info 注入客户端地址，供每 IP 限速层读取。
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    info!(listen = %config.listen, "rpcrouter listening");

    // 优雅退出：收到 SIGTERM/SIGINT 后停收新请求并排空在飞请求；
    // 排空超过 shutdown_deadline_ms 则强制退出。
    let drain_deadline = Duration::from_millis(config.server.shutdown_deadline_ms);
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = serve => result?,
        () = drain_after_signal(shutdown_signal(), drain_deadline) => {
            // 排空超时：axum 的连接是独立 tokio::spawn 任务，#[tokio::main] 在 main 返回时
            // drop runtime 会无限等待这些任务（慢客户端/挂起连接会让进程退不出去），
            // 且无法与正常退出（码 0）区分。故这里立即用非零退出码硬退出。
            error!(deadline_ms = %config.server.shutdown_deadline_ms,
                "shutdown drain deadline exceeded; forcing process exit");
            std::process::exit(forced_shutdown_exit_code());
        }
    }
    Ok(())
}

/// 强制退出（排空超时）使用的进程退出码，非零以便与正常退出（0）区分。
fn forced_shutdown_exit_code() -> i32 {
    1
}

fn spawn_chainlist_refresh(
    chainlist: Arc<ChainlistLoader>,
    registry: Arc<Registry>,
    store: Arc<dyn StateStore>,
    refresh_interval: Duration,
    metrics: Arc<rpcrouter::metrics::Metrics>,
) {
    supervisor::spawn("chainlist-refresh", metrics.clone(), move || {
        let chainlist = Arc::clone(&chainlist);
        let registry = Arc::clone(&registry);
        let store = Arc::clone(&store);
        let metrics = Arc::clone(&metrics);
        async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                match chainlist.refresh().await {
                    Ok(Some(result)) => {
                        if matches!(
                            result.source,
                            rpcrouter::chainlist::RefreshSource::Network
                                | rpcrouter::chainlist::RefreshSource::NotModified
                        ) {
                            let refresh_state = chainlist.refresh_state().await;
                            if let Err(error) = store
                                .set_catalog_metadata(
                                    &catalog_document(result.catalog.as_ref()),
                                    refresh_state.etag.as_deref(),
                                    unix_seconds(),
                                )
                                .await
                            {
                                tracing::warn!(error=%error, "state catalog update failed");
                            }
                        }
                        registry.set_catalog(result.catalog).await;
                        registry.apply_snapshot(&result.snapshot).await;
                        if matches!(
                            result.source,
                            rpcrouter::chainlist::RefreshSource::Network
                                | rpcrouter::chainlist::RefreshSource::NotModified
                        ) {
                            registry
                                .record_chainlist_refresh(unix_seconds(), result.source.label());
                        }
                        metrics.record_chainlist_refresh(result.source.label());
                        if result.rejected_network_snapshot {
                            metrics.record_chainlist_refresh("rejected");
                        }
                        metrics.record_catalog_records_skipped(result.records_skipped);
                        info!(source = ?result.source, "chainlist refresh completed");
                    }
                    Ok(None) => {
                        info!("chainlist refresh skipped because another refresh is running")
                    }
                    Err(error) => {
                        error!(error = %error, "chainlist refresh exhausted all fallbacks")
                    }
                }
            }
        }
    });
}

/// 后台 housekeeping：每 30 秒执行一次 idle 降级 + LRU 淘汰。
fn spawn_housekeeping(registry: Arc<Registry>, metrics: Arc<rpcrouter::metrics::Metrics>) {
    supervisor::spawn("housekeeping", metrics, move || {
        let registry = Arc::clone(&registry);
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.tick().await; // 跳过第一次立即触发。
            loop {
                interval.tick().await;
                registry.housekeeping().await;
            }
        }
    });
}

fn spawn_state_flush(
    registry: Arc<Registry>,
    store: Arc<dyn StateStore>,
    metrics: Arc<rpcrouter::metrics::Metrics>,
    interval: Duration,
    state_runtime: Arc<tokio::sync::RwLock<StateRuntimeSnapshot>>,
) {
    supervisor::spawn("state-flush", metrics.clone(), move || {
        let registry = Arc::clone(&registry);
        let store = Arc::clone(&store);
        let metrics = Arc::clone(&metrics);
        let state_runtime = Arc::clone(&state_runtime);
        async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let started = std::time::Instant::now();
                let result = async {
                    store
                        .set_hot_chains(&registry.hot_chain_timestamps())
                        .await?;
                    let snapshots = registry.take_dirty_health(2_000).await;
                    let result = store.flush_health(&snapshots).await;
                    if result.is_err() {
                        registry.restore_dirty_health(&snapshots).await;
                    }
                    result
                }
                .await;
                metrics.set_state_dirty_endpoints(registry.dirty_endpoint_count().await);
                metrics.record_state_flush(
                    if result.is_ok() { "success" } else { "error" },
                    started.elapsed(),
                );
                let dirty = registry.dirty_endpoint_count().await as u64;
                let mut snapshot = state_runtime.write().await;
                snapshot.last_flush_unix = unix_seconds();
                snapshot.last_flush_result =
                    if result.is_ok() { "success" } else { "error" }.to_owned();
                snapshot.last_flush_duration_ms =
                    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
                snapshot.dirty_endpoints = dirty;
                drop(snapshot);
                if let Err(error) = result {
                    tracing::warn!(error=%error,"state flush failed");
                }
            }
        }
    });
}

fn spawn_state_reconnect(
    store: Arc<ResilientStore>,
    registry: Arc<Registry>,
    forwarder: Arc<Forwarder>,
    metrics: Arc<rpcrouter::metrics::Metrics>,
) {
    supervisor::spawn("state-reconnect", metrics.clone(), move || {
        let store = Arc::clone(&store);
        let registry = Arc::clone(&registry);
        let forwarder = Arc::clone(&forwarder);
        async move {
            let mut delay = Duration::from_secs(1);
            loop {
                if store.health().await {
                    delay = Duration::from_secs(1);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                match store.reconnect().await {
                    Ok(true) => {
                        if let Ok(overrides) = store.load_overrides().await {
                            registry.apply_overrides(&overrides).await;
                            forwarder.apply_state_overrides(&overrides);
                        }
                        info!("state store reconnected and Redis overrides applied");
                        delay = Duration::from_secs(1);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(error=%error,"state store reconnect failed");
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
    });
}

fn spawn_state_ping(
    store: Arc<dyn StateStore>,
    metrics: Arc<rpcrouter::metrics::Metrics>,
    state_runtime: Arc<tokio::sync::RwLock<StateRuntimeSnapshot>>,
) {
    supervisor::spawn("state-ping", metrics.clone(), move || {
        let store = Arc::clone(&store);
        let metrics = Arc::clone(&metrics);
        let state_runtime = Arc::clone(&state_runtime);
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let up = store.health().await;
                metrics.set_state_store_up(up);
                let mut snapshot = state_runtime.write().await;
                snapshot.up = up;
                snapshot.writable = store.writable().await;
                snapshot.last_ping_unix = unix_seconds();
            }
        }
    });
}

/// 等待 SIGTERM 或 SIGINT（Ctrl-C）。任一信号到达即返回。
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            error!(error = %error, "failed to install shutdown signal handler");
        }
    }
    info!("shutdown signal received");
}

/// 收到退出信号后启动排空 deadline 计时：信号一到即开始倒计时，
/// 到期返回，触发主循环强制退出。
///
/// `signal` 抽象成参数以便逻辑层测试：生产传 `shutdown_signal()`，测试传已触发的 future。
async fn drain_after_signal(
    signal: impl std::future::Future<Output = ()> + Send,
    deadline: Duration,
) {
    signal.await;
    info!(deadline = ?deadline, "graceful shutdown started; draining in-flight requests");
    tokio::time::sleep(deadline).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_shutdown_uses_nonzero_exit_code() {
        // 强退必须非零，正常退出（Ok(())）隐式为 0，二者据此区分（供进程管理器/脚本判断）。
        assert_ne!(forced_shutdown_exit_code(), 0);
    }

    #[test]
    fn drain_after_signal_waits_for_deadline() {
        // 逻辑层验证 drain_after_signal：信号一触发即开始倒计时，至少等到 deadline 才返回。
        let start = tokio::time::Instant::now();
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("runtime")
            .block_on(async {
                // 传一个已触发的信号 future（async {} 立即完成），模拟 SIGTERM 已到达。
                drain_after_signal(async {}, Duration::from_millis(80)).await;
            });
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "drain deadline window must be respected"
        );
    }
}
