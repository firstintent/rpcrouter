//! W6b 管理面：只读观测与显式鉴权的运行时控制。
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{
        HeaderMap, Method, StatusCode, Uri,
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower_http::cors::CorsLayer;
use tracing::warn;

use crate::{
    autoenable::AutoEnableManager,
    chainlist::{ChainlistLoader, catalog_document},
    config::Config,
    forward::Forwarder,
    metrics::Metrics,
    probe::ProbeManager,
    registry::{EndpointState, Registry},
    state::{
        ChainOverrideState, EndpointOverrideState, Overrides, StateExport, StateRuntimeSnapshot,
        StateStore, endpoint_key,
    },
};

#[derive(Clone)]
pub struct AdminState {
    pub registry: Arc<Registry>,
    pub forwarder: Arc<Forwarder>,
    pub metrics: Arc<Metrics>,
    pub store: Arc<dyn StateStore>,
    pub chainlist: Option<Arc<ChainlistLoader>>,
    pub probe: Option<Arc<ProbeManager>>,
    pub config: Config,
    pub started: Instant,
    pub state_runtime: Arc<tokio::sync::RwLock<StateRuntimeSnapshot>>,
    pub public_cache: Arc<tokio::sync::Mutex<Option<PublicCache>>>,
    pub auto_enable: Option<Arc<AutoEnableManager>>,
}

#[derive(Clone)]
pub struct PublicCache {
    pub built_at: Instant,
    pub overview: Value,
    pub rows: Arc<Vec<PublicChainRow>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct ChainQuery {
    pub state: Option<String>,
    /// 公共接口用：`all` 返回全目录，缺省只返回已开启（available）的链。
    pub scope: Option<String>,
    pub q: Option<String>,
    pub testnet: Option<bool>,
    pub sort: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
pub struct CacheClear {
    pub chain_id: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct SettingsPatch {
    #[serde(default, deserialize_with = "double_option::deserialize")]
    pub block_time_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "double_option::deserialize")]
    pub confirmation_depth: Option<Option<u64>>,
    #[serde(default, deserialize_with = "double_option::deserialize")]
    pub tip_ttl_ms: Option<Option<u64>>,
    #[serde(default, deserialize_with = "double_option::deserialize")]
    pub max_block_lag: Option<Option<u64>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct EndpointAction {
    pub url: String,
    pub seconds: Option<u64>,
    pub rps: Option<u32>,
    pub concurrency: Option<usize>,
}

mod double_option {
    use serde::{Deserialize, Deserializer};
    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Some(Option::<T>::deserialize(deserializer)?))
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfirmReset {
    pub confirm: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointRow {
    pub url: String,
    pub tracking: Option<String>,
    pub state: String,
    pub strikes: u32,
    pub cooling_until_unix: Option<u64>,
    pub latency_ewma_ms: f64,
    pub lag: u64,
    pub rps: u32,
    pub concurrency: usize,
    pub disabled: bool,
    pub source: String,
    pub last_fault: Option<String>,
    pub stats: crate::registry::EndpointStatsSnapshot,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainRow {
    pub chain_id: u64,
    pub name: String,
    pub short_name: Option<String>,
    pub is_testnet: bool,
    pub status: Option<String>,
    pub state: String,
    pub pinned: bool,
    pub disabled: bool,
    pub pin_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_candidate: Option<crate::autoenable::CandidateProgress>,
    pub catalog_endpoints: usize,
    pub endpoints: usize,
    pub active: usize,
    pub cooling: usize,
    pub probation: usize,
    pub head: u64,
    pub last_ingress_unix: u64,
    pub ingress_total: u64,
    pub cache_hits_total: u64,
    pub cache_lookups_total: u64,
    pub upstream_total: u64,
    pub user_visible_errors_total: u64,
    pub settings: Value,
    #[serde(rename = "endpointRows", skip_serializing_if = "Option::is_none")]
    pub endpoint_rows: Option<Vec<EndpointRow>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicChainRow {
    pub chain_id: u64,
    pub name: String,
    pub short_name: Option<String>,
    pub is_testnet: bool,
    pub native_symbol: Option<String>,
    pub explorer_url: Option<String>,
    pub status: Option<String>,
    pub state: String,
    pub catalog_endpoints: usize,
    pub endpoints: usize,
    pub active: usize,
    pub head: u64,
    pub ingress_total: u64,
    pub cache_hits_total: u64,
    pub cache_lookups_total: u64,
}

/// 默认排序键：生命周期优先（pinned > hot > dormant > disabled）→ 有活跃端点 / 有端点者优先
/// → 主网优先于测试网 → 流量降序 → chainId 升序兜底。
fn priority_key(r: &ChainRow) -> (u8, bool, bool, bool, std::cmp::Reverse<u64>, u64) {
    priority_values(
        &r.state,
        r.active,
        r.endpoints,
        r.is_testnet,
        r.ingress_total,
        r.chain_id,
    )
}

fn public_priority_key(r: &PublicChainRow) -> (u8, bool, bool, bool, std::cmp::Reverse<u64>, u64) {
    priority_values(
        &r.state,
        r.active,
        r.endpoints,
        r.is_testnet,
        r.ingress_total,
        r.chain_id,
    )
}

fn priority_values(
    state: &str,
    active: usize,
    endpoints: usize,
    is_testnet: bool,
    ingress_total: u64,
    chain_id: u64,
) -> (u8, bool, bool, bool, std::cmp::Reverse<u64>, u64) {
    let rank = match state {
        "pinned" => 0,
        "hot" => 1,
        "dormant" => 2,
        "disabled" => 3,
        _ => 4,
    };
    (
        rank,
        active == 0,
        endpoints == 0,
        is_testnet,
        std::cmp::Reverse(ingress_total),
        chain_id,
    )
}

fn err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message.into()}})),
    )
        .into_response()
}

#[allow(clippy::result_large_err)]
fn auth(headers: &HeaderMap, method: &Method, token: Option<&str>) -> Result<(), Response> {
    if let Some(token) = token {
        let expected = format!("Bearer {token}");
        if headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) != Some(expected.as_str()) {
            return Err(err(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "Bearer token is required",
            ));
        }
    } else if *method != Method::GET && *method != Method::HEAD {
        return Err(err(
            StatusCode::FORBIDDEN,
            "admin_disabled",
            "write operations require admin.auth_token",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn valid_endpoint_url(raw: &str, allow_private: bool) -> Result<String, Response> {
    if raw.contains("${") {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "endpoint URL contains a template",
        ));
    }
    let mut url = reqwest::Url::parse(raw).map_err(|_| {
        err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "endpoint URL is invalid",
        )
    })?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "endpoint URL must be HTTPS without userinfo",
        ));
    }
    if !allow_private {
        let host = url.host_str().unwrap_or_default();
        if host.eq_ignore_ascii_case("localhost") {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "private endpoint is not allowed",
            ));
        }
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            let private = match ip {
                std::net::IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
                std::net::IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local(),
            };
            if ip.is_loopback() || private {
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    "invalid_argument",
                    "private endpoint is not allowed",
                ));
            }
        }
    }
    url.set_fragment(None);
    Ok(url.to_string())
}

pub fn router(state: AdminState) -> Router {
    let enabled = state.config.admin.enabled;
    let mut router = Router::new()
        .route("/admin/api/overview", get(overview))
        .route("/admin/api/chains", get(chains))
        .route("/admin/api/chains/{id}", get(chain_detail))
        .route("/admin/api/overrides", get(overrides))
        .route("/admin/api/state", get(state_info))
        .route("/admin/api/state/export", get(state_export))
        .route("/admin/api/chainlist/refresh", post(chainlist_refresh))
        .route("/admin/api/cache/clear", post(cache_clear))
        .route("/admin/api/chains/{id}/settings", put(chain_settings))
        .route(
            "/admin/api/chains/{id}/endpoints/{action}",
            post(endpoint_action),
        )
        .route("/admin/api/chains/{id}/{action}", post(chain_action))
        .route(
            "/admin/api/state/import",
            post(state_import).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
        .route("/admin/api/state/reset", post(state_reset))
        .route("/dashboard", get(static_file))
        .route("/dashboard/", get(static_file))
        .route("/dashboard/{*path}", get(static_file));
    if state.config.admin.public_site {
        router = router
            .route("/api/public/overview", get(public_overview))
            .route("/api/public/chains", get(public_chains))
            .route("/api/public/chains/{id}", get(public_chain_detail))
            .route("/", get(public_index))
            .route("/chain/{id}", get(public_index))
            .route("/chain/{id}/", get(public_index));
    }
    if !state.config.admin.cors_allow_origins.is_empty() {
        let origins = state
            .config
            .admin
            .cors_allow_origins
            .iter()
            .filter_map(|origin| origin.parse::<axum::http::HeaderValue>().ok())
            .collect::<Vec<_>>();
        let layer = CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::list(origins))
            .allow_headers([AUTHORIZATION, CONTENT_TYPE])
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ]);
        router = router.layer(layer);
    }
    if enabled {
        router.with_state(state)
    } else {
        Router::new().with_state(state)
    }
}

fn public_json(value: Value) -> Response {
    let mut response = Json(value).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        axum::http::HeaderValue::from_static("public, max-age=5"),
    );
    response
}

async fn public_index(State(s): State<AdminState>, uri: Uri) -> Response {
    let path = uri.path();
    if path != "/"
        && !path.strip_prefix("/chain/").is_some_and(|rest| {
            let rest = rest.trim_end_matches('/');
            !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
        })
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(dir) = &s.config.admin.static_dir else {
        return StatusCode::NOT_FOUND.into_response();
    };
    read_static_file(dir, "index.html").await
}

async fn public_overview(State(s): State<AdminState>) -> Response {
    let (overview, _) = public_snapshot(&s).await;
    public_json(overview)
}

async fn public_chains(State(s): State<AdminState>, Query(query): Query<ChainQuery>) -> Response {
    if query.q.as_deref().is_some_and(|q| q.chars().count() > 64) {
        return err(StatusCode::BAD_REQUEST, "invalid_argument", "q is too long");
    }
    let (_, cached_rows) = public_snapshot(&s).await;
    // 缺省只列已开启的链；搜索或 scope=all 时才展开全目录（disabled 链始终不可见）。
    let full_scope = query.scope.as_deref() == Some("all")
        || query.q.as_deref().is_some_and(|q| !q.trim().is_empty());
    let mut rows = cached_rows
        .iter()
        .filter(|r| full_scope || r.state == "available")
        .cloned()
        .collect::<Vec<_>>();
    if let Some(state) = query.state.as_deref().filter(|x| *x != "all") {
        rows.retain(|r| r.state.eq_ignore_ascii_case(state));
    }
    if let Some(testnet) = query.testnet {
        rows.retain(|r| r.is_testnet == testnet);
    }
    if let Some(q) = query.q.as_deref() {
        let q = q.to_ascii_lowercase();
        rows.retain(|r| {
            r.chain_id.to_string().contains(&q)
                || r.name.to_ascii_lowercase().contains(&q)
                || r.short_name
                    .as_deref()
                    .is_some_and(|x| x.to_ascii_lowercase().contains(&q))
        });
    }
    match query.sort.as_deref() {
        Some("name") => rows.sort_by_key(|r| r.name.clone()),
        Some("traffic") => rows.sort_by_key(|r| std::cmp::Reverse(r.ingress_total)),
        Some("chainId") => rows.sort_by_key(|r| r.chain_id),
        _ => rows.sort_by_key(public_priority_key),
    }
    let total = rows.len();
    let offset = query.offset.unwrap_or(0).min(total);
    let limit = query.limit.unwrap_or(100).min(200);
    let items = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    public_json(json!({"total":total,"items":items}))
}

async fn public_chain_detail(State(s): State<AdminState>, Path(id): Path<u64>) -> Response {
    let (_, rows) = public_snapshot(&s).await;
    let Some(row) = rows.iter().find(|row| row.chain_id == id).cloned() else {
        return err(StatusCode::NOT_FOUND, "not_found", "unknown chain");
    };
    public_json(serde_json::to_value(row).unwrap_or_else(|_| json!({})))
}

async fn public_snapshot(s: &AdminState) -> (Value, Arc<Vec<PublicChainRow>>) {
    let mut cache = s.public_cache.lock().await;
    if let Some(snapshot) = cache
        .as_ref()
        .filter(|x| x.built_at.elapsed() < Duration::from_secs(5))
    {
        return (snapshot.overview.clone(), snapshot.rows.clone());
    }
    let metrics = s.metrics.chain_snapshots();
    let rows = build_rows_with_metrics(s, None, &metrics).await;
    let catalog = s.registry.catalog().await;
    // disabled 链对外完全不可见：在快照阶段就剔除，后续过滤无需再判断。
    let public_rows = Arc::new(
        rows.iter()
            .filter(|row| row.state != "disabled")
            .map(|row| public_row(row, catalog.as_deref()))
            .collect::<Vec<_>>(),
    );
    let counts = s.registry.chain_counts().await;
    let summaries = s.registry.summaries().await;
    let traffic = summaries.iter().fold(
        crate::metrics::ChainMetricsSnapshot::default(),
        |mut total, summary| {
            let current = metrics.get(&summary.chain_id).copied().unwrap_or_default();
            total.ingress += current.ingress;
            total.cache_hits += current.cache_hits;
            total.cache_lookups += current.cache_lookups;
            total.upstream += current.upstream;
            total
        },
    );
    let overview = json!({
        "process":{"version":env!("CARGO_PKG_VERSION"),"uptimeSeconds":s.started.elapsed().as_secs()},
        "chains":{"catalog":public_rows.len(),"available":public_rows.iter().filter(|r| r.state == "available").count(),"pinned":counts.pinned,"hot":counts.hot,"dormant":counts.dormant,"disabled":counts.disabled,"serving":public_rows.iter().filter(|r| r.state == "available").count()},
        "endpoints":{"materialized":summaries.iter().map(|x| x.endpoints).sum::<usize>(),"active":summaries.iter().map(|x| x.active).sum::<usize>()},
        "traffic":{"ingressTotal":traffic.ingress,"cacheHitsTotal":traffic.cache_hits,"cacheLookupsTotal":traffic.cache_lookups,"upstreamTotal":traffic.upstream},
        "rpc":{"pathTemplate":"/rpc/{chainId}"}
    });
    *cache = Some(PublicCache {
        built_at: Instant::now(),
        overview: overview.clone(),
        rows: Arc::clone(&public_rows),
    });
    (overview, public_rows)
}

/// 对外只暴露两档状态：有活跃端点的已开启链是 available，其余是 unverified。
/// 内部生命周期词（pinned/hot/dormant）不出现在公共接口里。
fn public_state(row: &ChainRow) -> &'static str {
    if matches!(row.state.as_str(), "pinned" | "hot") && row.active > 0 {
        "available"
    } else {
        "unverified"
    }
}

fn public_row(row: &ChainRow, catalog: Option<&crate::chainlist::Catalog>) -> PublicChainRow {
    let chain = catalog.and_then(|c| c.lookup(row.chain_id));
    PublicChainRow {
        chain_id: row.chain_id,
        name: row.name.clone(),
        short_name: row.short_name.clone(),
        is_testnet: row.is_testnet,
        native_symbol: chain.and_then(|x| x.native_symbol.clone()),
        explorer_url: chain.and_then(|x| {
            x.explorer_url.as_deref().and_then(|url| {
                let parsed = reqwest::Url::parse(url).ok()?;
                matches!(parsed.scheme(), "http" | "https").then(|| url.to_owned())
            })
        }),
        status: row.status.clone(),
        state: public_state(row).to_owned(),
        catalog_endpoints: row.catalog_endpoints,
        endpoints: row.endpoints,
        active: row.active,
        head: row.head,
        ingress_total: row.ingress_total,
        cache_hits_total: row.cache_hits_total,
        cache_lookups_total: row.cache_lookups_total,
    }
}

async fn overview(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let counts = s.registry.chain_counts().await;
    let summaries = s.registry.summaries().await;
    let mut active = 0;
    let mut cooling = 0;
    let mut probation = 0;
    for x in &summaries {
        active += x.active;
        cooling += x.cooling;
        probation += x.probation;
    }
    let rs = match s.chainlist.as_ref() {
        Some(loader) => Some(loader.refresh_state().await),
        None => None,
    };
    let rs = rs.unwrap_or_else(|| crate::chainlist::RefreshState {
        source: crate::chainlist::RefreshSource::Fixture,
        last_refresh_unix: 0,
        etag: None,
        catalog_chains: s.registry.catalog_chain_count() as usize,
        catalog_endpoints: s.registry.catalog_endpoint_count() as usize,
        last_error: None,
        refreshing: false,
    });
    let total = s.registry.catalog().await.map_or(0, |c| c.chains.len());
    let metric_snapshots = s.metrics.chain_snapshots();
    let traffic = summaries
        .iter()
        .map(|x| {
            metric_snapshots
                .get(&x.chain_id)
                .copied()
                .unwrap_or_default()
        })
        .fold(
            crate::metrics::ChainMetricsSnapshot::default(),
            |mut a, b| {
                a.ingress += b.ingress;
                a.cache_hits += b.cache_hits;
                a.cache_lookups += b.cache_lookups;
                a.coalesced += b.coalesced;
                a.upstream += b.upstream;
                a.user_visible_errors += b.user_visible_errors;
                a.hedges += b.hedges;
                a
            },
        );
    let runtime = s.registry.runtime_overrides();
    let auto = auto_enable_json(&s).await;
    Json(json!({"autoEnable":auto,"process":{"version":env!("CARGO_PKG_VERSION"),"uptimeSeconds":s.started.elapsed().as_secs()},"chainlist":{"source":rs.source.label(),"lastRefreshUnix":rs.last_refresh_unix,"etag":rs.etag,"catalogChains":rs.catalog_chains,"catalogEndpoints":rs.catalog_endpoints,"refreshSeconds":s.config.chainlist.refresh_seconds,"lastError":rs.last_error,"refreshing":rs.refreshing},"chains":{"catalog":total,"pinned":counts.pinned,"hot":counts.hot,"dormant":counts.dormant,"disabled":counts.disabled},"endpoints":{"materialized":summaries.iter().map(|x|x.endpoints).sum::<usize>(),"active":active,"cooling":cooling,"probation":probation},"traffic":{"ingressTotal":traffic.ingress,"cacheHitsTotal":traffic.cache_hits,"cacheLookupsTotal":traffic.cache_lookups,"coalescedTotal":traffic.coalesced,"upstreamTotal":traffic.upstream,"userVisibleErrorsTotal":traffic.user_visible_errors,"ingressRejectedTotal":s.metrics.ingress_rejected_total(),"hedgesTotal":traffic.hedges,"inFlight":s.metrics.in_flight()},"state":{"backend":s.store.backend_name(),"overrides":runtime.chains.len()+runtime.endpoints.len()},"probe":{"queueDepth":s.registry.probe_queue_depth.load(std::sync::atomic::Ordering::Relaxed),"inFlight":s.registry.probe_in_flight.load(std::sync::atomic::Ordering::Relaxed),"maxConcurrency":s.config.probe.max_concurrency},"cache":{"entries":s.forwarder.cache().entry_count(),"weightedBytes":s.forwarder.cache().weighted_size(),"maxBytes":s.config.cache.max_bytes},"total":total})).into_response()
}

/// 自动开启状态块：关闭时 enabled=false，其余计数为 0。
async fn auto_enable_json(s: &AdminState) -> Value {
    match s.auto_enable.as_ref() {
        Some(manager) => {
            let status = manager.status().await;
            json!({
                "enabled": true,
                "chains": status.chains,
                "candidates": status.candidates,
                "pending": status.pending,
                "capped": status.capped,
                "lastScanAt": status.last_scan_at,
                "promotionsTotal": status.promotions_total,
            })
        }
        None => json!({
            "enabled": false,
            "chains": 0,
            "candidates": 0,
            "pending": 0,
            "capped": false,
            "lastScanAt": 0,
            "promotionsTotal": 0,
        }),
    }
}

async fn chains(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Query(query): Query<ChainQuery>,
) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let mut rows = build_rows(&s, None).await;
    if let Some(state) = query.state.as_deref().filter(|x| *x != "all") {
        rows.retain(|r| r.state.eq_ignore_ascii_case(state));
    }
    if let Some(testnet) = query.testnet {
        rows.retain(|r| r.is_testnet == testnet);
    }
    if let Some(q) = query.q.as_deref() {
        let q = q.to_ascii_lowercase();
        rows.retain(|r| {
            r.chain_id.to_string().contains(&q)
                || r.name.to_ascii_lowercase().contains(&q)
                || r.short_name
                    .as_deref()
                    .is_some_and(|x| x.to_ascii_lowercase().contains(&q))
        });
    }
    match query.sort.as_deref() {
        Some("name") => rows.sort_by_key(|r| r.name.clone()),
        Some("traffic") => rows.sort_by_key(|r| std::cmp::Reverse(r.ingress_total)),
        Some("chainId") => rows.sort_by_key(|r| r.chain_id),
        _ => rows.sort_by_key(priority_key),
    }
    let total = rows.len();
    let offset = query.offset.unwrap_or(0).min(total);
    let limit = query.limit.unwrap_or(100).min(200);
    let items = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Json(json!({"total":total,"items":items})).into_response()
}

async fn chain_detail(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "not_found", "unknown chain");
    };
    let mut rows = build_rows(&s, Some(id)).await;
    rows.pop().map_or_else(
        || err(StatusCode::NOT_FOUND, "not_found", "unknown chain"),
        |r| Json(r).into_response(),
    )
}

async fn overrides(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    Json(s.registry.runtime_overrides()).into_response()
}
async fn state_info(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    let snapshot = s.state_runtime.read().await.clone();
    Json(json!({
        "backend": snapshot.backend,
        "namespace": snapshot.namespace,
        "instanceId": snapshot.instance_id,
        "up": snapshot.up,
        "writable": snapshot.writable,
        "schemaVersion": snapshot.schema_version,
        "dirtyEndpoints": snapshot.dirty_endpoints,
        "lastFlushUnix": snapshot.last_flush_unix,
        "lastFlushResult": snapshot.last_flush_result,
        "lastFlushDurationMs": snapshot.last_flush_duration_ms,
        "lastPingUnix": snapshot.last_ping_unix,
    }))
    .into_response()
}
async fn state_export(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(&headers, &Method::GET, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    match s.store.export().await {
        Ok(x) => Json(x).into_response(),
        Err(e) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        ),
    }
}

async fn chainlist_refresh(State(s): State<AdminState>, headers: HeaderMap) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    let Some(loader) = &s.chainlist else {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_found",
            "chainlist loader unavailable",
        );
    };
    match loader.refresh().await {
        Ok(Some(x)) => {
            let chain_count = x.catalog.chains.len();
            let refresh = loader.refresh_state().await;
            if let Err(error) = s
                .store
                .set_catalog_metadata(
                    &catalog_document(x.catalog.as_ref()),
                    refresh.etag.as_deref(),
                    crate::registry::unix_seconds(),
                )
                .await
            {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            s.registry.set_catalog(x.catalog).await;
            s.registry.apply_snapshot(&x.snapshot).await;
            *s.public_cache.lock().await = None;
            Json(json!({"source":x.source.label(),"chains":chain_count})).into_response()
        }
        Ok(None) => err(
            StatusCode::CONFLICT,
            "conflict",
            "chainlist refresh is already running",
        ),
        Err(e) => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        ),
    }
}
async fn cache_clear(
    State(s): State<AdminState>,
    headers: HeaderMap,
    body: Result<Json<CacheClear>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(value) => value,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "invalid JSON body",
            );
        }
    };
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if let Some(id) = body.chain_id {
        s.forwarder.cache().clear_chain(id).await
    } else {
        s.forwarder.cache().clear().await
    };
    audit(&s, "cache.clear", "all").await;
    Json(json!({"ok":true})).into_response()
}

async fn chain_action(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path((id, action)): Path<(u64, String)>,
) -> Response {
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if !["activate", "demote", "pin", "unpin", "enable", "disable"].contains(&action.as_str()) {
        return err(StatusCode::NOT_FOUND, "not_found", "unknown chain action");
    };
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "unknown_chain", "unknown chain");
    }
    let mut override_value = existing_chain_override(&s, id).await;
    match action.as_str() {
        "activate" => {
            let mut hot = s.registry.hot_chain_snapshot();
            if !hot.iter().any(|(chain_id, _)| *chain_id == id) {
                hot.push((id, crate::registry::unix_seconds()));
            }
            if let Err(error) = s.store.set_hot_chains(&hot).await {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            let _ = s.registry.resolve_for_request(id).await;
        }
        "demote" => {
            let hot = s
                .registry
                .hot_chain_snapshot()
                .into_iter()
                .filter(|(chain_id, _)| *chain_id != id)
                .collect::<Vec<_>>();
            if let Err(error) = s.store.set_hot_chains(&hot).await {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    error.to_string(),
                );
            }
            s.registry.demote(id, "admin").await;
        }
        "pin" => {
            override_value.pinned = Some(true);
        }
        "unpin" => {
            override_value.pinned = Some(false);
        }
        "enable" => {
            override_value.disabled = Some(false);
        }
        "disable" => {
            override_value.disabled = Some(true);
        }
        _ => {}
    }
    if ["pin", "unpin", "enable", "disable"].contains(&action.as_str()) {
        if let Err(r) = persist_chain(&s, id, &override_value).await {
            return r;
        };
        if action == "pin" {
            s.registry.set_pinned(id, true).await;
        }
        if action == "unpin" {
            s.registry.set_pinned(id, false).await;
        }
        if action == "enable" {
            s.registry.set_disabled(id, false).await;
        }
        if action == "disable" {
            s.registry.set_disabled(id, true).await;
        }
    }
    audit(&s, &format!("chain.{action}"), &id.to_string()).await;
    *s.public_cache.lock().await = None;
    Json(json!({"chainId":id,"state":s.registry.resolve_for_request(id).await.map_or_else(|| "unknown".to_owned(), |value| format!("{:?}", value.state_label()).to_ascii_lowercase())})).into_response()
}

async fn chain_settings(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
    patch: Result<Json<SettingsPatch>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(patch) = match patch {
        Ok(value) => value,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "invalid JSON body",
            );
        }
    };
    if let Err(r) = auth(&headers, &Method::PUT, s.config.admin.auth_token.as_deref()) {
        return r;
    }
    if !s.registry.chain_in_catalog(id).await {
        return err(StatusCode::NOT_FOUND, "unknown_chain", "unknown chain");
    }
    if patch
        .block_time_ms
        .flatten()
        .is_some_and(|v| !(100..=600_000).contains(&v))
        || patch
            .confirmation_depth
            .flatten()
            .is_some_and(|v| !(1..=100_000).contains(&v))
        || patch
            .tip_ttl_ms
            .flatten()
            .is_some_and(|v| !(100..=60_000).contains(&v))
        || patch.max_block_lag.flatten().is_some_and(|v| v > 10_000)
    {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "settings value is out of range",
        );
    }
    let mut v = existing_chain_override(&s, id).await;
    if let Some(x) = patch.block_time_ms {
        v.block_time_ms = x
    }
    if let Some(x) = patch.confirmation_depth {
        v.confirmation_depth = x
    }
    if let Some(x) = patch.tip_ttl_ms {
        v.tip_ttl_ms = x
    }
    if let Some(x) = patch.max_block_lag {
        v.max_block_lag = x
    }
    if let Err(r) = persist_chain(&s, id, &v).await {
        return r;
    };
    s.registry.apply_override(id, v.clone()).await;
    s.forwarder
        .apply_chain_settings(id, v.confirmation_depth, v.tip_ttl_ms);
    audit(&s, "chain.settings", &id.to_string()).await;
    Json(v).into_response()
}

async fn endpoint_action(
    State(s): State<AdminState>,
    headers: HeaderMap,
    Path((id, action)): Path<(u64, String)>,
    body: Result<Json<EndpointAction>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(value) => value,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "invalid JSON body",
            );
        }
    };
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    let mut body = body;
    if action == "add" {
        body.url = match valid_endpoint_url(&body.url, s.config.admin.allow_private_endpoints) {
            Ok(url) => url,
            Err(response) => return response,
        };
    }
    if body.rps.is_some_and(|value| !(1..=100).contains(&value)) {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "rps must be between 1 and 100",
        );
    }
    if body
        .concurrency
        .is_some_and(|value| !(1..=64).contains(&value))
    {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "concurrency must be between 1 and 64",
        );
    }
    if action == "cool" && !(1..=604800).contains(&body.seconds.unwrap_or(60)) {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "cool seconds must be between 1 and 604800",
        );
    }
    if action == "add" {
        if !s.registry.chain_in_catalog(id).await {
            return err(StatusCode::NOT_FOUND, "unknown_chain", "unknown chain");
        }
        let value = EndpointOverrideState {
            url: body.url.clone(),
            disabled: Some(false),
            rps: body.rps,
            concurrency: body.concurrency,
        };
        if let Err(response) = persist_endpoint(&s, id, &value).await {
            return response;
        }
        s.registry.set_endpoint_override(id, value).await;
        audit(&s, "endpoint.add", &body.url).await;
        return Json(json!({"url":body.url,"state":"probation"})).into_response();
    }
    if !s.registry.endpoint_known(id, &body.url).await {
        return err(StatusCode::NOT_FOUND, "not_found", "endpoint not found");
    }
    let endpoint = match s.registry.endpoint(id, &body.url).await {
        Some(e) => e,
        None if action == "add" => {
            if !s.registry.add_runtime_endpoint(id, body.url.clone()).await {
                return err(StatusCode::NOT_FOUND, "unknown_chain", "chain unavailable");
            };
            let value = EndpointOverrideState {
                url: body.url.clone(),
                disabled: Some(false),
                rps: body.rps,
                concurrency: body.concurrency,
            };
            if let Err(r) = persist_endpoint(&s, id, &value).await {
                let _ = s.registry.remove_runtime_endpoint(id, &body.url).await;
                return r;
            };
            audit(&s, "endpoint.add", &body.url).await;
            return Json(json!({"url":body.url,"state":"probation"})).into_response();
        }
        None if action == "enable" || action == "limits" => {
            let mut value = s
                .registry
                .runtime_endpoint_override(id, &body.url)
                .unwrap_or(EndpointOverrideState {
                    url: body.url.clone(),
                    disabled: None,
                    rps: None,
                    concurrency: None,
                });
            if action == "enable" {
                value.disabled = Some(false);
            }
            if action == "limits" {
                if body.rps.is_some() {
                    value.rps = body.rps;
                }
                if body.concurrency.is_some() {
                    value.concurrency = body.concurrency;
                }
            }
            if let Err(r) = persist_endpoint(&s, id, &value).await {
                return r;
            }
            s.registry.set_endpoint_override(id, value).await;
            audit(&s, &format!("endpoint.{action}"), &body.url).await;
            return Json(json!({"url":body.url,"state":"dormant"})).into_response();
        }
        None => return err(StatusCode::NOT_FOUND, "not_found", "endpoint not found"),
    };
    match action.as_str() {
        "disable" => {
            let mut v = s
                .registry
                .runtime_endpoint_override(id, &body.url)
                .unwrap_or(EndpointOverrideState {
                    url: body.url.clone(),
                    disabled: None,
                    rps: None,
                    concurrency: None,
                });
            v.disabled = Some(true);
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "enable" => {
            let mut v = s
                .registry
                .runtime_endpoint_override(id, &body.url)
                .unwrap_or(EndpointOverrideState {
                    url: body.url.clone(),
                    disabled: None,
                    rps: None,
                    concurrency: None,
                });
            v.disabled = Some(false);
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "cool" => endpoint.cool_for(std::time::Duration::from_secs(body.seconds.unwrap_or(60))),
        "reset" => endpoint.reset_health(),
        "limits" => {
            let mut v = s
                .registry
                .runtime_endpoint_override(id, &body.url)
                .unwrap_or(EndpointOverrideState {
                    url: body.url.clone(),
                    disabled: None,
                    rps: None,
                    concurrency: None,
                });
            if body.rps.is_some() {
                v.rps = body.rps;
            }
            if body.concurrency.is_some() {
                v.concurrency = body.concurrency;
            }
            if let Err(r) = persist_endpoint(&s, id, &v).await {
                return r;
            };
            s.registry.set_endpoint_override(id, v).await;
        }
        "remove" => {
            if !s.registry.runtime_endpoint_exists(id, &body.url).await {
                return err(
                    StatusCode::CONFLICT,
                    "conflict",
                    "endpoint is not a runtime endpoint",
                );
            };
            if let Err(e) = s
                .store
                .delete_endpoint_override(&endpoint_key(id, &body.url))
                .await
            {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "state_store_unavailable",
                    e.to_string(),
                );
            };
            let _ = s.registry.remove_runtime_endpoint(id, &body.url).await;
        }
        "probe" => {
            let Some(probe) = &s.probe else {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "not_found",
                    "probe manager unavailable",
                );
            };
            let outcome = probe.probe_endpoint(id, endpoint).await;
            audit(&s, "endpoint.probe", &body.url).await;
            let outcome = match outcome {
                crate::probe::ProbeOutcome::Passed => json!({"state":"passed"}),
                crate::probe::ProbeOutcome::Skipped => json!({"state":"skipped"}),
                crate::probe::ProbeOutcome::Failed(kind) => {
                    json!({"state":"failed","fault":format!("{kind:?}").to_ascii_lowercase()})
                }
                crate::probe::ProbeOutcome::RemovedWrongChain { actual } => {
                    json!({"state":"removedWrongChain","actualChainId":actual})
                }
            };
            return Json(json!({"outcome":outcome})).into_response();
        }
        _ => {
            return err(
                StatusCode::NOT_FOUND,
                "not_found",
                "unknown endpoint action",
            );
        }
    };
    audit(&s, &format!("endpoint.{action}"), &body.url).await;
    Json(json!({"url":body.url,"state":format!("{:?}",endpoint.state(Instant::now().into())).to_ascii_lowercase()})).into_response()
}

async fn state_import(
    State(s): State<AdminState>,
    headers: HeaderMap,
    value: Result<Json<StateExport>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(value) = match value {
        Ok(value) => value,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "invalid JSON body",
            );
        }
    };
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if let Err(e) = s.store.import(&value).await {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        );
    };
    s.registry.apply_overrides(&value.overrides).await;
    s.forwarder.apply_state_overrides(&value.overrides);
    s.registry.restore_health(&value.health).await;
    s.registry.activate_restored_hot(&value.hot_chains).await;
    // 自动开启集合以导入内容为准，否则运行中的进程要等重启才认账。
    s.registry
        .sync_auto_pinned(value.auto_chains.keys().copied());
    audit(&s, "state.import", "namespace").await;
    Json(json!({"ok":true})).into_response()
}
async fn state_reset(
    State(s): State<AdminState>,
    headers: HeaderMap,
    value: Result<Json<ConfirmReset>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(value) = match value {
        Ok(value) => value,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "invalid JSON body",
            );
        }
    };
    if let Err(r) = auth(
        &headers,
        &Method::POST,
        s.config.admin.auth_token.as_deref(),
    ) {
        return r;
    }
    if !value.confirm {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "confirm must be true",
        );
    };
    if let Err(e) = s.store.reset().await {
        return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        );
    };
    s.registry.apply_overrides(&Overrides::default()).await;
    s.forwarder.apply_state_overrides(&Overrides::default());
    s.registry.sync_auto_pinned(std::iter::empty());
    s.forwarder.cache().clear().await;
    audit(&s, "state.reset", "namespace").await;
    Json(json!({"ok":true})).into_response()
}

async fn static_file(State(s): State<AdminState>, uri: Uri) -> Response {
    let Some(dir) = &s.config.admin.static_dir else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let raw = uri.path().strip_prefix("/dashboard").unwrap_or("");
    let path = raw.trim_start_matches('/');
    if path.is_empty() {
        return read_static_file(dir, "index.html").await;
    }
    if raw.starts_with("//")
        || path.contains('%')
        || path.starts_with('/')
        || path.split('/').any(|part| part.is_empty() || part == "..")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let candidate = dir.join(path);
    if tokio::fs::metadata(&candidate).await.is_ok() {
        return read_static_file(dir, path).await;
    }
    if std::path::Path::new(path).extension().is_some() {
        return StatusCode::NOT_FOUND.into_response();
    }
    read_static_file(dir, "index.html").await
}
async fn read_static_file(dir: &std::path::Path, relative: &str) -> Response {
    let Ok(root) = tokio::fs::canonicalize(dir).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let candidate = dir.join(relative);
    let Ok(full) = tokio::fs::canonicalize(&candidate).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !full.starts_with(&root) || !full.is_file() {
        return StatusCode::NOT_FOUND.into_response();
    }
    match tokio::fs::read(&full).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(CONTENT_TYPE, content_type(&full))],
            Body::from(bytes),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
fn content_type(path: &std::path::Path) -> &'static str {
    match path.extension().and_then(|x| x.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("map") => "application/json",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

async fn audit(s: &AdminState, what: &str, target: &str) {
    if let Err(e) = s.store.append_audit(what, target).await {
        warn!(error=%e,what,target,"admin audit append failed")
    }
}
async fn existing_chain_override(s: &AdminState, id: u64) -> ChainOverrideState {
    s.registry.runtime_chain_override(id)
}
async fn persist_chain(
    s: &AdminState,
    id: u64,
    value: &ChainOverrideState,
) -> Result<(), Response> {
    if !s.store.writable().await {
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            "state store is not writable",
        ));
    }
    s.store.put_chain_override(id, value).await.map_err(|e| {
        err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            e.to_string(),
        )
    })?;
    Ok(())
}
async fn persist_endpoint(
    s: &AdminState,
    id: u64,
    value: &EndpointOverrideState,
) -> Result<(), Response> {
    if !s.store.writable().await {
        return Err(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "state_store_unavailable",
            "state store is not writable",
        ));
    }
    s.store
        .put_endpoint_override(&endpoint_key(id, &value.url), value)
        .await
        .map_err(|e| {
            err(
                StatusCode::SERVICE_UNAVAILABLE,
                "state_store_unavailable",
                e.to_string(),
            )
        })?;
    Ok(())
}

async fn build_rows(s: &AdminState, only: Option<u64>) -> Vec<ChainRow> {
    let metrics = s.metrics.chain_snapshots();
    build_rows_with_metrics(s, only, &metrics).await
}

async fn build_rows_with_metrics(
    s: &AdminState,
    only: Option<u64>,
    metric_snapshots: &HashMap<u64, crate::metrics::ChainMetricsSnapshot>,
) -> Vec<ChainRow> {
    let catalog = s.registry.catalog().await;
    let candidates = match s.auto_enable.as_ref() {
        Some(manager) => manager.progress_snapshot().await,
        None => HashMap::new(),
    };
    let summaries: HashMap<u64, _> = s
        .registry
        .summaries()
        .await
        .into_iter()
        .map(|x| (x.chain_id, x))
        .collect();
    let mut ids = Vec::new();
    if let Some(c) = &catalog {
        ids.extend(c.chains.iter().map(|x| x.chain_id));
    }
    if let Some(id) = only {
        ids.retain(|x| *x == id);
    }
    ids.sort_unstable();
    ids.dedup();
    let mut rows = Vec::new();
    for id in ids {
        let c = catalog.as_ref().and_then(|x| x.lookup(id));
        let summary = summaries.get(&id);
        let state = summary.map_or("dormant".to_owned(), |x| {
            format!("{:?}", x.state).to_ascii_lowercase()
        });
        let metrics = metric_snapshots.get(&id).copied().unwrap_or_default();
        let settings = s.registry.chain_settings(id);
        let endpoint_rows = if only.is_some() {
            Some(build_endpoint_rows(s, id, c).await)
        } else {
            None
        };
        rows.push(ChainRow{chain_id:id,name:c.map_or_else(||format!("Chain {id}"),|x|x.name.clone()),short_name:c.and_then(|x|x.short_name.clone()),is_testnet:c.is_some_and(|x|x.is_testnet),status:c.and_then(|x|x.status.clone()),state:state.clone(),pinned:state=="pinned",disabled:state=="disabled",pin_source:s.registry.pin_source(id).map(str::to_string),auto_candidate:candidates.get(&id).cloned(),catalog_endpoints:c.map_or(0,|x|x.endpoints.len()),endpoints:summary.map_or(0,|x|x.endpoints),active:summary.map_or(0,|x|x.active),cooling:summary.map_or(0,|x|x.cooling),probation:summary.map_or(0,|x|x.probation),head:summary.map_or(0,|x|x.head),last_ingress_unix:s.registry.chain_last_ingress(id),ingress_total:metrics.ingress,cache_hits_total:metrics.cache_hits,cache_lookups_total:metrics.cache_lookups,upstream_total:metrics.upstream,user_visible_errors_total:metrics.user_visible_errors,settings:json!({"blockTimeMs":settings.0,"confirmationDepth":settings.1,"tipTtlMs":settings.2,"maxBlockLag":settings.3,"source":settings.4}),endpoint_rows});
    }
    rows
}
async fn build_endpoint_rows(
    s: &AdminState,
    id: u64,
    c: Option<&crate::chainlist::CatalogChain>,
) -> Vec<EndpointRow> {
    let mut out = Vec::new();
    for e in s.registry.all_endpoints(id).await {
        let state = e.state(Instant::now().into());
        let (name, strikes, cooling) = match state {
            EndpointState::Active => ("active".to_owned(), 0, None),
            EndpointState::Probation { passes } => ("probation".to_owned(), passes as u32, None),
            EndpointState::Cooling { until, strikes } => (
                "cooling".to_owned(),
                strikes,
                Some(
                    crate::registry::unix_seconds()
                        + until
                            .saturating_duration_since(Instant::now().into())
                            .as_secs(),
                ),
            ),
        };
        let tracking = c.and_then(|x| {
            x.endpoints
                .iter()
                .find(|x| x.url == e.url())
                .and_then(|x| x.tracking.clone())
        });
        let h = e.health_snapshot(id);
        out.push(EndpointRow {
            url: e.url().to_owned(),
            tracking,
            state: name,
            strikes,
            cooling_until_unix: cooling,
            latency_ewma_ms: e.latency_ewma_micros() as f64 / 1000.0,
            lag: e.lag(),
            rps: e.rps(),
            concurrency: e.concurrency(),
            disabled: false,
            source: if c.is_some() {
                "chainlist".into()
            } else {
                "runtime".into()
            },
            last_fault: None,
            stats: e.stats(),
        });
        let _ = h;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        chain_id: u64,
        state: &str,
        active: usize,
        endpoints: usize,
        testnet: bool,
        ingress: u64,
    ) -> ChainRow {
        ChainRow {
            chain_id,
            pin_source: None,
            auto_candidate: None,
            name: format!("Chain {chain_id}"),
            short_name: None,
            is_testnet: testnet,
            status: None,
            state: state.to_owned(),
            pinned: state == "pinned",
            disabled: state == "disabled",
            catalog_endpoints: endpoints,
            endpoints,
            active,
            cooling: 0,
            probation: 0,
            head: 0,
            last_ingress_unix: 0,
            ingress_total: ingress,
            cache_hits_total: 0,
            cache_lookups_total: 0,
            upstream_total: 0,
            user_visible_errors_total: 0,
            settings: Value::Null,
            endpoint_rows: None,
        }
    }

    #[test]
    fn priority_sort_puts_pinned_hot_and_endpoint_backed_chains_first() {
        let mut rows = [
            row(5, "dormant", 0, 0, false, 0),
            row(4, "disabled", 3, 3, false, 900),
            row(3, "hot", 0, 2, false, 50),
            row(2, "hot", 2, 2, false, 10),
            row(1, "pinned", 1, 3, false, 0),
            row(6, "dormant", 0, 0, false, 5),
            row(7, "hot", 2, 2, true, 999),
            row(8, "pinned", 0, 0, false, 100),
        ];
        rows.sort_by_key(priority_key);
        let ids: Vec<u64> = rows.iter().map(|r| r.chain_id).collect();
        // pinned(有活跃) > pinned(无端点) > hot 主网有活跃 > hot 测试网有活跃 > hot 无活跃但有端点
        // > dormant 按流量 > disabled 垫底
        assert_eq!(ids, vec![1, 8, 2, 7, 3, 6, 5, 4]);
    }

    #[test]
    fn public_row_rejects_non_http_explorer_urls() {
        let row = row(1, "hot", 1, 1, false, 0);
        let catalog = crate::chainlist::Catalog {
            chains: vec![crate::chainlist::CatalogChain {
                chain_id: 1,
                name: "One".into(),
                short_name: None,
                chain: None,
                slug: None,
                is_testnet: false,
                native_symbol: None,
                explorer_url: Some("javascript:alert(1)".into()),
                status: None,
                tvl: None,
                endpoints: Vec::new(),
            }],
            by_id: HashMap::from([(1, 0)]),
        };
        assert!(public_row(&row, Some(&catalog)).explorer_url.is_none());
    }
}
