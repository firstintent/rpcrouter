use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const MAX_BATCH_SIZE: usize = 100;

/// 默认请求体大小上限（256 KiB）。
pub const DEFAULT_MAX_BODY_BYTES: usize = 256 * 1024;
/// 默认全局并发上限（在飞请求数）。
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 1024;
/// 默认优雅退出排空 deadline（10 秒）。
pub const DEFAULT_SHUTDOWN_DEADLINE_MS: u64 = 10_000;

/// 环境变量覆写的前缀：`RPCROUTER_*`。
pub const ENV_PREFIX: &str = "RPCROUTER_";

/// 读取非空环境变量；未设置或为空串时返回 `None`。
fn env_non_empty(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

/// 解析布尔环境变量，接受 1/0/true/false（大小写不敏感）。
fn parse_bool(raw: &str, key: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{key} is not a valid boolean"),
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub listen: SocketAddr,
    pub metrics_enabled: bool,
    /// 固定激活链（pinned），语义已从 v1 的"全部链"升级为"启动即激活、永不降级"。
    /// `discovery.enabled = false` 时等价 v1 行为（只服务 pinned 链）。
    pub chains: Vec<u64>,
    pub server: ServerConfig,
    pub chainlist: ChainlistConfig,
    pub discovery: DiscoveryConfig,
    pub upstream: UpstreamConfig,
    pub probe: ProbeConfig,
    pub cache: CacheConfig,
    pub hedging: HedgingConfig,
    pub chain_overrides: Vec<ChainOverride>,
    pub state: StateConfig,
    pub admin: AdminConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8545"
                .parse()
                .expect("valid default listen address"),
            metrics_enabled: true,
            chains: vec![1, 143],
            server: ServerConfig::default(),
            chainlist: ChainlistConfig::default(),
            discovery: DiscoveryConfig::default(),
            upstream: UpstreamConfig::default(),
            probe: ProbeConfig::default(),
            cache: CacheConfig::default(),
            hedging: HedgingConfig::default(),
            chain_overrides: Vec::new(),
            state: StateConfig::default(),
            admin: AdminConfig::default(),
        }
    }
}

impl Config {
    /// 配置文件不存在时使用内置默认值，保证零配置启动。
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match fs::read_to_string(path) {
            Ok(contents) => Self::from_toml(&contents)
                .with_context(|| format!("failed to parse config {}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => {
                Err(error).with_context(|| format!("failed to read config {}", path.display()))
            }
        }
    }

    /// 用 `RPCROUTER_` 前缀的环境变量覆写加载后的配置关键项，用于容器化/托管部署
    /// 在不改动 config.toml 的前提下调整 listen 地址、启用链、缓存容量等。
    /// 只在环境变量存在时覆写；空字符串按未设置处理。
    pub fn apply_env_overrides(&mut self) -> Result<()> {
        if let Some(raw) = env_non_empty("RPCROUTER_LISTEN") {
            self.listen = raw
                .parse()
                .context("RPCROUTER_LISTEN is not a valid listen address")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_CHAINS") {
            let mut chains = Vec::new();
            for part in raw.split(',').map(str::trim) {
                if part.is_empty() {
                    continue;
                }
                let id: u64 = part.parse().with_context(|| {
                    format!("RPCROUTER_CHAINS entry `{part}` is not a chain id")
                })?;
                chains.push(id);
            }
            self.chains = chains;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_CACHE_MAX_BYTES") {
            self.cache.max_bytes = raw
                .parse()
                .context("RPCROUTER_CACHE_MAX_BYTES is not a valid byte count")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_METRICS_ENABLED") {
            self.metrics_enabled = parse_bool(&raw, "RPCROUTER_METRICS_ENABLED")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_CHAINLIST_REFRESH_SECONDS") {
            self.chainlist.refresh_seconds = raw
                .parse()
                .context("RPCROUTER_CHAINLIST_REFRESH_SECONDS is not a valid duration")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_CHAINLIST_CACHE_PATH") {
            self.chainlist.cache_path = PathBuf::from(raw);
        }
        if let Some(raw) = env_non_empty("RPCROUTER_DISCOVERY_ENABLED") {
            self.discovery.enabled = parse_bool(&raw, "RPCROUTER_DISCOVERY_ENABLED")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_DISCOVERY_MAX_HOT_CHAINS") {
            self.discovery.max_hot_chains = raw
                .parse()
                .context("RPCROUTER_DISCOVERY_MAX_HOT_CHAINS is not a valid integer")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_DISCOVERY_IDLE_SECONDS") {
            self.discovery.idle_seconds = raw
                .parse()
                .context("RPCROUTER_DISCOVERY_IDLE_SECONDS is not a valid integer")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_AUTO_ENABLE_ENABLED") {
            self.discovery.auto_enable.enabled = parse_bool(&raw, "RPCROUTER_AUTO_ENABLE_ENABLED")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_AUTO_ENABLE_MIN_ENDPOINTS") {
            self.discovery.auto_enable.min_endpoints =
                raw.parse().context("invalid auto min endpoints")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_AUTO_ENABLE_MAX_CHAINS") {
            self.discovery.auto_enable.max_chains =
                raw.parse().context("invalid auto max chains")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_STATE_BACKEND") {
            self.state.backend = raw;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_REDIS_URL") {
            self.state.redis_url = raw;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_STATE_NAMESPACE") {
            self.state.namespace = raw;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_STATE_RESET") {
            self.state.reset = parse_bool(&raw, "RPCROUTER_STATE_RESET")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_STATE_RESTORE_HOT") {
            self.state.restore_hot = parse_bool(&raw, "RPCROUTER_STATE_RESTORE_HOT")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_STATE_REQUIRED") {
            self.state.required = parse_bool(&raw, "RPCROUTER_STATE_REQUIRED")?;
        }
        if let Some(raw) = env_non_empty("RPCROUTER_ADMIN_TOKEN") {
            self.admin.auth_token = Some(raw);
        }
        if let Some(raw) = env_non_empty("RPCROUTER_ADMIN_STATIC_DIR") {
            self.admin.static_dir = Some(PathBuf::from(raw));
        }
        if let Some(raw) = env_non_empty("RPCROUTER_ADMIN_PUBLIC_SITE") {
            self.admin.public_site = parse_bool(&raw, "RPCROUTER_ADMIN_PUBLIC_SITE")?;
        }
        self.validate()
    }

    pub fn from_toml(contents: &str) -> Result<Self> {
        let config: Self = toml::from_str(contents).context("invalid TOML")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        // discovery.enabled=false 时 chains 必须非空（至少 pinned 链）。
        if !self.discovery.enabled && self.chains.is_empty() {
            bail!("chains must not be empty when discovery is disabled");
        }
        // discovery.enabled=true 时 chains 允许为空（完全依赖目录动态激活）。
        let unique_chains: HashSet<_> = self.chains.iter().copied().collect();
        if unique_chains.len() != self.chains.len() {
            bail!("chains must not contain duplicates");
        }
        if self
            .discovery
            .deny
            .iter()
            .any(|chain_id| unique_chains.contains(chain_id))
        {
            bail!("discovery.deny must not contain pinned chains");
        }
        if self.server.batch_limit == 0 || self.server.batch_limit > MAX_BATCH_SIZE {
            bail!("server.batch_limit must be between 1 and {MAX_BATCH_SIZE}");
        }
        if self.server.max_body_bytes == 0 {
            bail!("server.max_body_bytes must be greater than zero");
        }
        if self.server.max_concurrent_requests == 0 {
            bail!("server.max_concurrent_requests must be greater than zero");
        }
        if self.server.shutdown_deadline_ms == 0 {
            bail!("server.shutdown_deadline_ms must be greater than zero");
        }
        if self.server.per_ip_rate_limit.enabled
            && (self.server.per_ip_rate_limit.requests_per_second == 0
                || self.server.per_ip_rate_limit.burst == 0)
        {
            bail!("per_ip_rate_limit rate and burst must be greater than zero");
        }
        if let Some(token) = &self.server.metrics_auth_token {
            if token.is_empty() {
                bail!("server.metrics_auth_token must not be empty");
            }
            if token.bytes().any(is_header_control) {
                bail!(
                    "server.metrics_auth_token contains characters not allowed in an HTTP header value"
                );
            }
        }
        if self.chainlist.refresh_seconds == 0 {
            bail!("chainlist.refresh_seconds must be greater than zero");
        }
        if self.discovery.max_hot_chains == 0 {
            bail!("discovery.max_hot_chains must be greater than zero");
        }
        let a = &self.discovery.auto_enable;
        if a.min_endpoints == 0
            || a.max_chains == 0
            || a.probe_batch == 0
            || a.promote_after_rounds == 0
            || a.probe_concurrency == 0
        {
            bail!("discovery.auto_enable numeric limits must be greater than zero");
        }
        if self.state.backend != "redis"
            && self.state.backend != "file"
            && self.state.backend != "memory"
        {
            bail!("state.backend must be redis, file, or memory");
        }
        if self.state.namespace.trim().is_empty() {
            bail!("state.namespace must not be empty");
        }
        if self.state.flush_interval_ms == 0 || self.state.health_ttl_seconds == 0 {
            bail!("state intervals must be greater than zero");
        }
        if let Some(token) = &self.admin.auth_token
            && (token.is_empty() || token.bytes().any(is_header_control))
        {
            bail!("admin.auth_token is not a valid HTTP header value");
        }
        if self.admin.auth_token.is_some() && self.admin.cors_allow_origins.iter().any(|x| x == "*")
        {
            bail!("admin.cors_allow_origins '*' cannot be used with admin.auth_token");
        }
        if self.discovery.idle_seconds == 0 {
            bail!("discovery.idle_seconds must be greater than zero");
        }
        if self.upstream.request_timeout_ms == 0 || self.upstream.deadline_ms == 0 {
            bail!("upstream timeouts must be greater than zero");
        }
        if self.upstream.slow_threshold_ms == 0 {
            bail!("upstream.slow_threshold_ms must be greater than zero");
        }
        if self.upstream.max_attempts == 0 || self.upstream.max_attempts > 4 {
            bail!("upstream.max_attempts must be between 1 and 4");
        }
        if self.upstream.default_rps == 0 || self.upstream.default_concurrency == 0 {
            bail!("upstream rate and concurrency limits must be greater than zero");
        }
        if self.probe.min_interval_seconds == 0
            || self.probe.max_interval_seconds < self.probe.min_interval_seconds
        {
            bail!("probe interval must be a valid nonzero range");
        }
        if self.probe.max_concurrency == 0 || self.probe.request_timeout_ms == 0 {
            bail!("probe concurrency and timeout must be greater than zero");
        }
        if self.cache.max_bytes == 0 || self.cache.immutable_ttl_seconds < 60 * 60 {
            bail!("cache capacity must be nonzero and immutable TTL must be at least one hour");
        }
        if self.hedging.delay_ms == 0
            || self.hedging.max_percent == 0
            || self.hedging.max_percent > 10
            || self.hedging.min_active_endpoints < 2
        {
            bail!("hedging requires a delay, max_percent 1..=10, and at least two endpoints");
        }
        // chain_overrides 不再要求 chain_id 在 chains 列表中（动态链激活时生效）。
        for chain in &self.chain_overrides {
            for endpoint in &chain.endpoint_overrides {
                if endpoint.rps == Some(0) || endpoint.concurrency == Some(0) {
                    bail!("endpoint limits must be greater than zero");
                }
            }
        }
        Ok(())
    }

    pub fn chain_override(&self, chain_id: u64) -> Option<&ChainOverride> {
        self.chain_overrides
            .iter()
            .find(|chain| chain.chain_id == chain_id)
    }

    pub fn endpoint_limits(&self, chain_id: u64, url: &str) -> (u32, usize) {
        let configured = self
            .chain_override(chain_id)
            .and_then(|chain| chain.endpoint_overrides.iter().find(|item| item.url == url));
        (
            configured
                .and_then(|endpoint| endpoint.rps)
                .unwrap_or(self.upstream.default_rps),
            configured
                .and_then(|endpoint| endpoint.concurrency)
                .unwrap_or(self.upstream.default_concurrency),
        )
    }

    pub fn lag_threshold(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.max_block_lag)
            .unwrap_or(self.probe.max_block_lag)
    }

    pub fn confirmation_depth(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.confirmation_depth)
            .unwrap_or(64)
    }

    pub fn block_time_ms(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.block_time_ms)
            .unwrap_or(match chain_id {
                1 => 12_000,
                143 => 400,
                _ => 2_000,
            })
    }

    pub fn tip_ttl_ms(&self, chain_id: u64) -> u64 {
        self.chain_override(chain_id)
            .and_then(|chain| chain.tip_ttl_ms)
            .unwrap_or_else(|| self.block_time_ms(chain_id).min(2_000))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub batch_limit: usize,
    /// 优雅退出时等待在飞请求排空的 deadline（毫秒）。超时强制退出。
    pub shutdown_deadline_ms: u64,
    /// 入口请求体大小上限（字节），超限返回 HTTP 413 + JSON-RPC 错误体。
    pub max_body_bytes: usize,
    /// 全局并发上限（同时在飞的入口请求数），超限快速拒绝（HTTP 503 + JSON-RPC 错误体）。
    pub max_concurrent_requests: usize,
    /// 每 IP 限速配置，默认关闭。
    pub per_ip_rate_limit: PerIpRateLimitConfig,
    /// /metrics 端点 bearer token 鉴权开关，None 表示不鉴权。
    pub metrics_auth_token: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            batch_limit: MAX_BATCH_SIZE,
            shutdown_deadline_ms: DEFAULT_SHUTDOWN_DEADLINE_MS,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            per_ip_rate_limit: PerIpRateLimitConfig::default(),
            metrics_auth_token: None,
        }
    }
}

/// 每 IP 限速配置。默认关闭（`enabled = false`）。
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct PerIpRateLimitConfig {
    pub enabled: bool,
    /// 每秒允许的请求数。
    pub requests_per_second: u64,
    /// 突发容量（token bucket 桶大小）。
    pub burst: u64,
}

impl Default for PerIpRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            requests_per_second: 20,
            burst: 40,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ChainlistConfig {
    pub url: String,
    /// 刷新间隔秒数。默认 3600（1h），v1 是 21600（6h）。
    pub refresh_seconds: u64,
    pub stale_grace_seconds: u64,
    pub cache_path: PathBuf,
}

impl Default for ChainlistConfig {
    fn default() -> Self {
        Self {
            url: "https://chainlist.org/rpcs.json".to_owned(),
            refresh_seconds: 3600,
            stale_grace_seconds: 24 * 60 * 60,
            cache_path: PathBuf::from("./data/rpcs.json"),
        }
    }
}

/// 动态全链目录配置。
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// 是否启用动态目录发现。false = 只服务 pinned 链（等价 v1 行为）。
    pub enabled: bool,
    /// 是否包含测试网链。
    pub include_testnets: bool,
    /// chainId 拒绝列表（403）。
    pub deny: Vec<u64>,
    /// 非 pinned 热链数量上限。默认 256。
    pub max_hot_chains: usize,
    /// 非 pinned 链无流量后降级秒数。默认 600（10 分钟）。
    pub idle_seconds: u64,
    pub auto_enable: AutoEnableConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct AutoEnableConfig {
    pub enabled: bool,
    pub min_endpoints: usize,
    pub max_chains: usize,
    pub max_candidates: usize,
    pub max_endpoints_per_chain: usize,
    pub candidate_interval_seconds: u64,
    pub probe_batch: usize,
    pub probe_concurrency: usize,
    pub promote_after_rounds: usize,
    pub min_active_endpoints: usize,
    pub head_tolerance_blocks: u64,
}
impl Default for AutoEnableConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_endpoints: 5,
            max_chains: 400,
            max_candidates: 512,
            max_endpoints_per_chain: 8,
            candidate_interval_seconds: 30,
            probe_batch: 32,
            probe_concurrency: 8,
            promote_after_rounds: 2,
            min_active_endpoints: 2,
            head_tolerance_blocks: 64,
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            include_testnets: true,
            deny: Vec::new(),
            max_hot_chains: 256,
            idle_seconds: 600,
            auto_enable: AutoEnableConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    pub request_timeout_ms: u64,
    pub slow_threshold_ms: u64,
    pub deadline_ms: u64,
    pub max_attempts: usize,
    pub default_rps: u32,
    pub default_concurrency: usize,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: 5_000,
            slow_threshold_ms: 4_000,
            deadline_ms: 15_000,
            max_attempts: 4,
            default_rps: 15,
            default_concurrency: 8,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ChainOverride {
    pub chain_id: u64,
    pub block_time_ms: Option<u64>,
    pub confirmation_depth: Option<u64>,
    pub tip_ttl_ms: Option<u64>,
    pub max_block_lag: Option<u64>,
    pub extra_endpoints: Vec<String>,
    pub disabled_endpoints: Vec<String>,
    pub endpoint_overrides: Vec<EndpointOverride>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EndpointOverride {
    pub url: String,
    pub rps: Option<u32>,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ProbeConfig {
    pub min_interval_seconds: u64,
    pub max_interval_seconds: u64,
    pub max_concurrency: usize,
    pub request_timeout_ms: u64,
    pub max_block_lag: u64,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            min_interval_seconds: 15,
            max_interval_seconds: 30,
            max_concurrency: 32,
            request_timeout_ms: 5_000,
            max_block_lag: 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub max_bytes: u64,
    pub immutable_ttl_seconds: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            immutable_ttl_seconds: 60 * 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct HedgingConfig {
    pub enabled: bool,
    pub delay_ms: u64,
    pub max_percent: u32,
    pub min_active_endpoints: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct StateConfig {
    pub backend: String,
    pub redis_url: String,
    pub namespace: String,
    pub required: bool,
    pub flush_interval_ms: u64,
    pub health_ttl_seconds: u64,
    pub reset: bool,
    pub restore_hot: bool,
    pub file_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    pub enabled: bool,
    pub public_site: bool,
    pub auth_token: Option<String>,
    pub static_dir: Option<PathBuf>,
    pub cors_allow_origins: Vec<String>,
    pub allow_private_endpoints: bool,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            public_site: true,
            auth_token: None,
            static_dir: None,
            cors_allow_origins: Vec::new(),
            allow_private_endpoints: false,
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: "redis".to_owned(),
            redis_url: "redis://127.0.0.1:6379/0".to_owned(),
            namespace: "rpcrouter".to_owned(),
            required: false,
            flush_interval_ms: 2_000,
            health_ttl_seconds: 86_400,
            reset: false,
            restore_hot: true,
            file_path: PathBuf::from("./data/state.json"),
        }
    }
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            delay_ms: 300,
            max_percent: 10,
            min_active_endpoints: 2,
        }
    }
}

/// HTTP header 值中不允许的字节：除 HTAB(0x09) 外的控制字符（0x00–0x1F）与 DEL(0x7F)。
fn is_header_control(byte: u8) -> bool {
    (byte < 0x20 && byte != 0x09) || byte == 0x7f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_uses_safe_defaults() {
        let config = Config::load("this-file-must-not-exist.toml").expect("load defaults");
        assert_eq!(config.chains, [1, 143]);
        assert_eq!(config.upstream.default_rps, 15);
        assert_eq!(config.upstream.default_concurrency, 8);
        assert_eq!(config.upstream.max_attempts, 4);
    }

    #[test]
    fn rejects_unsafe_phase_one_limits() {
        let error = Config::from_toml(
            r#"
                chains = [1]
                [server]
                batch_limit = 101
            "#,
        )
        .expect_err("batch limit should fail");
        assert!(error.to_string().contains("batch_limit"));
    }

    #[test]
    fn endpoint_limit_override_wins() {
        let config = Config::from_toml(
            r#"
                chains = [1]
                [[chain_overrides]]
                chain_id = 1
                [[chain_overrides.endpoint_overrides]]
                url = "https://rpc.example"
                rps = 7
                concurrency = 3
            "#,
        )
        .expect("parse config");
        assert_eq!(config.endpoint_limits(1, "https://rpc.example"), (7, 3));
        assert_eq!(config.endpoint_limits(1, "https://other.example"), (15, 8));
    }

    #[test]
    fn repository_config_is_valid() {
        let config = Config::from_toml(include_str!("../config.toml")).expect("repository config");
        assert_eq!(config.chains, [1, 143, 56, 137, 42161, 8453, 10, 43114]);
        for (chain_id, block_time_ms, confirmation_depth, tip_ttl_ms) in [
            (1, 12_000, 64, 2_000),
            (143, 400, 64, 400),
            (56, 750, 64, 750),
            (137, 2_000, 128, 2_000),
            (42161, 250, 64, 250),
            (8453, 2_000, 64, 2_000),
            (10, 2_000, 64, 2_000),
            (43114, 2_000, 32, 2_000),
        ] {
            assert_eq!(config.block_time_ms(chain_id), block_time_ms);
            assert_eq!(config.confirmation_depth(chain_id), confirmation_depth);
            assert_eq!(config.tip_ttl_ms(chain_id), tip_ttl_ms);
        }
    }

    #[test]
    fn partial_config_can_select_one_chain() {
        let config = Config::from_toml("chains = [1]").expect("single-chain config");
        assert_eq!(config.chains, [1]);
        assert!(config.chain_overrides.is_empty());
    }

    #[test]
    fn discovery_enabled_allows_empty_chains() {
        let config = Config::from_toml("chains = []\n[discovery]\nenabled = true")
            .expect("discovery-only config");
        assert!(config.chains.is_empty());
        assert!(config.discovery.enabled);
    }

    #[test]
    fn deny_must_not_overlap_pinned_chains() {
        let error = Config::from_toml(
            r#"
                chains = [1]
                [discovery]
                enabled = true
                deny = [1]
            "#,
        )
        .expect_err("pinned deny overlap must fail");
        assert!(error.to_string().contains("pinned chains"));
    }

    #[test]
    fn discovery_disabled_requires_non_empty_chains() {
        let error = Config::from_toml("chains = []\n[discovery]\nenabled = false")
            .expect_err("empty chains with discovery disabled should fail");
        assert!(error.to_string().contains("chains must not be empty"));
    }

    #[test]
    fn hardening_defaults_are_safe() {
        let config = Config::default();
        assert_eq!(config.server.shutdown_deadline_ms, 10_000);
        assert_eq!(config.server.max_body_bytes, 256 * 1024);
        assert_eq!(config.server.max_concurrent_requests, 1024);
        assert!(!config.server.per_ip_rate_limit.enabled);
        assert_eq!(config.server.metrics_auth_token, None);
    }

    #[test]
    fn hardening_rejects_unsafe_values() {
        assert!(
            Config::from_toml("chains=[1]\n[server]\nmax_body_bytes=0")
                .unwrap_err()
                .to_string()
                .contains("max_body_bytes")
        );
        assert!(
            Config::from_toml("chains=[1]\n[server]\nmax_concurrent_requests=0")
                .unwrap_err()
                .to_string()
                .contains("max_concurrent_requests")
        );
        assert!(
            Config::from_toml("chains=[1]\n[server]\nshutdown_deadline_ms=0")
                .unwrap_err()
                .to_string()
                .contains("shutdown_deadline_ms")
        );
        assert!(
            Config::from_toml(
                "chains=[1]\n[server.per_ip_rate_limit]\nenabled=true\nrequests_per_second=0"
            )
            .unwrap_err()
            .to_string()
            .contains("per_ip_rate_limit")
        );
        assert!(
            Config::from_toml("chains=[1]\n[server]\nmetrics_auth_token=\"\"")
                .unwrap_err()
                .to_string()
                .contains("metrics_auth_token")
        );
    }

    /// 环境变量是进程全局状态，跨测试并发出可能互相干扰；用静态锁串行化。
    fn with_env<'a>(vars: &[(&str, &str)], f: impl FnOnce() + 'a) {
        static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (key, value) in vars {
            // Rust 2024 中设置环境变量是不安全操作，测试内显式授权。
            unsafe { std::env::set_var(*key, *value) };
        }
        f();
        for (key, _) in vars {
            unsafe { std::env::remove_var(*key) };
        }
    }

    #[test]
    fn env_overrides_listen_chains_and_cache() {
        with_env(
            &[
                ("RPCROUTER_LISTEN", "127.0.0.1:9999"),
                ("RPCROUTER_CHAINS", "1, 56,137"),
                ("RPCROUTER_CACHE_MAX_BYTES", "1048576"),
                ("RPCROUTER_METRICS_ENABLED", "0"),
                ("RPCROUTER_CHAINLIST_REFRESH_SECONDS", "120"),
                ("RPCROUTER_CHAINLIST_CACHE_PATH", "/tmp/rpcrs.json"),
            ],
            || {
                let mut config = Config::default();
                config
                    .apply_env_overrides()
                    .expect("env overrides should apply");
                assert_eq!(config.listen.to_string(), "127.0.0.1:9999");
                assert_eq!(config.chains, [1, 56, 137]);
                assert_eq!(config.cache.max_bytes, 1_048_576);
                assert!(!config.metrics_enabled);
                assert_eq!(config.chainlist.refresh_seconds, 120);
                assert_eq!(
                    config.chainlist.cache_path,
                    std::path::PathBuf::from("/tmp/rpcrs.json")
                );
            },
        );
    }

    #[test]
    fn env_overrides_admin_settings() {
        with_env(
            &[
                ("RPCROUTER_ADMIN_TOKEN", "secret"),
                ("RPCROUTER_ADMIN_STATIC_DIR", "/tmp/dashboard"),
                ("RPCROUTER_ADMIN_PUBLIC_SITE", "false"),
            ],
            || {
                let mut config = Config::default();
                config
                    .apply_env_overrides()
                    .expect("admin env overrides should apply");
                assert_eq!(config.admin.auth_token.as_deref(), Some("secret"));
                assert_eq!(
                    config.admin.static_dir,
                    Some(std::path::PathBuf::from("/tmp/dashboard"))
                );
                assert!(!config.admin.public_site);
            },
        );
    }

    #[test]
    fn hardening_accepts_configured_values() {
        let config = Config::from_toml(
            r#"
                chains = [1]
                [server]
                shutdown_deadline_ms = 5000
                max_body_bytes = 1048576
                max_concurrent_requests = 64
                metrics_auth_token = "secret"
                [server.per_ip_rate_limit]
                enabled = true
                requests_per_second = 50
                burst = 100
            "#,
        )
        .expect("valid hardening config");
        assert_eq!(config.server.shutdown_deadline_ms, 5_000);
        assert_eq!(config.server.max_body_bytes, 1_048_576);
        assert_eq!(config.server.max_concurrent_requests, 64);
        assert!(config.server.per_ip_rate_limit.enabled);
        assert_eq!(config.server.metrics_auth_token.as_deref(), Some("secret"));
    }

    #[test]
    fn env_empty_value_is_ignored() {
        with_env(
            &[("RPCROUTER_LISTEN", ""), ("RPCROUTER_CHAINS", "")],
            || {
                let mut config = Config::default();
                config
                    .apply_env_overrides()
                    .expect("empty env should be ignored");
                assert_eq!(config.listen.to_string(), "0.0.0.0:8545");
                assert_eq!(config.chains, [1, 143]);
            },
        );
    }

    #[test]
    fn env_invalid_values_are_rejected() {
        with_env(&[("RPCROUTER_CACHE_MAX_BYTES", "not-a-number")], || {
            let mut config = Config::default();
            let error = config
                .apply_env_overrides()
                .expect_err("invalid cache size should fail");
            assert!(error.to_string().contains("RPCROUTER_CACHE_MAX_BYTES"));
        });
    }

    #[test]
    fn invalid_public_site_env_is_rejected() {
        with_env(&[("RPCROUTER_ADMIN_PUBLIC_SITE", "maybe")], || {
            let mut config = Config::default();
            let error = config
                .apply_env_overrides()
                .expect_err("invalid public site boolean");
            assert!(error.to_string().contains("RPCROUTER_ADMIN_PUBLIC_SITE"));
        });
    }
}
