# rpcrouter 运维手册（OPERATIONS）

面向生产/长期运行的操作指引。覆盖：监控指标与告警语义、Grafana 使用、soak 长跑用法、
以及常见故障排查。所有指标名均以 `src/metrics.rs` 实际导出为准；改动指标时请同步更新
`ops/prometheus/alerts.yml` 与 `ops/grafana/dashboards/rpcrouter-overview.json`。

---

## 1. 监控栈总览

| 组件 | 暴露端口 | 用途 |
|---|---|---|
| 网关 `/metrics` | 8545（同网关） | Prometheus 文本格式指标 |
| Prometheus | 9090 | 抓取 /metrics、求值告警 |
| Grafana | 3000 | 可视化仪表盘（自动加载 provisioning） |

用 `docker compose --profile monitoring up -d` 一键拉起 Prometheus + Grafana 监控栈，
网关 `rpcrouter` 服务由基础 profile 启动。监控组件依赖：

- `./ops/prometheus/prometheus.yml` → 容器 `/etc/prometheus/prometheus.yml`
- `./ops/prometheus/alerts.yml`   → 通过 rule_files 加载（与 prometheus.yml 同目录）
- `./ops/grafana/provisioning/`   → 容器 `/etc/grafana/provisioning/`
- `./ops/grafana/dashboards/`     → 容器 `/var/lib/grafana/dashboards/`

Grafana 默认登录：`admin / admin`（compose 内 `GF_SECURITY_ADMIN_PASSWORD=admin`）。

## 2. 指标字典（与 src/metrics.rs 一致）

按标签维度组织。全部为 Prometheus 计数器（Counter，累加，用 `rate()` 看速率）或 Gauge。

| 指标 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `rpcrouter_chain_ingress_requests_total` | Counter | `chain_id` | 入口 JSON-RPC 请求总数；`rate()` 即 QPS |
| `rpcrouter_ingress_rejected_total` | Counter | `reason` | **入口防护**拒绝计数（见 §3 语义边界） |
| `rpcrouter_in_flight_requests` | Gauge | — | 入口在飞请求数（过载信号，RAII 保证回落） |
| `rpcrouter_cache_lookups_total` | Counter | `chain_id` | 可缓存请求的查找次数 |
| `rpcrouter_cache_hits_total` | Counter | `chain_id` | 响应缓存命中数 |
| `rpcrouter_cache_misses_total` | Counter | `chain_id` | 缓存未命中、进入 singleflight |
| `rpcrouter_cache_hit_ratio` | Gauge | `chain_id` | 命中率 = hits/lookups（0..1） |
| `rpcrouter_coalesced_requests_total` | Counter | `chain_id` | 由 in-flight leader 服务的折叠跟随者 |
| `rpcrouter_coalesce_ratio` | Gauge | `chain_id` | 折叠占比 = coalesced/misses |
| `rpcrouter_chain_upstream_requests_total` | Counter | `chain_id,endpoint` | 数据面上游请求（`rate()` = 上游 QPS） |
| `rpcrouter_user_visible_errors_total` | Counter | `chain_id` | **上游承诺**失败（请求耗尽所有端点） |
| `rpcrouter_cold_start_failures_total` | Counter | `chain_id` | 冷启动失败（请求时没有 Active 端点）；不计入 UVE |
| `rpcrouter_request_latency_seconds` | Histogram | `chain_id` | 端到端请求延迟（p50/p99 用 `histogram_quantile`） |
| `rpcrouter_failover_depth` | Histogram | `chain_id` | 完成前失败上游尝试次数 |
| `rpcrouter_hedge_attempts_total` | Counter | `chain_id` | 二次 hedge 请求数 |
| `rpcrouter_fallback_attempts_total` | Counter | `chain_id` | 公共池失败后的付费兜底尝试次数。标签不含 URL |
| `rpcrouter_fallback_successes_total` | Counter | `chain_id` | 付费兜底返回了可用响应的次数 |
| `rpcrouter_fallback_failures_total` | Counter | `chain_id` | 付费兜底尝试失败 |
| `rpcrouter_fallback_skipped_total` | Counter | `chain_id,reason` | 配了兜底但没发出。reason ∈ `deadline`/`unavailable`/`cooling`/`no_token` |
| `rpcrouter_fallback_latency_seconds` | Histogram | `chain_id` | 兜底那一次的耗时 |
| `rpcrouter_endpoints_by_archive` | Gauge | `chain_id,status` | 已物化端点的归档结论。status ∈ `yes`/`no`/`unknown`。含付费兜底，不含 URL |
| `rpcrouter_archive_probes_total` | Counter | `chain_id,result` | 归档探测轮次。result ∈ `yes`/`full_node`/`no`/`unknown`。`full_node` 是近处能解析内部交易、远处被裁掉 |
| `rpcrouter_archive_probe_latency_seconds` | Histogram | `chain_id` | 作出归档结论的那一次历史读取耗时。不进用户请求延迟 |
| `rpcrouter_hedge_ratio` | Gauge | `chain_id` | hedge 占比 = hedges/upstream |
| `rpcrouter_endpoint_requests_total` | Counter | `chain_id,endpoint` | 端点全部请求（含健康探针） |
| `rpcrouter_endpoint_rate_limited_total` | Counter | `chain_id,endpoint` | 端点收到的限频响应 |
| `rpcrouter_endpoint_cooling_events_total` | Counter | `chain_id,endpoint` | 端点进入 Cooling 的次数 |
| `rpcrouter_endpoint_state` | Gauge（单热） | `chain_id,endpoint,state` | state ∈ `active`/`cooling`/`probation`，当前态=1 |
| `rpcrouter_chains` | Gauge | `state` | pinned/hot/dormant/disabled 生命周期链数量 |
| `rpcrouter_chain_pinned` | Gauge | `chain_id` | materialized 链是否 pinned（1/0） |
| `rpcrouter_catalog_chains` | Gauge | — | 目录链数量（含 dormant/0 端点链） |
| `rpcrouter_catalog_endpoints` | Gauge | — | 目录过滤后的公开端点数量 |
| `rpcrouter_catalog_records_skipped_total` | Counter | — | 容错解析时跳过的畸形 chainlist 记录累计数 |
| `rpcrouter_probe_queue_depth` | Gauge | — | 等待探测的有界队列深度 |
| `rpcrouter_probe_in_flight` | Gauge | — | 当前在飞探针数 |
| `rpcrouter_chainlist_last_refresh_timestamp_seconds` | Gauge | — | 最近一次新鲜 chainlist 刷新 Unix 时间 |
| `rpcrouter_chainlist_refresh_total` | Counter | `source` | network/not_modified/memory/disk/fixture/rejected 刷新次数 |
| `rpcrouter_chain_activations_total` | Counter | — | dormant → hot 激活次数 |
| `rpcrouter_chain_demotions_total` | Counter | `reason` | idle/lru/admin 降级次数 |
| `rpcrouter_state_store_up` | Gauge | — | 持久状态存储可达性（1/0） |
| `rpcrouter_state_flush_total` | Counter | `result` | write-behind flush 成功/失败 |
| `rpcrouter_state_flush_duration_seconds` | Histogram | — | flush 延迟 |
| `rpcrouter_state_dirty_endpoints` | Gauge | — | 等待 flush 的脏端点数 |

### 3. 语义边界：三类失败互斥（重要）

这是本项目最容易混淆的一对，代码注释与告警注释均强调：

- **`rpcrouter_user_visible_errors_total`** 是**上游承诺**指标：请求**已进入数据面转发**且至少有一个 Active 端点，
  但所有上游端点耗尽，调用方收到 `-32000`。它是项目对调用方唯一「硬承诺」的成败指标。
- **`rpcrouter_cold_start_failures_total`** 是冷启动失败指标：请求进入数据面时没有 Active 端点，
  仅尝试 Probation/可恢复池或全部 Cooling，失败时仍返回 HTTP 200/-32000，但错误体附带
  `data.reason="cold_start"`。
- **`rpcrouter_ingress_rejected_total`** 是**入口侧防护**指标：请求在转发**之前**被入口层拒绝。
  原因（`reason` 标签）：
  - `overload`：全局并发超限（默认 1024 在飞），HTTP 503。
  - `body_too_large`：请求体超过 `server.max_body_bytes`（默认 256 KiB），HTTP 413。
  - `rate_limited`：每 IP 限速命中（可选开关），HTTP 429。
  - `unknown_chain`：链不在目录，HTTP 404。
  - `no_endpoints`：已知链没有公开端点，HTTP 503。
  - `chain_disabled`：链被 deny/运行时禁用，HTTP 403。

三者完全独立、不可混算：入口拒绝、冷启动失败、上游承诺失败分别只进入各自指标。
查询「调用方是否受损」只看 `user_visible_errors`；
查询「网关是否过载/配置过紧」看 `ingress_rejected`。Grafana 也分面板展示。

## 4. 告警规则（ops/prometheus/alerts.yml）

| 告警 | 触发表达式 | 严重度 | 含义 |
|---|---|---|---|
| `RpcrouterUserVisibleErrors` | `sum by(chain_id)(rate(rpcrouter_user_visible_errors_total[2m])) > 0` 持续 2m | critical | 上游承诺失败，链端点全不可用 |
| `RpcrouterChainActiveEndpointsLow` | `sum by(chain_id)(rpcrouter_endpoint_state{state="active"}) < 2 and on(chain_id) rpcrouter_chain_pinned == 1` 持续 5m | warning | pinned 链 active 端点不足；动态链不触发 |
| `RpcrouterCacheHitRatioDropped` | `max_over_time(rpcrouter_cache_hit_ratio[1m]) < 0.8` 持续 5m | warning | 缓存命中率骤降，数据面压力放大 |
| `RpcrouterUpstreamRateLimitedSpike` | 429 速率 / 上游速率 > 10% 持续 5m | warning | 上游限频占比突增 |
| `RpcrouterIngressRejectionsSpiking` | `sum by(reason)(rate(rpcrouter_ingress_rejected_total[3m])) > 0` 持续 3m | warning | 入口防护持续拒绝请求 |
| `RpcrouterChainlistRefreshStale` | `time() - rpcrouter_chainlist_last_refresh_timestamp_seconds > 7200` 持续 5m | warning | 目录成功刷新已超过 2 小时 |
| `RpcrouterProbeQueueBackingUp` | `rpcrouter_probe_queue_depth > 128` 持续 5m | warning | 探针有界队列持续积压 |

阈值默认值均附中文注释理由（`alerts.yml` 内）。修改后可用 `promtool` 校验（见 §6）。

## 5. Grafana 使用

1. 拉起监控栈：`docker compose --profile monitoring up -d`。
2. 打开 `http://<host>:3000`，登录 `admin/admin`。
3. 进入仪表盘 **rpcrouter 网关总览**（provisioning 自动注册，无需手动 import）：
   - **总 QPS（入口）**：全量 + 分链 `rpcrouter_chain_ingress_requests_total` 的 `rate()`。
   - **缓存命中率**：`rpcrouter_cache_hit_ratio` 按链，低于 80% 会显示黄色/红色阈值。
   - **每链 active 端点数**：`sum(rpcrouter_endpoint_state{state="active"}) by(chain_id)`，低于 2 显示红色。
   - **上游错误分类**：限频速率 + 冷却事件速率。
   - **延迟 p50/p99**：`histogram_quantile` over `rpcrouter_request_latency_seconds_bucket`。
   - **入口拒绝速率**：`rpcrouter_ingress_rejected_total` 按 `reason` 分。
   - **在飞请求数**、**用户可见错误**：独立面板。
4. 数据源 `Prometheus` 指向 `http://prometheus:9090`（provisioning 自动配置，默认数据源）。
5. 改面板：UI 改后 `allowUiUpdates` 允许写回，但 `updateIntervalSeconds` 会周期性重新同步
   文件，持久化改动请直接编辑 JSON 并重启 grafana 服务。

## 6. 校验监控文件

```sh
# promtool 校验告警规则与抓取配置（镜像 entrypoint 是 prometheus，需显式换成 promtool）
docker run --rm --entrypoint promtool -v "$PWD/ops/prometheus:/etc/prometheus:ro" \
  prom/prometheus:latest check rules /etc/prometheus/alerts.yml
docker run --rm --entrypoint promtool -v "$PWD/ops/prometheus:/etc/prometheus:ro" \
  prom/prometheus:latest check config /etc/prometheus/prometheus.yml

# 校验 compose（含 monitoring profile 的挂载路径）
docker compose --profile monitoring config
```

## 7. soak 长跑（scripts/soak.sh）

低 QPS（≤5）真实网络长跑，观察端点摘除/回池分布与内存曲线。与 `loadtest.sh` 的高压互补。

```sh
# 默认指向本机已运行的网关，跑 1 小时、QPS 5、链 1/143/56
scripts/soak.sh

# 显式参数：目标地址、时长、QPS、链、网关 PID、采样间隔
scripts/soak.sh --url http://127.0.0.1:8545 --duration 86400 --qps 3 \
  --chains 1,143,56,137 --pid "$(pgrep -f rpcrouter | head -n1)" --interval 60 --out data/soak
```

参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--url` | `http://127.0.0.1:8545` | 网关入口 base URL |
| `--duration` | `3600` | 长跑时长（秒） |
| `--qps` | `5` | 每链负载上限（1..=5，超过报错） |
| `--chains` | `1,143,56` | 逗号分隔的 chain_id 列表 |
| `--pid` | 自动 pgrep | 网关进程 PID（用于 RSS 采样） |
| `--interval` | `15` | 采样间隔（秒） |
| `--method` | `eth_blockNumber` | 压测的 JSON-RPC 方法 |
| `--out` | `data/soak` | 输出目录 |

产出文件：

- `metrics-<ts>.txt`：每次采样的原始 `/metrics` 快照。
- `rss.csv`：`timestamp,rss_kib` 内存曲线数据。
- `events.csv`：端点摘除（`removal`）与回池（`return_to_pool`）事件，格式 `ts,chain_id,event,endpoint`。
- `summary.json`：结束时汇总——请求 ok/error/拒绝、`user_visible_errors`/`ingress_rejected`
  增量、端点摘除/回池事件计数、内存曲线 min/max/last。

## 8. 常见故障排查

| 症状 | 排查 |
|---|---|
| `user_visible_errors` 告警 | 该链所有上游端点耗尽。查 `rpcrouter_endpoint_state` 看是否大范围 cooling/probation；查 `rpcrouter_endpoint_rate_limited_total` 是否上游集体限频；查 `chainlist` 刷新后端点池是否锐减。 |
| `ingress_rejected{reason="overload"}` | 在飞请求超过 `max_concurrent_requests`。查 `rpcrouter_in_flight_requests` 是否长期贴近上限；是则提高并发或拆多实例。 |
| `ingress_rejected{reason="body_too_large"}` | 客户端请求体超 `max_body_bytes`（默认 256 KiB）。属正常防护，若合法客户端频繁触发说明上游 batch 过大，需客户端拆批。 |
| `ingress_rejected{reason="rate_limited"}` | 单 IP 触达每 IP 限速。属预期防护；误伤则上调 `per_ip_rate_limit`。 |
| cache 命中率骤降 | 工作负载是否涌入冷数据/非幂等请求；`immutable_ttl_seconds` 是否被误调小；缓存容量 `max_bytes` 是否过小导致频繁逐出。 |
| 延迟 p99 上涨 | 查 `rpcrouter_failover_depth`（失败重试是否变多）、`rpcrouter_hedge_ratio`（hedge 占比）、上游端点 slow/5xx 是否变多。 |
| 上游 429 率突增 | 是否触达端点 rps 上限（`default_rps` 或 `endpoint_overrides`）；用 `rpcrouter_endpoint_rate_limited_total` 按 endpoint 定位是哪个端点。 |
| `/metrics` 抓取失败/401 | compose 默认未鉴权；若生产启用了 `server.metrics_auth_token`，需在 `prometheus.yml` 的 scrape 加 `authorization` 头（见文件内注释）。 |
| soak 无 RSS 数据 | `--pid` 未生效或 `/proc/<pid>/status` 不可读（跨容器 PID 命名空间差异）；确认用宿主机侧 PID。 |

## 9. 状态存储

Redis 是默认持久镜像，key 使用 `{namespace}:` 前缀。内存仍是数据面唯一真相，响应缓存不
写入 Redis。`required=false` 时 Redis 断连会自动切换到 `data/state.json`，告警
`RpcrouterStateStoreDown`，重连后执行完整覆写；`required=true` 在启动阶段直接失败。

`RPCROUTER_STATE_RESET=1`（或启动参数等价配置）会清空当前 namespace；`export/import/reset`
库 API 用于运维脚本。健康快照按 `health_ttl_seconds` 过期，冷却中的端点重启恢复为 Cooling，
其余端点恢复为 Probation，Active 必须重新通过探针。

### 运行时控制

Admin API 的写操作先写入 StateStore，成功后才更新内存运行态；收到
`503 state_store_unavailable` 时应先检查 Redis/File 镜像与
`rpcrouter_state_store_up`。常用操作：

```sh
curl http://127.0.0.1:8545/admin/api/chains/1
curl -X POST -H 'Authorization: Bearer secret' \
  -H 'Content-Type: application/json' -d '{"url":"https://rpc.example"}' \
  http://127.0.0.1:8545/admin/api/chains/1/endpoints/disable
curl -X PUT -H 'Authorization: Bearer secret' \
  -H 'Content-Type: application/json' -d '{"tipTtlMs":1000}' \
  http://127.0.0.1:8545/admin/api/chains/1/settings
```

未配置 `admin.auth_token` 时只读 GET 可用，写操作固定返回 403；生产环境应配置
token、TLS 和反向代理限流。变更前用 `state/export` 备份，`state/import` 恢复，
`state/reset` 必须显式提交 `{"confirm":true}`。每次控制操作追加审计记录。

## 10. 扩容/缩容与故障接管

使用 `docker compose --profile cluster up -d redis rpcrouter-1 rpcrouter-2 rpcrouter-3 nginx`
启动 nginx（宿主机 18545）+ 3 实例。扩容流程：先加入新实例、
健康检查 `/healthz`、再 reload nginx；缩容流程：先从 upstream 摘除、等待连接排空后停止。
一致性哈希只迁移约 `1/N` 链；实例故障由 nginx `proxy_next_upstream` 交给环上下一实例，
该实例从 Redis 健康镜像获知冷却端点并走冷启动 Probation 路径。
