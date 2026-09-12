# rpcrouter

rpcrouter 是面向 EVM 链的 JSON-RPC 路由网关。它从 Chainlist 汇集无需 API key 的公开 RPC，
在每条链内按健康度选点，并把上游限频、超时和故障通过冷却、换点重试与只读请求 hedging
对调用方隐藏。响应缓存和 in-flight 折叠承接高重复读流量，使公开节点池只处理少量未命中；
所有端点同时不可用时，网关才返回 `-32000`。

仓库随附的 `config.toml` 启用 Ethereum、Monad、BSC、Polygon、Arbitrum One、Base、
OP Mainnet 和 Avalanche C-Chain。项目仅代理 HTTP JSON-RPC，不提供 WebSocket、鉴权或计费。

## 工作原理

请求按 `chainId` 进入独立端点池。缓存把确定老块、链 tip 和不可缓存方法分开处理；相同缓存键
的并发 miss 只由一个 leader 请求上游。Active 端点使用 P2C 评分选点，评分综合延迟 EWMA 与
块高滞后。429、quota、HTML、5xx、超时等端点故障会触发指数冷却，冷却到期后须连续通过两次
探针才能回池；请求参数错误、`execution reverted` 等链级错误则原样透传。

每个端点默认受 15 rps 令牌桶和 8 个并发位保护。探针也计入同一额度，网关不会通过伪装或
激进重试规避公共服务条款。

## 快速开始

需要 Rust 1.97 或更高版本。仓库内配置可以直接启动：

```sh
cargo run --release --bin rpcrouter
```

默认监听 `0.0.0.0:8545`。启动后，端点先处于 Probation；完成健康探针后才承接流量。

```sh
curl -sS http://127.0.0.1:8545/rpc/1 \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

用 `RPCROUTER_CONFIG=/path/to/config.toml` 指定其他配置文件；`RUST_LOG` 控制日志级别。

## 配置

根级 `listen`、`metrics_enabled` 和 `chains` 分别控制监听地址、指标端点和链允许清单。
主要配置段如下：

| 配置段 | 用途 |
|---|---|
| `server` | JSON-RPC batch 上限（最大 100） |
| `chainlist` | 数据源、1 小时刷新、陈旧宽限和磁盘缓存路径 |
| `discovery` | 动态目录、测试网、deny、热链上限与 idle 降级；关闭时仅服务 pinned 链 |
| `discovery.auto_enable` | 自动开启优质主网链：静态门槛、候选池与探测预算、晋级轮次、集合上限 |
| `upstream` | 单次/总超时、重试次数、默认端点 rps 与并发限制 |
| `probe` | 15–30 秒探针抖动、全局并发和允许块高滞后 |
| `cache` | 按响应字节加权的容量（默认 512 MiB）和不可变 TTL |
| `hedging` | 只读请求第二发延迟、全局占比和健康池门槛 |
| `chain_overrides` | 每链块时间、确认深度 K、tip TTL、附加/屏蔽端点及端点限额；可用于非 pinned 链 |
| `state` | Redis/File/Memory 状态镜像、命名空间、required、flush 周期与健康快照 TTL |
| `admin` | Admin API 开关、公共主页开关、Bearer token、SPA 静态目录与 CORS 来源 |

`chains` 是 pinned 链列表：启动即激活且永不因 idle/LRU 降级。动态目录链首个请求才激活，
随后无流量自动降级为 dormant；`discovery.deny` 链返回 403。

容器部署可用 `RPCROUTER_DISCOVERY_ENABLED`、`RPCROUTER_DISCOVERY_MAX_HOT_CHAINS` 和
`RPCROUTER_DISCOVERY_IDLE_SECONDS` 覆写动态目录策略。

状态存储环境变量：`RPCROUTER_STATE_BACKEND=redis|file`、`RPCROUTER_REDIS_URL`、
`RPCROUTER_STATE_NAMESPACE`、`RPCROUTER_STATE_RESET=1`。默认 Redis 不可达时自动降级为
内存 + `data/state.json`，不会中断 RPC 流量；`RPCROUTER_STATE_REQUIRED=true` 用于必须持久化的部署。
管理面可用 `RPCROUTER_ADMIN_TOKEN`、`RPCROUTER_ADMIN_STATIC_DIR` 与 `RPCROUTER_ADMIN_PUBLIC_SITE` 覆写 token、SPA 目录和公共主页开关。

仓库配置使用偏保守的缓存确认深度。BSC 按 Maxwell 升级后的约 750ms 出块配置；Polygon
约 2s、Arbitrum 约 250ms，Base、OP 与 Avalanche 约 2s。tip TTL 不超过对应块时间和 2s。
附加公开端点前应先确认其服务条款，并通过 `endpoint_overrides` 下调供应商声明的额度。

## HTTP 接口

- `POST /rpc/{chainId}`：接收 JSON-RPC 2.0 单请求或 batch；batch 拆分并发执行后按输入保序。
- `GET /chains`：返回各链端点总数、Active/Cooling/Probation 数量、生命周期 `state` 及 head。
- `GET /healthz`：进程存活时返回 `{"status":"ok"}`；不代表任一上游当前可用。
- `GET /metrics`：Prometheus 文本指标；`metrics_enabled = false` 时返回 404。
- `GET /api/public/overview`：无需登录的公共运行概览，返回服务链数、活跃端点和流量摘要。
- `GET /api/public/chains`：无需登录的只读链目录，支持 `q`、`testnet`、`state`、`sort`、`limit`、`offset`；disabled 链不可见。
- `GET /api/public/chains/{id}`：无需登录的单链接入信息；未知或 disabled 链返回 404。公共响应只含裁剪后的链摘要，且带 `Cache-Control: public, max-age=5`。

`/metrics` 包含按链的入口、缓存命中/折叠、上游、用户可见错误、延迟、failover 和 hedge
指标，以及按端点的请求、429、冷却事件与状态。JSON-RPC 上游耗尽仍使用 HTTP 200，响应体为
code `-32000` 的标准 JSON-RPC error。
冷启动时若尚无 Active 端点，耗尽错误会在同一错误体中附带 `data.reason="cold_start"`，且不计入用户可见错误承诺指标。

未知链返回 HTTP 404，目录中无公开端点的已知链返回 HTTP 503，deny/disabled 链返回 HTTP
403；这些入口拒绝计入 `ingress_rejected{reason}`，不计入 `user_visible_errors`。

## 性能验证

Phase 3 本机离线压测以 10,000 QPS 调度 60 秒，共 600,000 次请求：实际
9,999.836 QPS，hit + fold 为 99.9948%，p99 4.126ms，用户可见错误为 0，且 mock 端点
峰值 3 QPS，未超过 15 rps 上限。环境、方法和 429 摘除/回池时间线见
[Phase 3 压测报告](docs/reports/loadtest-phase3.md)。

## Admin API

`admin.enabled` 默认开启；未配置 token 时 GET 只读接口开放，所有写操作返回
`403 admin_disabled`。配置 token 后所有 `/admin/api/*` 请求必须携带
`Authorization: Bearer <token>`。管理接口不会在 `/rpc` 请求路径访问状态存储。

```sh
curl http://127.0.0.1:8545/admin/api/overview
curl -H 'Authorization: Bearer secret' \
  'http://127.0.0.1:8545/admin/api/chains?state=dormant&limit=200'
curl -X POST -H 'Authorization: Bearer secret' \
  -H 'Content-Type: application/json' \
  -d '{"confirm":true}' http://127.0.0.1:8545/admin/api/state/reset
```

设置 `admin.static_dir` 后 `/dashboard/` 与任意不存在的 dashboard 路径均回退到
`index.html`，用于托管独立 React dashboard；静态资源不要求 token。`admin.public_site = true`
（默认）时，`/` 与 `/chain/{id}` 也返回同一个 SPA 入口，公共页无需 token；设为 `false`
可只保留运维后台。公共主页负责普通开发者浏览链目录和 curl 接入示例，`/dashboard/*`
仍是需要 bearer token 的运维控制台。

### Dashboard

Dashboard 是 `dashboard/` 下的独立 React/Vite 工程。开发时运行 `npm ci && npm run dev`
（默认把 `/admin` 与 `/api` 代理到本机 8545，可用 `VITE_API_BASE` 覆写）；构建运行
`npm run build`，产物位于 `dashboard/dist`。Dockerfile 会在 node 构建阶段生成该产物，
并以 `RPCROUTER_ADMIN_STATIC_DIR=/app/dashboard` 默认由网关托管 `/dashboard/`，也可将
静态目录交给任意 Web 服务器并反代 `/admin/api`。在 Settings 页面输入 Bearer token；
token 只存浏览器 localStorage，并仅通过 Authorization 请求头发送。
由于 Vite `base=/dashboard/`，`vite dev` 访问根路径会重定向到 `/dashboard/`；公共页本地预览请使用
`vite preview` 配合网关，或直接连接已构建的后端静态托管入口。

## 部署

本仓库提供 Docker、docker-compose 与 systemd 三种部署方式。三种方式都支持用
`RPCROUTER_*` 环境变量覆写配置关键项（listen 地址、启用链、缓存容量等），详见
`src/config.rs` 的 `apply_env_overrides`。

### 单条 docker 命令

多阶段构建已封装在 `Dockerfile` 中（rust:1.97 builder → debian:bookworm-slim），
运行时挂载 `data/` 卷以复用 chainlist 磁盘缓存：

```sh
# 先构建镜像
docker build -t rpcrouter:latest .

# 一条命令起服务：仅暴露 8545，data 卷跨重启复用缓存
docker run -d --name rpcrouter \
  -p 8545:8545 \
  -v rpcrouter-data:/app/data \
  rpcrouter:latest
```

如需通过环境变量调整启用链：

```sh
docker run -d --name rpcrouter \
  -p 8545:8545 \
  -e RPCROUTER_CHAINS=1,56,137 \
  -e RPCROUTER_CACHE_MAX_BYTES=268435456 \
  -v rpcrouter-data:/app/data \
  rpcrouter:latest
```

### docker-compose

`docker-compose.yml` 提供网关服务（含 data 卷与环境变量覆写示例）：

```sh
docker compose up -d
# 含 Prometheus/Grafana 监控（需先由 ops 工作流提供 ./ops/ 下文件）：
docker compose --profile monitoring up -d
```

### 多实例分片

方案 A 使用 nginx 对 `/rpc/{chainId}` 做一致性哈希，3 个无状态网关共享同一 Redis：

```sh
docker compose --profile cluster up -d --build redis rpcrouter-1 rpcrouter-2 rpcrouter-3 nginx
# 分片入口：http://127.0.0.1:18545/rpc/{chainId}
```

compose 使用 `deploy/nginx-shard-compose.conf`，独立部署模板为 `deploy/nginx-shard.conf`。
扩容时在对应文件增加实例并 reload；每条链固定落一个实例，故缓存、
in-flight 折叠和端点限流不会按实例数放大。Redis 使用 appendonly 卷，故障时实例会用本地
镜像继续出流量，恢复后自动回灌。

### systemd

样例位于 `deploy/rpcrouter.service`。安装后 `Restart=always` 保证异常退出自动拉起，
`LimitNOFILE=65536` 消除高并发下的 fd 瓶颈（压测验证的关键点）：

```sh
sudo cp deploy/rpcrouter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rpcrouter
```

## 部署注意事项

- 使用 `cargo build --release --bin rpcrouter`，以受限的非 root 账户运行，并将配置和
  `data/rpcs.json` 所在目录持久化；滚动前先通过 `/healthz` 和 `/chains` 检查实例状态。
- 高并发部署应提高文件描述符上限，例如 shell 的 `ulimit -n 65535` 或 systemd 的
  `LimitNOFILE=65535`，同时确认反向代理的连接池、请求体大小和超时不少于网关 deadline。
- 只在可信网络暴露管理接口；如需公网服务，应在前置代理实现 TLS、访问控制和入口限流。
- release 构建不会改变对外保护：诚实发送 `rpcrouter/<version>` User-Agent，默认严格限制
  单端点 15 rps/8 并发，429 时退避换点。运营者应尊重每个公共 RPC 的条款，不把多端点聚合
  当作绕过单一供应商配额的手段。

### 自动开启优质链（W9）

启用 `discovery.auto_enable` 后，网关会从动态目录中自动发现并开启优质 EVM 主网链。默认档位为：去重后的公开 HTTPS 端点至少 5 个，每链每轮最多抽样 8 个端点，连续 2 轮至少 2 个端点通过 `eth_chainId` 与区块高度校验后晋级。候选扫描使用有界并发和本地探针结果，不依赖 Prometheus；状态通过 Admin API 和 Dashboard 的候选视图展示。

自动集合遵循只增不减策略，达到 `max_chains` 后停止新增且不会淘汰已开启链。需要移除链时，请在 Dashboard 的链列表执行人工 Unpin 或 Disable；人工墓碑会阻止后续自动重新加入。可通过 `RPCROUTER_AUTO_ENABLE_ENABLED`、`RPCROUTER_AUTO_ENABLE_MIN_ENDPOINTS` 和 `RPCROUTER_AUTO_ENABLE_MAX_CHAINS` 调整开关与档位。
