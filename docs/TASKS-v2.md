# TASKS v2 — 动态全链目录 + 状态控制 Dashboard（dsh 执行，主会话验收）

> 统一门槛（每个工作流交付时必须全绿）：
> Rust：`cargo fmt --check` && `cargo clippy --all-targets -- -D warnings` && `cargo test`
> （本机用 `rustup override set 1.97.0`，系统默认 1.95 会被 cargo 拒绝）。
> 前端：`npm run lint` && `npm run typecheck` && `npm test` && `npm run build`。
> **测试禁止访问外网**：chainlist 用 `fixtures/`，上游行为用进程内 mock；前端测试 mock fetch。
> 实现细节以 `docs/DESIGN-v2.md` 为准（v1 部分见 `docs/DESIGN.md`），本文只定范围与验收。
> 流程：maker 在独立 worktree 分支实现 → checker 单轮对抗审查 → maker 修 must-fix →
> 主会话合入 main。提交英文 conventional 风格，粒度按逻辑步骤。

## W5 — 动态目录与链生命周期（分支 `w5-dynamic-chains`）✅ 2026-08-25 合入 main（25 commit，
112 用例 + 3 ignored；验收 a–g 全过；真实网络 80 链 smoke 见 docs/reports/loadtest-w5.md）

范围（DESIGN-v2 §1–§5、§8）：
1. `chainlist`：`parse_and_filter` 升级为解析**全部链**为 `Catalog`（保留 name/shortName/
   chain/slug/isTestnet/nativeCurrency.symbol/explorers[0].url/status/tvl、端点 `tracking`），
   过滤规则不变；`discovery.enabled=false` 时只保留 pinned 链。刷新状态（source/时间/etag/
   错误/进行中）可查询；提供手动刷新入口（供 W6 调用），与周期刷新互斥。
   默认 `refresh_seconds` 改 3600（config.toml、docker-compose、README 同步）。
2. `config`：`[discovery]`（enabled/include_testnets/deny/max_hot_chains/idle_seconds）+
   环境变量覆写；`chains` 语义升级为 pinned；`chain_overrides` 不再要求 chain_id 在
   `chains` 内；校验规则见 DESIGN-v2 §5。
3. `registry`：Catalog 持有与整体替换；ChainState 增加 pinned/disabled/last_ingress；
   `resolve_for_request`（热路径廉价、dormant 惰性 materialize）；activate/demote/pin/
   unpin/set_disabled；housekeeping（idle 降级 + LRU 上限，pinned 永不淘汰）；
   `hot_chain_ids`；summaries 增加 `state`。
4. `server`/`forward`：路由层解析链一次；未知链 404、无端点 503、禁用 403（JSON-RPC 错误体 +
   `ingress_rejected{reason}`，**不计** `user_visible_errors`）；`Classifier` 对任意链
   给默认参数。
5. `probe`：调度对象改为 hot 链；有界工作池；激活 kick；队列/在飞指标。
6. `metrics`：DESIGN-v2 §8 新指标；`ops/prometheus/alerts.yml` 的 ActiveEndpointsLow 只盯
   pinned 链；Grafana 仪表盘加「链生命周期/目录/探针」行；`docs/OPERATIONS.md` 指标字典同步。
7. fixture：`fixtures/rpcs.sample.json` 扩充（从本机 `data/rpcs.json` 裁剪，≤80KB）：
   含现有 8 链、≥2 条 testnet、≥1 条 0 端点链、≥1 条带 status 的链、带 `tracking` 的端点、
   `${KEY}` 模板与 wss 条目（用于过滤断言）。现有 `fixture_covers_every_repository_chain`
   等测试保持通过。
8. 文档：README「配置/HTTP 接口」段落更新；`docs/DESIGN-v2.md` 如有实现偏差回写说明。

验收（全部离线）：
- a) 目录解析：fixture 全部链进目录（含 0 端点链），端点过滤/去重断言，testnet 计数正确；
- b) 生命周期：dormant 链首个请求成功（进程内 mock 上游，冷启动 Probation 路径）并变 hot；
     idle 超时后降级（可注入时钟或把 idle_seconds 设为 1s 真等）；超过 max_hot_chains 时
     LRU 淘汰且 pinned 不被淘汰；再次请求可重新激活；
- c) 语义边界：未知链 → 404 + `-32000` 体，`user_visible_errors` 不增、`ingress_rejected
     {reason="unknown_chain"}` +1；0 端点链 → 503；deny 链 → 403；batch 请求同样只解析一次；
- d) 广度：**50 条链冷启动用例**——50 个进程内 mock 上游（各自返回不同 eth_chainId），
     目录 50 链均 dormant，并发各打 1 个 `eth_blockNumber`，全部成功、0 用户可见错误、
     错 chainId 的端点被剔除；
- e) 探针：dormant 链不被探测；激活后该链端点在 kick 后被立即探测；调度器在 500 个端点、
     并发 4 的情况下不堆积重复任务（断言同一端点同一时刻至多一个在飞探针）；
- f) `discovery.enabled=false` 行为等价 v1（现有全部测试不改语义通过）；
- g) 性能：`scripts/ci-smoke.sh` 通过；本机 `cargo run --release --bin loadtest`（10k×60s）
     与 loadtest-phase3.md 对比 p99 无明显退化（±20% 内），结果写进交付说明。

## W6 — 状态存储层（Redis）+ Admin REST API（分支 `w6-state-admin`）✅ 2026-08-25 合入 main
（133 用例 + 6 ignored；两轮 checker 共 17 条 must-fix 全修；报告 docs/reports/w6-state-admin.md）

> 2026-08-25 增补：用户决定用 Redis 做持久状态（重启不丢），要求支持从 0 初始化与整体覆盖，
> 方案见 DESIGN-v2 §11。本工作流拆两段串行交付：**W6a 状态存储层** → **W6b Admin REST API**，
> 同一分支、一次 checker 审查。本机无 redis-server，集成测试用
> `docker run -d --name rpcrouter-redis -p 127.0.0.1:6379:6379 redis:7-alpine`（用完可停）。

### W6a 状态存储层（DESIGN-v2 §11）
0. **后台任务监督器**（W5 checker S5 遗留，前置）：chainlist 刷新 / housekeeping / 探针调度 /
   probe worker / 状态 flush 统一由 supervisor 管理，任务 panic 或退出时 error 日志 + 指数退避
   自动重启，指标 `rpcrouter_background_task_restarts_total{task}`；测试：注入 panic 的任务被
   重启且计数 +1。
1. `state` 模块：`StateStore` trait + `MemoryStore` / `FileStore`（`data/state.json` 原子写）/
   `RedisStore`（`redis` crate，tokio-comp + connection-manager，pipeline/MULTI）；`[state]` 配置 +
   环境变量；`required` 语义；断连降级与后台重连、恢复后全量 flush。
2. 数据模型按 §11.2；bootstrap（从零初始化、schema 版本处理）、`chains:hot` 预激活、`health:*`
   冷却期恢复；write-behind flush 任务（脏端点集合、批量上限）；审计 stream。
3. 覆写叠加：runtime（store）> config.toml > default，供 Registry/Classifier 在激活/请求时使用。
4. 重置/覆盖：`--reset-state` / `RPCROUTER_STATE_RESET=1` / `import()` / `export()` / `reset()`。
5. 指标 4 个（§11.4）+ alerts 一条（state store down）+ docker-compose 加 redis 服务 +
   OPERATIONS「状态存储」章节。
6. 多实例分片部署样例（DESIGN-v2 §12 方案 A，用户已选定）：docker-compose 增加 `cluster`
   profile（nginx 用 `deploy/nginx-shard.conf` + 3 个 rpcrouter 实例共用同一 Redis），README
   「部署」新增「多实例分片」小节，OPERATIONS 新增「扩容/缩容与故障接管」说明；本机用
   compose 起 cluster 后对 3 条不同链各打一次请求，确认每条链只落在一个实例（看各实例
   `/chains` 的 hot 集合）——结果写进交付说明。

W6a 验收（单测跑 Memory/File 实现，全离线；Redis 实现用 `#[ignore]` 集成测试，`REDIS_URL` 未设时跳过）：
- a) 从零初始化：空 store 启动 → meta/catalog 写入、覆写为空；再次启动不重复 seed；schema 版本
     不一致按 `required` 分支处理；
- b) 重启恢复：pin/disable/limits/settings 覆写重启后仍生效；冷却期端点重启后为 Cooling 且
     `cooling_until` 保留；热链预激活；Active 端点重启后为 Probation（不恢复 Active）；
- c) 覆盖与重置：export → 改动 → import 后内存态与 store 一致；reset 后等价于空库首启；换
     namespace 互不干扰；
- d) 降级：store 不可达（错误 URL）且 `required=false` → 正常提供 RPC 服务、`state_store_up=0`、
     写操作排队/丢弃有日志；`required=true` → 启动失败并给出明确错误；
- e) 热路径零 store 调用（用 MemoryStore 计数器断言 10k 次请求期间 store 调用数不随请求数增长）；
- f) Redis 集成（`#[ignore]`，CI service container 跑）：a–c 在真实 Redis 上通过；pipeline 批量
     2000+ 端点 flush 一次完成；`--reset-state` 清空命名空间但不动其他前缀的 key。

### W6b Admin REST API（DESIGN-v2 §6、§9）
1. `[admin]` 配置（enabled/auth_token/static_dir/cors_allow_origins）+ 环境变量；
   鉴权规则：token 已配置 → `/admin/api/*` 全部 Bearer；未配置 → GET 开放、写操作 403
   `admin_disabled`；`enabled=false` → `/admin` 整体 404。CORS 仅对配置的 origin 开放。
2. 只读接口：`overview`、`chains`（过滤/搜索/排序/分页）、`chains/{id}`（含端点行）、
   `overrides`。链列表在 2877 链目录下 `?state=dormant&limit=200` 响应 < 50ms（本机）。
3. 控制接口：chainlist refresh、cache clear、链 activate/demote/pin/unpin/enable/disable、
   链 settings 覆写、端点 disable/enable/cool/reset/probe/limits/add/remove。
   端点 `limits` 运行时生效（可重建 Endpoint 对象，健康状态重置可接受）；`probe` 同步
   返回 ProbeOutcome。
4. 状态管理接口：`GET /admin/api/state`、`GET /admin/api/state/export`、
   `POST /admin/api/state/import`、`POST /admin/api/state/reset {"confirm":true}`；所有控制
   接口的持久化经 W6a 的 `StateStore`（同步写成功才返回 200，失败返回 503 `state_store_unavailable`）；
   每个管理操作追加审计记录。
5. 静态托管：`static_dir` 配置时 `/dashboard/` 提供 SPA（index.html fallback），无 token
   要求；目录不存在时启动告警。
6. 文档：README 新增「Admin API」章节（含 curl 示例）；OPERATIONS 新增「运行时控制」章节。

W6b 验收（离线，axum `oneshot` + 进程内 mock 上游 + MemoryStore）：
- a) 鉴权矩阵：无 token 配置时 GET 200 / POST 403；配置 token 后无头 401、错 token 401、
     对头 200；`enabled=false` 404；
- b) 每个控制接口至少一个用例断言**运行态确实变化**（如 disable 端点后 candidates 不含它；
     cool 后 state=Cooling 且流量归零；reset 后 Probation；pin 后 housekeeping 不淘汰；
     settings 覆写后 Classifier 出的 tip TTL 变化；cache clear 后下一次请求打到上游）；
- c) 覆写持久化 round-trip：写→重建 Registry + store 加载→生效；store 不可达时控制接口 503 且
     内存态不变；export/import/reset 接口行为与 W6a-c 一致；审计 stream 有记录；
- d) overview/chains/chains/{id} 字段与 DESIGN-v2 §6 契约一致（用 serde 结构体 + 快照断言）；
- e) `static_dir` 存在时 `/dashboard/`、`/dashboard/chains/1` 均返回 index.html；三门槛全绿。

## W7 — React Dashboard（分支 `w7-dashboard`，目录 `dashboard/`）✅ 2026-08-25 合入 main
（四门槛 6 用例；真实后端联调 + docker 镜像验证；报告 docs/reports/p3-acceptance.md）

> 设置页增加「状态存储」卡片（后端/连通性/最近 flush/脏端点数）与 export / import / reset
> 操作（reset 需二次确认）。

范围（DESIGN-v2 §7）：
1. 脚手架：Vite + React 18 + TypeScript + eslint + vitest + testing-library；`package.json`
   scripts：dev/build/preview/lint/typecheck/test；`vite.config.ts` 代理 `/admin` →
   `http://127.0.0.1:8545`（可用 `VITE_API_BASE` 覆写）；`dashboard/README.md`。
   锁文件入库（`package-lock.json`）；`.gitignore` 忽略 `dashboard/node_modules`、
   `dashboard/dist`。
2. API 客户端：一处封装 fetch（bearer 头、错误体解析、超时）；TanStack Query 轮询；
   类型与 DESIGN-v2 §6 契约一致（`src/api/types.ts`）。
3. 页面：总览（stat tiles + 5 分钟折线 + 刷新/清缓存）、链列表（搜索/过滤/排序/分页、
   行操作）、链详情（参数卡可编辑、端点表 + 操作、危险操作二次确认）、设置（token、
   轮询间隔、主题）。路由：`/dashboard/`、`/dashboard/chains/:id`、`/dashboard/settings`
   （`base: '/dashboard/'`）。
4. 视觉：DESIGN-v2 §7 的色彩与标记规范；亮/暗两套；状态永远色块 + 文字；无外链资源；
   响应式到 1024px 宽。
5. CI：`.github/workflows/ci.yml` 增加 `dashboard` job（node 22、`npm ci`、lint/typecheck/
   test/build），与 Rust job 并行（Rust job 在 W6 已加 `services: redis` 跑 `--ignored` 集成用例）。
6. 部署：`Dockerfile` 增加 node 构建阶段把 `dashboard/dist` 拷进镜像 `/app/dashboard`，
   `RPCROUTER_ADMIN_STATIC_DIR=/app/dashboard` 默认开启；docker-compose 示例同步；
   README 新增「Dashboard」章节（开发/构建/托管/鉴权）。

验收：
- a) 前端四门槛全绿；测试覆盖：token 头注入与 401 处理、链表过滤/排序、状态→颜色+文字映射、
     QPS 增量计算（两次快照）、危险操作确认；
- b) 主会话本机联调：起网关（配置 admin token）+ `npm run dev`，总览/列表/详情/控制操作
     实际生效（截图或录屏路径写进交付说明）；
- c) `docker build` 成功且镜像内 `/dashboard/` 可打开。

## W8 — 公共只读主页（分支 `w8-public-site`）✅ 2026-08-26 合入 main（8 commit；一轮 checker 1 must-fix + 6 should-fix 全修；
103 单测 + w8_public 7 例；前端 12 例）

> 2026-08-26 用户决策：对外默认页面改为无需登录的只读公共主页，dashboard 退为运维后台。
> 方案见 DESIGN-v2 §14；本工作流一轮 checker。

范围：
1. `admin.rs`（或拆 `src/public_api.rs`）：`GET /api/public/overview`、`GET /api/public/chains`、
   `GET /api/public/chains/{id}` 无鉴权只读接口，字段严格按 §14.1 的 `PublicChainRow`（复用
   `build_rows` 后**映射裁剪**，禁止直接序列化 `ChainRow`）；disabled 链不出现在列表且详情 404；
   `sort` 复用 `priority_key` 等既有排序；响应带 `Cache-Control: public, max-age=5`。
2. 静态托管：`GET /`、`GET /chain/{id}` 返回 index.html；`admin.public_site`（默认 true）+
   环境变量 `RPCROUTER_ADMIN_PUBLIC_SITE` 关闭开关；`config.toml` 样例与 README 配置表同步。
3. 前端：`publicFetch`（无 token）；`PublicLayout` / `PublicHomePage` / `PublicChainPage`；
   路由树按 §14.2；dashboard 顶栏加「Public site」链接；`CurlExample` 抽成可复用组件（支持传
   method/params）；vite dev proxy 增加 `/api`。
4. 文档：README「HTTP 接口」补公共 API 三条 + 「Dashboard」段说明公共页 / 后台分工；
   DESIGN-v2 §13 追加「W8 偏差记录」（如有）。

验收（全部离线）：
- a) `auth_token` 已配置时，三个公共接口**不带** Authorization 仍 200；带错误 token 也 200（忽略）；
- b) 公共 chains 响应体不含 `endpointRows`/`settings`/`userVisibleErrorsTotal`/端点 URL 字符串；
     disabled 链（用 `/admin/api/chains/{id}/disable` 制造）不在列表、详情 404；未知链 404；
- c) `/`、`/chain/1`、`/chain/1/` 返回 index.html（200，`text/html`），`/chain/../x` 与
     `/%2e%2e` 类路径 404；`public_site=false` 时三者与 `/api/public/*` 均 404，`/dashboard/` 仍可用；
     `static_dir` 未配置时 `/` 404；
- d) `/chains`（v1 JSON）与 `/admin/api/*` 行为不变（现有测试全绿）；
- e) 前端：`PublicHomePage` 渲染测试（mock fetch 返回 overview + 2 条链，断言 tiles 与表格行、
     不含 Authorization 头）；`PublicChainPage` 渲染测试（断言 curl 文本含 `/rpc/<id>`）；
     四门槛全绿；`npm run build` 产物在 `/` 与 `/dashboard/` 两个入口都能加载（手动 `vite preview`
     或 curl 断言 index.html 引用的 asset 路径为 `/dashboard/assets/...`）。

## W9 — 自动开启优质链（分支 `w9-auto-enable`，DESIGN-v2 §15）

> 2026-09-12 用户决策：默认按规则批量开启优质 EVM 链，不需要人工在控制台配置；
> **只增不减，减法只有人工**；自动开启的判定与观察**不依赖 Prometheus**。
> 档位已拍板：主网 + 端点数 ≥ 5 + 每链采样 8 端点起步（约 190 条链）。本工作流一轮 checker。

范围：

1. `src/config.rs`：新增 `DiscoveryConfig.auto_enable`（§15.5 全部字段 + 默认值）、三个环境
   变量、校验（`min_endpoints ≥ 1`、`max_chains ≥ 1`、`probe_batch ≥ 1`、
   `promote_after_rounds ≥ 1`、`probe_concurrency ≥ 1`）；`config.toml` 样例与 README 配置表同步。
2. `src/state.rs`：`chains:auto` 读写（`load_auto_chains` / `put_auto_chain`），Memory/File/Redis
   三后端 + `export`/`import`/`reset` 覆盖 + `meta.schema_version` bump（旧库无 key = 空集合）。
3. `src/autoenable.rs`（新模块，交 supervisor 监督）：§15.3 候选构建 + 轮转分批探测 + 连续轮次
   晋级；探测复用 `probe` 的 reqwest client 配置与 `signals::classify_response`，**不**为候选链
   建 `ChainState`；状态存储不可写时暂停晋级并记 WARN。
4. `src/registry.rs`：自动开启标记复用 pinned 分支（不 idle 降级、不参与 `max_hot_chains` LRU），
   新增 `set_auto_pinned` / `auto_chain_ids` / `pin_source(chain_id)`；启动时按 `chains:auto`
   预热（跳过带 `pinned=false` 或 `disabled=true` 墓碑的链）；规则引擎跳过墓碑链。
5. `src/admin.rs`：`/admin/api/chains` 增 `pinSource`、`autoCandidate`；`/admin/api/overview` 增
   `autoEnable` 块；公共 API 按 §15.7 改造（`state` 两档 `available|unverified`、默认只列
   available、`q` 或 `scope=all` 返回全目录、overview 增 `chains.available`）。
6. `src/metrics.rs`：只加**无 chain_id 标签**的标量（`rpcrouter_auto_enable_chains`、`_candidates`、
   `_promotions_total`、`_probe_failures_total`）；`metrics_enabled = false` 时功能不受影响。
7. `dashboard/`：链表 pinSource 列 + 候选视图（轮次/上次失败原因）；公共首页默认列已开启链、
   搜索可查全目录；四门槛全绿。
8. 文档：README 增「自动开启」章节（规则、档位、如何人工减法）；DESIGN-v2 §13 追加 W9 偏差记录。

验收（全部离线，测试禁止访问外网）：

- a) 用内置 fixture 目录 + 本地 mock 上游驱动一轮完整晋级：候选构建命中预期链集合；
     连续 2 轮合格才晋级；chainId 不匹配的假节点端点不计入合格；合格端点不足 2 个不晋级；
- b) **只增不减**：已开启链的全部 mock 端点变为不可用后，链仍在 `chains:auto` 且 `state=pinned`，
     公共页显示 `unverified`；进程重启后集合完整恢复；状态存储只读时晋级被跳过且不进内存；
- c) **人工减法**：`unpin` 后链回 dormant 且后续多轮扫描不再被自动加回；`disable` 后 403 且同样
     不被加回；`pin` 可重新纳入；
- d) 上限：`max_chains` 设小值时停止新增且不淘汰已开启链，overview 的 `pending`/`capped` 正确；
- e) 公共 API：默认列表只含 available；`q` 搜索能命中 dormant 链；disabled 链仍不可见；
     `/api/public/chains/{id}` 对 available 与 unverified 均 200，disabled 404；
- f) 探测预算：单轮探测端点数 ≤ `probe_batch × max_endpoints_per_chain`，并发不超过
     `probe_concurrency`（用计数断言）；候选扫描不阻塞启动；
- g) `metrics_enabled = false` 时自动开启功能与 Admin API 展示完全正常；
- h) 10k QPS 压测复跑（本地 mock 上游，pinned 链）：p99 与 UVE 不劣于 W5 报告
     （p99 1.5ms / UVE 0），报告写 `docs/reports/loadtest-w9.md`；
- i) 门槛：`cargo fmt --check` && `cargo clippy -- -D warnings` && `cargo test` 全绿；
     前端 `npm run lint && npm run typecheck && npm test && npm run build` 全绿。

## 交付说明模板（maker 每轮结束时用）

```
分支/提交：...
完成项：#... 
未完成/偏离 DESIGN 的点与原因：...
门槛结果：fmt/clippy/test（用例数）/（前端四门槛）
验收对照：a) ... b) ...
性能数字（W5）：...
需要主会话决策的问题：...
```
