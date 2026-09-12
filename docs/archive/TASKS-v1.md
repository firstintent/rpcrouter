# TASKS — 分阶段实现计划（Codex 执行，主会话验收）

> 统一门槛（每个 Phase 交付时必须全绿）：
> `cargo fmt --check` && `cargo clippy --all-targets -- -D warnings` && `cargo test`。
> **测试禁止访问外网**：chainlist 用 `fixtures/` 样例，上游行为用进程内/本地 mock。
> 实现细节以 `docs/DESIGN.md` 为准，本文件只定范围与验收。

## 通用工程约定

- 单 crate `rpcrouter`（lib + bin）；mock 上游放 `src/bin/mock-upstream.rs`（Phase 2 起）。
- 模块建议（可合理调整，保持单一职责）：`config` / `chainlist` / `registry` / `classify`
  （方法缓存分类）/ `cache` / `select` / `forward` / `signals`（限频判定）/ `probe` /
  `server` / `metrics`。
- `fixtures/rpcs.sample.json`：真实 rpcs.json schema 的裁剪样本（至少含 ETH(1) 与
  Monad(143)），单测与离线冷启动共用。
- 依赖以 DESIGN §0 清单为基准；新增重量级依赖要在交付说明里给理由。
- 提交：英文 conventional 风格，粒度按逻辑步骤；不改 `docs/research/`。

## Phase 1 — 骨架 + chainlist 接入 + ETH/Monad 透传跑通（任务 #4）

范围：
1. Cargo 工程 + `config.toml` 加载（DESIGN §10，内置默认值零配置可跑，默认 chains=[1,143]）
   + tracing 初始化。
2. chainlist 拉取/解析/过滤（https-only、剔 `${KEY}`、去重）+ 周期刷新（默认 6h）+ ETag +
   三级回退（内存 → `./data/rpcs.json` 磁盘缓存 → 内置 fixture）。
3. Registry 端点池（本阶段不评分：全部视为 Active，轮转或随机选点）+ `GET /chains`。
4. `POST /rpc/{chainId}`：单个 + batch（拆分/保序合并/上限 100）；换点重试（≤4 点）；
   简版失败信号（非 JSON、429、5xx、超时 → 换点）；全败返回 DESIGN §6 的 -32000；
   `GET /healthz`。
5. per-endpoint 令牌桶 + 并发位（DESIGN §4 出站保护，第一天就要有）。

验收：
- `cargo run` 后 `curl :8545/rpc/1` 和 `/rpc/143` 的 `eth_blockNumber` 真实往返成功
  （人工 smoke，不进 CI）。
- 单测（离线）：chainlist 解析过滤（fixture）、batch 拆合保序、失败换点与全败错误体
  （进程内 mock）。三门槛全绿。

## Phase 2 — 限频摘除/回池 + 探针 + 无错误语义（任务 #5）

范围：DESIGN §5 信号分类全集；端点状态机 Active/Cooling/Probation（指数冷却、Retry-After、
strikes 衰减）；健康探针 + head 跟踪 + latency/lag 评分；P2C 选点；链级错误透传白名单；
`src/bin/mock-upstream.rs`（可配：429 阈值、限频 message、HTML 页、错 chainId、块高滞后、
延迟、5xx）。

验收（全部离线，基于 mock-upstream 的集成测试）：
- a) 某端点持续 429 → 进 Cooling、流量归零、到期探针通过后回池（含指数退避断言）；
- b) 429 风暴中用户 0 感知：并发请求全部最终成功，`user_visible_errors == 0`；
- c) `execution reverted` 等链级错误原样透传、不触发换点；
- d) 错 chainId 端点被剔除。三门槛全绿。

## Phase 3 — 缓存 + 折叠 + 10k QPS（任务 #6）

范围：DESIGN §3 全套（分桶、head 掺键、按链 TTL）；in-flight 折叠；RawValue 热路径；
hedging（全局 ≤10% 比例闸 + 池不健康自动关）；`/metrics`（DESIGN §8 指标集）；
`scripts/loadtest.sh`（mock-upstream + oha/vegeta 任一）。

验收：压测报告写入 `docs/reports/loadtest-phase3.md`：
- 单链 10000 QPS × 60s（本机、mock 上游 5ms 延迟档）；缓存+折叠命中 ≥98%；
  p99 ≤ 50ms；`user_visible_errors == 0`；
- 压测中段注入 429 风暴：摘除/回池正常、用户仍 0 感知；
- mock 上游侧观测：单端点入站 QPS 从未超其配置上限。三门槛全绿。

## Phase 4 — 铺开与收尾（任务 #7，主会话主导）

主流链配置铺开（56/137/42161/8453/10/43114）；真实网络 soak（低 QPS 长跑）；README
（部署/配置/接口文档）；AGENTS.md 状态更新；最终验收报告。
