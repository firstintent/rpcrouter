# rpcrouter — 项目指南

## 这是什么

rpcrouter 是一个 Rust 实现的区块链节点 RPC 路由网关：聚合 [chainlist](https://chainlist.org)
上各链的公开 RPC 节点池，对外暴露统一的 JSON-RPC 入口（按 chainId 路由），通过池内
负载均衡、健康评分与透明失败转移，提供免 API key、无单点限流的 RPC 服务。

## 硬指标（验收标准）

1. **单链扛住 10000 QPS**——必然依赖请求去重 + 响应缓存，公开节点池只承接缓存未命中。
2. **智能摘除限频节点**——识别 429 / 各家限频错误码 / HTML 错误页 / 超时，把被限频的
   公共节点冷却摘除，恢复后自动回池。
3. **用户端无错误感知**——上游失败对调用方透明（failover / 重试 / hedging），只有全池
   耗尽才返回错误。
4. 链覆盖：**先用 ETH 和 Monad 跑通全流程**，随后铺开主流链（BSC、Polygon、Arbitrum、
   Base、OP、Avalanche 等）。

## 协作模式（ccteam 三角分工）

- **主会话（Claude）**：仓库治理——方案制定、任务拆解、评审把关、提交管理。
  不亲自写大规模实现代码，控制自身上下文膨胀。
- **Grok**：调研——开源项目复用评估、chainlist 数据源、限频行为盘点等。产出落到
  `docs/research/`。
- **Codex / dsh**：实现——按 `docs/TASKS*.md` 的阶段任务开发，交付可编译、带测试的代码。
  P1 起实际用 ccteam 调度 dsh（maker=xy/deepseek-v4-pro，checker=xy/deepseek-v4-flash），
  maker 在独立 git worktree 分支开发，checker 单轮对抗审查，主会话合入。

全程自主推进，不向用户中途提问。

## 需求管理流程（2026-09-12 起固定为这个形式）

**每条新需求先建提案目录，再谈方案。** 顺序不可颠倒：需求正文没定稿就不写 DESIGN、不派开发。

1. 建 `docs/proposals/YYYY-MM-DD-<slug>/`（日期 = 需求提出日，slug 用英文短横线）。
2. 写需求正文：用户原话逐字保留、澄清过程、定稿后的需求陈述、明确不在范围内的事、
   逐条决策与理由、数据依据（附可复算脚本）。文件拆分与写作约定见 `docs/proposals/README.md`。
3. 需求定稿后再写架构方案（`docs/DESIGN*.md`）与任务拆解（`docs/TASKS*.md`），然后才派开发。
4. 在 `docs/proposals/README.md` 的索引表登记一行，交付后更新状态。
5. 提案原文定稿后不改；后续变更追加「变更记录」小节或新开提案并互链。

三类文档分工不重复：**提案回答「要什么」，设计回答「怎么建」，任务回答「谁做什么、怎么算完」。**

## 关键文档

- `docs/README.md` — 文档地图（先看这个）
- `docs/proposals/` — 需求提案，按日期分目录；索引在 `docs/proposals/README.md`
- `docs/DESIGN.md` — v1 架构方案（现行基线，主会话维护）
- `docs/DESIGN-v2.md` — v2 增补：动态目录 / 状态存储 / Admin API / 公共主页 / 自动开启
- `docs/TASKS-v2.md` — v2 阶段任务拆解与验收标准（W5 起）
- `docs/ROADMAP.md` — v1 后规划 P1–P5 与各项状态
- `docs/OPERATIONS.md` — 部署与排障手册
- `docs/reports/` — 压测与验收报告
- `docs/research/` — Grok 调研产出（只读参考）
- `docs/archive/` — 已完成不再维护的文档（`TASKS-v1.md` 为 v1 阶段任务拆解）

## 技术与工程约定

- Rust edition 2024（工具链 1.97+）；async 栈用 tokio。
- 提交门槛：`cargo fmt --check` && `cargo clippy -- -D warnings` && `cargo test` 全绿。
- 单测放各模块 `#[cfg(test)]`；**测试禁止访问外网**——chainlist/节点数据用内置样例，
  上游行为用本地 mock；压测也用本地 mock 上游。
- 日志与对外错误消息用英文（便于检索），代码注释与文档用中文。
- 公开节点质量参差（限频、2xx 返回 HTML、区块滞后、假数据）：写任何上游交互逻辑时
  默认**上游不可信**。
- 不做规避第三方服务条款的事（伪装 UA 绕封锁、对单一节点激进重试等）；对单个公共
  端点设并发/频率上限，遇 429 退避换节点而不是硬打。

## 当前状态

- [x] 仓库治理初始化（本文件、git）
- [x] Grok 调研：开源复用评估 / chainlist 数据方案（docs/research/）
- [x] DESIGN.md / TASKS.md（v1 任务拆解已移入 `docs/archive/TASKS-v1.md`）
- [x] Codex 分阶段实现 Phase 1–4（含冷启动 Probation 兜底修复）
- [x] 验收：8 链真实 E2E；10k QPS 压测双跑验证（docs/reports/loadtest-phase3.md）；
      429 摘除/回池时间线复现；`user_visible_errors == 0`

v1 已交付（2026-07-26 验收）。后续任务规划统一沉淀在 `docs/ROADMAP.md`。

- [x] ROADMAP P1 部署 + P2 生产可用（2026-08-20 验收，maker/checker 对抗流程交付；
      真实环境验收见 `docs/reports/prod-readiness.md`：docker 8 链 smoke、加固特性实测、
      监控栈实跑、CI 首跑绿灯、30 分钟真实网络 soak 0 错误）。遗留：24h soak。
- [x] ROADMAP P3 动态全链目录 + 状态控制 Dashboard（2026-08-25 立项并交付；W5 → W6 → W7，
      验收报告 docs/reports/p3-acceptance.md）。前端为独立 React 工程 `dashboard/`，前端门槛：
      `npm run lint && npm run typecheck && npm test && npm run build`。
  - [x] W5 动态目录 + 链生命周期（2026-08-25 合入：目录 2887 链 / 5562 端点，pinned/hot/dormant/
        disabled 生命周期，未知链 404 / 无端点 503 / 禁用 403 不计 UVE，冷启动失败单独计数，
        有界探针池；10k 压测 p99 1.5ms、UVE 0，见 docs/reports/loadtest-w5.md）。
  - [x] W6 Redis 状态存储层 + Admin REST API（2026-08-25 合入：StateStore Memory/File/Redis，结构化
        key 为真相、从零初始化/覆盖/重置、Redis 不可达 ≤3s 降级启动且控制写 503；后台任务监督器；
        `/admin/api/*` 只读 + 控制接口，bearer 鉴权、静态托管防穿越、输入校验；compose redis +
        cluster 分片 profile（方案 A）；见 docs/reports/w6-state-admin.md）。
  - [x] W7 React dashboard（2026-08-25 合入：总览/链列表/链详情/设置，四门槛 + CI job + 镜像内置）。
  - [x] W8 公共只读主页（2026-08-26 合入：`/` 与 `/chain/{id}` 无需登录的只读公共页 + `/api/public/*`
        无鉴权裁剪接口（5s 服务端 memo），dashboard 退为运维后台；`admin.public_site` 开关；DESIGN-v2 §14）。
- [ ] W9 自动开启优质链（2026-09-12 立项，开发中）：按规则批量常驻开启优质 EVM 主网链
      （主网 + 去重 https 端点 ≥5 + 每链采样 8 端点，约 190 条），**只增不减、减法只有人工**，
      判定与观察不依赖 Prometheus；需求 `docs/proposals/2026-09-12-auto-enable-chains/`、
      方案 DESIGN-v2 §15、任务 TASKS-v2 W9。
