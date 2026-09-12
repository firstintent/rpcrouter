# 04 交付范围与验收

详细任务拆解与逐条验收在 `docs/TASKS-v2.md` W9，这里只记范围边界与验收口径，不重复清单。

## 改动面

| 模块 | 改什么 |
|---|---|
| `src/config.rs` | `discovery.auto_enable` 配置块、三个环境变量、参数校验 |
| `src/state.rs` | `chains:auto` 读写，Memory/File/Redis 三后端 + export/import/reset + schema 版本 |
| `src/autoenable.rs`（新） | 候选构建、轮转分批探测、连续轮次晋级，交后台任务监督器管 |
| `src/registry.rs` | 自动开启标记复用 pinned 分支、启动预热、墓碑跳过 |
| `src/admin.rs` | 管理接口增 `pinSource` / `autoCandidate` / `autoEnable` 块；公共接口改两档状态与默认过滤 |
| `src/metrics.rs` | 只加无链标签的全局标量 |
| `dashboard/` | 链表 pinSource 列与候选视图；公共首页默认列已开启链 |

## 验收口径

- 全部离线：目录用内置 fixture，上游用本地 mock，**测试禁止访问外网**。
- 重点验的是三条硬约束：只增不减（端点全死仍在集合、重启恢复、存储只读时不晋级）、
  人工减法生效且不被自动加回、关掉指标开关功能完整。
- 性能不回退：10k QPS 压测复跑，p99 与用户可见错误数不劣于 W5 报告（p99 1.5ms、UVE 0），
  报告写 `docs/reports/loadtest-w9.md`。
- 门槛：`cargo fmt --check` && `cargo clippy -- -D warnings` && `cargo test` 全绿；
  前端 `npm run lint && npm run typecheck && npm test && npm run build` 全绿。

## 协作安排

主会话做方案与拆解，maker 在独立 worktree 分支实现，一轮对抗审查，主会话复核合入并推送。
**不部署**：上线统一走 mydevops 的 docker 流程。
