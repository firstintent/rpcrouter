# 文档地图

按「要什么 → 怎么建 → 谁做什么 → 做完没有」四层组织，找文档先看这里。

| 层 | 目录 / 文件 | 说明 |
|---|---|---|
| 需求 | `proposals/` | 每条需求一个 `YYYY-MM-DD-<slug>/` 目录，放需求正文与决策依据。索引见 `proposals/README.md`。 |
| 架构 | `DESIGN.md` | v1 架构：节点池、健康评分、缓存去重、失败转移。仍是现行基线。 |
| 架构 | `DESIGN-v2.md` | v2 增补：动态目录与链生命周期、状态存储、Admin API、公共主页、自动开启。 |
| 任务 | `TASKS-v2.md` | v2 阶段任务拆解与验收标准（W5 起）。 |
| 规划 | `ROADMAP.md` | v1 之后的优先级排布 P1–P5 与各项状态。 |
| 运维 | `OPERATIONS.md` | 部署、配置、排障、告警的操作手册。 |
| 验收 | `reports/` | 压测与验收报告，一次交付一份，见下表。 |
| 调研 | `research/` | Grok 调研产出，只读参考，不随代码更新。 |
| 存档 | `archive/` | 已完成且不再维护的文档。`TASKS-v1.md` 是 v1 阶段任务拆解。 |

## 验收报告

| 报告 | 内容 |
|---|---|
| `reports/loadtest-phase3.md` | v1 收官：10k QPS 双跑、429 摘除与回池时间线 |
| `reports/prod-readiness.md` | P1 + P2：docker 8 链 smoke、加固特性实测、监控栈、CI、30 分钟真实网络 soak |
| `reports/loadtest-w5.md` | W5 动态目录：10k QPS p99 1.5ms、用户可见错误 0 |
| `reports/w6-state-admin.md` | W6 状态存储层与 Admin API |
| `reports/p3-acceptance.md` | P3 整体验收 |

## 约定

- 文档与注释用中文，日志与对外错误消息用英文。
- 需求定稿后提案原文不改，变更追加小节或新开提案并互链。
- 设计与实现冲突时，先改文档再改代码；偏离设计要在 `DESIGN-v2.md` §13 记偏差。
