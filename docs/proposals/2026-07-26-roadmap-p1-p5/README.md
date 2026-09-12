# v1 之后的规划 P1–P5（追溯补录）

| 项 | 值 |
|---|---|
| 提出日期 | 2026-07-26（v1 收官时制定，2026-08-25 增补 P3 后原 P3/P4 顺延） |
| 状态 | P1 / P2 / P3 已交付，P4 / P5 未开工 |
| 正文 | `docs/ROADMAP.md`（本提案只做索引，细节以 ROADMAP 为准） |
| 验收记录 | `docs/reports/prod-readiness.md`、`docs/reports/p3-acceptance.md` |

> 本文 2026-09-12 追溯补录。ROADMAP.md 本身就是需求正文，这里只登记它在提案体系里的位置。

| 项 | 内容 | 状态 |
|---|---|---|
| P1 | 部署：Dockerfile 多阶段、compose、systemd、release profile 调优 | ✅ 2026-08-20 |
| P2 | 生产可用：优雅退出、入口防护、指标鉴权、告警与仪表盘、CI、soak | ✅ 2026-08-20（24h soak 仍遗留） |
| P3 | 动态全链目录 + 状态控制 Dashboard | ✅ 2026-08-25（见单独提案） |
| P4 | 命名链路由（`/rpc/ethereum` 之类） | 未开工 |
| P5 | 多实例横向扩展 Phase B（Redis 集群特性） | 未开工 |

## 一条需要记住的语义边界（P2 定的）

`user_visible_errors` 只统计**上游侧承诺失败**：请求已进入数据面转发，但所有上游端点耗尽。
入口防护（过载 503、请求体过大 413、每 IP 限速 429）发生在转发之前，属于入口侧拒绝，
只累计到 `rpcrouter_ingress_rejected_total`，不计入用户可见错误。告警不可混用二者。
