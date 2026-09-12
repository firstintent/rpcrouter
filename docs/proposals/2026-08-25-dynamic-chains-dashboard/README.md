# 动态全链目录 + 状态控制 Dashboard（追溯补录）

| 项 | 值 |
|---|---|
| 提出日期 | 2026-08-25 |
| 状态 | 已交付（同日 W5 → W6 → W7 串行合入） |
| 架构方案 | `docs/DESIGN-v2.md` §1–§13 |
| 任务拆解 | `docs/TASKS-v2.md` W5 / W6 / W7 |
| 验收记录 | `docs/reports/loadtest-w5.md`、`docs/reports/w6-state-admin.md`、`docs/reports/p3-acceptance.md` |

> 本文 2026-09-12 追溯补录，还原自 DESIGN-v2、TASKS-v2 与当时的提交记录。

一句话：**支持 chainlist 上全部链的实时动态获取，并提供状态控制 Dashboard**，
不再只服务配置文件里手写的那几条链。

## 用户当时明确的三条

1. 主会话做架构设计，开发派给独立会话；
2. Dashboard 是**独立 React 工程**放仓库根目录 `dashboard/`，经 RESTful API 与 Rust 通信，
   不是嵌入式 HTML；
3. 持久状态用 **Redis**：内存仍是数据面真相，Redis 只做镜像不进热路径；必须支持从零初始化、
   整体覆盖导入、reset；Redis 不可用要能降级启动；另做 file 后端作零依赖回退。

横向扩展选定**方案 A**：按 chainId 一致性哈希分片部署，零代码改动；Redis 集群特性列入 P5。

## 交付形态

- 链生命周期四态 `pinned` / `hot` / `dormant` / `disabled`，未知链 404、无端点 503、
  禁用 403，三者都不计入用户可见错误。
- 目录规模 2887 链 / 5562 端点，有界探针池，10k QPS 下 p99 1.5ms、用户可见错误 0。
- 状态存储层三后端 + Admin REST API（bearer 鉴权、静态托管防穿越、输入校验）。
- React dashboard：总览 / 链列表 / 链详情 / 设置，四道前端门槛加进 CI 并内置进镜像。
