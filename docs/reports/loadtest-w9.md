# W9 自动开启压测记录

## 离线压测（本地 mock 上游）

release 构建，进程内 mock 上游，10,000 QPS × 60 秒（`scripts/loadtest.sh`，
`QPS=10000 DURATION=60 CONCURRENCY=64`），分支 `w9-auto-enable`：

| 指标 | W9 | W5 基线 |
|---|---|---|
| achieved_qps | 9,999.782 | 9,999.699 |
| p50 | 0.152ms | 0.169ms |
| p99 | 1.036ms | 1.494ms |
| hit + coalesce | 99.99483% | 99.9948% |
| user_visible_errors | 0 | 0 |
| failed_requests | 0 | 0 |

限频摘除链路同样复现：storm 端点在 20s 触发 429 后被摘除，40.2s 进入 Probation 通过，
55.3s 回到 Active，全程 `user_visible_errors = 0`，单端点 rps 未突破配置上限（storm 2/15、
healthy 3/15）。原始数据 `data/loadtest-phase3.json`。

结论：热路径无回退，尾延迟略优于 W5。

## 自动开启对热路径的影响

压测二进制 `src/bin/loadtest.rs` 是独立的进程内负载框架，不启动 `AutoEnableManager`。
自动开启对热路径的影响由设计与用例共同约束，不靠压测数字：

- 候选评估器是独立后台任务，**不为候选链建 `ChainState`**、不写 `chains:hot`，
  `resolve_for_request` 的实现未改动（仍是 DashMap get + 原子读）；
- 候选探测的全局并发由共享 semaphore 限死，`tests/w9_auto_enable.rs` 的
  `global_probe_concurrency_is_bounded` 用共享计数器断言峰值不超过 `probe_concurrency`；
- 单轮探测端点数上界为 `probe_batch × max_endpoints_per_chain`（默认 32 × 8）；
- 新增指标全部是无 `chain_id` 标签的标量，`/metrics` 序列数不随链数增长。

常驻探针的额外负载来自晋级后的链，按起步档位（约 190 条主网链、每链最多 8 个端点）估算，
20s 间隔下约 65 probe/s，候选探测另加约 8.5 probe/s，对单个公共端点仍是分钟级一次。

## 复算方式

```bash
QPS=10000 DURATION=60 CONCURRENCY=64 bash scripts/loadtest.sh   # 输出 data/loadtest-phase3.json
cargo test --test w9_auto_enable --test w9_api                   # 自动开启的行为与接口用例
```
