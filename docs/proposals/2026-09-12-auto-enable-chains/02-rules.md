# 02 筛选规则细则

规则分两段：**静态目录信息出候选**，**实测探测结果定开启**。
只靠 chainlist 元数据判断不了「稳定、质量好」，端点数多不等于能用；只靠实测又会把
探测成本铺到 2900 条链上，所以先用静态门槛把候选池收窄。

## 第一段：静态候选

每轮从最新 Catalog 重建，命中全部条件才进候选池：

| 条件 | 取值 |
|---|---|
| 非测试网 | `is_testnet == false` |
| 不在拒绝名单 | 不在 `discovery.deny` |
| 无人工墓碑 | 无 `pinned = false`，无 `disabled = true` |
| 不重复 | 不在自动集合、不是配置 pinned |
| 端点数门槛 | 去重后公开 https 端点数 ≥ `min_endpoints`（默认 5） |

候选池上限 `max_candidates`（默认 512）。超出时按打分排序取前 N，打分只决定**探测优先级**，
不作为门槛：端点数 + `tracking=none` 端点数加权 + `tvl` 加分 + `status=active` 加分。

`status` 与 `tvl` 覆盖太稀（2923 条链里分别只有 276 条和 174 条有值），因此不能当门槛。

## 第二段：实测探测

- **轮转分批**：每 `candidate_interval_seconds`（默认 30）取 `probe_batch`（默认 32）条候选链，
  指针滚动，512 条候选约 8 分钟扫完一遍。有界并发 `probe_concurrency`（默认 8）。
- **采样**：每链最多 `max_endpoints_per_chain`（默认 8）个端点，优先 `tracking=none`。
- **单端点合格**：HTTP 200 + 无 JSON-RPC error + **`eth_chainId` 与目录 chainId 一致** +
  `eth_blockNumber` 可解析。chainId 校验既防假节点，也防目录本身写错。
- **单轮链合格**：合格端点 ≥ `min_active_endpoints`（默认 2），且其中至少 2 个端点的 head 落在
  `[max_head - head_tolerance_blocks, max_head]`（默认 64 块，兼容快链）。
- **晋级**：连续 `promote_after_rounds`（默认 2）轮合格。中断则计数归零，这只是「还没加入」，
  不构成减法。

「稳定、多、质量好」三个词分别落到：连续多轮合格与实测成功率、静态端点数门槛、
chainId 校验与 head 一致性。

## 晋级动作

1. 写状态存储 `chains:auto`（写失败则不晋级，下一轮重试）；
2. registry 标记为自动开启，**复用 pinned 分支**：不 idle 降级、不参与 `max_hot_chains` LRU 淘汰，
   另记 `pin_source = auto` 供展示；
3. materialize + 探针 kick，端点从 Probation 起步走常规探针节奏。

候选评估器**不为候选链建 `ChainState`**，也不写 `chains:hot`，避免几百条链的运行态污染数据面。

## 上限与墓碑

- 自动集合达到 `max_chains`（默认 400）→ 停止新增，不淘汰，pending 数量在 Admin API 与日志暴露。
- 人工 `unpin` / `disable` 是墓碑，规则引擎永远跳过；人工 `pin` 或清除覆写后可重新纳入。
- 启动时读 `chains:auto` 预热，跳过带墓碑的条目。

## 已开启链失效时怎么办

保持开启（只增不减），端点级冷却摘除照常工作，公共页显示为 `unverified`。
运维在后台能看到活跃端点数为 0，自行决定是否人工取消开启。
