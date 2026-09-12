# 03 数据摸底与预算测算

数据源：本地 `data/rpcs.json`（chainlist 快照，2026-09-12，2923 条链）。
过滤口径与代码一致：只留 https、剔 `${KEY}` 模板、剔带 userinfo、去 fragment、去重。

## 端点数分布

| 门槛 | 主网链数 | 含测试网 |
|---|---|---|
| ≥ 1 端点 | 1860 | 2827 |
| ≥ 3 端点 | 592 | 718 |
| ≥ 5 端点 | 193 | 232 |
| ≥ 8 端点 | 90 | 104 |

主网约 1907 条，测试网约 1016 条。元数据覆盖率低：`status = active` 只有 168 条、
`incubating` 108 条、无一条 `deprecated`；带 `tvl` 的 174 条，其中 88 条 ≥ 1e6。
结论：这两个字段只能当打分项。

## 探针预算

| 方案 | 常驻端点数 | 20s 间隔下探针 QPS |
|---|---|---|
| 现状 8 条 pinned | 332 | 17 |
| ≥ 5 端点 193 链，每链上限 8 | 1298 | 65 |
| ≥ 3 端点 592 链，每链上限 8 | 2591 | 130 |

候选探测另计：`probe_batch × max_endpoints_per_chain / candidate_interval_seconds`
= 32 × 8 / 30 ≈ 8.5 次每秒。

对单个公共端点而言仍是分钟级一次，符合项目「不对单一节点激进重试、设并发与频率上限」的约定。

## 复算脚本

```python
import json
d = json.load(open('data/rpcs.json'))

def eps(c):
    out = set()
    for r in c.get('rpc', []):
        u = r['url'] if isinstance(r, dict) else r
        if not u.startswith('https://') or '${' in u or '{' in u:
            continue
        if '@' in u.split('://', 1)[1].split('/', 1)[0]:
            continue
        out.add(u.split('#')[0])
    return out

rows = [(c.get('chainId'), len(eps(c)),
         bool(c.get('testnet')) or 'testnet' in (c.get('name', '') + str(c.get('shortName', ''))).lower())
        for c in d]
main = [r for r in rows if not r[2]]
for thr in (1, 3, 5, 8):
    sel = [r for r in main if r[1] >= thr]
    print(thr, len(sel), sum(min(r[1], 8) for r in sel))
```

数字会随 chainlist 刷新漂移，重跑脚本即可复算。
