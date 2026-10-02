# Sepolia 测试网加入默认常驻链

| 项 | 值 |
|---|---|
| 提出日期 | 2026-10-02 |
| 提出人 | 用户（会话内口头需求） |
| 状态 | 已交付（源码与部署配置已改，待 mydevops 上线） |
| 架构方案 | 无新增，沿用 `docs/DESIGN-v2.md` pinned 链语义 |
| 任务拆解 | 无，纯配置变更，主会话直接完成 |

一句话：**把 Ethereum Sepolia（chainId 11155111）加入 `chains` 默认常驻（pinned）列表，
源码仓库样例配置与 mydevops 生产配置同步更新。**

## 用户原话

> Sepolia 打开这个网络，加入到默认。更新部署配置

## 主会话的理解

- 「打开这个网络」：Sepolia 常驻开启，不再依赖首个请求触发 hot 激活、也不会 idle 降级。
- 「加入到默认」：加入配置文件的 `chains` 列表（pinned），和 ETH / Monad 等 8 条默认链同级。
- 「更新部署配置」：同步 `/home/ubuntu/rob/mydevops/configs/rpcrouter/config.toml`。
  按既有约定主会话不执行上线，由用户在 mydevops 部署。

## 决策

**D1 走 `config.chains`（pinned），不走控制台 pin 或自动开启。**
W9 决策 D2 定了自动开启只收主网、测试网不做白名单，这条不动。控制台 pin 只写状态存储，
不进「默认」，换环境或重置状态就丢；`config.chains` 优先级最高（W9 D3），部署配置即真相。

**D2 链参数沿用主网：`block_time_ms = 12000`、`confirmation_depth = 64`、`tip_ttl_ms = 2000`。**
Sepolia 是以太坊 PoS 测试网，slot 12 秒、终局机制与主网一致；64 块确认深度对不可变缓存足够保守。

**D3 不手工维护 `extra_endpoints` / `disabled_endpoints`。**
目录里有串链端点（shardeum 两条域名已失效、1rpc 对 `eth_chainId` 无结果），探针的
`eth_chainId` 校验会把它们挡在池外；限频与鉴权失败的端点走既有冷却与回池。手工黑名单
会和 chainlist 每小时刷新互相打架，不值得。

## 不在范围内

- 不部署、不重启线上服务（部署归 mydevops）。
- 不改自动开启规则，测试网仍不进自动集合。
- 不顺带开启 Hoodi 等其他测试网。
- 不做压测：pinned 列表增一条不改数据面代码，10k QPS 压测结论（loadtest-w9）不受影响。

## 数据依据

快照：本地 `data/rpcs.json`（chainlist 缓存，2026-10-02 09:28）。Sepolia 共 35 条 rpc 记录，
按代码口径（只留 https、剔模板与 userinfo、去 fragment、去重）剩 **29** 个端点。

2026-10-02 对 29 个端点各发一次 `eth_chainId` + `eth_blockNumber`（8 并发，单次不重试）：

| 结果 | 数量 | 明细 |
|---|---|---|
| 可用 | 11 | 头块落差全部 ≤ 1 块 |
| 串链 / 无结果 | 1 | public.1rpc.io |
| DNS 失效 | 5 | 含 2 条 shardeum 串链域名 |
| 429 限频 | 4 | onfinality、alchemy demo、tatum、routeme |
| 401 / 403 | 3 | 需 key 或封禁 |
| 其他 HTTP 错误 | 3 | 404 / 400 / 521 |
| 超时 / TLS 失败 | 2 | |

11 个可用端点远高于 `hedging.min_active_endpoints = 2`，足够承接。新增探针负载按 29 个端点、
15–30 秒间隔计约 1–2 次每秒，对单个公共端点仍是分钟级。

上线前线上状态（同日 `/api/public/chains/11155111`）：Sepolia 在目录内，`unverified`，
0 个物化端点、0 请求；状态存储里没有针对它的人工覆写。

### 复算脚本

下面脚本存为 `probe_sepolia.py`，在仓库根目录执行 `python3 probe_sepolia.py [data/rpcs.json]`
（会访问公网，只用于摸底，不进测试）：

```python
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

SNAP = sys.argv[1] if len(sys.argv) > 1 else 'data/rpcs.json'
CHAIN = 11155111

def endpoints():
    for c in json.load(open(SNAP)):
        if c.get('chainId') == CHAIN:
            urls = [r['url'] if isinstance(r, dict) else r for r in c.get('rpc', [])]
            return sorted({u.split('#')[0].rstrip('/') for u in urls
                           if u.startswith('https://') and '{' not in u and '@' not in u.split('/')[2]})
    return []

def call(url, method):
    body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': []}).encode()
    req = urllib.request.Request(url, body, {'content-type': 'application/json', 'user-agent': 'rpcrouter-baseline'})
    t = time.time()
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read()).get('result'), (time.time() - t) * 1000

def check(url):
    try:
        cid, _ = call(url, 'eth_chainId')
        if cid is None or int(cid, 16) != CHAIN:
            return url, 'wrong_chain', cid, None
        head, ms = call(url, 'eth_blockNumber')
        return url, 'ok', int(head, 16), round(ms)
    except Exception as e:
        return url, 'fail', type(e).__name__ + ':' + str(e)[:60], None

eps = endpoints()
with ThreadPoolExecutor(8) as ex:
    rows = list(ex.map(check, eps))
ok = [r for r in rows if r[1] == 'ok']
top = max((r[2] for r in ok), default=0)
for r in rows:
    lag = f' lag={top - r[2]}' if r[1] == 'ok' else ''
    print(f'{r[1]:12} {r[0]} {r[2]}{lag} {r[3] or ""}ms')
print(f'\nendpoints={len(eps)} ok={len(ok)} '
      f'wrong_chain={sum(1 for r in rows if r[1] == "wrong_chain")} '
      f'fail={sum(1 for r in rows if r[1] == "fail")} head={top}')
```

## 改动面

| 位置 | 改动 |
|---|---|
| `config.toml` | `chains` 追加 11155111；新增 Sepolia `chain_overrides` |
| `src/config.rs` | `repository_config_is_valid` 断言同步 |
| `docker-compose.yml` / `deploy/rpcrouter.service` | `RPCROUTER_CHAINS` 样例同步 |
| `README.md` | 默认链清单补 Sepolia |
| mydevops `configs/rpcrouter/config.toml` | 同 `config.toml` 两处改动 |

## 上线方式

生产配置只读挂载进容器，本次不改二进制，重启网关即可生效；
线上镜像已包含 W9，无需重建（mydevops `deploy/rpcrouter-deploy.sh --skip-build`）。
生效后验收：`/api/public/chains/11155111` 的 `state` 为 `available`、`active` ≥ 2
（后台链详情显示 pinned），
`eth_chainId` 经网关返回 `0xaa36a7`。

## 变更记录

- 2026-10-02：用户授权后 `docker compose restart rpcrouter` 上线（只重启、不重建镜像），
  `rpcrouter_chain_pinned{chain_id="11155111"} = 1`，可用端点 11/29。
