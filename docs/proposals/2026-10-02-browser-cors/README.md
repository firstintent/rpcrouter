# 浏览器跨域调用（CORS）放开

| 项 | 值 |
|---|---|
| 提出日期 | 2026-10-02 |
| 提出人 | 用户（转述外部调用方反馈，会话内追加口径） |
| 状态 | 已上线（2026-10-02，mydevops Caddy 热加载） |
| 架构方案 | 无新增：跨域在入口 Caddy 处理，网关代码不变 |
| 任务拆解 | 无，部署配置变更，主会话直接完成 |

一句话：**`https://rpc.cryptostack.ai` 的 `/rpc/*` 与 `/api/public/*` 对所有来源开放浏览器跨域调用，
预检由 Caddy 直接回 204；`/admin` 不开。**

## 用户原话

先问：

> 目前rpcrouter对外部调用者有跨域禁止访问的问题吗？

主会话实测确认有问题后，用户转来调用方反馈：

> 这是用户反馈,修复然后上线：
> RPC 的具体报错
> 浏览器跨域调你的节点前，会先发一个 OPTIONS「预检」请求。你的节点（经 Caddy）回 HTTP 405，只允许 POST；正式请求的响应里也没有 Access-Control-Allow-Origin 头，所以浏览器直接拦下。后端服务器调用不走这套检查，所以 swap 后端一直用得好好的。
> 现在页面里的钱包访问 Robinhood，走的是官方公共 RPC rpc.mainnet.chain.robinhood.com。
> 要改的话，在节点的 Caddy 上做三件事：
> 对 OPTIONS 回 204；
> 带上 Access-Control-Allow-Origin，值为 https://excore.cryptostack.ai 和 https://console.cryptostack.ai；
> 带上 Access-Control-Allow-Methods: POST, OPTIONS 和 Access-Control-Allow-Headers: content-type，并且 POST 的响应里也带 Allow-Origin。

紧接着追加口径（覆盖上面的两域名白名单）：

> 放开所有域名的跨域

## 修复前的现象（2026-10-02 实测）

| 请求 | 结果 |
|---|---|
| `OPTIONS /rpc/1`（带 Origin 与预检头） | 405，只有 `allow: POST`，无 CORS 头 |
| 带 Origin 的 `POST /rpc/1` | 200 有结果，但无 `Access-Control-Allow-Origin` |
| `GET /api/public/chains/1` | 200，无 CORS 头 |

根因：网关只有 admin 路由能配 CORS（`admin.cors_allow_origins`，生产未开），
`/rpc/{chain_id}` 与 `/api/public/*` 没有 CORS；Caddy 也没加。

## 决策

**D1 在入口 Caddy 做，不改网关代码。**
调用方反馈指定了 Caddy；Caddy 热加载零中断、即时上线，不需要重建 rpcrouter 镜像，
也就不会连带上线 main 上尚未部署的提交。代价是其他部署形态（源码 compose、cluster）
不自带跨域，需要时再在网关加可配置的 CORS 层。

**D2 来源放开为 `*`（用户追加口径），不做白名单。**
与主流公共 RPC 一致（publicnode、cloudflare-eth 均回 `*`）。不带凭证，`*` 不会让浏览器附带 cookie。

**D3 只覆盖 `/rpc/*` 与 `/api/public/*`。**
两者都是无鉴权的公开面。`/admin/*` 不加 CORS：靠 bearer token 的运维接口没有跨域场景，
且 `Allow-Headers` 不含 `authorization`，跨域带 token 的请求会被浏览器拦下。

**D4 方法 `GET, POST, OPTIONS`，请求头只放 `content-type`，预检缓存 86400 秒。**
在反馈的 `POST, OPTIONS` 基础上加 `GET`，给 `/api/public/*` 用。主流 SDK（viem / ethers / web3.js）
只发 `content-type`。Chrome 预检缓存上限 2 小时，86400 对其他浏览器生效。

**D5 CORS 头用 `defer` 统一覆盖。**
网关将来若自己也发 CORS 头，不会出现重复的 `Access-Control-Allow-Origin`（重复会被浏览器判为非法）。

## 不在范围内

- 网关代码层的可配置 CORS（D1 的后续路径）。
- 每来源限速 / 浏览器流量配额：放开后浏览器流量可能上升，现有的每 IP 限速
  （`server.per_ip_rate_limit`）默认关闭，需要时再开。
- WebSocket。

## 改动面

| 位置 | 改动 |
|---|---|
| mydevops `gateway/sites/rpcrouter.caddy` | 新增 `@cors` 头注入与 `@preflight` 204 |
| `docs/OPERATIONS.md` | 故障排查表补跨域一行 |

## 上线与验收（2026-10-02）

`caddy validate` 通过 → `caddy reload` 热加载。验收：

- `OPTIONS /rpc/1` → 204，带 `Allow-Origin: *`、`Allow-Methods`、`Allow-Headers`、`Max-Age`。
- `POST /rpc/1`（任意 Origin）→ 200，带 `Allow-Origin: *`，且该头只出现一次。
- `GET /api/public/chains/1` → 带 `Allow-Origin: *`。
- `/admin/api/overview` → 无 CORS 头，预检仍是 405。
- 无头 Chromium 从 `http://127.0.0.1:18777` 发跨域 `fetch`：`/rpc/1`、`/rpc/11155111`、
  `/api/public/chains/1` 成功；`/admin/api/overview` 被浏览器拦下（`Failed to fetch`）。
- 同一 Caddy 上的其他站点（at / llm / ccd）响应正常。

回滚：恢复 `rpcrouter.caddy` 旧版后再 `caddy reload`。
