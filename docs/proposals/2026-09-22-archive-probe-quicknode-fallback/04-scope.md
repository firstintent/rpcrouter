# 范围与验收

改动面：`probe`、`registry`、`forward`、`server` 的零端点短路、`config`、Admin 链详情、Dashboard 链详情、`config.toml` 样例。测试不访问外网，上游用本地 mock。

验收：

- `cargo fmt --check`、`cargo clippy -- -D warnings`、`cargo test` 通过。
- 区块 1 返回余额 → `archive=yes` 且归档时延 > 0；返回 missing trie node → `archive=no`，端点仍按存活探针晋级，failure 不增加。
- 链头低于门槛时不发 `eth_getBalance`，结论保持 `unknown`。
- 同一端点在 `archive_interval` 内不重复归档读取。
- 公共池成功时兜底请求数为 0。公共池失败后兜底成功，用户可见错误为 0，指标里没有 URL 路径中的密钥。
- 公共池与兜底都失败，且公共池当时是 Active，用户可见错误 +1。
- 公共池为空但配了兜底时，请求由兜底完成。
- Dashboard 链详情能看到 Archive / Archive latency 列；配了兜底时显示脱敏 URL。
