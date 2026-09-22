use std::{sync::OnceLock, time::Duration};

use regex::Regex;
use reqwest::{
    StatusCode,
    header::{CONTENT_TYPE, HeaderMap, RETRY_AFTER},
};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultKind {
    RateLimited,
    Authentication,
    ServerError,
    ClientError,
    Html,
    NonJson,
    InvalidResponse,
    RpcError,
    Timeout,
    Transport,
    Slow,
    Lagging,
}

impl FaultKind {
    pub fn requires_immediate_cooling(self) -> bool {
        matches!(self, Self::RateLimited | Self::Authentication)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureSignal {
    pub kind: FaultKind,
    pub retry_after: Option<Duration>,
}

impl FailureSignal {
    pub const fn new(kind: FaultKind) -> Self {
        Self {
            kind,
            retry_after: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResponseClassification {
    Valid(Value),
    Degraded { response: Value, fault: FaultKind },
    Failure(FailureSignal),
}

pub fn classify_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    elapsed: Duration,
    request_id: &Value,
    slow_threshold: Duration,
) -> ResponseClassification {
    let body_text = String::from_utf8_lossy(body);

    if status == StatusCode::TOO_MANY_REQUESTS {
        return ResponseClassification::Failure(FailureSignal {
            kind: FaultKind::RateLimited,
            retry_after: parse_retry_after(headers),
        });
    }
    if status == StatusCode::FORBIDDEN && is_quota_message(&body_text) {
        return ResponseClassification::Failure(FailureSignal {
            kind: FaultKind::RateLimited,
            retry_after: parse_retry_after(headers),
        });
    }
    if status == StatusCode::FORBIDDEN && is_authentication_message(&body_text) {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::Authentication));
    }
    if status.is_server_error() {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::ServerError));
    }
    if is_html(headers, &body_text) {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::Html));
    }

    let response: Value = match serde_json::from_slice(body) {
        Ok(response) => response,
        Err(_) => {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::NonJson));
        }
    };
    let Some(object) = response.as_object() else {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::InvalidResponse));
    };
    if object.get("id") != Some(request_id)
        || (!object.contains_key("result") && !object.contains_key("error"))
    {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::InvalidResponse));
    }

    if let Some(error) = object.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_quota_message(message) {
            return ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::RateLimited,
                retry_after: parse_retry_after(headers),
            });
        }
        if is_authentication_message(message) {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::Authentication));
        }
        if !is_chain_error(error) {
            return ResponseClassification::Failure(FailureSignal::new(FaultKind::RpcError));
        }
    } else if !status.is_success() {
        return ResponseClassification::Failure(FailureSignal::new(FaultKind::ClientError));
    }

    if elapsed > slow_threshold {
        ResponseClassification::Degraded {
            response,
            fault: FaultKind::Slow,
        }
    } else {
        ResponseClassification::Valid(response)
    }
}

pub fn is_chain_error(error: &Value) -> bool {
    let code = error.get("code").and_then(Value::as_i64);
    if matches!(code, Some(-32700 | -32600 | -32601 | -32602)) {
        return true;
    }
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    message.contains("execution reverted")
        || message.contains("revert reason")
        || message.contains("already known")
        || message.contains("nonce too low")
        || message.contains("insufficient funds")
}

pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    Some(
        retry_at
            .duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn is_quota_message(message: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            Regex::new(
                r"(?i)rate[ _-]?limit|too many requests|request rate exceeded|compute units?|capacity|throttl|quota",
            )
            .expect("valid quota signal regex")
        })
        .is_match(message)
}

fn is_authentication_message(message: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            Regex::new(
                r"(?i)api[ _-]?key|authentication|unauthori[sz]ed|project[ _-]?id|access[ _-]?token|missing credentials|invalid credentials",
            )
            .expect("valid authentication signal regex")
        })
        .is_match(message)
}

/// 归档探测结论。传输失败、限频和无法判断的错误保持 `Inconclusive`，
/// 不把「不是归档节点」写成健康故障。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveClass {
    Yes,
    No,
    Inconclusive,
}

/// 一次历史读取的结论。余额、取块和 `debug_traceTransaction` 共用这套分类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceClass {
    /// 返回了 trace 结果，说明这个区块的状态能被重放。
    Ok,
    /// 明确是历史状态被裁掉，或没有那么老的数据。
    Pruned,
    /// 节点没开 debug 命名空间。不能据此判断是不是归档。
    Unsupported,
    Inconclusive,
}

/// 近块和远块两次 trace 合在一起的结论。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceVerdict {
    /// 近处和更早的区块都能 trace。
    Archive,
    /// 近处能 trace，更早的区块被裁掉。这是默认全节点的形状。
    FullNode,
    /// 没有 debug 接口。
    Unavailable,
    Inconclusive,
}

/// debug 结论优先。没有 debug 接口或这次 trace 说不清时，退回余额探测的结论。
pub fn resolve_archive_class(balance: ArchiveClass, trace: TraceVerdict) -> ArchiveClass {
    match trace {
        TraceVerdict::Archive => ArchiveClass::Yes,
        TraceVerdict::FullNode => ArchiveClass::No,
        TraceVerdict::Unavailable | TraceVerdict::Inconclusive => balance,
    }
}

/// 用一次历史状态读取判断端点能否提供归档状态。
///
/// 调用方固定查询区块 1 的余额。能返回数量就是归档；错误信息表明状态已被裁剪、
/// 或不提供那么老的区块，则不是归档。限频、鉴权、5xx、HTML 和超时都不下结论。
pub fn classify_archive_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &Value,
) -> ArchiveClass {
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return ArchiveClass::Inconclusive;
    }
    let body_text = String::from_utf8_lossy(body);
    if is_html(headers, &body_text) {
        return ArchiveClass::Inconclusive;
    }
    let Ok(response) = serde_json::from_slice::<Value>(body) else {
        return ArchiveClass::Inconclusive;
    };
    let Some(object) = response.as_object() else {
        return ArchiveClass::Inconclusive;
    };
    if object.get("id") != Some(request_id) {
        return ArchiveClass::Inconclusive;
    }
    if let Some(error) = object.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // 先看历史状态语义。付费套餐拒绝归档时常常同时带 project id / api key，
        // 但存活探针刚成功过，这只说明该端点不能提供归档状态。
        if is_historical_state_unavailable(message) {
            return ArchiveClass::No;
        }
        return ArchiveClass::Inconclusive;
    }
    if !status.is_success() {
        return ArchiveClass::Inconclusive;
    }
    match object.get("result").and_then(Value::as_str) {
        Some(result) if is_hex_quantity(result) => ArchiveClass::Yes,
        _ => ArchiveClass::Inconclusive,
    }
}

/// 历史读取的响应。结果只要是非空 JSON 值就算这次调用完成。
pub fn classify_trace_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &Value,
) -> TraceClass {
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return TraceClass::Inconclusive;
    }
    let body_text = String::from_utf8_lossy(body);
    if is_html(headers, &body_text) {
        return TraceClass::Inconclusive;
    }
    let Ok(response) = serde_json::from_slice::<Value>(body) else {
        return TraceClass::Inconclusive;
    };
    let Some(object) = response.as_object() else {
        return TraceClass::Inconclusive;
    };
    if object.get("id") != Some(request_id) {
        return TraceClass::Inconclusive;
    }
    if let Some(error) = object.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if is_historical_state_unavailable(message) {
            return TraceClass::Pruned;
        }
        if is_debug_unsupported(error.get("code").and_then(Value::as_i64), message) {
            return TraceClass::Unsupported;
        }
        return TraceClass::Inconclusive;
    }
    if !status.is_success() {
        return TraceClass::Inconclusive;
    }
    match object.get("result") {
        Some(Value::Null) | None => TraceClass::Inconclusive,
        Some(_) => TraceClass::Ok,
    }
}

/// 从 `callTracer` 的调用树里数内部交易。根调用是交易本身，不算内部交易。
/// 认不出 trace 形状时返回 `None`。
pub fn internal_transaction_count(trace: &Value) -> Option<usize> {
    match trace {
        Value::Object(_) => Some(nested_internal_count(trace)),
        Value::Array(frames) => {
            if frames
                .iter()
                .any(|frame| frame.get("traceAddress").is_some())
            {
                return Some(
                    frames
                        .iter()
                        .filter(|frame| {
                            frame
                                .get("traceAddress")
                                .and_then(Value::as_array)
                                .is_some_and(|address| !address.is_empty())
                        })
                        .count(),
                );
            }
            if frames.iter().all(Value::is_object) {
                return Some(frames.iter().map(nested_internal_count).sum());
            }
            None
        }
        _ => None,
    }
}

/// 选一笔用来解析内部交易的交易。优先带 calldata 的调用，其次任意一笔。
pub fn transaction_hash_for_trace(block: &Value) -> Option<&str> {
    let transactions = block.get("transactions")?.as_array()?;
    let preferred = transactions.iter().find(|tx| has_calldata(tx));
    let transaction = preferred.or_else(|| transactions.first())?;
    match transaction {
        Value::String(hash) => Some(hash.as_str()),
        Value::Object(_) => transaction.get("hash").and_then(Value::as_str),
        _ => None,
    }
}

fn has_calldata(transaction: &Value) -> bool {
    let Some(input) = transaction.get("input").and_then(Value::as_str) else {
        return false;
    };
    let digits = input.strip_prefix("0x").unwrap_or(input);
    !digits.is_empty() && digits.chars().any(|character| character != '0')
}

fn nested_internal_count(trace: &Value) -> usize {
    let Some(calls) = trace.get("calls").and_then(Value::as_array) else {
        return 0;
    };
    calls
        .iter()
        .map(|call| 1 + nested_internal_count(call))
        .sum()
}

fn is_debug_unsupported(code: Option<i64>, message: &str) -> bool {
    if code == Some(-32601) {
        return true;
    }
    let message = message.to_ascii_lowercase();
    message.contains("does not exist")
        || message.contains("method not found")
        || message.contains("not supported")
        || message.contains("namespace")
        || (message.contains("debug") && message.contains("not available"))
}

fn is_hex_quantity(value: &str) -> bool {
    let Some(digits) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    else {
        return false;
    };
    !digits.is_empty()
        && digits
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

fn is_historical_state_unavailable(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    const NEEDLES: &[&str] = &[
        "missing trie node",
        "trie node",
        "historical state",
        "state is not available",
        "state not available",
        "state unavailable",
        "world state",
        "old state",
        "header not found",
        "block not found",
        "unknown block",
        "requires archive",
        "not an archive",
        "non-archive",
        "archive node",
        "archive state",
        "state histor",
        "ancient block",
        "missing historical",
        "history has been",
        "has been pruned",
        "pruned",
    ];
    !message.is_empty() && NEEDLES.iter().any(|needle| message.contains(needle))
}

fn is_html(headers: &HeaderMap, body: &str) -> bool {
    let content_type_is_html = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/html"));
    let trimmed = body.trim_start().to_ascii_lowercase();
    content_type_is_html || trimmed.starts_with("<!doctype html") || trimmed.starts_with("<html")
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderValue;
    use serde_json::json;

    use super::*;

    fn classify(status: StatusCode, headers: HeaderMap, value: Value) -> ResponseClassification {
        classify_response(
            status,
            &headers,
            value.to_string().as_bytes(),
            Duration::from_millis(10),
            &json!(1),
            Duration::from_secs(4),
        )
    }

    #[test]
    fn recognizes_http_rate_limit_and_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("17"));
        assert_eq!(
            classify_response(
                StatusCode::TOO_MANY_REQUESTS,
                &headers,
                b"rate limited",
                Duration::ZERO,
                &json!(1),
                Duration::from_secs(4),
            ),
            ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::RateLimited,
                retry_after: Some(Duration::from_secs(17)),
            })
        );
    }

    #[test]
    fn recognizes_provider_quota_and_authentication_messages() {
        for message in [
            "rate limit exceeded",
            "Too Many Requests",
            "request rate exceeded",
            "compute unit capacity exhausted",
            "request throttled",
            "monthly quota exceeded",
        ] {
            let result = classify(
                StatusCode::OK,
                HeaderMap::new(),
                json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000, "message":message}}),
            );
            assert!(matches!(
                result,
                ResponseClassification::Failure(FailureSignal {
                    kind: FaultKind::RateLimited,
                    ..
                })
            ));
        }

        let auth = classify(
            StatusCode::OK,
            HeaderMap::new(),
            json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000, "message":"API key is required"}}),
        );
        assert!(matches!(
            auth,
            ResponseClassification::Failure(FailureSignal {
                kind: FaultKind::Authentication,
                ..
            })
        ));
    }

    #[test]
    fn passes_chain_errors_through() {
        for error in [
            json!({"code":3, "message":"execution reverted: denied"}),
            json!({"code":-32601, "message":"method not found"}),
            json!({"code":-32602, "message":"invalid params"}),
        ] {
            let response = json!({"jsonrpc":"2.0", "id":1, "error":error});
            assert_eq!(
                classify(StatusCode::OK, HeaderMap::new(), response.clone()),
                ResponseClassification::Valid(response)
            );
        }
    }

    #[test]
    fn rejects_html_non_json_server_errors_and_wrong_ids() {
        let mut html_headers = HeaderMap::new();
        html_headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
        let cases = [
            (
                classify_response(
                    StatusCode::OK,
                    &html_headers,
                    b"<html>bad gateway</html>",
                    Duration::ZERO,
                    &json!(1),
                    Duration::from_secs(4),
                ),
                FaultKind::Html,
            ),
            (
                classify_response(
                    StatusCode::OK,
                    &HeaderMap::new(),
                    b"not json",
                    Duration::ZERO,
                    &json!(1),
                    Duration::from_secs(4),
                ),
                FaultKind::NonJson,
            ),
            (
                classify(
                    StatusCode::BAD_GATEWAY,
                    HeaderMap::new(),
                    json!({"id":1,"result":"unused"}),
                ),
                FaultKind::ServerError,
            ),
            (
                classify(
                    StatusCode::OK,
                    HeaderMap::new(),
                    json!({"jsonrpc":"2.0","id":2,"result":"0x1"}),
                ),
                FaultKind::InvalidResponse,
            ),
        ];
        for (classification, expected) in cases {
            assert!(matches!(
                classification,
                ResponseClassification::Failure(FailureSignal { kind, .. }) if kind == expected
            ));
        }
    }

    #[test]
    fn marks_slow_valid_response_as_degraded() {
        let response = json!({"jsonrpc":"2.0", "id":1, "result":"0x1"});
        assert_eq!(
            classify_response(
                StatusCode::OK,
                &HeaderMap::new(),
                response.to_string().as_bytes(),
                Duration::from_secs(5),
                &json!(1),
                Duration::from_secs(4),
            ),
            ResponseClassification::Degraded {
                response,
                fault: FaultKind::Slow,
            }
        );
    }

    #[test]
    fn classifies_archive_balances_and_pruned_history() {
        let id = json!("rpcrouter-probe-archive");
        let yes = classify_archive_response(
            StatusCode::OK,
            &HeaderMap::new(),
            json!({"jsonrpc":"2.0","id":id,"result":"0x0"})
                .to_string()
                .as_bytes(),
            &id,
        );
        assert_eq!(yes, ArchiveClass::Yes);

        for message in [
            "missing trie node abc",
            "historical state 0x1 is not available",
            "project ID does not have access to archive state",
            "header not found",
            "state histories haven't been fully indexed yet",
        ] {
            let class = classify_archive_response(
                StatusCode::OK,
                &HeaderMap::new(),
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":message}})
                    .to_string()
                    .as_bytes(),
                &id,
            );
            assert_eq!(class, ArchiveClass::No, "{message}");
        }

        let limited = classify_archive_response(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            b"slow down",
            &id,
        );
        assert_eq!(limited, ArchiveClass::Inconclusive);
        let internal = classify_archive_response(
            StatusCode::OK,
            &HeaderMap::new(),
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"internal error"}})
                .to_string()
                .as_bytes(),
            &id,
        );
        assert_eq!(internal, ArchiveClass::Inconclusive);
    }

    #[test]
    fn classifies_debug_traces_and_prefers_them_over_balance() {
        let id = json!("rpcrouter-probe-trace");
        let ok = classify_trace_response(
            StatusCode::OK,
            &HeaderMap::new(),
            json!({"jsonrpc":"2.0","id":id,"result":{"type":"CALL","gasUsed":"0x0"}})
                .to_string()
                .as_bytes(),
            &id,
        );
        assert_eq!(ok, TraceClass::Ok);
        let pruned = classify_trace_response(
            StatusCode::OK,
            &HeaderMap::new(),
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"missing trie node"}})
                .to_string()
                .as_bytes(),
            &id,
        );
        assert_eq!(pruned, TraceClass::Pruned);
        let missing = classify_trace_response(
            StatusCode::OK,
            &HeaderMap::new(),
            json!({
                "jsonrpc":"2.0",
                "id":id,
                "error":{"code":-32601,"message":"the method debug_traceCall does not exist/is not available"}
            })
            .to_string()
            .as_bytes(),
            &id,
        );
        assert_eq!(missing, TraceClass::Unsupported);
        // 历史状态不可用要先于 “not available”，不能当成没开 debug。
        assert_eq!(
            resolve_archive_class(ArchiveClass::Yes, TraceVerdict::FullNode),
            ArchiveClass::No
        );
        assert_eq!(
            resolve_archive_class(ArchiveClass::No, TraceVerdict::Archive),
            ArchiveClass::Yes
        );
        assert_eq!(
            resolve_archive_class(ArchiveClass::Yes, TraceVerdict::Unavailable),
            ArchiveClass::Yes
        );
    }

    #[test]
    fn parses_internal_transactions_and_picks_a_contract_call() {
        let trace = json!({
            "type": "CALL",
            "from": "0x0000000000000000000000000000000000000001",
            "to": "0x0000000000000000000000000000000000000002",
            "calls": [
                {
                    "type": "DELEGATECALL",
                    "from": "0x0000000000000000000000000000000000000002",
                    "to": "0x0000000000000000000000000000000000000003",
                    "calls": [
                        {"type": "STATICCALL", "from": "0x3", "to": "0x4"}
                    ]
                }
            ]
        });
        assert_eq!(internal_transaction_count(&trace), Some(2));
        assert_eq!(internal_transaction_count(&json!({"type":"CALL"})), Some(0));
        let flat = json!([
            {"type":"call","traceAddress":[]},
            {"type":"call","traceAddress":[0]},
            {"type":"create","traceAddress":[0,0]}
        ]);
        assert_eq!(internal_transaction_count(&flat), Some(2));
        assert_eq!(internal_transaction_count(&json!("0x1")), None);

        let block = json!({
            "transactions": [
                {"hash":"0xplain","input":"0x"},
                {"hash":"0xcontract","input":"0xa9059cbb"}
            ]
        });
        assert_eq!(transaction_hash_for_trace(&block), Some("0xcontract"));
        assert_eq!(
            transaction_hash_for_trace(&json!({"transactions":[]})),
            None
        );
    }
}
