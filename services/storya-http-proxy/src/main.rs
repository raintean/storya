use std::{env, error::Error, fmt, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{
            ACCEPT_RANGES, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
            CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LOCATION, RANGE,
        },
    },
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, Url, redirect::Policy};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct ProxyState {
    client: Client,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ByteRange {
    end_inclusive: u64,
    start: u64,
}

impl fmt::Display for ByteRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "bytes={}-{}", self.start, self.end_inclusive)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProxyTarget {
    head: bool,
    range: Option<ByteRange>,
    url: Url,
}

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    status: &'static str,
}

#[derive(Debug)]
struct ProxyError {
    message: String,
    status: StatusCode,
}

const PROXY_CONTENT_RANGE_HEADER: &str = "x-storya-proxy-content-range";
const PROXY_CONTENT_LENGTH_HEADER: &str = "x-storya-proxy-content-length";
const PROXY_STATUS_HEADER: &str = "x-storya-proxy-status";
const PROXY_CONTENT_TYPE_HEADER: &str = "x-storya-proxy-content-type";
const CLOUDFLARE_CDN_CACHE_CONTROL_HEADER: &str = "cloudflare-cdn-cache-control";
const EDGE_CACHE_CONTROL_VALUE: &str = "public, max-age=31536000";
const PROXY_CONTENT_TYPE_VALUE: &str = "image/jpeg";
const HEAD_DESCRIPTOR_PREFIX: &str = "storya-proxy-head-v1\n";
const RANGE_DESCRIPTOR_PREFIX: &str = "storya-proxy-range-v1\n";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let address = env::var("STORYA_HTTP_PROXY_ADDRESS").unwrap_or_else(|_| "0.0.0.0:80".to_owned());
    let listener = TcpListener::bind(&address).await?;
    let client = Client::builder()
        .redirect(Policy::none())
        .http1_only()
        .build()?;
    let app = create_router(client);

    info!(%address, "storya-http-proxy listening");
    axum::serve(listener, app).await?;

    Ok(())
}

fn create_router(client: Client) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/proxy/{target}",
            get(proxy_request)
                .head(proxy_request)
                .options(proxy_preflight),
        )
        .with_state(ProxyState { client })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "storya-http-proxy",
        status: "ok",
    })
}

async fn proxy_preflight() -> Response {
    with_cors(StatusCode::NO_CONTENT.into_response())
}

async fn proxy_request(
    State(state): State<ProxyState>,
    Path(encoded_target): Path<String>,
    method: Method,
    request_headers: HeaderMap,
) -> Result<Response, ProxyError> {
    let started_at = Instant::now();
    let target = decode_target(&encoded_target)?;
    log_incoming_request(&method, &request_headers, &target);
    if target.head && method != Method::GET {
        warn!(
            request_method = %method,
            expected_method = "GET",
            target_host = target.url.host_str().unwrap_or("-"),
            target_path = target.url.path(),
            descriptor_kind = "head",
            "proxy descriptor method mismatch"
        );
        return Err(ProxyError::bad_request(
            "Proxy HEAD descriptor 必须通过物理 GET 传输",
        ));
    }
    if target.range.is_some() && method != Method::GET {
        warn!(
            request_method = %method,
            expected_method = "GET",
            target_host = target.url.host_str().unwrap_or("-"),
            target_path = target.url.path(),
            descriptor_kind = "range",
            "proxy descriptor method mismatch"
        );
        return Err(ProxyError::bad_request(
            "Proxy Range descriptor 只允许使用 GET",
        ));
    }

    let upstream_method = if target.head {
        Method::HEAD
    } else {
        method.clone()
    };
    let mut upstream_headers = copy_request_headers(&request_headers);
    upstream_headers.remove(RANGE);
    if let Some(range) = target.range {
        let value = HeaderValue::from_str(&range.to_string())
            .map_err(|_| ProxyError::bad_request("Proxy Range 无法转换为 HTTP header"))?;
        upstream_headers.insert(RANGE, value);
    }
    let upstream_range = upstream_headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    info!(
        request_method = %method,
        upstream_method = %upstream_method,
        target_host = target.url.host_str().unwrap_or("-"),
        target_path = target.url.path(),
        descriptor_kind = descriptor_kind(&target),
        upstream_range = %upstream_range,
        "proxy upstream request"
    );
    let upstream = state
        .client
        .request(upstream_method.clone(), target.url.clone())
        .headers(upstream_headers)
        .send()
        .await
        .map_err(ProxyError::upstream)?;

    let upstream_status = upstream.status();
    let upstream_version = upstream.version();
    let mut response_headers = copy_response_headers(upstream.headers());
    rewrite_redirect_location(upstream_status, &target, &mut response_headers)?;
    let downstream_status =
        prepare_downstream_response(&target, upstream_status, &mut response_headers)?;
    info!(
        request_method = %method,
        upstream_method = %upstream_method,
        target_host = target.url.host_str().unwrap_or("-"),
        target_path = target.url.path(),
        descriptor_kind = descriptor_kind(&target),
        upstream_range = %upstream_range,
        upstream_status = upstream_status.as_u16(),
        upstream_version = ?upstream_version,
        downstream_status = downstream_status.as_u16(),
        range_wrapped = target.range.is_some() && upstream_status == StatusCode::PARTIAL_CONTENT,
        content_range = response_headers
            .get(CONTENT_RANGE)
            .or_else(|| response_headers.get(PROXY_CONTENT_RANGE_HEADER))
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        content_length = response_headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        cache_control = response_headers
            .get(CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        cdn_cache_control = response_headers
            .get(CLOUDFLARE_CDN_CACHE_CONTROL_HEADER)
            .or_else(|| response_headers.get("cdn-cache-control"))
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        response_head_ms = started_at.elapsed().as_millis(),
        "proxy upstream response"
    );
    let body = if target.head || upstream_method == Method::HEAD {
        Body::empty()
    } else {
        Body::from_stream(upstream.bytes_stream())
    };
    let mut response = Response::new(body);
    *response.status_mut() = downstream_status;
    *response.headers_mut() = response_headers;
    Ok(with_cors(response))
}

fn decode_target(encoded_target: &str) -> Result<ProxyTarget, ProxyError> {
    let value = encoded_target
        .strip_suffix(".jpg")
        .ok_or_else(|| ProxyError::bad_request("Proxy URL 必须以 .jpg 结尾"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ProxyError::bad_request("Proxy URL 包含无效的 Base64URL target"))?;
    let descriptor = String::from_utf8(bytes)
        .map_err(|_| ProxyError::bad_request("Proxy URL target 不是 UTF-8"))?;
    let (target, range, head) = decode_target_descriptor(&descriptor)?;
    let mut url =
        Url::parse(target).map_err(|_| ProxyError::bad_request("Proxy URL target 不是有效 URL"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ProxyError::bad_request(
            "Proxy URL target 必须使用 HTTP 或 HTTPS",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ProxyError::bad_request("Proxy URL target 不能包含用户信息"));
    }
    url.set_fragment(None);
    Ok(ProxyTarget { head, range, url })
}

fn decode_target_descriptor(
    descriptor: &str,
) -> Result<(&str, Option<ByteRange>, bool), ProxyError> {
    if let Some(target) = descriptor.strip_prefix(HEAD_DESCRIPTOR_PREFIX) {
        return Ok((target, None, true));
    }
    let Some(value) = descriptor.strip_prefix(RANGE_DESCRIPTOR_PREFIX) else {
        return Ok((descriptor, None, false));
    };
    let mut fields = value.splitn(3, '\n');
    let start = fields
        .next()
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| ProxyError::bad_request("Proxy Range 起点无效"))?;
    let end_inclusive = fields
        .next()
        .and_then(|field| field.parse::<u64>().ok())
        .ok_or_else(|| ProxyError::bad_request("Proxy Range 终点无效"))?;
    let target = fields
        .next()
        .ok_or_else(|| ProxyError::bad_request("Proxy Range descriptor 缺少 target"))?;
    if end_inclusive < start {
        return Err(ProxyError::bad_request("Proxy Range 终点不能小于起点"));
    }
    Ok((
        target,
        Some(ByteRange {
            end_inclusive,
            start,
        }),
        false,
    ))
}

fn encode_proxy_path(target: &ProxyTarget) -> Result<String, ProxyError> {
    let descriptor = match (target.head, target.range) {
        (true, None) => format!("{HEAD_DESCRIPTOR_PREFIX}{}", target.url),
        (true, Some(_)) => {
            return Err(ProxyError::bad_request(
                "HEAD Proxy target 不能同时包含 Range",
            ));
        },
        (false, Some(range)) => format!(
            "{RANGE_DESCRIPTOR_PREFIX}{}\n{}\n{}",
            range.start, range.end_inclusive, target.url
        ),
        (false, None) => target.url.to_string(),
    };
    let encoded = URL_SAFE_NO_PAD.encode(descriptor);
    Ok(format!("/proxy/{encoded}.jpg"))
}

fn rewrite_redirect_location(
    status: StatusCode,
    target: &ProxyTarget,
    headers: &mut HeaderMap,
) -> Result<(), ProxyError> {
    if !status.is_redirection() {
        return Ok(());
    }
    let Some(location) = headers.get(LOCATION) else {
        return Ok(());
    };
    let location = location
        .to_str()
        .map_err(|_| ProxyError::upstream_response("上游 Location 不是有效字符串"))?;
    let redirect_target = target
        .url
        .join(location)
        .map_err(|_| ProxyError::upstream_response("上游 Location 不是有效 URL"))?;
    if redirect_target.scheme() != "http" && redirect_target.scheme() != "https" {
        return Err(ProxyError::upstream_response(
            "上游 Location 必须使用 HTTP 或 HTTPS",
        ));
    }
    let redirect_target = ProxyTarget {
        head: target.head,
        range: target.range,
        url: redirect_target,
    };
    let redirect_path = encode_proxy_path(&redirect_target)?;
    let value = HeaderValue::from_str(&redirect_path)
        .map_err(|_| ProxyError::upstream_response("无法编码上游 Location"))?;
    headers.insert(LOCATION, value);
    Ok(())
}

fn prepare_downstream_response(
    target: &ProxyTarget,
    upstream_status: StatusCode,
    headers: &mut HeaderMap,
) -> Result<StatusCode, ProxyError> {
    if target.head {
        set_proxy_status(headers, upstream_status)?;
        if let Some(content_length) = headers.remove(CONTENT_LENGTH) {
            headers.insert(PROXY_CONTENT_LENGTH_HEADER, content_length);
        }
        set_cache_pass(headers);
        return Ok(upstream_status);
    }
    if target.range.is_none() {
        return Ok(upstream_status);
    }
    set_proxy_status(headers, upstream_status)?;
    if upstream_status != StatusCode::PARTIAL_CONTENT {
        set_cache_pass(headers);
        return Ok(upstream_status);
    }

    if let Some(content_range) = headers.remove(CONTENT_RANGE) {
        headers.insert(PROXY_CONTENT_RANGE_HEADER, content_range);
    }
    swap_content_type(headers);
    set_edge_cache_policy(headers);
    Ok(StatusCode::OK)
}

fn set_proxy_status(headers: &mut HeaderMap, status: StatusCode) -> Result<(), ProxyError> {
    let status_value = HeaderValue::from_str(status.as_str())
        .map_err(|_| ProxyError::upstream_response("无法编码上游状态码"))?;
    headers.insert(PROXY_STATUS_HEADER, status_value);
    Ok(())
}

fn swap_content_type(headers: &mut HeaderMap) {
    if let Some(real_content_type) = headers.remove(CONTENT_TYPE) {
        headers.insert(
            HeaderName::from_static(PROXY_CONTENT_TYPE_HEADER),
            real_content_type,
        );
    }
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static(PROXY_CONTENT_TYPE_VALUE),
    );
}

fn set_edge_cache_policy(headers: &mut HeaderMap) {
    headers.remove("cdn-cache-control");
    headers.insert(
        HeaderName::from_static(CLOUDFLARE_CDN_CACHE_CONTROL_HEADER),
        HeaderValue::from_static(EDGE_CACHE_CONTROL_VALUE),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.remove(ACCEPT_RANGES);
}

fn set_cache_pass(headers: &mut HeaderMap) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        HeaderName::from_static("cdn-cache-control"),
        HeaderValue::from_static("no-store"),
    );
    headers.insert(
        HeaderName::from_static(CLOUDFLARE_CDN_CACHE_CONTROL_HEADER),
        HeaderValue::from_static("no-store"),
    );
    headers.remove(ACCEPT_RANGES);
}

fn log_incoming_request(method: &Method, headers: &HeaderMap, target: &ProxyTarget) {
    let mut header_names = headers.keys().map(HeaderName::as_str).collect::<Vec<_>>();
    header_names.sort_unstable();
    let descriptor_range = target.range.map(|range| range.to_string());
    info!(
        request_method = %method,
        target_scheme = target.url.scheme(),
        target_host = target.url.host_str().unwrap_or("-"),
        target_path = target.url.path(),
        descriptor_kind = descriptor_kind(target),
        descriptor_range = descriptor_range.as_deref().unwrap_or("-"),
        incoming_range = header_value(headers, "range"),
        cf_ray = header_value(headers, "cf-ray"),
        cf_ew_via = header_value(headers, "cf-ew-via"),
        cdn_loop = header_value(headers, "cdn-loop"),
        via = header_value(headers, "via"),
        x_forwarded_proto = header_value(headers, "x-forwarded-proto"),
        cache_control = header_value(headers, "cache-control"),
        accept_encoding = header_value(headers, "accept-encoding"),
        user_agent = header_value(headers, "user-agent"),
        header_names = %header_names.join(","),
        "proxy incoming request"
    );
}

fn descriptor_kind(target: &ProxyTarget) -> &'static str {
    if target.head {
        "head"
    } else if target.range.is_some() {
        "range"
    } else {
        "full"
    }
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
}

fn copy_request_headers(source: &HeaderMap) -> HeaderMap {
    copy_headers(source, is_blocked_request_header)
}

fn copy_response_headers(source: &HeaderMap) -> HeaderMap {
    copy_headers(source, is_blocked_response_header)
}

fn copy_headers(source: &HeaderMap, is_blocked: fn(&HeaderName) -> bool) -> HeaderMap {
    let mut target = HeaderMap::with_capacity(source.len());
    for (name, value) in source {
        if !is_blocked(name) {
            target.append(name.clone(), value.clone());
        }
    }
    target
}

fn is_blocked_request_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "host"
            | "content-length"
            | "keep-alive"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "cf-connecting-ip"
            | "cf-ew-via"
            | "cf-ipcountry"
            | "cf-ray"
            | "cf-visitor"
            | "cdn-loop"
            | "x-forwarded-for"
            | "x-forwarded-proto"
            | "x-real-ip"
            | "if-match"
            | "if-modified-since"
            | "if-none-match"
            | "if-range"
            | "if-unmodified-since"
            | "range"
            | PROXY_CONTENT_LENGTH_HEADER
            | PROXY_CONTENT_RANGE_HEADER
            | PROXY_STATUS_HEADER
            | PROXY_CONTENT_TYPE_HEADER
    )
}

fn is_blocked_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-connection"
            | "set-cookie"
            | "set-cookie2"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "clear-site-data"
            | "access-control-allow-credentials"
            | "access-control-allow-headers"
            | "access-control-allow-methods"
            | "access-control-allow-origin"
            | "access-control-expose-headers"
            | "access-control-max-age"
            | PROXY_CONTENT_LENGTH_HEADER
            | PROXY_CONTENT_RANGE_HEADER
            | PROXY_STATUS_HEADER
            | PROXY_CONTENT_TYPE_HEADER
    )
}

fn with_cors(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, OPTIONS"),
    );
    headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
    headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("*"));
    headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    response
}

impl ProxyError {
    fn bad_request(message: &str) -> Self {
        Self {
            message: message.to_owned(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    fn upstream(error: reqwest::Error) -> Self {
        Self {
            message: format!("上游请求失败: {error}"),
            status: StatusCode::BAD_GATEWAY,
        }
    }

    fn upstream_response(message: &str) -> Self {
        Self {
            message: message.to_owned(),
            status: StatusCode::BAD_GATEWAY,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            warn!(status = %self.status, error = %self.message, "proxy request failed");
        }
        let mut response = (self.status, self.message).into_response();
        set_cache_pass(response.headers_mut());
        with_cors(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_round_trip() {
        let target = ProxyTarget {
            head: false,
            range: None,
            url: Url::parse("https://media.example.com/视频/a.ts?token=a/b+c==")
                .expect("测试 URL 必须有效"),
        };
        let path = encode_proxy_path(&target).expect("Proxy path 必须可编码");
        let encoded = path
            .strip_prefix("/proxy/")
            .expect("Proxy path 必须有正确前缀");
        let decoded = decode_target(encoded).expect("编码后的 target 必须可解码");
        assert_eq!(decoded, target);
    }

    #[test]
    fn range_target_round_trip() {
        let target = ProxyTarget {
            head: false,
            range: Some(ByteRange {
                end_inclusive: 2_097_151,
                start: 0,
            }),
            url: Url::parse("https://media.example.com/video/segment.m4s?token=test")
                .expect("测试 URL 必须有效"),
        };
        let path = encode_proxy_path(&target).expect("Range Proxy path 必须可编码");
        let encoded = path
            .strip_prefix("/proxy/")
            .expect("Proxy path 必须有正确前缀");
        let decoded = decode_target(encoded).expect("Range target 必须可解码");
        assert_eq!(decoded, target);
    }

    #[test]
    fn head_target_round_trip() {
        let target = ProxyTarget {
            head: true,
            range: None,
            url: Url::parse("https://media.example.com/video/segment.m4s")
                .expect("测试 URL 必须有效"),
        };
        let path = encode_proxy_path(&target).expect("HEAD Proxy path 必须可编码");
        let encoded = path
            .strip_prefix("/proxy/")
            .expect("Proxy path 必须有正确前缀");
        let decoded = decode_target(encoded).expect("HEAD target 必须可解码");
        assert_eq!(decoded, target);
    }

    #[test]
    fn wraps_partial_response_as_cacheable_object() {
        let target = ProxyTarget {
            head: false,
            range: Some(ByteRange {
                end_inclusive: 2_097_151,
                start: 0,
            }),
            url: Url::parse("https://media.example.com/video/segment.m4s")
                .expect("测试 URL 必须有效"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-2097151/10485760"),
        );
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("2097152"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("video/mp2t"));

        let status =
            prepare_downstream_response(&target, StatusCode::PARTIAL_CONTENT, &mut headers)
                .expect("Partial Content 包装必须成功");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(PROXY_STATUS_HEADER),
            Some(&HeaderValue::from_static("206"))
        );
        assert_eq!(
            headers.get(PROXY_CONTENT_RANGE_HEADER),
            Some(&HeaderValue::from_static("bytes 0-2097151/10485760"))
        );
        assert!(!headers.contains_key(CONTENT_RANGE));
        assert_eq!(
            headers.get(CONTENT_LENGTH),
            Some(&HeaderValue::from_static("2097152"))
        );
        assert_eq!(
            headers.get(CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        assert_eq!(
            headers.get(CLOUDFLARE_CDN_CACHE_CONTROL_HEADER),
            Some(&HeaderValue::from_static("public, max-age=31536000"))
        );
        assert_eq!(
            headers.get(CONTENT_TYPE),
            Some(&HeaderValue::from_static("image/jpeg"))
        );
        assert_eq!(
            headers.get(PROXY_CONTENT_TYPE_HEADER),
            Some(&HeaderValue::from_static("video/mp2t"))
        );
    }

    #[test]
    fn bypasses_cache_for_head_and_ignored_range() {
        for target in [
            ProxyTarget {
                head: true,
                range: None,
                url: Url::parse("https://media.example.com/video/segment.m4s")
                    .expect("测试 URL 必须有效"),
            },
            ProxyTarget {
                head: false,
                range: Some(ByteRange {
                    end_inclusive: 9,
                    start: 0,
                }),
                url: Url::parse("https://media.example.com/video/segment.m4s")
                    .expect("测试 URL 必须有效"),
            },
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_LENGTH, HeaderValue::from_static("10"));
            let downstream = prepare_downstream_response(&target, StatusCode::OK, &mut headers)
                .expect("Cache pass 响应处理必须成功");
            assert_eq!(downstream, StatusCode::OK);
            assert_eq!(
                headers.get(CACHE_CONTROL),
                Some(&HeaderValue::from_static("no-store"))
            );
            assert_eq!(
                headers.get("cdn-cache-control"),
                Some(&HeaderValue::from_static("no-store"))
            );
            assert_eq!(
                headers.get(PROXY_STATUS_HEADER),
                Some(&HeaderValue::from_static("200"))
            );
            if target.head {
                assert!(!headers.contains_key(CONTENT_LENGTH));
                assert_eq!(
                    headers.get(PROXY_CONTENT_LENGTH_HEADER),
                    Some(&HeaderValue::from_static("10"))
                );
            } else {
                assert_eq!(
                    headers.get(CONTENT_LENGTH),
                    Some(&HeaderValue::from_static("10"))
                );
            }
        }
    }

    #[test]
    fn redirect_preserves_range_descriptor() {
        let target = ProxyTarget {
            head: false,
            range: Some(ByteRange {
                end_inclusive: 99,
                start: 0,
            }),
            url: Url::parse("https://media.example.com/path/segment.m4s")
                .expect("测试 URL 必须有效"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(LOCATION, HeaderValue::from_static("../final.m4s"));
        rewrite_redirect_location(StatusCode::FOUND, &target, &mut headers)
            .expect("Range redirect 必须可重写");

        let location = headers
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .expect("重写后的 Location 必须存在");
        let encoded = location
            .strip_prefix("/proxy/")
            .expect("重写后的 Location 必须是 Proxy path");
        let redirected = decode_target(encoded).expect("重写后的 Location 必须可解码");
        assert_eq!(redirected.range, target.range);
        assert_eq!(
            redirected.url.as_str(),
            "https://media.example.com/final.m4s"
        );
    }

    #[test]
    fn rejects_non_http_target() {
        let encoded = format!("{}.jpg", URL_SAFE_NO_PAD.encode("file:///etc/passwd"));
        let error = decode_target(&encoded).expect_err("非 HTTP target 必须被拒绝");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn strips_proxy_and_cookie_headers() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert("range", HeaderValue::from_static("bytes=0-9"));
        request_headers.insert("cf-ray", HeaderValue::from_static("test"));
        let copied_request = copy_request_headers(&request_headers);
        assert!(!copied_request.contains_key("range"));
        assert!(!copied_request.contains_key("cf-ray"));

        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-range", HeaderValue::from_static("bytes 0-9/20"));
        response_headers.insert("set-cookie", HeaderValue::from_static("session=test"));
        let copied_response = copy_response_headers(&response_headers);
        assert!(copied_response.contains_key("content-range"));
        assert!(!copied_response.contains_key("set-cookie"));
    }
}
