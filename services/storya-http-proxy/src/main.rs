use std::{env, error::Error, time::Instant};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
            CONTENT_LENGTH, CONTENT_RANGE, LOCATION, RANGE,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let address = env::var("STORYA_HTTP_PROXY_ADDRESS").unwrap_or_else(|_| "0.0.0.0:80".to_owned());
    let listener = TcpListener::bind(&address).await?;
    let client = Client::builder().redirect(Policy::none()).build()?;
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
    let upstream = state
        .client
        .request(method.clone(), target.clone())
        .headers(copy_request_headers(&request_headers))
        .send()
        .await
        .map_err(ProxyError::upstream)?;

    let status = upstream.status();
    let mut response_headers = copy_response_headers(upstream.headers());
    rewrite_redirect_location(status, &target, &mut response_headers)?;
    info!(
        method = %method,
        target_host = target.host_str().unwrap_or("-"),
        status = status.as_u16(),
        range = request_headers
            .get(RANGE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        content_range = response_headers
            .get(CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        content_length = response_headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("-"),
        response_head_ms = started_at.elapsed().as_millis(),
        "proxy request"
    );
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from_stream(upstream.bytes_stream())
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    Ok(with_cors(response))
}

fn decode_target(encoded_target: &str) -> Result<Url, ProxyError> {
    let value = encoded_target
        .strip_suffix(".bin")
        .ok_or_else(|| ProxyError::bad_request("Proxy URL 必须以 .bin 结尾"))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ProxyError::bad_request("Proxy URL 包含无效的 Base64URL target"))?;
    let target = String::from_utf8(bytes)
        .map_err(|_| ProxyError::bad_request("Proxy URL target 不是 UTF-8"))?;
    let mut url = Url::parse(&target)
        .map_err(|_| ProxyError::bad_request("Proxy URL target 不是有效 URL"))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ProxyError::bad_request(
            "Proxy URL target 必须使用 HTTP 或 HTTPS",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ProxyError::bad_request("Proxy URL target 不能包含用户信息"));
    }
    url.set_fragment(None);
    Ok(url)
}

fn encode_proxy_path(target: &Url) -> String {
    let encoded = URL_SAFE_NO_PAD.encode(target.as_str());
    format!("/proxy/{encoded}.bin")
}

fn rewrite_redirect_location(
    status: StatusCode,
    target: &Url,
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
        .join(location)
        .map_err(|_| ProxyError::upstream_response("上游 Location 不是有效 URL"))?;
    if redirect_target.scheme() != "http" && redirect_target.scheme() != "https" {
        return Err(ProxyError::upstream_response(
            "上游 Location 必须使用 HTTP 或 HTTPS",
        ));
    }
    let value = HeaderValue::from_str(&encode_proxy_path(&redirect_target))
        .map_err(|_| ProxyError::upstream_response("无法编码上游 Location"))?;
    headers.insert(LOCATION, value);
    Ok(())
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
        with_cors((self.status, self.message).into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_round_trip() {
        let target = Url::parse("https://media.example.com/视频/a.ts?token=a/b+c==")
            .expect("测试 URL 必须有效");
        let path = encode_proxy_path(&target);
        let encoded = path
            .strip_prefix("/proxy/")
            .expect("Proxy path 必须有正确前缀");
        let decoded = decode_target(encoded).expect("编码后的 target 必须可解码");
        assert_eq!(decoded, target);
    }

    #[test]
    fn rejects_non_http_target() {
        let encoded = format!("{}.bin", URL_SAFE_NO_PAD.encode("file:///etc/passwd"));
        let error = decode_target(&encoded).expect_err("非 HTTP target 必须被拒绝");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn strips_proxy_and_cookie_headers() {
        let mut request_headers = HeaderMap::new();
        request_headers.insert("range", HeaderValue::from_static("bytes=0-9"));
        request_headers.insert("cf-ray", HeaderValue::from_static("test"));
        let copied_request = copy_request_headers(&request_headers);
        assert_eq!(
            copied_request.get("range"),
            Some(&HeaderValue::from_static("bytes=0-9"))
        );
        assert!(!copied_request.contains_key("cf-ray"));

        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-range", HeaderValue::from_static("bytes 0-9/20"));
        response_headers.insert("set-cookie", HeaderValue::from_static("session=test"));
        let copied_response = copy_response_headers(&response_headers);
        assert!(copied_response.contains_key("content-range"));
        assert!(!copied_response.contains_key("set-cookie"));
    }
}
