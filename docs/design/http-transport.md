# HTTP Transport 设计

本文描述 Storya 当前采用的通用 HTTP Transport、基于普通 Fetch 的多域名 HTTP Proxy、HTTP-over-WebSocket 线协议、客户端连接池和 Edge Worker relay。Transport 只表达 HTTP 请求与响应，不理解 HLS、Segment、Range 调度或其他媒体业务。

## 组件边界

```text
storya-hls-loader
        |
        v
storya-transport
  |
  +---- FetchHttpTransport --------------------------------------> HTTP 源站
  |
  +---- ProxyHttpTransport ----> Cloudflare CDN ----> storya-http-proxy ----> HTTP 源站
  |
  +---- WebSocketHttpTransport ---------------------> storya-edge-worker ----> HTTP 源站
```

- `storya-transport` 提供统一的 `HttpTransport` 接口，以及 Browser Fetch、HTTP Proxy 和 WebSocket 三种实现。
- `storya-hls-loader` 继续负责 Range 规划、并发调度、抢占、慢速补救和重试，只把构造完成的 HTTP Request 交给 Transport。
- `storya-http-proxy` 是 Rust 实现的无状态 HTTP proxy。第一版只接受 GET/HEAD，直接流式转发上游响应，不理解媒体语义，也不建立本地缓存。
- `storya-edge-worker` 是边缘能力的单一 Cloudflare Worker 部署单元。当前只有 `/transport` HTTP relay 和 `/health`；未来媒体能力必须使用独立模块和接口，不得进入 Transport 协议。
- Transport 控制消息 Schema 位于 `storya-protocol/proto/transport`。

## HTTP 接口

`HttpTransport` 接受 Web 标准 `Request`，返回只包含加载器所需 HTTP 语义的响应:

```ts
interface HttpTransport {
  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>
  destroy(): void
}
```

响应包含 status、status text、最终 URL、headers、body 和 `arrayBuffer()`。调用方交给 HLS 会话的 Transport 所有权随会话转移；会话销毁时同时销毁 Transport。

当前 Proxy 和 WebSocket 实现只接受 GET 和 HEAD，不支持请求体。`Range`、`Content-Range`、缓存控制和条件请求都按普通 HTTP header 传输，proxy 和 relay 不解析媒体含义。Fetch 实现保持浏览器原生请求行为。

## HTTP Proxy Transport

`ProxyHttpTransport` 接受一个或多个 HTTP(S) Proxy Origin。每次请求按轮询顺序选择一个 Origin；HLS 调度器的重试、抢占和慢速补救会自然形成新的请求，从而继续轮换 Origin。Transport 不按目标 URL 建立固定映射，也不在 Origin 之间同步缓存状态。

目标 URL 使用 UTF-8 和无 padding 的 Base64URL 编码为以下地址：

```text
/proxy/<base64url(target-url)>.jpg
```

`.jpg` 后缀让 Cloudflare 把该路径当作可缓存静态图片资源，配合 `Content-Type: image/jpeg` 获得更长的边缘缓存有效期。第一版不加密，也没有签名；该编码只用于把完整目标 URL 安全放入 path，不是访问控制。公开生产部署前需要增加服务端签发或等价的授权机制。

Transport 通过标准 Fetch 发送原请求的 method 和 headers，不发送浏览器 cookie。Rust proxy 解码目标 URL、限制 scheme 为 HTTP/HTTPS、过滤逐跳和代理基础设施 header，并流式返回上游 status、headers 和 body。上游重定向的 `Location` 会被解析并重新编码为同一 Proxy Origin 下的 `/proxy/...jpg`，因此浏览器仍按标准 Fetch 重定向流程执行，最终响应 URL 也能由 Transport 还原为真实上游 URL。

Rust proxy 为成功和错误响应统一增加 CORS header，并暴露全部响应 header，使浏览器可以读取 `Content-Range`、`Content-Length`、ETag 和缓存诊断信息。任意目标 URL、无签名和未拦截内网地址的组合只适合当前开发验证，不是公开代理的安全终态。

`storya-http-proxy` 默认监听 `0.0.0.0:80`，部署环境可以通过 `STORYA_HTTP_PROXY_ADDRESS` 覆盖完整监听地址。

多个 Cloudflare for SaaS custom hostname 可以指向同一个 `storya-http-proxy` fallback origin。客户端只需要拿到可用 Origin 列表；域名更换时重建 Transport 即可，不要求 target 到域名的稳定映射。

## WebSocket 事务

每条 WebSocket 同时只承载一个 HTTP 事务。事务完整结束后连接才能复用，不在单条 WebSocket 内进行请求多路复用；HTTP 并发来自多条 WebSocket 连接。

```text
idle -> requesting -> streaming -> idle
             |            |
             +-> canceling+-> idle
```

客户端为每条连接维护递增 sequence。sequence 只隔离取消竞态和迟到帧，不允许并发事务。

控制帧包括:

- `REQUEST_HEAD`
- `RESPONSE_HEAD`
- `RESPONSE_END`
- `CANCEL` / `CANCELED`
- `PING` / `PONG`
- `ERROR`

Frame 使用 1 字节 kind、4 字节大端 sequence 和 payload。控制 payload 使用 Protobuf，`RESPONSE_BODY` payload 是原始响应字节。relay 使用 BYOB reader 将上游响应聚合成不超过 256 KiB 的 Frame；流结束时发送不足 256 KiB 的尾帧。聚合不对首帧做特殊处理，也不保留上游流原始分块边界。

HTTP 4xx、5xx 仍然是正常 `RESPONSE_HEAD`，只有协议错误、上游 Fetch 失败或资源限制才发送 `ERROR`。每个事务只能以 `RESPONSE_END`、`CANCELED` 或 `ERROR` 中的一种状态结束。

## 取消

Request 的 AbortSignal 或 response body cancel 会立即使本地消费者结束，并发送 `CANCEL`。连接进入 canceling，在 relay 确认上游 Fetch 和 body reader 已经停止后返回 `CANCELED`；收到确认前该连接不能承载新请求。

取消确认超过 10 秒时直接关闭连接。连接关闭或协议错误会使当前事务产生 Transport failure，HLS 加载器继续按原有策略决定是否重试。

relay 使用 Cloudflare runtime 的标准 WebSocket 自动关闭握手。客户端关闭 WebSocket 时，runtime 自动回送 Close，relay 的关闭回调只终止并等待仍在进行的上游事务收敛，不重复调用 `close()`。消息处理、上游代理和取消属于同一条被 ExecutionContext 跟踪的异步任务链；关闭和错误事件会输出结构化 Worker 日志。连接池因请求次数、寿命或空闲回收而发起的 `1000` 关闭属于正常生命周期，客户端诊断事件不记录为 error。

## 响应界限与流控

WebSocket Transport 不实现应用层 flow control。加载器的 Range 请求本身有明确字节边界，未知长度请求使用 Transport 的响应上限；客户端请求头声明 `max_response_bytes`，relay 还施加 64 MiB 全局硬上限。

relay 使用 BYOB `readAtLeast()` 读取 Cloudflare Fetch body，读满 256 KiB 或遇到流结束后发送一个 Frame，不等待客户端 ACK。小于 256 KiB 的响应上限会直接作为聚合大小，避免为小请求分配过大的缓冲区。客户端和 relay 都统计实际响应字节，源站忽略 Range 或返回超限响应时取消上游请求并返回错误。

Worker 在 WebSocket upgrade response 中使用空的 `Sec-WebSocket-Extensions`，明确拒绝浏览器自动提供的 `permessage-deflate`。Transport 主要承载已经压缩的媒体字节，重复压缩通常不能减少流量，却会持续消耗 Worker CPU。正常的 `1000` 空闲连接关闭不写结构化日志，异常关闭和仍有活动事务的关闭继续保留诊断信息。

## 连接池

HLS 当前最多同时执行 6 个 GET/Range 请求，WebSocket Transport 默认最多保留 12 条连接，为正在取消、建立或退休的连接提供替换空间。12 是连接上限，不改变加载器的请求并发上限。

连接池按以下规则运行:

- 初始不建立连接，有请求且无空闲连接时立即扩容。
- HTTP 请求完成后连接回到空闲池并按串行方式复用。
- 多余空闲连接持续 30 秒后缩容，至少暂时保留一条已经建立的空闲连接。
- 每条连接发送 40 个请求或存活约 90 秒后退休；寿命带有正负 10% 抖动。
- 退休只阻止分配新请求，活动事务可以正常结束；事务结束后关闭连接。
- 请求次数在 `REQUEST_HEAD` 发出时增加，失败和取消请求也计数，PING/PONG 不计数。

连接池不做慢连接判断。当前请求是否过慢、是否需要补救继续由 HLS 调度器决定。

## 心跳与死链接

连接连续 30 秒没有任何收发数据时，客户端发送应用层 `PING`；10 秒内没有收到对应 `PONG` 就关闭连接。业务数据同样更新连接活动时间，正在持续传输时不会额外发送心跳。

心跳只判断连接是否存活，不使用 RTT 估算吞吐量。连接的 wire activity 与 HTTP business activity 独立记录，PING/PONG 不阻止空闲连接缩容。

## Relay 与缓存

`storya-edge-worker` 将 HTTP request head 还原为 Cloudflare `Request`，通过普通 `fetch()` 回源，并原样传输 status、最终 URL、headers 和 body。relay 过滤 Host、Connection、Transfer-Encoding 等 hop-by-hop 或运行时管理的 request header。

浏览器不会把自动生成的 `Referer` 和 `User-Agent` 暴露在 JavaScript `Request.headers` 中。relay 因此从 WebSocket 握手继承浏览器 `User-Agent`，并根据握手 `Origin` 生成只包含 origin 的 `Referer`；Transport 请求显式提供同名 header 时优先使用显式值。该行为用于保持普通浏览器 HTTP 请求语义，不包含站点或媒体特例。

Fetch 子请求继续使用 Cloudflare 标准 HTTP 缓存语义。relay 当前不强制 `cacheEverything` 或自定义 TTL，也不手工缓存 206；`Cache-Control`、ETag、Last-Modified、Age 和 CF-Cache-Status 等信息按响应头传给客户端。

当前 relay 只限制 URL 必须使用 HTTP 或 HTTPS，尚未实现鉴权、请求额度、内网地址拦截或重定向逐跳检查。这些属于公开部署前的安全工作，不改变 Transport 协议。

## HTTP Proxy 与缓存

`storya-http-proxy` 是无状态直通服务，不使用本地缓存。Cloudflare 位于 Proxy Origin 和 Rust service 之间，按 `cloudflare-cdn-cache-control` 缓存 `/proxy/...jpg` 响应。对于 Range chunk（上游返回 206），Rust proxy 把响应包装为 200 可缓存对象：原始 206 状态码移入 `x-storya-proxy-status`、`Content-Range` 移入 `x-storya-proxy-content-range`，浏览器侧输出 `Cache-Control: no-store`，CF 边缘强制 `cloudflare-cdn-cache-control: public, max-age=31536000`（一年），并把 `Content-Type` 改写为 `image/jpeg`、真实类型存入 `x-storya-proxy-content-type` 供客户端还原。HEAD、Range 未命中（非 206）和错误响应统一 `no-store`，不进入边缘缓存；完整 GET（init segment 等）保持上游响应透传。

对于可缓存的 `.jpg` URL，Cloudflare 在 HEAD MISS 时可能向 Rust origin 发送 GET 并缓存完整响应。当前明确接受该行为，将其视为缓存预热；服务和客户端都不增加 HEAD 绕过参数或独立路径。

## 可观测性

Edge Worker 开启持久化 Workers Logs 和 invocation logs，head sampling rate 为 1。Cloudflare Dashboard 会保留全部采样到的 Fetch 和 WebSocket invocation、运行时异常及代码产生的结构化日志；当前不启用 traces。

Rust proxy 在收到上游响应头后输出一条结构化 INFO 日志，包含 method、目标 host、status、Range、Content-Range、Content-Length 和响应头耗时。日志不记录完整目标 URL、query 或 body chunk；上游请求和响应异常继续使用 WARN。

## 实现状态

通用接口、Fetch Transport、Proxy Transport、WebSocket Transport、Rust HTTP proxy、动态连接池、取消、心跳、连接老化、响应上限、Transport Schema 和 Edge Worker relay 均已实现。HLS 加载器默认使用 Fetch；调用方显式提供 Proxy 或 WebSocket Transport 后切换网络执行路径，其他加载与调度逻辑不变。

## 修改历史

- 2026-08-06: Proxy URL 改用 `.jpg` 后缀以延长 Cloudflare 边缘默认缓存；Range chunk 把 `cloudflare-cdn-cache-control` 强制设为一年，并通过 `x-storya-proxy-content-type` 把 `Content-Type` 改写为 `image/jpeg`、真实类型由客户端还原。
- 2026-08-06: 增加多 Origin `ProxyHttpTransport` 和无状态 `storya-http-proxy`，采用 `/proxy/<base64url>.bin` 执行标准 GET/HEAD 直通。
- 2026-08-06: 禁用 WebSocket 压缩，将响应聚合提高到 256 KiB，并减少正常连接关闭日志以降低 relay CPU 消耗。
- 2026-08-05: relay 恢复 runtime 标准 WebSocket 自动关闭握手，并将消息处理、上游代理和取消纳入 ExecutionContext 跟踪。
- 2026-08-05: Edge Worker 开启持久化 Workers Logs 和完整 invocation logs。
- 2026-08-05: relay 改用 BYOB 聚合读取，将上游小块合并成 128 KiB 响应帧，减少 WebSocket 发送和读取唤醒次数。
- 2026-08-05: 建立通用 HTTP Transport、HTTP-over-WebSocket 串行连接协议、动态连接池和 `storya-edge-worker` relay。
