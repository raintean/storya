# HTTP Transport 设计

本文描述 Storya 当前采用的通用 HTTP Transport、HTTP-over-WebSocket 线协议、客户端连接池和 Edge Worker relay。Transport 只表达 HTTP 请求与响应，不理解 HLS、Segment、Range 调度或其他媒体业务。

## 组件边界

```text
storya-hls-loader
        |
        v
storya-transport
  |            |
  |            +---- WebSocketHttpTransport ----+
  |                                              |
  +---- FetchHttpTransport                       v
                                           storya-edge-worker
                                                  |
                                                  v
                                            Cloudflare fetch
                                                  |
                                                  v
                                               HTTP 源站
```

- `storya-transport` 提供统一的 `HttpTransport` 接口，以及 Fetch 和 WebSocket 两种实现。
- `storya-hls-loader` 继续负责 Range 规划、并发调度、抢占、慢速补救和重试，只把构造完成的 HTTP Request 交给 Transport。
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

当前 WebSocket 实现只接受 GET 和 HEAD，不支持请求体。`Range`、`Content-Range`、缓存控制和条件请求都按普通 HTTP header 传输，relay 不解析媒体含义。Fetch 实现保持浏览器原生请求行为。

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

Frame 使用 1 字节 kind、4 字节大端 sequence 和 payload。控制 payload 使用 Protobuf，`RESPONSE_BODY` payload 是原始响应字节。响应体按不超过 64 KiB 的 Frame 连续发送。

HTTP 4xx、5xx 仍然是正常 `RESPONSE_HEAD`，只有协议错误、上游 Fetch 失败或资源限制才发送 `ERROR`。每个事务只能以 `RESPONSE_END`、`CANCELED` 或 `ERROR` 中的一种状态结束。

## 取消

Request 的 AbortSignal 或 response body cancel 会立即使本地消费者结束，并发送 `CANCEL`。连接进入 canceling，在 relay 确认上游 Fetch 和 body reader 已经停止后返回 `CANCELED`；收到确认前该连接不能承载新请求。

取消确认超过 10 秒时直接关闭连接。连接关闭或协议错误会使当前事务产生 Transport failure，HLS 加载器继续按原有策略决定是否重试。

## 响应界限与流控

WebSocket Transport 不实现应用层 flow control。加载器的 Range 请求本身有明确字节边界，未知长度请求使用 Transport 的响应上限；客户端请求头声明 `max_response_bytes`，relay 还施加 64 MiB 全局硬上限。

relay 边读取 Cloudflare Fetch body 边发送固定大小 Frame，不等待客户端 ACK。客户端和 relay 都统计实际响应字节，源站忽略 Range 或返回超限响应时取消上游请求并返回错误。

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

## 实现状态

通用接口、Fetch Transport、WebSocket Transport、动态连接池、取消、心跳、连接老化、响应上限、Transport Schema 和 Edge Worker relay 均已实现。HLS 加载器默认使用 Fetch；调用方显式提供 WebSocket Transport 后使用 relay，其他加载与调度逻辑不变。

## 修改历史

- 2026-08-05: 建立通用 HTTP Transport、HTTP-over-WebSocket 串行连接协议、动态连接池和 `storya-edge-worker` relay。
