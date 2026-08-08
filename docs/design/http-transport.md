# HTTP Transport 设计

本文描述 Storya 当前采用的通用 HTTP Transport、普通 Fetch、基于 HTTP 的多域名 Proxy，以及基于连接池的 HTTP-over-WebSocket relay。Transport 只表达 HTTP 请求与响应，不理解 HLS、Segment、Range 调度或媒体业务。

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

- `storya-transport` 提供统一的 `HttpTransport` 接口和三种网络实现。
- `storya-hls-loader` 的 `ParallelSegmentLoader` 持有一个 `HttpTransport`, 内部 Worker 通过它完成 Range Chunk 加载。默认使用 Fetch, example 可以选择 HTTP Proxy 或 WebSocket relay。HLS 语义始终留在 Loader 中。
- `storya-http-proxy` 是 Rust 实现的无状态 HTTP proxy。
- `storya-edge-worker` 是无状态 WebSocket HTTP relay，每条连接串行处理请求，连接池提供并发。
- WebSocket relay 的控制消息由 `storya-protocol` 中的 Protobuf schema 描述，媒体 body 使用原始二进制 frame。

## HTTP 接口

`HttpTransport` 接受 Web 标准 `Request`，返回加载器所需的 HTTP 响应：

```ts
interface HttpTransport {
  readonly rangeRequestMode?: 'resumable' | 'stable'

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>
  destroy(): void
}
```

Fetch、Proxy 和 WebSocket Transport 都返回流式 response。Transport 不暴露加载器调度策略；超时、抢占和停滞补救由 Loader 通过标准 `Request.signal` 与 `ReadableStream.cancel()` 表达。

## HTTP Proxy Transport

`ProxyHttpTransport` 接受一个或多个 HTTP(S) Proxy Origin。每个 Range 使用目标 URL 和分片位置稳定选择 Origin，域名更换时重建 Transport 即可。

目标 URL、逻辑 method 和可选 Range 使用 UTF-8 与无 padding Base64URL 编码为：

```text
/proxy/<descriptor>.jpg
```

`.jpg` 后缀和 `image/jpeg` Content-Type 用于获得 Cloudflare 静态资源缓存语义。Rust proxy 将上游 206 包装为可缓存的 200，把原始 status、Content-Range、Content-Length 和 Content-Type 放入 `x-storya-proxy-*` header，由客户端恢复。

Rust proxy 不建立本地缓存。HEAD、Range 未命中和错误响应不进入 CDN 缓存。当前 descriptor 没有签名，公开部署前仍需增加授权、Origin allowlist 和内网地址防护。

## WebSocket 事务

每条 WebSocket 同时只处理一个 HTTP 事务。事务结束或取消确认后连接回到 idle，可以继续处理下一个 request，语义类似没有 pipelining 的 HTTP/1.1 Keep-Alive。

```text
connecting -> idle -> busy -> idle
                    |
                    +-> closed
```

加载器并发和单连接并发彼此独立。HTTP 并发由连接池中的多条 WebSocket 提供，不在单条连接上进行 multiplexing。

每个事务使用以下 frame：

```text
client ---- REQUEST_HEAD -----------> Worker
client <--- RESPONSE_HEAD ----------- Worker
client <--- RESPONSE_BODY (0..N) ---- Worker
client <--- RESPONSE_END ------------ Worker

client ---- CANCEL -----------------> Worker
client <--- CANCELED ---------------- Worker
```

`ERROR` 可以在 response head 前终止请求，也可以在流式 body 期间终止响应。每个 frame 携带 sequence，用于隔离取消竞态和上一事务的迟到消息，不用于 multiplexing。Worker 在已有活动事务时收到新的 `REQUEST_HEAD` 会关闭连接。

## 二进制编码

每个 frame 使用 5 字节固定头：1 字节 `TransportFrameKind` 加 4 字节大端 sequence。`REQUEST_HEAD`、`RESPONSE_HEAD` 和 `ERROR` payload 使用 Protobuf；`RESPONSE_BODY` payload 直接承载媒体字节；`RESPONSE_END`、`CANCEL` 和 `CANCELED` 没有 payload。

`HttpRequestHead` 包含 method、URL、request headers 和 `max_response_bytes`。`HttpResponseHead` 包含 status、status text、最终 URL 和原始 response headers。`TransportError` 区分无效请求、不支持的 method、响应过大、上游失败和内部失败；HTTP 4xx、5xx 仍然是正常 HTTP response。

Worker 以 128 KiB 为目标读取上游 body。每次为 frame header 和 body 一次性分配连续缓冲区，BYOB reader 直接写入 header 后方，再把同一视图发送给 WebSocket。客户端 frame decoder 使用 `Uint8Array.subarray()` 暴露 payload，`ReadableStream` 继续传递该视图，不执行媒体数据复制。最后一个 frame 可以小于 128 KiB；Cloudflare runtime 和浏览器内部是否复制不属于协议保证。

## 流式 Response 与上层调度

WebSocket Transport 收到 `RESPONSE_HEAD` 后立即返回 `HttpTransportResponse`，后续 `RESPONSE_BODY` 逐帧进入 `ReadableStream`，`RESPONSE_END` 关闭流。

Fetch、Proxy 和 WebSocket 对 Loader 统一表现为流式 Transport。`ParallelSegmentLoader` 使用 `ReadableStreamDefaultReader` 逐段读取 body, 实时更新 Chunk 已接收字节。它通过 Request AbortSignal 和 body cancel 表达窗口取消、正式读取抢占、响应头超时、响应流量空闲超时与完整请求超时。

连续无数据或请求超时后的重新领取属于 Loader 的 Chunk 调度, 不属于 `FetchHttpTransport`。Transport 仍然只负责把 Fetch response 原样暴露为统一流接口。基于历史吞吐判断“持续有数据但明显过慢”的补救尚未迁移。

## 响应上限

协议允许的 Response body 上限为 32 MiB。客户端为每个请求声明更小的 `maxResponseBytes` 时，Worker 使用更小值。

Worker 在以下位置验证上限：

- 上游 Content-Length 已知时，在读取 body 前拒绝超限响应。
- Content-Length 未知或不可信时，累计读取不超过 `maxResponseBytes + 1`，多出的一个字节用于检测越界。
- 客户端按累计收到的 body frame 字节再次验证本次请求上限。

HEAD 的 body 必须为空。当前生产 Chunk 默认为 2 MiB，因此正常媒体 Range 远低于协议硬上限。

## 取消与超时

Loader 决定何时取消或补救请求，不把“慢连接”概念传给 Transport。Loader 中止 attempt 时触发 `Request.signal`；response head 已经到达后停止读取时还会取消 `ReadableStream`。当前响应头和完整请求时限来自 hls.js `fragLoadPolicy`, body 连续 5 秒没有数据时视为停滞, 同一 Chunk 默认补救 2 次。

WebSocket Transport 把这两种标准取消入口映射为当前 sequence 的 `CANCEL`。Worker 先清空活动事务，再取消 pending BYOB read 并 Abort 上游 Fetch，随后回复 `CANCELED`。客户端只有收到 `CANCELED` 后才把连接恢复为 idle；确认超时会关闭连接，避免未收敛事务被错误复用。取消确认超时由调用方通过 `cancelTimeoutMs` 配置。

Transport 保留不依赖协议消息的生命周期超时：

- connect timeout：连接建立超时后关闭连接。
- cancel timeout：`CANCEL` 长时间没有确认时关闭连接。
- idle timeout：回收超过空闲保留下限的连接。

WebSocket 关闭时 Worker 使用相同的非阻塞清理顺序，确保 pending BYOB read 能够结束，避免 Worker invocation 因未收敛的读取任务被 runtime 判定为挂起。

## 连接池

连接池完全按需创建连接，不主动预热，也不主动补足最低空闲数。所有池化数值由调用方显式配置，Transport 不提供业务默认值。

Example 当前配置：

- 最多 12 条连接。
- 空闲回收下限为 6 条。
- 每条连接最多处理 50 个请求。
- 空闲 30 秒后可以回收。
- connect timeout 为 10 秒。
- cancel timeout 为 10 秒。
- 默认 Response 上限为 32 MiB。

分配 request 时优先选择创建时间最晚、年龄最小的 idle 连接。老连接因为较少获得新请求，会自然积累空闲时间并进入普通 idle 回收流程。

idle timeout 只关闭超出 `minIdleConnections` 的空闲连接。如果池从未扩展到该数量，不会主动创建连接；故障或最大复用次数使连接数降低时也不补建。

连接发送第 50 个 request 时标记 retiring。该 response 完成后关闭连接，不再接受新 request。连接池不再按绝对寿命回收连接，也不发送应用层心跳。

## Relay 行为

Worker 只接受 GET 和 HEAD。它过滤 Host、Connection、Transfer-Encoding 等 hop-by-hop 或运行时管理的 header，从 WebSocket 握手继承浏览器 User-Agent，并根据握手 Origin 生成只包含 origin 的 Referer。

Worker 使用普通 `fetch()` 回源，跟随重定向，先返回 status、最终 URL 和 headers，再流式返回 body。当前只限制 URL scheme 为 HTTP/HTTPS，尚未实现鉴权、请求额度、内网地址拦截或逐跳重定向校验。

Worker 禁用 `permessage-deflate`。媒体通常已经压缩，重复压缩只会增加 CPU。

Worker 使用 Paid 计划运行，CPU time 上限为 300 秒，单次调用 subrequest 上限为 10,000。生产入口只使用 Dashboard 管理的 Custom Domain，关闭 `workers.dev` 和 Preview URL；Wrangler 配置不声明 routes，避免覆盖 Dashboard 已有的 Custom Domain。

## 可观测性

连接池自定义 debug 回调记录连接创建、建立和关闭，包含连接年龄、请求次数、池大小和关闭原因。正常回收原因只有 `idle` 与 `max-requests`。调用方使用内置 `debug: true` 时，控制台只输出连接关闭事件，并采用与 Proxy Transport 一致的单行可读格式，不输出连接创建、建立或逐请求日志。

Edge Worker 保留持久化 Workers Logs 和 invocation logs。正常 request/response 热路径不输出逐请求或逐消息日志，异常异步任务才写结构化错误。

## 实现状态

Fetch、Proxy、WebSocket Transport、Rust HTTP proxy、Protobuf 控制帧、128 KiB 流式 Worker relay、CANCEL 和串行复用连接池均已实现。新 `ParallelSegmentLoader` 已接入统一 Transport, 并完成流式进度、响应头/空闲/完整请求超时和停滞补救; 基于历史吞吐的慢速补救尚未迁移。

## 修改历史

- 2026-08-08: 新 Loader 改为逐段读取统一 Transport response body, 恢复响应头、流量空闲和完整请求超时; 停滞 Chunk 由 Loader 取消并重新领取, 不向 Fetch Transport 引入调度状态。
- 2026-08-08: 新 `ParallelSegmentLoader` 接回统一 `HttpTransport`, 默认 Fetch, example 恢复 Fetch、HTTP Proxy 和 WebSocket relay 选择; 慢速补救明确为尚未迁移。
- 2026-08-07: WebSocket relay 恢复流式 response 和 CANCEL；控制消息使用 Protobuf，body 使用 128 KiB 原始 frame；删除 `responseMode`，Loader 对所有 Transport 统一启用首字节、流量空闲和慢速补救。继续保留单连接串行 Keep-Alive、现有连接池策略，不恢复心跳和最大连接寿命。
- 2026-08-07: Edge Worker 切换为 Paid 运行限制，CPU time 上限设为 300 秒、subrequest 上限设为 10,000，并关闭 `workers.dev` 与 Preview URL，仅保留 Dashboard 管理的 Custom Domain。
- 2026-08-07: wire protocol 升级为 version 2；Response 删除 status text、正常响应的空 error message 和未重定向 URL，response header 改为固定 ID 白名单，Request header 长度字段同步收紧。
- 2026-08-07: 从零重写 WebSocket relay。每个事务改为一条 Request 和一条完整 Response，删除 Protobuf、多帧 body、取消协议、心跳和最大连接寿命；连接池改为优先复用年轻连接、空闲下限回收和每连接 50 次复用，Loader 增加 buffered response 模式。
- 2026-08-06: 增加多 Origin `ProxyHttpTransport` 和无状态 `storya-http-proxy`，采用稳定 Range descriptor 缓存媒体 Chunk。
- 2026-08-05: 建立通用 HTTP Transport 和 Edge Worker relay。
