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
- `storya-hls-loader` 负责 Range 规划、并发调度、抢占、重试和流式 Transport 的慢速补救。
- `storya-http-proxy` 是 Rust 实现的无状态 HTTP proxy。
- `storya-edge-worker` 是无状态 WebSocket HTTP relay，每条连接串行处理请求，连接池提供并发。
- WebSocket relay 的手写二进制 codec 位于 `storya-protocol/typescript/http-relay.ts`，不使用 Protobuf。

## HTTP 接口

`HttpTransport` 接受 Web 标准 `Request`，返回加载器所需的 HTTP 响应：

```ts
interface HttpTransport {
  readonly rangeRequestMode?: 'resumable' | 'stable'
  readonly responseMode: 'streaming' | 'buffered'

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>
  destroy(): void
}
```

Fetch 和 Proxy Transport 使用 `streaming` response。WebSocket Transport 使用 `buffered` response，只有完整 WebSocket response message 到达后才返回 `HttpTransportResponse`。

## HTTP Proxy Transport

`ProxyHttpTransport` 接受一个或多个 HTTP(S) Proxy Origin。每个 Range 使用目标 URL 和分片位置稳定选择 Origin，域名更换时重建 Transport 即可。

目标 URL、逻辑 method 和可选 Range 使用 UTF-8 与无 padding Base64URL 编码为：

```text
/proxy/<descriptor>.jpg
```

`.jpg` 后缀和 `image/jpeg` Content-Type 用于获得 Cloudflare 静态资源缓存语义。Rust proxy 将上游 206 包装为可缓存的 200，把原始 status、Content-Range、Content-Length 和 Content-Type 放入 `x-storya-proxy-*` header，由客户端恢复。

Rust proxy 不建立本地缓存。HEAD、Range 未命中和错误响应不进入 CDN 缓存。当前 descriptor 没有签名，公开部署前仍需增加授权、Origin allowlist 和内网地址防护。

## WebSocket 事务

每条 WebSocket 同时只处理一个 HTTP 事务。事务收到完整 response 后连接回到 idle，可以继续处理下一个 request，语义类似没有 pipelining 的 HTTP/1.1 Keep-Alive。

```text
connecting -> idle -> busy -> idle
                    |
                    +-> closed
```

加载器并发和单连接并发彼此独立。HTTP 并发由连接池中的多条 WebSocket 提供，不在单条连接上进行 multiplexing。

每个事务只有两条应用消息：

```text
client ---- one Request message ----> Worker
client <--- one Response message ---- Worker
```

方向和连接状态已经确定消息含义，因此协议没有 message kind、request ID 或 sequence。Worker 在 busy 状态收到第二条 request，或者客户端在 idle 状态收到 response，都视为协议错误并关闭连接。

## 二进制编码

Request 和 Response 使用固定宽度大端整数、长度前缀 UTF-8 字符串和原始 payload。每条消息携带一字节协议版本，不依赖 Protobuf runtime。

Request 包含 method、`maxResponseBytes`、URL 和原始 request headers。Response 包含 outcome、HTTP status、可选最终 URL、固定集合内的 response headers 和 payload。HTTP outcome 的 payload 是完整 body；错误 outcome 的 payload 是 UTF-8 error message。协议不传输 `statusText`。Response outcome 区分正常 HTTP、无效请求、响应过大、上游失败和内部失败；HTTP 4xx、5xx 仍然是正常 HTTP outcome。

当前 wire version 为 2。Request 固定头为 11 字节：

| Offset | 类型 | 字段                    |
| -----: | ---- | ----------------------- |
|      0 | u8   | protocol version        |
|      1 | u8   | method，GET=0、HEAD=1   |
|      2 | u8   | header count            |
|      3 | u32  | max response body bytes |
|      7 | u32  | URL UTF-8 bytes         |

固定头之后依次写入 URL 和 request headers。每条 request header 由 u8 name length、u16 value length、name UTF-8 bytes 和 value UTF-8 bytes 组成。

Response 固定头为 13 字节：

| Offset | 类型 | 字段                                    |
| -----: | ---- | --------------------------------------- |
|      0 | u8   | protocol version                        |
|      1 | u8   | outcome                                 |
|      2 | u16  | HTTP status，错误 outcome 时为 0        |
|      4 | u8   | response header count                   |
|      5 | u32  | redirected final URL UTF-8 bytes        |
|      9 | u32  | body bytes 或 error message UTF-8 bytes |

固定头之后依次写入可选 final URL、response headers 和 payload。没有重定向时 final URL 为空，由客户端复用 request URL。每条 response header 由 u8 header ID、u16 value length 和 value UTF-8 bytes 组成，不重复传输 header name。所有整数使用大端编码，不插入 padding。

Response 只承载 Loader 和 Transport 当前消费的 headers：

|  ID | Header            |
| --: | ----------------- |
|   0 | `accept-ranges`   |
|   1 | `age`             |
|   2 | `cache-control`   |
|   3 | `cf-cache-status` |
|   4 | `content-length`  |
|   5 | `content-range`   |
|   6 | `content-type`    |
|   7 | `etag`            |
|   8 | `expires`         |
|   9 | `last-modified`   |

其他上游 response header 不进入 WebSocket message。新增 header 必须先确认真实消费者，再追加稳定 ID；不能改变已有 ID 含义。

codec 解码 body 时返回原消息的 `Uint8Array.subarray()`，WebSocket Response 和 Loader buffered 路径继续传递同一视图，不复制媒体数据。Worker 根据已知 Content-Length 或请求上限一次性分配最终 response buffer，并使用 BYOB reader 直接把上游 body 读入 metadata 后方。应用代码不执行 body 拼接；Cloudflare runtime 和浏览器内部是否复制不属于协议保证。

## Buffered Response 与上层调度

WebSocket Transport 不提供流式 body。Worker 完整读取上游响应后发送一条 Response message，客户端收到整条消息后才解析 status、headers 和 body。

因此 HLS Loader 在 buffered 模式下：

- 不启用首字节超时。
- 不启用响应流量空闲超时。
- 不执行请求进行中的吞吐判断和慢速救援。
- 继续使用完整请求加载超时。
- 请求完成后用完整字节数和总耗时更新历史吞吐。
- Range 重试始终从稳定 Chunk 起点重新开始。

这项取舍减少 Worker 和浏览器的消息事件、流控制与内存复制，但无法在一个 Response 下载过程中观察进度。

## 响应上限

协议允许的 Response body 上限为 32 MiB。客户端为每个请求声明更小的 `maxResponseBytes` 时，Worker 使用更小值。

Worker 在以下位置验证上限：

- 上游 Content-Length 已知时，在读取 body 前拒绝超限响应。
- Content-Length 未知时，最多读取 `maxResponseBytes + 1`，多出的一个字节用于检测越界。
- 客户端解码后再次验证实际 body 不超过本次请求上限。

HEAD 的 body 必须为空。当前生产 Chunk 默认为 2 MiB，因此正常媒体 Range 远低于协议硬上限。

## 取消与超时

协议不提供 CANCEL。上层 Abort 只使旧加载 attempt 失效，WebSocket Transport 不发送消息、不停止 Worker Fetch，也不提前复用 busy 连接。Response 最终到达后，旧 attempt 丢弃结果，连接重新回到 idle。

Transport 保留不依赖协议消息的生命周期超时：

- connect timeout：连接建立超时后关闭连接。
- transaction timeout：完整 Response 长时间未到达时关闭连接，避免连接永久占用池容量。
- idle timeout：回收超过空闲保留下限的连接。

WebSocket 关闭时 Worker 同步清空活动事务，立即触发 BYOB reader cancel 以结束 pending read，再通过 AbortController 终止上游 Fetch，但不等待 cancel Promise；随后立即完成服务端关闭握手。这属于连接资源清理，不是独立的取消协议。

## 连接池

连接池完全按需创建连接，不主动预热，也不主动补足最低空闲数。所有池化数值由调用方显式配置，Transport 不提供业务默认值。

Example 当前配置：

- 最多 12 条连接。
- 空闲回收下限为 6 条。
- 每条连接最多处理 50 个请求。
- 空闲 30 秒后可以回收。
- connect timeout 为 10 秒。
- transaction timeout 为 60 秒。
- 默认 Response 上限为 32 MiB。

分配 request 时优先选择创建时间最晚、年龄最小的 idle 连接。老连接因为较少获得新请求，会自然积累空闲时间并进入普通 idle 回收流程。

idle timeout 只关闭超出 `minIdleConnections` 的空闲连接。如果池从未扩展到该数量，不会主动创建连接；故障或最大复用次数使连接数降低时也不补建。

连接发送第 50 个 request 时标记 retiring。该 response 完成后关闭连接，不再接受新 request。连接池不再按绝对寿命回收连接，也不发送应用层心跳。

## Relay 行为

Worker 只接受 GET 和 HEAD。它过滤 Host、Connection、Transfer-Encoding 等 hop-by-hop 或运行时管理的 header，从 WebSocket 握手继承浏览器 User-Agent，并根据握手 Origin 生成只包含 origin 的 Referer。

Worker 使用普通 `fetch()` 回源，跟随重定向，返回 status、最终 URL、headers 和完整 body。当前只限制 URL scheme 为 HTTP/HTTPS，尚未实现鉴权、请求额度、内网地址拦截或逐跳重定向校验。

Worker 禁用 `permessage-deflate`。媒体通常已经压缩，重复压缩会增加 CPU，且整包 Response 会进一步放大压缩成本。

Worker 使用 Paid 计划运行，CPU time 上限为 300 秒，单次调用 subrequest 上限为 10,000。生产入口只使用 Dashboard 管理的 Custom Domain，关闭 `workers.dev` 和 Preview URL；Wrangler 配置不声明 routes，避免覆盖 Dashboard 已有的 Custom Domain。

## 可观测性

连接池自定义 debug 回调记录连接创建、建立和关闭，包含连接年龄、请求次数、池大小和关闭原因。正常回收原因只有 `idle` 与 `max-requests`。调用方使用内置 `debug: true` 时，控制台只输出连接关闭事件，并采用与 Proxy Transport 一致的单行可读格式，不输出连接创建、建立或逐请求日志。

Edge Worker 保留持久化 Workers Logs 和 invocation logs。正常 request/response 热路径不输出逐请求或逐消息日志，异常异步任务才写结构化错误。

## 实现状态

Fetch、Proxy、WebSocket Transport、Rust HTTP proxy、极简二进制 codec、buffered Worker relay、串行复用连接池和 Loader buffered 模式均已实现。

## 修改历史

- 2026-08-07: Edge Worker 切换为 Paid 运行限制，CPU time 上限设为 300 秒、subrequest 上限设为 10,000，并关闭 `workers.dev` 与 Preview URL，仅保留 Dashboard 管理的 Custom Domain。
- 2026-08-07: wire protocol 升级为 version 2；Response 删除 status text、正常响应的空 error message 和未重定向 URL，response header 改为固定 ID 白名单，Request header 长度字段同步收紧。
- 2026-08-07: 从零重写 WebSocket relay。每个事务改为一条 Request 和一条完整 Response，删除 Protobuf、多帧 body、取消协议、心跳和最大连接寿命；连接池改为优先复用年轻连接、空闲下限回收和每连接 50 次复用，Loader 增加 buffered response 模式。
- 2026-08-06: 增加多 Origin `ProxyHttpTransport` 和无状态 `storya-http-proxy`，采用稳定 Range descriptor 缓存媒体 Chunk。
- 2026-08-05: 建立通用 HTTP Transport 和 Edge Worker relay。
