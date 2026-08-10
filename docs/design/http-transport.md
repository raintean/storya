# HTTP Transport 设计

本文描述 Storya 当前采用的通用 HTTP Transport、普通 Fetch 和基于连接池的 HTTP-over-WebSocket relay。Transport 只表达 HTTP 请求与响应，不理解 HLS、Segment、Range 调度或媒体业务。

## 组件边界

```text
storya-hls-loader
        |
        v
storya-transport
  |
  +---- FetchHttpTransport -------------------------> HTTP 源站
  |
  +---- WebSocketHttpTransport ----> storya-websocket-transport ----> HTTP 源站
```

- `storya-transport` 提供统一的 `HttpTransport` 接口和两种网络实现。
- `storya-transport/src/websocket` 集中客户端 WebSocket Transport、单连接状态机、连接池、response 和相关类型；通用 HTTP 接口、Fetch Transport 和统计保留在 `src` 根目录。
- `storya-hls-loader` 的 `ParallelSegmentLoader` 持有一个 `HttpTransport`; `SegmentPlanningWork` 通过它执行 HEAD, `SegmentFetchWork` 通过它执行 GET。默认使用 Fetch, example 可以选择 WebSocket relay。HLS 语义始终留在 `storya-hls-loader` 中。
- `storya-websocket-transport` 是无状态 WebSocket HTTP relay，每条连接串行处理请求，连接池提供并发。
- WebSocket relay 的控制消息由 `storya-protocol` 中的 Protobuf schema 描述，媒体 body 使用原始二进制 frame。

## HTTP 接口

`HttpTransport` 接受 Web 标准 `Request`，返回加载器所需的 HTTP 响应：

```ts
interface HttpTransport {
  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>
  destroy(): void
}
```

Fetch 和 WebSocket Transport 都返回流式 response。Transport 不暴露加载器调度策略；窗口失效、超时、停滞和相对慢速补救由 Loader 通过标准 `Request.signal` 与 `ReadableStream.cancel()` 表达。

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

`ERROR` 可以在 response head 前终止请求，也可以在流式 body 期间终止响应。协议不携带 request ID 或 sequence，连接双方完全通过单事务状态机确定 frame 所属事务。客户端只有收到 `RESPONSE_END`、`ERROR` 或 `CANCELED` 后才会复用连接；Worker 在已有活动事务时收到新的 `REQUEST_HEAD` 会关闭连接。

## 二进制编码

每个 frame 使用 1 字节固定头，只包含 `TransportFrameKind`。`REQUEST_HEAD`、`RESPONSE_HEAD` 和 `ERROR` payload 使用 Protobuf；`RESPONSE_BODY` payload 直接承载媒体字节；`RESPONSE_END`、`CANCEL` 和 `CANCELED` 没有 payload。

`HttpRequestHead` 包含 method、URL、request headers 和 `max_response_bytes`。`HttpResponseHead` 包含 status、status text、最终 URL 和原始 response headers。协议为 `TransportError` 定义无效请求、不支持的 method、响应过大、上游失败和内部失败; 当前 Worker 实际发送无效请求、响应过大和上游失败三类。HTTP 4xx、5xx 仍然是正常 HTTP response。

Worker 以 256 KiB 为目标读取上游 body。每个 frame 单次分配 header 和 body 的连续缓冲区，BYOB reader 直接写入 header 后方，填入 1 字节 kind 后将同一视图交给 WebSocket，避免在 JavaScript 层为发送 frame 再复制一次 body。该缓冲区发送后即丢弃，不尝试复用；Cloudflare runtime 和浏览器内部是否复制不属于协议保证。客户端 frame decoder 使用 `Uint8Array.subarray()` 暴露 payload，`ReadableStream` 继续传递该视图。最后一个 frame 可以小于 256 KiB。

## 流式 Response 与上层调度

WebSocket Transport 收到 `RESPONSE_HEAD` 后立即返回 `HttpTransportResponse`，后续 `RESPONSE_BODY` 逐帧进入 `ReadableStream`，`RESPONSE_END` 关闭流。

Fetch 和 WebSocket 对 HLS Loader 统一表现为流式 Transport。HEAD 与 GET 共用 `SegmentLoadWorker` 固定并发池。`SegmentFetchWork` 使用 `ReadableStreamDefaultReader` 逐段读取 GET body, 实时更新 Chunk 已接收字节。两种 Work 都通过 Request AbortSignal 表达窗口取消和请求超时; response head 已返回的 GET 还会使用 body cancel。调度优先级只影响空闲 Worker 的下一次领取, 不取消已经发出的有效请求。

连续无数据、相对同期 GET 明显过慢或请求超时由 `SegmentFetchWork` 结束当前 attempt; 救援额度内会把 Chunk 恢复为可调度状态, 后续重新领取属于 `SegmentLoadWorker` 的调度。额度耗尽后不再检测相对慢速, 但再次停滞会取消请求并快速失败 Segment。Transport 不参与这些判断。

## 响应上限

协议允许的 Response body 上限为 32 MiB。客户端为每个请求声明更小的 `maxResponseBytes` 时，Worker 使用更小值。

Worker 在以下位置验证上限：

- 上游 Content-Length 已知时，在读取 body 前拒绝超限响应。
- Content-Length 未知或不可信时，累计读取不超过 `maxResponseBytes + 1`，多出的一个字节用于检测越界。
- 客户端按累计收到的 body frame 字节再次验证本次请求上限。

HEAD 的 body 必须为空。当前生产 Chunk 默认为 2 MiB，因此正常媒体 Range 远低于协议硬上限。

## 取消与超时

`SegmentPlanningWork` 和 `SegmentFetchWork` 决定何时结束当前请求, 不把 HLS 超时或“慢连接”概念传给 Transport。Work 中止请求时触发 `Request.signal`; GET response head 已经到达后停止读取时还会取消 `ReadableStream`。当前响应头和完整请求时限来自 hls.js `fragLoadPolicy`。默认 rescue 在 GET body 连续 4 秒没有数据时判定停滞; 已经观察满同一窗口、具有至少 2 个同期 peer 的请求如果低于 peer 中位速率的 25%, 且重新请求预计更早完成, 也会触发相同的取消和重新领取流程。`rescue: false` 关闭两类检测; 次数耗尽后只关闭相对慢速检测, stall 检测仍用于快速失败。

WebSocket Transport 把这两种标准取消入口映射为 `CANCEL`。由于每条连接只有一个活动事务，CANCEL 始终作用于当前事务。Worker 先清空活动事务，再取消 pending BYOB read 并 Abort 上游 Fetch，随后回复 `CANCELED`。客户端收到 `CANCELED`、`RESPONSE_END` 或 `ERROR` 后才把连接恢复为 idle；确认超时会关闭连接，避免未收敛事务被错误复用。取消与正常结束竞态时，Worker 已经发送的终止 frame 仍然可以作为复用屏障。取消确认超时由调用方通过 `cancelTimeoutMs` 配置。

Transport 保留不依赖协议消息的生命周期超时：

- connect timeout：连接建立超时后关闭连接。
- cancel timeout：`CANCEL` 长时间没有确认时关闭连接。
- idle timeout：回收超过空闲保留下限的连接。

WebSocket 关闭时 Worker 使用相同的非阻塞清理顺序，确保 pending BYOB read 能够结束，避免 Worker invocation 因未收敛的读取任务被 runtime 判定为挂起。

## 连接池

连接池完全按需创建连接，不主动预热，也不主动补足最低空闲数。所有池化数值由调用方显式配置，Transport 不提供业务默认值。

Example 当前配置：

- 最多 12 条连接。
- 空闲连接保留数量为 6 条。
- 每条连接最多处理 500 个请求。
- 空闲 30 秒后可以回收。
- connect timeout 为 10 秒。
- cancel timeout 为 10 秒。
- 默认 Response 上限为 32 MiB。

Pending request 使用 FIFO 队列。分配 request 时优先选择最近完成过事务的 idle 连接；MRU idle 队列让最近确认健康的连接继续服务请求，让长期未使用的连接自然位于回收端。请求入队、取消、分配以及 idle 连接从两端加入、取得和删除均为 O(1)。

连接池只维护一个 idle timer，始终指向最久未使用且允许回收的连接；timer 到期后从最老的 idle 连接开始关闭，直到达到保留数量或剩余连接尚未超时。`retainedIdleConnections` 只表示空闲保留数量，不主动预建连接；故障或最大复用次数使连接数降低时也不补建。

连接发送第 500 个 request 时标记 retiring。该 response 完成后关闭连接，不再接受新 request。连接池不再按绝对寿命回收连接，也不发送应用层心跳。

## Relay 行为

Worker 只接受 GET 和 HEAD。它过滤 Host、Connection、Transfer-Encoding 等 hop-by-hop 或运行时管理的 header，从 WebSocket 握手继承浏览器 User-Agent，并根据握手 Origin 生成只包含 origin 的 Referer。

Worker 使用带有 Cloudflare Fetch Cache 配置的 `fetch()` 回源，跟随重定向，先返回 status、最终 URL 和 headers，再流式返回 body。所有 GET/HEAD 子请求启用 `cacheEverything`; 200-299 响应忽略源站缓存头并强制使用一年的 Edge TTL，300-399 响应立即过期，400-599 响应不进入缓存。缓存 key 保持 Cloudflare 默认行为，包含完整目标 URL，不使用自定义 key。当前只限制 URL scheme 为 HTTP/HTTPS，尚未实现鉴权、请求额度、内网地址拦截或逐跳重定向校验。

Worker 禁用 `permessage-deflate`。媒体通常已经压缩，重复压缩只会增加 CPU。

Worker 使用 Paid 计划运行，CPU time 上限为 5 秒，单次调用 subrequest 上限为 1,000。生产入口只使用 Dashboard 管理的 Custom Domain，关闭 `workers.dev` 和 Preview URL；Wrangler 配置不声明 routes，避免覆盖 Dashboard 已有的 Custom Domain。

## 可观测性

Fetch 和 WebSocket Transport 共用 `TransportStatistics`。请求通过各自参数校验并正式进入 Transport 后开始计数, `HttpTransportResponse` 在统一边界包装 body, 因而两者按相同口径统计请求、成功、失败、取消和调用方实际消费的响应字节。统计包装流不预取 body, 未被调用方读取的数据不计入响应字节。每个具体 Transport 通过 `getStatistics()` 返回只读快照; 有变化时默认每 5 秒输出一次单行摘要。

缓存分类读取 response 的 `CF-Cache-Status`: `HIT`、`REVALIDATED`、`STALE`、`UPDATING` 计为命中, `MISS`、`EXPIRED` 计为未命中, `BYPASS`、`DYNAMIC` 单独计数, 缺失或未知值计为 unknown。Fetch 表示目标响应可见的缓存状态。WebSocket 隧道本身不参与 HTTP CDN 缓存; WebSocket Transport Worker 转发的状态表示配置在 Worker 子请求上的 Cloudflare Fetch Cache, 所以它的摘要标记为“Worker Fetch 缓存”。

WebSocket 连接池自定义 debug 回调仍然独立记录连接创建、建立和关闭，包含连接年龄、请求次数、池大小和关闭原因。正常回收原因只有 `idle` 与 `max-requests`。调用方使用内置 `debug: true` 时，控制台只额外输出连接关闭事件, 不把连接生命周期混入请求统计。

WebSocket Transport Worker 保留持久化 Workers Logs 和 invocation logs。正常 request/response 热路径不输出逐请求或逐消息日志，异常异步任务才写结构化错误。

## 实现状态

Fetch、WebSocket Transport、统一请求/流量/缓存统计、Protobuf 控制帧、256 KiB 流式 Worker relay、CANCEL 和串行复用连接池均已实现。`ParallelSegmentLoader` 已接入统一 Transport, 并完成流式进度、响应头/完整请求超时、停滞补救和同期 GET 相对慢速补救。

## 修改历史

- 2026-08-10: 客户端 WebSocket Transport 实现收敛到 `storya-transport/src/websocket` 子目录，与通用 HTTP 抽象、Fetch Transport 和统计实现分层。
- 2026-08-10: WebSocket frame 删除只用于串行事务隔离的 sequence，固定头从 5 字节缩减为 1 字节；双方改由严格的单事务状态机和终止 frame 保证连接安全复用。
- 2026-08-10: Worker body frame 从 128 KiB 调整为 256 KiB，并重构为单次分配连续缓冲区、避免 JavaScript 层二次复制；连接池改用 FIFO pending、MRU idle 和池级单 timer，`minIdleConnections` 重命名为 `retainedIdleConnections`。
- 2026-08-10: Example 的单连接最大请求数从 50 调整为 500；Worker CPU time 和 subrequest 上限分别收紧为 5 秒和 1,000。
- 2026-08-10: 新增服务端 Worker 能力的顶层 `workers/` 分类，并将 `services/storya-edge-worker` 迁移为 `workers/storya-websocket-transport`，叶子项目名只表达 WebSocket Transport relay 职责。
- 2026-08-10: 删除没有产品消费者且与现有网络路径能力重叠的 `ProxyHttpTransport` 和 `storya-http-proxy`; Transport 收敛为 Fetch 与 WebSocket relay。
- 2026-08-10: 统计包装流改为零预取, 只记录调用方实际读取的 response body 字节, 避免取消请求多算一个预取 chunk。
- 2026-08-09: WebSocket Transport Worker 的 GET/HEAD 子请求启用 `cacheEverything`, 200-299 响应强制使用一年 Edge TTL, 重定向立即过期, 错误响应不缓存; WebSocket Transport 将对应统计标记为 Worker Fetch 缓存。
- 2026-08-09: 删除已无消费者的 `rangeRequestMode`; 当前 Loader 的 rescue 对所有 Transport 都从原 Chunk 起点完整重下, Transport 不再暴露旧版部分续传策略。
- 2026-08-09: WebSocket Transport 接入统一 `TransportStatistics`, 按调用方实际消费的 response body 统计请求与字节, 将 WebSocket Transport Worker 转发的 `CF-Cache-Status` 明确标记为上游缓存; 三种 Transport 同时公开统计快照。
- 2026-08-09: Loader 的 rescue 检测改为可关闭的统一策略; body 连续 2 秒无数据判定停滞, 持续有数据但明显低于同期 GET 中位速率且重试预计更快时也取消当前请求并重新领取, Transport 接口不增加慢速语义。
- 2026-08-08: 新 Loader 改为逐段读取统一 Transport response body, 恢复响应头、流量空闲和完整请求超时; 停滞 Chunk 由 Loader 取消并重新领取, 不向 Fetch Transport 引入调度状态。
- 2026-08-08: 新 `ParallelSegmentLoader` 接回统一 `HttpTransport`, 默认 Fetch, example 恢复 Fetch、HTTP Proxy 和 WebSocket relay 选择; 慢速补救明确为尚未迁移。
- 2026-08-07: WebSocket relay 恢复流式 response 和 CANCEL；控制消息使用 Protobuf，body 使用 128 KiB 原始 frame；删除 `responseMode`，Loader 对所有 Transport 统一启用首字节、流量空闲和慢速补救。继续保留单连接串行 Keep-Alive、现有连接池策略，不恢复心跳和最大连接寿命。
- 2026-08-07: WebSocket Transport Worker 切换为 Paid 运行限制，CPU time 上限设为 300 秒、subrequest 上限设为 10,000，并关闭 `workers.dev` 与 Preview URL，仅保留 Dashboard 管理的 Custom Domain。
- 2026-08-07: wire protocol 升级为 version 2；Response 删除 status text、正常响应的空 error message 和未重定向 URL，response header 改为固定 ID 白名单，Request header 长度字段同步收紧。
- 2026-08-07: 从零重写 WebSocket relay。每个事务改为一条 Request 和一条完整 Response，删除 Protobuf、多帧 body、取消协议、心跳和最大连接寿命；连接池改为优先复用年轻连接、空闲下限回收和每连接 50 次复用，Loader 增加 buffered response 模式。
- 2026-08-06: 增加多 Origin `ProxyHttpTransport` 和无状态 `storya-http-proxy`，采用稳定 Range descriptor 缓存媒体 Chunk。
- 2026-08-05: 建立通用 HTTP Transport 和 WebSocket Transport Worker relay。
