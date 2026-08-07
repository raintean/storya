# HLS 并行加载器设计

本文描述 `storya-hls-loader` 当前采用的加载模型。它只负责获取浏览器能够播放的 HLS 原始媒体数据，不负责解码、转码、解密或向 SourceBuffer 追加媒体。

## 设计目标

- 小 Segment 通过跨 Segment 预填充利用并发，大 Segment 继续支持 Segment 内 Range 并行。
- 所有媒体请求共享全局并发上限、优先级、抢占、慢速补救和吞吐量估算。
- hls.js 继续负责 ABR、AES 解密、TS transmux、媒体解析和播放状态机。
- 不同分辨率、独立音轨或其他内容序列不得混合数据和需求状态。
- fLoader 只表达 hls.js 的读取需求，不直接拥有共享下载任务。
- 网络执行通过通用 `HttpTransport` 完成，加载与调度逻辑不依赖 Fetch 或 WebSocket。

## 虚拟流

虚拟流是同一份连续媒体内容的逻辑 Segment 序列。每个主 rendition 和独立音轨分别形成虚拟流；混合音视频的主 rendition 作为一条 main A/V 流处理。

虚拟流不把所有 Segment 复制进一个连续 ArrayBuffer。它维护有序的 Segment 元数据、原始媒体数据、下载状态、真实网络统计和读取需求。位置使用 Segment 及其内部字节位置表达，以支持未知长度、直播列表滑动、byte range 和 discontinuity。

每条活跃虚拟流以 hls.js 最近的明确加载需求作为需求前沿，并向后准入 6 个未来 Segment。`missing`、`loading` 和 `ready` Segment 都占用预填充位置；只有 hls.js 请求新的 Segment 或窗口失效时才向前移动，避免连接空闲时无限下载。

## 需求信号

- fLoader `load()` 同时建立一次 reader 需求和持续的播放需求，立即激活对应虚拟流、更新需求前沿并等待目标 Segment ready。
- reader 在数据交付后即可释放，但播放需求必须持续到 hls.js 触发 `FRAG_BUFFERED`，确认 Segment 已经进入媒体缓冲。
- fLoader `abort()` 撤销该 reader；清晰度或音轨切换淘汰旧虚拟流时同时撤销旧流的播放需求。没有其他需求且不再位于有效预填充窗口的下载才会中止。
- 下载失败不会被解释为播放需求消失。失败 Segment 保留播放需求和失败状态，暂停该流的后续预取，并等待 hls.js 按自身退避策略重新发起 reader。
- `LEVEL_LOADED` 和 `AUDIO_TRACK_LOADED` 用于建立和更新虚拟流拓扑，不代替 fLoader 的硬需求。
- 新虚拟流的硬需求会使同媒体类型的旧虚拟流失活；同一虚拟流中的非连续需求会重建预填充窗口。

## 填充与调度

流填充器观察所有虚拟流中的硬需求和预填充窗口，创建 Segment 下载并把完成的数据写回所属虚拟流。

- 所有虚拟流合计最多同时运行 6 个 GET/Range 请求。
- 首个媒体请求直接发送 Range GET，并优先从 `Content-Range` 得到 Segment 总长度；只有 header 不可读、缺失或总长度为 `*` 时才补发 HEAD。
- 补救 HEAD 不计入该并发上限。
- 小于等于 2 MiB 的 Segment 只需要首个 Range GET；源站不支持 Range 并返回 200 时直接顺序读取同一个响应。
- 较大的 Segment 按 2 MiB Chunk 拆分，并与其他 Segment 的任务共同调度。
- 硬需求优先于纯预填充，之后按媒体播放截止时间、Segment 内交付前沿和创建顺序排序。
- 当前音频和主画面不固定分配连接数，而是按播放截止时间竞争同一并发预算。

现有请求保护和补救规则继续生效：请求至少运行 300ms 才允许被抢占；已完成 80% 或预计 300ms 内完成时不抢占、不执行慢速补救；至少存在 3 个吞吐量样本后才进行慢速检测；低于全局基线 35% 并持续 1 秒时进行补救；单个 Chunk 最多补救 2 次。

## fLoader 边界

fLoader 把 Fragment 映射到虚拟流中的 Segment，并注册独立 reader。

- ready Segment 立即返回。
- loading Segment 复用同一下载并等待，不创建重复请求。
- missing Segment 通过硬需求促使填充器开始下载。
- reader abort 后由填充器决定共享下载是否仍然有价值。
- 数据交付只表示 `ready`，不会释放播放需求或清除缓存；只有 `FRAG_BUFFERED` 才确认本次播放需求已经满足。
- 返回给 hls.js 的 stats 必须来自真实网络下载，缓存命中不能伪造瞬时带宽。

当前明确关闭 hls.js 的实验性 progressive 模式，因此 fLoader 只在完整 Segment ready 后交付数据。虚拟流内部仍可按 Chunk 并行填充。

## Transport 边界

单 Segment 下载器负责构造标准 GET、HEAD 和 Range Request，并通过注入的 `HttpTransport` 执行。默认 Transport 使用浏览器 Fetch；调用方也可以传入 `ProxyHttpTransport`，通过多个普通 HTTP Origin 访问 `storya-http-proxy`，或者传入 `WebSocketHttpTransport`，通过 `storya-edge-worker` 转发相同的 HTTP 请求。三种 Transport 都提供流式 Response。

未知长度的非原子 Segment 不再先发 HEAD。首个 Chunk 直接请求 `bytes=0-<chunk-end>`：206 且 `Content-Range` 可读时立即记录总长度并规划剩余 Chunk；源站忽略 Range 并返回 200 时把该响应直接作为顺序响应继续读取；只有 206 无法提供总长度时才使用 HEAD 的 `Content-Length` 补救。这样在 Proxy Transport 统一暴露 `Content-Range` 后，正常路径不产生探测请求；直连源站受 CORS 限制时仍有后备路径。

Transport 不参与虚拟流、预填充、请求优先级、抢占或“慢连接”判断。AbortController 取消一次 Chunk attempt 后，Fetch 和 Proxy Transport 取消 Fetch；WebSocket Transport 把同一 AbortSignal 映射为当前事务的 `CANCEL`，Worker 取消上游 Fetch 并以 `CANCELED` 确认。

首字节超时、流量空闲超时、完整加载超时和请求内慢速救援统一应用于所有 Transport。稳定 Range Transport 被补救时保持原 Chunk 请求边界，并丢弃新响应中已经接收过的前缀。

WebSocket Transport 通过 Protobuf response head 暴露上游 response headers，通过 128 KiB 原始 body frame 向 Web Stream 交付数据。Loader 只通过标准 `HttpTransportResponse` 消费这些信息，不感知 WebSocket frame。

## 生命周期

- 播放列表更新时复用内容标识相同的 Segment，并追加或移除元数据。
- 清晰度或音轨切换后停止旧虚拟流的新预填充，并取消没有 reader 的失效任务。
- 当前 Segment 在交付后继续保留，直到 hls.js 确认已经缓冲；失败时也不会因为 reader 结束而丢失播放需求。
- Seek 后以新的硬需求位置重建同一虚拟流的窗口。
- 已交付且不再位于有效窗口的数据可以释放，元数据继续保留。
- 首次媒体尚未进入 SourceBuffer 时，实验台切换清晰度会重建 hls.js 和加载会话并从当前播放位置启动，避免沿用旧 Fragment 的下一加载位置，也避免同一实例重启造成 SourceBuffer 与虚拟缓存生命周期错配。
- 加载会话销毁时移除 hls.js 事件监听，并取消全部 reader、填充任务和调度计时器。

## 公开接口

加载器通过播放会话对象同时提供 fLoader 构造器和 hls.js 事件绑定：

```ts
const transport = new WebSocketHttpTransport('wss://edge.example.com/transport', {
  cancelTimeoutMs: 10_000,
  connectTimeoutMs: 10_000,
  defaultMaxResponseBytes: 32 * 1024 * 1024,
  idleConnectionTimeoutMs: 30_000,
  maxConnections: 12,
  maxRequestsPerConnection: 50,
  minIdleConnections: 6,
})
const parallel = createHlsParallelLoader({ ...options, transport })
const hls = new Hls({
  fLoader: parallel.fragmentLoader,
  progressive: false,
})

parallel.attach(hls)
hls.loadSource(source)

parallel.destroy()
```

会话对象统一拥有虚拟流集合、流填充器、全局请求调度器、Transport 和 fLoader 生命周期。传入的 Transport 所有权转移给会话，并在 `destroy()` 时销毁。

### 诊断边界

会话对象提供只读的 `getDiagnostics()` 快照，用于实验台展示当前虚拟流、Segment、Chunk、全局并发和吞吐量估算。快照包含播放需求锚点、预填充窗口、缓存状态，以及 Chunk 的加载、慢速补救、网络重试和被抢占状态。

诊断信息只在调用时从现有运行状态组装，不建立事件订阅、历史队列或 UI 专用状态。实验台按固定间隔拉取快照；核心加载器不依赖 React，也不允许诊断消费者影响填充和调度决策。需要长期记录的离散事件继续通过 `onEvent` 输出。

## 实现状态

本文描述的会话接口、虚拟流、每流预填充窗口、跨流全局请求调度、播放需求生命周期、Transport 注入和薄 fLoader 均已实现。单 Segment 下载器继续负责首个 Range 探测、补救 HEAD、顺序 GET、Range 拆分、抢占和慢速补救；流填充器在它之上统一调度不同虚拟流及不同 Segment。

React 实验台已经接入会话接口，并在加载事件面板中区分即时需求、缓存命中、预填充、请求抢占和慢速补救。实验台可以为新播放会话选择 Browser Fetch、HTTP Proxy 或 WebSocket Relay Transport；HTTP Proxy 模式接受多个 Origin，WebSocket 模式接受 Worker 根 URL 或完整 Transport endpoint。实验台还会用虚拟流时间轴显示贯穿所有流的播放线、Segment 缓存和 Chunk 调度状态；流按激活状态优先，再按视频、音频和字幕排列。

## 修改历史

- 2026-08-07: WebSocket Transport 恢复流式 Response 和远端 CANCEL；移除 `responseMode`，首字节、流量空闲和慢速补救统一应用于 Fetch、Proxy 和 WebSocket，稳定 Range 补救保持原 Chunk 边界并丢弃重复前缀。
- 2026-08-07: WebSocket Response 收紧为 Loader 实际消费的固定 header 集合，未包含的上游 header 不再通过 `getResponseHeader()` 暴露。
- 2026-08-07: WebSocket Transport 改为 buffered Response；Loader 对该模式禁用首字节、流量空闲和请求内慢速检测，仅保留完整加载超时与完成后吞吐统计。
- 2026-08-06: 未知长度 Segment 改为优先从首个 Range 响应读取 `Content-Range`，只在无法取得总长度时补发 HEAD；实验台增加 HTTP Proxy Transport。
- 2026-08-05: 抽离通用 HTTP Transport，默认保留 Fetch，并支持通过 WebSocket 连接池和 Edge Worker 执行相同请求。
- 2026-08-05: 虚拟流时间轴移除独立缓冲边界线，缓存状态仅由 Segment 和 Chunk 表达。
- 2026-08-05: 实验台在首次媒体缓冲前切换清晰度时改为重建播放会话，修复目标流跳过首个 Segment 和缓存命中重试循环。
- 2026-08-05: 确立并完成虚拟流、每流 6 Segment 预填充窗口、全局 6 路请求调度、跨 reader 的持续播放需求、薄 fLoader 会话接口、只读诊断快照和可视化实验台。
