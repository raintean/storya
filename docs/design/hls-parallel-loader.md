# HLS 并行加载器设计

本文描述 `storya-hls-loader` 当前采用的 HLS Segment/Chunk 并行加载设计。实现基于 hls.js `1.7.0-rc.3`, 公开接口只包含 `ParallelStreamController` 和 `ParallelSegmentLoader`。

## 设计目标

- 保留 hls.js 原生的选片、ABR、解密、transmux、错误恢复和 SourceBuffer append 流程。
- 由 Controller 明确规划当前及后续 Segment 的激活窗口。
- 同时加载多个 Segment, 并在每个 Segment 内使用 Range Chunk 并行加载。
- hls.js 正式读取和后台窗口填充共享同一份 Segment/Chunk 状态和数据。
- 窗口推进只取消真正离开窗口且没有正式读取者的 Segment, 不使重叠部分失效。
- 所有共享状态修改保持同步、原子和可诊断, 不跨越 `await`。
- 不建立独立 Registry、Filler、Scheduler、Reader 或 Writer 领域组件。

## 非目标

当前设计不负责:

- 改写 hls.js 的顺序解析和 append 状态机。
- 让多个 Segment 同时进入 transmux 或 SourceBuffer。
- LL-HLS Part 的向前预加载窗口。Part 的正式 fLoader 请求仍然可以按 Chunk 加载。
- alternate audio 和 subtitle 的向前预加载窗口。数据模型支持多轨道, 当前只有 main Controller 维护窗口。
- 慢速连接识别、吞吐补救和网络重试策略。相关能力在基础 Chunk 调度稳定后再加入。

## 总体结构

```text
hls.js
  |
  +-- ParallelStreamController
  |      |
  |      +-- replaceWindow(main stream, [s1..s6])
  |      +-- super.loadFragment(s1)
  |
  +-- fLoader.load(s1)
         |
         +-- readerCount + 1
         +-- 观察全局 revision
         +-- Segment ready 后返回 ArrayBuffer

ParallelSegmentLoader
  |
  +-- streams: Map<StreamId, VirtualStream>
  +-- revision + listeners
  +-- logical workers
  +-- HttpTransport
  +-- fLoader 构造器
  +-- getDiagnostics()
```

共享数据结构为:

```text
VirtualStream
  +-- segments: Map<SegmentKey, VirtualStreamSegment>
  +-- window: SegmentKey[]

VirtualStreamSegment
  +-- chunks: VirtualStreamChunk[]

VirtualStreamChunk
```

`VirtualStream`、`VirtualStreamSegment` 和 `VirtualStreamChunk` 都是纯数据。它们不保存 callback、Promise resolver、listener、AbortController 或 Worker 对象。

## 公开组件

### ParallelStreamController

`ParallelStreamController` 继承 hls.js 默认 main `StreamController`。它不直接发送请求, 只把 hls.js 已经选择的 Fragment 和 Level 转换成有序窗口, 然后继续调用原生实现。

```ts
protected loadFragment(fragment, level, targetBufferTime) {
  segmentLoader.replaceWindow(/* current + following fragments */)
  super.loadFragment(fragment, level, targetBufferTime)
}
```

窗口默认包含当前 Segment 和后续最多 5 个 Segment, 总数最多 6 个。窗口顺序就是调度优先级。

Controller 不为预加载创建额外 fLoader, 因此不存在“旁路 fLoader”和“原生 fLoader”两套生命周期。Controller 只声明需求, Loader 中每个 Segment 只有一份 task 状态和数据。

Controller 在以下情况不建立完整 Segment 预加载窗口:

- hls.js 正在执行启动 bandwidth test。
- low-latency mode 正在使用 Part 列表。
- Level details 不存在。

这些情况下清除已有 main 窗口并完全交给 hls.js 原生流程。正式 fLoader 请求仍由 `ParallelSegmentLoader` 加载。

`stopLoad()` 只清除 Controller 当前维护的窗口, 不销毁 Loader、Worker 或整个 Hls session。

### ParallelSegmentLoader

`ParallelSegmentLoader` 是一个 Hls session 级对象, 同时承担:

- 多 VirtualStream 状态所有权。
- Segment 窗口更新和驱离。
- Chunk 规划、优先级选择和并发控制。
- 通过 `HttpTransport` 发送 Range 请求。
- hls.js fLoader 兼容。
- revision/listener 状态通知。
- 同步诊断快照。

它不把这些职责拆成独立运行时组件。Worker 是类内部启动的异步函数, Chunk 选择是同步私有方法。

## 多 VirtualStream

每条媒体轨道或 rendition 使用独立 VirtualStream:

```text
main:<level>
audio:<identity>
subtitle:<identity>
```

每个 VirtualStream 拥有自己的 `segments` 和有序 `window`。全局 Worker 在所有 VirtualStream 中选择 Chunk。

当前 `ParallelStreamController` 只收到 main `loadFragment()`, 因此只有 main stream 具有向前窗口。audio、subtitle、init Segment 和 Part 通过正式 fLoader 的 `readerCount` 获得直接加载优先级, 但当前不自动向前预加载。

如后续要求 audio/subtitle 预加载, 需要替换 hls.js 对应的 `audioStreamController` 和 `subtitleStreamController`, 共享数据和 Worker 模型无需改变。

## Segment 与 Chunk 状态

Segment 保存:

- 稳定 key、URL、媒体开始时间和时长。
- 原始 fLoader context 所需的 URL、header 和 byte range。
- `windowIndex`, 不在窗口时为 `null`。
- `readerCount`, 表示正式 fLoader 读取数量。
- 已知 Segment 长度或未知长度。
- sequential fallback 标记。
- Chunk 列表、最终 ArrayBuffer、Response metadata、统计和失败状态。

Chunk 保存:

- Segment 内本地 `[start, endExclusive)` 范围。
- `empty`、`filling`、`ready` 或 `failed` 状态。
- 当前 `fillId`、attempt 数量和失败描述。
- 完成前的 Uint8Array 数据。

Chunk 不保存 AbortController。AbortController 始终属于正在执行 Fetch 的 Worker 局部状态。

## Chunk 规划

默认 Chunk 大小为 2 MiB, 与 HTTP Proxy 的 shard 大小一致。默认全局网络并发为 6。

```ts
const chunkSize = 2 * 1024 * 1024
const maxConcurrency = 6
```

### 已知 byte range

如果 fLoader context 已经声明 `rangeStart` 和 `rangeEnd`, Segment 长度立即确定, 按 Segment 本地位置切分:

```text
resource range: [10 MiB, 15 MiB)

chunk 0: local [0, 2 MiB) -> request [10 MiB, 12 MiB)
chunk 1: local [2 MiB, 4 MiB) -> request [12 MiB, 14 MiB)
chunk 2: local [4 MiB, 5 MiB) -> request [14 MiB, 15 MiB)
```

不足半个 Chunk 的尾部合并到前一个 Chunk, 避免极小请求。

### 未知长度

完整 Segment 通常没有预先声明长度。此时先创建一个 discovery Chunk:

```text
Range: bytes=0-2097151
```

- 返回 `206` 且 JavaScript 可以读取有效 `Content-Range`: 取得资源总长度, 规划剩余 Chunk。
- 返回 `200`: Origin 忽略 Range, 将响应作为完整 Segment 并切换到 sequential 模式。
- 返回 `206` 但 CORS 隐藏 `Content-Range`: 如果 body 短于请求范围, 直接由实际长度确定终点; 否则发送一次无 body 的 HEAD, 从 CORS safelisted `Content-Length` 取得总长度, 保留 discovery 数据并规划剩余 Chunk。
- HEAD 也无法取得长度: 最后才丢弃 discovery 数据并回退到无 Range 的完整 GET。
- 已声明 HLS byte range 但 Origin 返回 `200`: 请求失败, 不能把整个资源误当成子范围。

discovery 请求本身始终贡献 Chunk 0 数据。HEAD 只在 Range 响应是 `206`、总长度不可见且本次恰好填满请求范围时使用, 不读取媒体 body。

不同 Range 响应在可用时比较 ETag 或 Last-Modified。资源标识变化时 Segment 失败, 避免拼接不同版本的数据。

## Transport

`ParallelSegmentLoader` 持有一个 `HttpTransport`。未提供时创建 `FetchHttpTransport`; 应用也可以传入 `ProxyHttpTransport` 或 `WebSocketHttpTransport`。Loader 只依赖标准 `Request`、response status/header/body 和 AbortSignal, Transport 不理解 Segment、窗口或 Chunk 优先级。

Proxy 为获得 CDN 缓存语义会把上游 `206` 包装成物理 `200`, `ProxyHttpTransport` 在返回 Loader 前恢复逻辑 status 和 `Content-Range`。因此浏览器 Network 面板可能显示 `200`, 而 Segment 诊断显示逻辑 `206`; 两者描述的是不同层级。

Loader 拥有传入的 Transport 生命周期, `ParallelSegmentLoader.destroy()` 会调用 `transport.destroy()`。应用不要在 Loader 仍工作时单独销毁 Transport。

## Worker 调度

`ParallelSegmentLoader` 构造时立即启动固定数量的逻辑 Worker。逻辑 Worker 是同一 JavaScript realm 中的异步循环, 不是浏览器 Web Worker。

```text
while not destroyed
  +-- 同步选择并占用最佳 Chunk
  +-- 没有工作时等待 revision 变化
  +-- 使用局部 AbortController 执行 Transport request
  +-- 同步验证 fillId 并提交结果
```

Chunk 优先级依次为:

1. 所属 Segment 的 `readerCount > 0`。
2. Segment 在 window 中的位置更靠前。
3. Chunk 在 Segment 内的位置更靠前。
4. 稳定 key 顺序, 保证比较结果确定。

正式读取需求可以抢占最低优先级的纯窗口预加载请求。Worker 收到 revision 变化后重新检查当前 Fill 是否仍存活, 以及是否需要让出网络槽。

## 粗粒度状态通知

Loader 持有实例级全局 revision 和 listeners:

```ts
revision: number
listeners: Set<() => void>
```

listener 不携带 Segment、Chunk 或结果数据, 只表达“共享状态可能变化”。Controller、fLoader 和 Worker 被唤醒后重新读取状态。

所有共享状态修改通过同步 transaction 完成:

```text
同步修改 streams/segments/window/chunks
  -> revision + 1
  -> 合并安排一次 microtask 通知
```

同一窗口更新中的 Segment upsert、window 替换和失活 Segment 驱离属于一次 transaction, 观察者不会看到 window 已更新但 Segment 尚未存在的中间状态。

`waitForChange(afterRevision, signal)` 在注册 listener 前同步检查 revision, 防止检查状态和等待之间丢失通知。

## 跨 await 一致性

任何共享状态读改写都不能跨越 `await`。Worker Fill 分为三个阶段:

1. 同步选择 Chunk, 设置 `state = filling` 和唯一 `fillId`。
2. 使用 Worker 局部变量和 AbortController 执行异步 Transport request。
3. response 返回后重新按 stream/segment/chunk key 查找状态, 只有 `fillId` 仍匹配才提交。

窗口变化、取消或重新分配会使旧 `fillId` 失效。迟到的 Fetch 结果无法覆盖新状态。

同一 JavaScript realm 的同步代码具有 run-to-completion 语义, 因此不需要 mutex。若未来把 Worker 迁移到真正的 Web Worker, 必须改成单一状态 authority 加消息协议, 不能直接共享本设计中的 Map。

## fLoader 读取

`ParallelSegmentLoader.fLoader` 是 hls.js 要求的可实例化构造器。每个 fLoader 实例只能执行一次 `load()`。

load 时:

```text
确保 VirtualStream 和 Segment 存在
readerCount + 1
revision + 1
循环检查 Segment state
未完成则等待全局 revision
```

ready 后复制最终 ArrayBuffer 再返回 hls.js, 避免 hls.js transfer buffer 破坏 Loader 中的 canonical 数据。

abort、destroy、timeout、success 或 failure 结束时只把 `readerCount - 1`, 不保存任何 Segment 级 callback。hls.js callbacks 只保存在该 fLoader 实例局部。

## Segment 完成

所有 Chunk ready 后按本地偏移组装唯一 canonical ArrayBuffer。组装完成后清除各 Chunk 的 Uint8Array, 只保留范围和状态供诊断, 避免同时长期持有 Chunk 数据和完整 Segment 两份内容。

LoaderStats 聚合所有 Chunk:

- `loading.start`: 首个 Chunk attempt 开始时间。
- `loading.first`: 首个 response head 时间。
- `loading.end`: Segment ready 或失败时间。
- `loaded`: 已完成 Chunk 的唯一字节总数。
- `total`: 已知 Segment 长度。
- `chunkCount`: 完成的 Chunk 数。
- `bwEstimate`: Segment 唯一字节数除以整体墙钟时间。

## Segment 存活与驱离

Segment 的存活条件只有:

```text
Segment 在 VirtualStream.window 中
或者
Segment.readerCount > 0
```

Worker Fill 不构成独立存活依据。

`ParallelSegmentLoader` 是唯一驱离决策者。Controller 只更新 window, fLoader 只更新 readerCount, Worker 只执行网络取消。

窗口从 `[s1..s6]` 推进到 `[s2..s7]` 时:

- s2-s6 的 Segment、ready 数据和已完成 Chunk 全部保留。
- 创建 s7。
- s1 在没有 reader 时驱离; 有 reader 时等读取结束再驱离。

驱离会使所有 fillId 失效、删除 Segment/Chunk 数据并增加 revision。Worker 被全局通知唤醒后中止局部 Fetch。迟到结果因 Segment 不存在或 fillId 不匹配而丢弃。

第一版不保留窗口之外的后向缓存, 因而媒体数据不会随播放时间无限增长。常驻数据上界主要由每条激活轨道的窗口和正式读取中的 Segment 决定。

## 错误行为

- 没有 reader 的预加载 Chunk 失败: Segment 标记失败并停止继续填充, 不无限重试。
- 后续 fLoader 正式读取失败的预加载 Segment: 清除失败 Chunk 并重新排队缺失部分。
- 有 reader 时请求失败: fLoader 收到 `onError`, 由 hls.js 原生重试状态机决定是否重新调用。
- timeout: 每个 fLoader 保持自己的完整加载 timeout; Worker request 同时遵循 hls.js 默认最大加载时间。
- abort: 只结束当前 fLoader 读取。Segment 仍在 window 中时 Worker 继续填充。

## 诊断

`ParallelSegmentLoader.getDiagnostics()` 同步返回深拷贝只读快照, 包含:

- timestamp、revision、destroyed。
- active request 数量和并发上限。
- 每个 Worker 的 idle/loading/stopped 状态和当前 Range。
- 每条 VirtualStream 的 window。
- Segment 的位置、状态、windowIndex、readerCount、逻辑 HTTP status、sequential 标记和字节统计。
- Chunk 的范围、状态、字节、fillId、attempt 和 failure。

诊断不返回媒体 ArrayBuffer、Response、callback、listener、AbortController 或可变内部对象, 不修改 revision, 也不参与调度。example 通过定时轮询快照绘制时间线。

## 生命周期

`ParallelStreamController` 由 hls.js 创建和销毁。它在一个 Hls 实例内经历多次 start/stop/seek/level switch, `stopLoad()` 只清理窗口。

`ParallelSegmentLoader` 由应用创建和销毁。一个实例只服务一个 Hls session, 构造时启动 Worker, `destroy()` 时取消 request、停止 Worker、销毁 Transport、唤醒 listener 并清空全部状态。销毁后不可复用。

推荐顺序:

```ts
const loader = new ParallelSegmentLoader()
const hls = new Hls({
  fLoader: loader.fLoader,
  progressive: false,
  streamController: ParallelStreamController,
})

// teardown
hls.destroy()
loader.destroy()
```

应用拥有 Hls 和 Loader; Hls 拥有 Controller; Controller 只借用 Loader, 不负责销毁它。

## 公开接口

```ts
import { ParallelSegmentLoader, ParallelStreamController } from 'storya-hls-loader'

const loader = new ParallelSegmentLoader()
const hls = new Hls({
  fLoader: loader.fLoader,
  progressive: false,
  streamController: ParallelStreamController,
})

const diagnostics = loader.getDiagnostics()
```

公开运行组件只有两个类。包同时导出默认配置常量、Loader options 和诊断 TypeScript 类型。

## 修改历史

- 2026-08-08: 完成新并行模型设计与实现。Controller 只维护有序窗口; Loader 直接持有多 VirtualStream、Segment、Chunk、revision/listeners、逻辑 Worker、fLoader、Transport 和诊断; Chunk 从第一版即为调度单位; 明确同步 transaction、fillId 一致性和窗口/reader 驱离规则; 对 CORS 隐藏 Content-Range 增加保留 discovery Chunk 的 HEAD 长度发现。
- 2026-08-07: 删除旧 VirtualStreamRegistry、StreamFiller、frontier 和相关诊断, 验证 hls.js 自定义 StreamController 与 fLoader 替换入口。
