# HLS 并行加载器设计

本文描述 `storya-hls-loader` 当前采用的设计。加载器只负责把 hls.js 的读取映射到虚拟流, 并通过多个独立填充器并行获取原始媒体字节。它不负责解码、解密、transmux、ABR 或向 SourceBuffer 追加媒体。

## 设计目标

- 一条实际轨道对应一条独立 VirtualStream, 多条 VirtualStream 可以同时被读取和填充。
- 虚拟流核心不理解音频、视频、清晰度、字幕等媒体类型。
- hls.js fLoader 只提交或取消 Segment 读取, 并在数据 ready 后消费结果。
- Segment 的存在表达轨道 topology, Chunk 的存在表达当前填充或缓存意图。
- 多个 StreamFiller 是长期运行且彼此独立的 worker, 自己观察、领取、抢占和补救。
- 实际媒体字节长期存放在 VirtualStreamChunk 中, 可以跨 filler 和网络 attempt 保留。
- 所有 filler 共享一份 HttpTransport, Transport 不理解虚拟流和调度语义。
- 状态变化只增加可观察 revision, 不由状态拥有者直接调用下游组件。
- 诊断是可删除的只读投影, 不反向影响核心状态。

## 总体结构

```text
                         playlist topology
                                |
                                v
hls.js fLoader -------> VirtualStreamRegistry <------- StreamFiller 1
 SegmentReader           +-- VirtualStream     <------- StreamFiller 2
                         |   +-- Segment        <------- StreamFiller 3
                         |       +-- Reader[]             |
                         |       +-- Chunk[]              |
                         |           +-- Writer?          v
                         |                         shared HttpTransport
                         |
                         +-- revision / snapshot
```

`HlsParallelLoaderSession` 是 composition root, 只负责创建和销毁 VirtualStreamRegistry、fLoader adapter、固定数量的 StreamFiller 和共享 HttpTransport。它不参与需求判断、优先级、任务分配、抢占或补救。

系统不包含 StreamFillerRegistry、中心 Scheduler 或共享任务队列。全局并发上限由长期运行的 StreamFiller 数量自然保证。

## 核心不变量

1. 一条实际轨道在一个加载会话内只有一个 VirtualStream。
2. VirtualStream 不保存 `kind`、`selected` 或 `active` 等媒体选择状态。
3. 一次 fLoader `load()` 对应一个 VirtualStreamSegmentReader。
4. Reader 只能取消自己, 多个 Reader 可以共享一个 Segment。
5. 一个 VirtualStreamChunk 同时最多存在一个 VirtualStreamChunkWriter。
6. Chunk 中已经接受的数据不属于 Writer, Writer 释放后数据继续存在。
7. 只有当前有效且 content version 一致的 Writer 才能修改 Chunk。
8. Filler 不接收 Session 或 Registry 发出的执行命令, 只观察状态并自行收敛。
9. Transport 只执行 HTTP request, 不理解 Reader、Segment、Chunk、Writer、优先级或补救。
10. 调用诊断接口不得修改状态、增加 revision 或影响 Filler 决策。

## VirtualStreamRegistry

VirtualStreamRegistry 是虚拟流领域状态的根。它保存多个 VirtualStream, 负责把 hls.js adapter 提供的稳定轨道身份和资源描述映射到对应 Stream 与 Segment。

Registry 提供三类能力:

```ts
interface VirtualStreamRegistry {
  readonly revision: number

  updateStream(
    streamId: VirtualStreamId,
    descriptors: readonly VirtualStreamSegmentDescriptor[],
  ): void

  mergeStream(sourceId: VirtualStreamId, targetId: VirtualStreamId): void

  createSegmentReader(request: VirtualStreamReadRequest): VirtualStreamSegmentReader

  tryAcquireChunkWriter(
    request: VirtualStreamChunkWriteRequest,
  ): VirtualStreamChunkWriter | undefined

  trySwitchChunkWriter(
    currentWriter: VirtualStreamChunkWriter,
    target: VirtualStreamChunkWriteRequest,
  ): VirtualStreamChunkWriter | undefined

  snapshot(): VirtualStreamRegistrySnapshot

  waitForChange(afterRevision: number, signal?: AbortSignal): Promise<number>
}
```

- Command: 创建 Reader、更新 topology、执行跨 Chunk 的原子 Writer 切换。
- Query: 返回不包含媒体 body 的只读 snapshot。
- Observe: 等待 revision 变化后重新读取最新 snapshot。

Registry、Stream、Segment 和 Chunk 共享同一个 change clock。内部发生有意义的状态变化时只增加 revision 并唤醒观察者, 不直接调用 fLoader 或 StreamFiller。

通知只表达“状态已经变化”。观察者可以跳过中间 revision, 因为当前事实始终可以从最新 snapshot 恢复。

### 稳定轨道身份

hls.js adapter 必须为每条实际轨道提供稳定 VirtualStreamId。VirtualStreamId 不得直接使用 Segment URL、playlist URL、CDN pathway 或重定向后的 URL, 因为这些地址可能在同一轨道生命周期内变化。

playlist topology 事件可能晚于首次 fLoader `load()` 到达。adapter 可以先建立 provisional 映射, topology 完整后必须把它归并到同一个 canonical VirtualStream, 不能为同一轨道保留两份 Segment、Reader 或 Chunk 状态。

媒体类型只允许作为 adapter 或诊断层的可选 label, 不得参与虚拟流行为和填充优先级。

hls.js adapter 必须先把 `FragmentLoaderContext`、`HlsConfig`、`LoaderConfiguration` 和 `LoaderStats` 转换成通用的 Segment descriptor、Resource request factory、Fill policy 和 statistics callback。VirtualStreamRegistry、VirtualStream、Segment、Chunk、Reader、Writer 和 StreamFiller 均不导入 hls.js 类型。

## VirtualStream

VirtualStream 表示同一条轨道上的有序 Segment 序列:

```ts
interface VirtualStream {
  readonly id: VirtualStreamId
  readonly segmentsByKey: ReadonlyMap<VirtualStreamSegmentKey, VirtualStreamSegment>
  readonly prefetchSequence: readonly VirtualStreamSegment[]
  readonly frontier: VirtualStreamFrontier | undefined
}
```

- `segmentsByKey` 保存 adapter 可以定位和读取的全部 Segment。
- `prefetchSequence` 保存参与 frontier 和前向预填充的有序 Segment。
- `frontier` 表示最近一次有效读取所在的轨道位置。

VirtualStream 不知道自身是音频、视频、字幕还是某个清晰度。音频和视频等多条流可以同时拥有 Reader、frontier、Chunk 和 Writer, 并在同一组 Filler 中并行竞争网络资源。

### 预填充窗口

每次创建 Reader 时, 对应 Stream 更新 frontier generation, 并物化以下窗口中的 Chunk:

```text
当前 frontier Segment + 后续 N 个 Segment
```

`N` 由公开配置 `prefetchAheadSegments` 指定, 默认值为 6:

```ts
interface HlsParallelLoaderOptions {
  prefetchAheadSegments?: number
}
```

该配置必须是非负整数:

- `0`: 只填充当前 Segment。
- `1`: 当前 Segment 加后续 1 个 Segment。
- `6`: 当前 Segment 加后续 6 个 Segment。

配置在会话创建时固定, 不支持运行期间动态修改。

Reader 在满足前被取消时, 如果它仍拥有最新且未确认的 frontier generation, 对应 provisional 窗口失效并重新计算。Reader 成功后, 其位置成为保留 frontier, 使有界预填充窗口可以继续存在。

Reader 对应的 Segment 最终填充失败时, 其位置仍然保留为 frontier, 但失败 Segment 成为预填充屏障。该 Stream 不继续填充屏障之后的纯预填充 Chunk, 直到 hls.js 创建新的 Reader 并使该 Segment 重试成功、读取位置改变或 content version 更新。

一条旧 Stream 没有等待 Reader 时, 其 Chunk 只有预填充优先级, 必须让位于任何直接读取。旧窗口不会继续向前扩张, 并在播放位置越过后自然淘汰。因此核心无需知道一次变化是否为清晰度、音轨或字幕切换。

## VirtualStreamSegment

VirtualStreamSegment 是 fLoader 可以独立读取的数据单元:

```ts
interface VirtualStreamSegment {
  readonly key: VirtualStreamSegmentKey
  readonly stream: VirtualStream
  readonly position: VirtualStreamPosition
  readonly resource: VirtualStreamResource
  readonly readers: ReadonlyMap<ReaderId, VirtualStreamSegmentReaderState>
  readonly chunks: readonly VirtualStreamChunk[]
}

interface VirtualStreamResource {
  readonly url: string
  readonly headers: Readonly<Record<string, string>>
  readonly rangeStart: number
  readonly rangeEnd: number | undefined
  readonly createRequest: (parameters: VirtualStreamRequestParameters) => Promise<Request>
}
```

Segment 保存:

- 在轨道上的稳定顺序和播放时间位置。
- URL、逻辑 byte range 和 request headers 等资源描述。
- 当前等待结果的 Reader。
- 已经物化的 Chunk。
- 聚合后的完整数据和真实网络统计。
- 内容版本、失败和重试信息。

虚拟流核心不区分普通媒体 Fragment、LL-HLS Part、Init Segment 或字幕资源。hls.js adapter 负责把可独立读取的外部资源映射为统一 Segment, 并决定它是否进入有序 `prefetchSequence`。填充器只处理 Segment 的位置和资源描述。

playlist topology 可以提前创建 Segment 元数据, 但不会为整个 playlist 立即创建 Chunk。只有存在 Reader、进入预填充窗口或保留 partial/ready 数据时, Segment 才持有 Chunk。

Segment 的全部目标 Chunk complete 后进入 ready。多个 Reader 共享同一份标准 Segment 数据, 但向 hls.js 交付时必须为每个 Reader 返回独立 ArrayBuffer, 避免一个消费者转移或分离 buffer 后影响其他消费者。

## VirtualStreamSegmentReader

VirtualStreamSegmentReader 表示一次 fLoader 对特定 Segment 的读取:

```ts
interface VirtualStreamSegmentReader {
  readonly id: ReaderId
  readonly segment: VirtualStreamSegment
  readonly result: Promise<VirtualStreamSegmentResult>

  cancel(): void
}
```

fLoader adapter 的职责严格限制为:

```text
load
  -> 映射 VirtualStream 和 VirtualStreamSegment
  -> 创建 VirtualStreamSegmentReader
  -> 等待 Reader result
  -> 复制数据并调用 hls.js callback

abort / destroy
  -> 取消自己的 Reader
```

Reader 状态直接保存在所属 Segment 中, 不建立独立 VirtualStreamDemand 模型。一个 Reader cancel 只移除自己的读取, 不会清除其他 Reader 或直接取消网络 attempt。

Reader 等待超时属于本次 hls.js 读取。超时后 fLoader 报告 `onTimeout` 并取消该 Reader, 但如果 Chunk 仍位于其他 Reader 或预填充窗口中, 当前 Writer 可以继续工作。共享填充任务不得继承最早 Reader 的完整生命周期超时。

`FRAG_BUFFERED` 不参与 Reader、frontier、Chunk 数据或预填充生命周期。fLoader 成功交付后 hls.js 已经拥有独立数据副本, 虚拟流缓存由自身窗口规则管理。

## VirtualStreamChunk

VirtualStreamChunk 是最小数据存储和并行填充单元:

```ts
interface VirtualStreamChunk {
  readonly key: VirtualStreamChunkKey
  readonly segment: VirtualStreamSegment
  readonly startOffset: number
  readonly endOffset: number | undefined
  readonly contentVersion: number
  readonly receivedLength: number
  readonly contentState: 'empty' | 'partial' | 'complete'
  readonly writer: VirtualStreamChunkWriterState | undefined
}
```

Chunk 保存已经接受的实际媒体字节。Transport Response body 由 Filler 持续写入 Chunk, Writer 释放、网络重试或抢占都不会自动删除已经接受的数据。

内容状态和执行所有权是两个独立维度:

| 内容状态   | Writer | 含义                         |
| ---------- | ------ | ---------------------------- |
| `empty`    | 无     | 等待 Filler 领取             |
| `empty`    | 有     | 已领取但尚未收到数据         |
| `partial`  | 有     | 正在填充或执行慢速补救       |
| `partial`  | 无     | 被抢占、等待重试或暂时无需求 |
| `complete` | 无     | 数据 ready                   |

`complete` 且仍有 Writer 只允许作为完成事务内部的瞬时状态, 不得长期暴露。

对于已知长度资源, Segment 根据配置的 Chunk 大小物化一个或多个 Chunk。对于未知长度资源, 先物化首个 discovery Chunk。首个 Range Response 确定总长度后, Segment 调整首个 Chunk 边界并物化剩余 Chunk。源站忽略 Range 并返回顺序 200 时, 首个 Chunk 接受完整 Response, 不再创建其他并行 Chunk。

Chunk 写入必须连续且不能超出目标边界。资源 validator 或 content version 变化时, 旧 Writer 立即失效, 已有数据按资源一致性规则整体清除。

## VirtualStreamChunkWriter

VirtualStreamChunkWriter 是 Chunk 发放给 StreamFiller 的排他写入能力:

```ts
interface VirtualStreamChunkWriter {
  readonly id: WriterId
  readonly fillerId: number
  readonly chunk: VirtualStreamChunk
  readonly contentVersion: number

  getFillPlan(rangeMode?: HttpTransportRangeRequestMode): VirtualStreamChunkFillPlan
  acceptResponse(metadata: VirtualStreamChunkResponseMetadata): void
  append(data: Uint8Array): void
  recordAttempt(metadata: VirtualStreamChunkAttemptMetadata): void
  complete(): void
  fail(error: ChunkFillFailure, retryAt?: number): void
  release(reason: ChunkWriterReleaseReason): void
}
```

Filler 通过 Registry 的同步 `tryAcquireChunkWriter()` 领取 Chunk。Registry 根据 Chunk key 定位实际对象, 检查 Writer 是否存在、校验 content version、分配 Writer ID 并更新 revision。这些步骤必须在同一个同步状态转换中完成, 从而在 JavaScript 单线程中提供原子排他语义。Writer 状态仍然存储在对应 Chunk 中, Registry 不维护另一份 Writer 表。

每次 append、complete、fail 或 release 都校验 Writer ID 与 content version。被抢占、已释放或指向旧内容版本的 Writer 不能继续写入, 迟到的 Response body 会被拒绝。

Writer 不是 StreamFiller, 也不负责网络。它只是 Filler 对一个 Chunk 的临时写入凭证。

## StreamFiller

会话创建固定数量的 StreamFiller, 每个 Filler 都是长期运行且相互独立的自主单元:

```ts
interface StreamFiller {
  readonly id: number

  start(): void
  destroy(): void
}
```

每个 Filler 自己维护当前 Writer、Transport attempt、AbortController、重试和慢速检测状态, 并持续执行以下循环:

```text
读取 VirtualStreamRegistry snapshot
  -> 计算当前最值得填充的 Chunk
  -> 尝试取得 Writer
  -> 执行或继续网络 attempt
  -> 同时观察 revision 和时间条件
  -> 继续、补救、抢占、失败或完成
  -> 重新观察
```

不存在任何组件调用 `filler.fill()`、`filler.ensure()` 或 `filler.cancel()`。Session 只在启动和销毁时调用 Filler 生命周期方法。

多个 Filler 可以同时处理不同 VirtualStream, 因此独立音频、视频、字幕或多个 rendition 的工作可以自然并行。系统不为任何媒体类型固定保留连接数。

### 任务选择

Chunk 的存在表达它已经进入填充或缓存生命周期。Filler 从全部 VirtualStream 中选择尚未 complete、当前可重试且没有 Writer 的 Chunk, 使用同一个纯优先级函数排序:

1. 有等待 Reader 的直接读取优先于纯预填充。
2. 播放截止时间更早的 Chunk 优先。
3. 同一 Segment 内更靠近完整交付前沿的 Chunk 优先。
4. 创建顺序和稳定 key 作为最终 tie-break。

播放截止时间只依赖 Segment 在轨道上的位置、当前播放位置和播放速度, 不依赖媒体类型。多个 Filler 对同一 snapshot 得出相同排序, 但同一 Chunk 只有一个 Filler 能原子取得 Writer, 失败者重新观察即可。

### 抢占

Filler 在持有 Writer 和执行网络 attempt 时继续观察 Registry revision。出现更高优先级的未领取 Chunk 后, 所有 Filler 使用相同规则判断当前并发集合是否仍然合理。

只有持有最低优先级且允许抢占任务的 Filler 主动让位。最短运行时间、完成比例和预计剩余时间继续作为保护条件。

为了避免先释放旧 Writer 后未取得新 Writer, Registry 提供跨两个 Chunk 的同步原子切换:

```ts
registry.trySwitchChunkWriter(currentWriter, targetChunk)
```

该方法只验证和修改两个 Chunk 上的 Writer 状态, 不决定哪个任务应当运行。调度决策始终由调用它的独立 Filler 做出。

抢占后当前 Transport attempt 被 abort, 原 Chunk 已经接受的 partial 数据保留, 以后可以由任意 Filler 取得新 Writer 并继续填充。

### 慢速补救与网络重试

一个 Writer 可以承载多个 Transport attempt:

```text
VirtualStreamChunkWriter
  +-- Attempt 1: 慢速或中断, abort
  +-- Attempt 2: 从已有位置继续
  +-- complete
```

慢速补救不释放 Writer, 只替换当前网络 attempt。Filler 根据当前 attempt 吞吐量和 VirtualStreamRegistry 中已有 Chunk 的真实网络统计判断慢速。样本不足、任务仍在保护期或接近完成时不补救。

稳定 Range Transport 被补救时保持原请求边界并丢弃已经写入 Chunk 的响应前缀。可恢复 Range Transport 可以从 `startOffset + receivedLength` 继续。

单次 attempt 的首字节超时、流量空闲超时和网络错误由 Filler 处理。补救与内部重试耗尽后, Writer 把 Chunk 标记为失败并释放。等待该 Segment 的 Reader 收到最终错误, hls.js 后续重试会创建新的 Reader 并重新提高相应 Chunk 的优先级。

## HttpTransport

一个加载会话中的全部 StreamFiller 共享同一份 HttpTransport:

```ts
interface HttpTransport {
  readonly rangeRequestMode?: 'resumable' | 'stable'

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>

  destroy(): void
}
```

Transport 只负责执行标准 HTTP Request 并返回流式 Response。它可以由 Fetch、HTTP Proxy 或 WebSocket relay 实现, 但不理解:

- VirtualStream、Segment 或 Chunk。
- Reader 或 Writer。
- 播放位置和优先级。
- 抢占、补救和重试。
- 预填充和缓存淘汰。

Filler 通过 Request.signal 和 Response body cancel 终止一次网络 attempt。Transport 负责把标准取消语义映射到底层 Fetch、Proxy 或 WebSocket 事务。

## 数据、失败与淘汰

- Chunk 数据由 Chunk 自己持有, 可以跨 Writer、抢占和 attempt 保留。
- Segment ready 后保留标准完整数据, 每个 Reader 消费独立副本。
- Reader cancel 不直接删除已经接受的数据。
- Writer 的一次 attempt 失败不删除 partial 数据。
- 资源 validator 或 content version 不一致时清除对应资源的全部旧数据。
- Segment 离开 frontier 窗口且没有 Reader、Writer 后, 删除其 Chunk 和媒体数据, 但可以继续保留 topology 元数据。
- 旧 Stream 的窗口不会在没有新 Reader 时继续扩张, 并随播放位置越过而自然淘汰。
- 会话销毁时取消全部 Reader、Writer、网络 attempt 和观察等待, 然后销毁共享 Transport。

## hls.js Adapter

加载器通过一个会话对象同时提供 fLoader constructor 和 topology 绑定:

```ts
const parallel = createHlsParallelLoader({
  getPlaybackRate: () => media.playbackRate,
  getPlaybackTime: () => media.currentTime,
  prefetchAheadSegments: 6,
  transport,
})

const hls = new Hls({
  fLoader: parallel.fragmentLoader,
  progressive: false,
})

parallel.attach(hls)
hls.loadSource(source)

parallel.destroy()
```

fLoader 方法足以表达 hls.js 的直接读取和取消。`attach()` 监听 playlist topology 与会话生命周期事件, 只用于建立稳定 Stream/Segment 映射, 不产生读取需求、不选择媒体类型, 也不直接控制 Filler。

当前继续关闭 hls.js progressive 模式。VirtualStreamChunk 可以流式接受和保存数据, 但 VirtualStreamSegmentReader 只在完整目标 Segment ready 后向 hls.js 交付结果。

传入 Session 的 Transport 在一个会话内由全部 Filler 共享, 并在 Session `destroy()` 时统一销毁。

## 诊断

公开诊断入口放在 Session:

```ts
interface HlsParallelLoader {
  getDiagnostics(): HlsParallelLoaderDiagnosticsSnapshot
}
```

Session 不保存诊断状态。`getDiagnostics()` 在调用时同步读取 VirtualStreamRegistry snapshot、各 Filler 当前运行状态和可选 Transport statistics, 再交给 `diagnostics.ts` 中的纯 projector 生成公开 DTO。

```text
VirtualStreamRegistry snapshot --+
                                 +--> diagnostics projector --> public snapshot
StreamFiller states -------------+
                                 |
Transport statistics ------------+
```

诊断快照包含:

- Registry revision、Stream、frontier 和窗口。
- Segment Reader 数量与 ready/failed 状态。
- Chunk 范围、实际字节数、内容状态、优先级和 Writer 身份。
- Filler 的 waiting、filling、rescuing、preempting 状态。
- 当前 attempt 范围、吞吐量、重试、补救和抢占计数。
- 不同层级的汇总计数和可选 Transport 状态。

诊断快照不得包含媒体 body、Promise、AbortController、Request、Response 或可变核心对象引用。调用诊断不能执行网络、创建订阅、增加 revision 或改变优先级。

实验台继续按固定间隔调用 `getDiagnostics()`。核心加载器不维护诊断历史队列。离散历史事件如果后续仍有明确需求, 通过独立事件出口设计, 不把 UI 埋点散布到 Stream、Segment、Chunk 和 Filler 中。

## 实现状态

本文描述的架构已经完成实现。当前代码包括:

- hls.js fLoader 接入。
- hls.js adapter 到通用 Segment descriptor、Resource、Fill policy 和 statistics 的单向转换。
- VirtualStreamRegistry、VirtualStream、VirtualStreamSegment 和 VirtualStreamChunk 状态层级。
- VirtualStreamSegmentReader 和 VirtualStreamChunkWriter 能力边界。
- 可配置的跨 Segment 预填充窗口。
- 2 MiB Range Chunk、未知长度探测和顺序 Response fallback。
- 6 个独立 StreamFiller、原子 Writer 领取与切换、请求保护、抢占、慢速补救和网络重试。
- Fetch、HTTP Proxy 和 WebSocket HttpTransport。
- Session 级只读诊断快照。

重构已经删除以下旧内部模型:

- Session 中的 `activeStreams`、需求状态和 `reconcile()`。
- `hardDemands` 与 `playbackDemand`。
- 单个 StreamFiller 的 `ensure()` 与 `cancel()`。
- 中心 RequestScheduler。
- 依赖 hls.js Loader callbacks 的 SegmentLoader。
- VirtualStream 的 `kind`、`active` 和 `anchor`。

## 修改历史

- 2026-08-07: 完成新架构代码迁移, 删除 Session 中心需求协调、RequestScheduler 和 SegmentLoader, 实现 Registry 状态中心、Reader/Writer 能力边界及多个独立 StreamFiller; hls.js 类型收敛在 adapter, 核心状态与 Filler 只使用通用 descriptor 和 policy。
- 2026-08-07: 删除上一版会话中心调度设计, 确立以 VirtualStreamRegistry 为状态中心、Reader 消费 Segment、Writer 填充 Chunk、多个独立 Filler 自主观察和抢占的新架构; 预填充窗口改为可配置的当前 Segment 加后续 N 个 Segment, 核心不再理解媒体类型或选择状态。
