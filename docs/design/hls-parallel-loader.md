# HLS 并行加载器设计

本文描述 `storya-hls-loader` 当前采用的 HLS Segment/Chunk 并行加载设计。实现基于 hls.js `1.7.0-rc.3`, 对外提供 `ParallelStreamController` 和 `ParallelSegmentLoader` 两个运行时类。

## 设计目标

- 保留 hls.js 原生的选片、ABR、解密、transmux、错误恢复和 SourceBuffer append 流程。
- 由 Controller 根据 hls.js 已经选择的 Fragment 规划有序 Segment 预加载窗口。
- 同时加载多个 Segment, 并在单个 Segment 内使用 Range Chunk 并行加载。
- 让后台预加载和 hls.js 正式读取共享唯一的 Segment/Chunk 状态与媒体数据。
- 窗口推进时保留重叠 Segment, 只驱离既不在窗口中、也没有正式读取者的 Segment。
- 使用一份可直接检查的共享数据模型协调 Controller、fLoader 和 Worker, 不建立细粒度命令或回调网络。
- 所有共享状态修改保持同步和原子, 异步 Transport 结果不能覆盖已经失效的新状态。
- 保留 Transport 注入能力, 支持 Fetch、HTTP Proxy 和 HTTP-over-WebSocket。

## 非目标

当前设计不负责:

- 改写 hls.js 的解密、transmux、append 或 SourceBuffer 状态机。
- 让多个 Segment 同时进入 hls.js 的解析和 append 流程。
- 根据播放器当前时间直接创建、推进或取消窗口。
- LL-HLS Part 的向前预加载窗口。Part 的正式 fLoader 请求仍然可以进入共享模型并按 Chunk 加载。
- alternate audio 和 subtitle 的向前预加载窗口。数据模型支持多 Stream, 当前只有 main Controller 规划窗口。
- 根据历史吞吐识别“持续收到数据但明显过慢”的连接。当前只处理响应头超时、连续无数据超时和完整请求超时。
- 将逻辑 Worker 迁移为浏览器 Web Worker。

## 设计原则

### 唯一共享状态

Controller、fLoader 和 Worker 操作同一个 `ParallelSegmentLoaderState`。不存在预加载专用缓存、正式读取专用缓存或两套 Segment 生命周期。

```text
ParallelSegmentLoaderState
  +-- streams: Map<StreamId, VirtualStream>
  +-- revision
  +-- nextGeneration
  +-- destroyed

VirtualStream
  +-- segments: Map<SegmentKey, VirtualStreamSegment>
  +-- window: SegmentKey[]

VirtualStreamSegment
  +-- metadata
  +-- readerCount
  +-- outcome
  +-- chunks: VirtualStreamChunk[]

VirtualStreamChunk
  +-- range
  +-- attempt
  +-- phase
```

`VirtualStream`、`VirtualStreamSegment` 和 `VirtualStreamChunk` 是带有局部不变量方法的数据模型。它们不持有 callback、Promise resolver、listener、AbortController、Transport 或 Worker。

### 主动参与者直接协作

系统有三个长期参与者和一个按 attempt 创建的执行对象:

- `ParallelStreamController` 把 hls.js 的 Fragment 选择转换为 `window`。
- `StoryaFragmentLoader` 把一次 hls.js fLoader 调用转换为 `readerCount` 生命周期, 并观察 Segment outcome。
- `ChunkFillWorker` 观察状态、选择并 claim Chunk, 状态变化时取消失效或被抢占的 Work。
- `ChunkFillWork` 负责一个 generation 对应的一次 Transport attempt, 包括读取、校验、提交和超时补救。

不建立 Registry、Filler、Scheduler、Owner、Reader、Writer 或 Worker host adapter。参与者可以同步读取共享状态, 修改则必须经过 `ParallelSegmentLoader.update()`。

### 数据方法只维护局部不变量

方法挂在最了解不变量的数据类上:

- `ParallelSegmentLoaderState` 负责确保 Stream、定位 Segment、分配 generation 和全局 reconcile。
- `VirtualStream` 负责确保 Segment、替换窗口和删除不再存活的 Segment。
- `VirtualStreamSegment` 负责 reader 计数、Chunk 规划、顺序 fallback、失败和最终组装。
- `VirtualStreamChunk` 负责 claim、进度、完成、释放、补救和失败等 phase 转换。

这些方法不负责跨对象调度, 也不产生新的 service 层。`ChunkFillWork` 做成 class, 是因为它拥有 generation、AbortController、timer 和明确的单次执行生命周期。Worker 内的 candidate 和 Work options interface 只是同步传值对象, 没有独立身份或行为。

## 总体结构

```text
hls.js
  |
  +-- ParallelStreamController
  |     +-- update(state => replace window)
  |     +-- super.loadFragment(current)
  |
  +-- StoryaFragmentLoader
        +-- update(state => readerCount + 1)
        +-- subscribe(global change)
        +-- ready 后复制数据并回调 hls.js
        +-- update(state => readerCount - 1)

ParallelSegmentLoader
  +-- state
  +-- update()
  +-- subscribe()
  +-- revision/listeners
  +-- ChunkFillWorker[]
  |     +-- activeWork: ChunkFillWork
  +-- HttpTransport
  +-- fLoader constructor
  +-- getDiagnostics()
```

网络下载可以并行和乱序完成, 但只有 hls.js 原生 `StreamController` 选中的当前 Fragment 会通过 fLoader 返回并进入后续解析和 append。并行发生在数据准备层, 不改变媒体时间线消费顺序。

## ParallelSegmentLoader

`ParallelSegmentLoader` 是一个 Hls session 级对象。它是共享状态和资源的所有者, 但不是承载所有业务操作的 facade。

它负责:

- 创建 `ParallelSegmentLoaderState`。
- 创建固定数量的 `ChunkFillWorker`。
- 持有所有 Worker 共用的 `HttpTransport`。
- 生成与当前实例绑定的 hls.js `fLoader` 构造器。
- 绑定唯一的 hls.js `HlsConfig`。
- 提供同步事务入口 `update()`。
- 提供粗粒度全局订阅 `subscribe()`。
- 生成只读诊断快照。
- 销毁 Worker、Transport 和全部共享状态。

Loader 不提供 `replaceWindow()`、`startReading()`、`inspectSegment()`、`takeNextChunk()` 或 `waitForChange()` 等角色专用代理方法。Controller、FragmentLoader、Worker 和 Work 在事务中直接调用数据模型的方法。

`ParallelSegmentLoader.fLoader` 通过内部 WeakMap 与 Loader 实例关联。`ParallelStreamController` 从 `HlsConfig.fLoader` 找回同一个 Loader, 从而保证窗口和正式读取一定操作同一份状态。一个 Loader 只能绑定一个 Hls 实例。

## 同步事务与全局观察

### 修改规则

所有共享状态修改必须放在同步事务中:

```ts
loader.update(state => {
  const segment = state.locateSegment(context)
  segment?.stopReading()
  return undefined
})
```

一次 `update()` 执行顺序为:

```text
同步执行 mutation
  -> state.reconcile()
  -> revision + 1
  -> 合并安排一次 microtask 通知
```

约束如下:

- mutation 不能返回 Promise, 不能跨越 `await`。
- `update()` 不能嵌套调用。
- mutation 内不调用 hls.js callback。
- 一次窗口替换中的 Segment 创建、window 替换和旧 Segment 驱离对观察者原子可见。
- transaction 返回后才能执行 Transport、hls.js callback 或其他外部代码。

当前实现运行在同一个 JavaScript realm。同步代码具有 run-to-completion 语义, 所以读取共享状态不需要 mutex。若未来迁移到真正的 Web Worker, 必须改为单一状态 authority 加消息协议, 不能继续直接共享 Map 和 class 实例。

### 通知规则

Loader 持有实例级全局 listener 集合。listener 不携带 Segment、Chunk、结果或 diff, 只表示“共享状态可能发生变化”。多个同步更新可以合并为一次 microtask 通知。

Controller 不订阅。FragmentLoader 和 Worker 持续订阅, 被通知后重新读取自己关心的状态:

```text
listener()
  -> 同步重新检查 state
  -> 条件不满足则返回
  -> 等待下一次全局通知
```

系统不建立 Promise waiter, 也不需要“先读取 revision、再注册一次性 waiter”的防丢通知协议。订阅者在注册后立即主动检查一次状态, 此后由持续 listener 唤醒。`revision` 用于诊断和表达全局变化次数, 不作为 FragmentLoader 的等待令牌。

## VirtualStream 与窗口

每条媒体轨道或 rendition 使用独立 StreamId:

```text
main:<level>
audio:<identity>
subtitle:<identity>
```

当前实现实际由 main `ParallelStreamController` 创建 `main:<level>` 窗口。audio、subtitle、init Segment 和 Part 可以由正式 fLoader 创建没有窗口、但具有 reader 的 Segment。

`VirtualStream.window` 是有序 SegmentKey 数组。数组顺序同时表达:

- 当前激活的预加载范围。
- Worker 的 Segment 优先级。

它不表达“已经播放”或“应该驱离”的时间边界。播放越过某个 Segment 只会使 hls.js 随后选择新的 Fragment, Controller 再根据新的 `loadFragment()` 调用替换窗口。当前播放时间不直接修改 window。

### SegmentKey

SegmentKey 由以下字段共同构成:

- fragment type
- level
- sequence number
- discontinuity counter
- Part index 或 `segment`
- URL
- rangeStart
- rangeEnd

URL 和 byte range 属于 identity。相同媒体序号但资源位置不同的请求不能错误复用数据。

### 窗口替换

`VirtualStream.replaceWindow()` 先确保新窗口内的 Segment 存在, 再替换有序 key 数组。事务结束时 reconcile 删除不再存活的 Segment。

例如:

```text
旧窗口: [s1, s2, s3, s4, s5, s6]
新窗口: [s2, s3, s4, s5, s6, s7]
```

结果为:

- s2-s6 保留原有 Chunk phase、已下载数据和 ready outcome。
- s7 新建并规划初始 Chunk。
- s1 没有 reader 时立即驱离。
- s1 仍有 reader 时保留到最后一个 reader 结束。

窗口重叠不触发重建, 播放经过窗口首个 Segment也不代表整个窗口失效。

## ParallelStreamController

`ParallelStreamController` 继承 hls.js 默认 main `StreamController`, 只覆写两个调度入口:

- `loadFragment()` 在调用原生实现前更新窗口。
- `stopLoad()` 清除当前 Controller 持有的窗口, 再调用原生实现。

普通 VOD/传统 HLS 流程中, 窗口包含当前 Fragment 和后续最多 5 个 Fragment, 总数最多 6 个。gap 或没有 URL 的 Fragment 不进入窗口。窗口顺序来自 Level details 中的 Fragment 顺序。

以下情况清空当前 main 窗口并交回 hls.js 原生行为:

- 启动 bandwidth test。
- Level details 尚不存在。
- low-latency mode 下存在 Part 列表。

Level/rendition 切换时, Controller 在同一次事务中清空旧 Stream 窗口并建立新 Stream 窗口。旧 Stream 中仍有正式 reader 的 Segment继续存活。

Controller 不创建预加载专用 fLoader, 不发送请求, 不读取 Segment outcome, 也不根据播放时间驱离 Segment。

## StoryaFragmentLoader

`ParallelSegmentLoader.fLoader` 是 hls.js 要求的可实例化构造器。每个 `StoryaFragmentLoader` 实例只执行一次 `load()`。

### load

`load()` 执行以下步骤:

1. 保存本次 hls.js context、callbacks 和 timeout 配置。
2. 在一个事务中确保 Stream/Segment 存在并调用 `segment.startReading()`。
3. `readerCount + 1`; 如果 Segment 曾失败, 清除失败 outcome 和 failed Chunk, 保留已经 ready 的 Chunk。
4. 注册全局 listener。
5. 设置本次正式读取的完整加载 timeout。
6. 立即检查一次 Segment outcome。

立即检查保证窗口已经预加载完成的 Segment 可以同步进入成功路径, 不必等待下一次 revision。

### 观察与回调

listener 被触发后直接定位 Segment:

- `pending`: 更新 LoaderStats 后返回, 继续订阅。
- `ready`: 复制 canonical ArrayBuffer, 结束 reader 生命周期, 再调用 `onProgress/onSuccess`。
- `failed`: 保存 failure 和 Response, 结束 reader 生命周期, 再调用 `onError`。
- Loader 已销毁或 reader 对应的 Segment 不存在: 作为内部加载错误结束。

hls.js callback 始终在 `loader.update()` 之外调用, 避免 callback 重入共享事务。

ready 时必须先执行 `data.slice(0)`, 再减少 readerCount。最后一个 reader 结束后, 窗口外 Segment 可能立即被 reconcile 驱离; 同时复制也避免 hls.js 转移或修改返回的 ArrayBuffer 破坏 Loader 中的 canonical 数据。

### 结束读取

success、failure、timeout、abort 和 destroy 都通过同一个 settle 边界:

```text
标记 settled
  -> 取消全局订阅
  -> 清理 fLoader timeout
  -> update(state => readerCount - 1)
  -> reconcile
  -> 在事务外执行对应 hls.js callback
```

`abort()` 只结束当前 fLoader reader。只要 Segment 仍在 window 中或还有其他 reader, Worker 就继续填充, 不会因为一次 hls.js abort 丢失共享预加载数据。

### LoaderStats

每个 FragmentLoader 持有 hls.js 要求的 `LoaderStats` 对象, 但统计来源直接投影自 Segment/Chunk:

- `loading.start`: Segment 首个 Chunk claim 时间。
- `loading.first`: 所有完成 attempt 中最早的 response head 时间。
- `loading.end`: ready 或 failed outcome 的完成时间。
- `loaded`: 所有 Chunk 当前唯一有效字节数。
- `total`: 已知 Segment 长度。
- `chunkCount`: ready Chunk 数量。
- `retry`: hls.js 初始 retry 加 Worker rescue 次数。
- `bwEstimate`: 唯一字节数除以整体墙钟时间。

预加载已经发生时, stats 描述该 Segment 的真实网络加载过程, 而不是 fLoader 从缓存读取所花的几乎为零的时间。

## ChunkFillWorker

`ParallelSegmentLoader` 构造时创建固定数量的 `ChunkFillWorker`。默认全局并发为 6。它们是同一 JavaScript realm 中的逻辑 Worker, 不是浏览器 Web Worker。

每个 Worker 持续订阅 Loader 的全局通知。空闲时, 通知只安排一次合并的调度 microtask:

```text
state changed
  -> schedule once
  -> scan best empty Chunk
  -> update(state => claim Chunk with generation)
  -> create ChunkFillWork
  -> work.run()
```

没有可领取 Chunk 时, Worker 保持 idle, 不创建 Promise waiter。下一次全局通知会重新安排扫描。

### 调度优先级

候选 Chunk 按以下顺序比较:

1. `readerCount > 0` 的正式读取 Segment 优先。
2. windowIndex 更小的 Segment 优先。
3. Segment 内 index 更小的 Chunk 优先。
4. ChunkKey 字典序作为稳定 tie-breaker。

所有 Worker 扫描同一份状态。claim 必须在同步事务中把 Chunk 从 `empty` 改为 `filling`, 因而同一 realm 内不会有两个 Worker 成功领取同一个 generation。

### 抢占

正在加载的 Worker 收到全局通知后检查:

- 当前 Segment/Chunk 是否仍存在。
- 当前 Chunk generation 是否仍匹配。
- 是否出现尚未被领取的正式 reader Chunk。

当全部网络槽被低优先级窗口预加载占用、同时出现正式 reader 需求时, 系统选择优先级最低的后台 filling Chunk 作为 victim。对应 Worker 调用 `activeWork.cancel()`, Work 中止本地请求并把当前 generation 释放为 `empty`, 随后重新参与普通优先级选择。

抢占只释放 attempt, 不使 Segment 失败, 也不增加 rescue 次数。

## ChunkFillWork 与 generation

Worker claim Chunk 时从全局状态分配单调递增的 generation, 然后创建一个 `ChunkFillWork`。一个 Work 只代表一个 generation 和一次 Transport attempt, 不能重复执行。

Work 固定保存:

- streamKey、segmentKey 和 chunkKey
- generation
- FragmentLoaderContext 快照引用
- request range
- 已知资源长度

Work 拥有本次 attempt 的 AbortController、body 分片数组和计时器。共享 Chunk 在 `filling` phase 中只保存 generation、workerId、startedAt 和 loadedBytes。

每次进度、完成、释放、补救或失败提交前, Work 都重新根据 key 定位 Chunk, 并验证 generation:

```text
Segment/Chunk 不存在
  -> 丢弃迟到结果

generation 不匹配
  -> 丢弃迟到结果

generation 匹配
  -> 在同步事务中提交
```

窗口驱离、失败传播、抢占或重新 claim 都会使旧 generation 失效。网络请求即使稍后返回, 也不能覆盖新 attempt 或重新创建已驱离 Segment。

## Segment 与 Chunk 状态

### Segment outcome

Segment 持有显式 outcome:

```text
pending
ready  { data, response, code, url, completedAt }
failed { failure, completedAt }
```

诊断展示的 `empty/filling/ready/failed` state 从 outcome 和 Chunk phase 推导:

- ready outcome -> `ready`
- failed outcome -> `failed`
- pending 且存在 filling Chunk -> `filling`
- 其他 pending -> `empty`

Segment 还保存 URL/range context、媒体起止时间、长度、validator、sequential 标记、readerCount 和统计时间。

### Chunk phase

Chunk phase 为:

```text
empty   { lastFailure }
filling { generation, workerId, startedAt, loadedBytes }
ready   { byteLength, data, response, url, firstByteAt, completedAt }
failed  { failure }
```

`attempt` 记录总 claim 次数, `rescueAttempts` 记录因 timeout 进行的补救次数。Chunk 完成前的 body 数据只保存在 Work 局部; 完整验证后才一次性进入 ready phase。

## Chunk 规划与 Range 行为

默认 Chunk 大小为 2 MiB, 与 HTTP Proxy shard 大小一致。尾部不足半个 Chunk 时合并到前一个 Chunk, 避免极小请求。

### 已知 HLS byte range

如果 fLoader context 声明 `[rangeStart, rangeEnd)`, Segment 长度立即确定。Chunk 使用 Segment 本地偏移保存, 发送请求时再加上 `resourceStart`:

```text
resource range: [10 MiB, 15 MiB)

chunk 0 local [0, 2 MiB) -> request [10 MiB, 12 MiB)
chunk 1 local [2, 4 MiB) -> request [12 MiB, 14 MiB)
chunk 2 local [4, 5 MiB) -> request [14 MiB, 15 MiB)
```

Origin 对声明 byte range 返回 `200` 时 Segment 失败, 因为不能把整个资源误当成子范围。

### 未知长度 Segment

普通完整 Segment 通常没有预先声明长度。初始只创建 discovery Chunk:

```text
Range: bytes=0-2097151
```

处理规则:

- 返回 `206` 且存在有效 `Content-Range`: 得到资源长度, 保留 discovery 数据并规划剩余 Chunk。
- 返回 `200`: Origin 忽略 Range, 直接把本次完整响应作为 sequential Segment, 不重复发送完整 GET。
- 返回 `206` 但浏览器因 CORS 看不到 `Content-Range`, 且 body 短于请求范围: 使用实际结束位置确定长度。
- 返回 `206`, `Content-Range` 不可见且 body 填满请求范围: 发送一次 HEAD, 从可见 `Content-Length` 取得长度, 保留 discovery 数据并规划剩余 Chunk。
- HEAD 仍无法取得长度: 丢弃 discovery attempt, 回退为不带 Range 的 sequential GET。

不同 Range response 在可用时比较 ETag 或 Last-Modified。validator 变化会使整个 Segment 失败, 避免拼接不同资源版本。

### Segment 组装

所有 Chunk ready 后按本地 start 顺序组装唯一 canonical ArrayBuffer。组装要求:

- Chunk 从 0 开始连续覆盖。
- 最终 cursor 等于 Segment length。
- 每个 Chunk 都存在完成数据和确定终点。

组装成功后, Chunk 保留 byteLength、Response、状态和诊断字段, 但释放各自的 Uint8Array, 避免长期同时持有完整 Segment 和全部 Chunk 两份媒体数据。

## Transport 与网络超时

`ParallelSegmentLoaderOptions.transport` 可以传入任意 `HttpTransport`; 未传入时创建 `FetchHttpTransport`。Loader 持有 Transport 生命周期, 所有 Worker 共享同一个实例。

Transport 只处理标准 `Request`、response head 和流式 body, 不理解 Stream、Segment、Chunk、窗口或优先级。`ChunkFillWork` 负责:

- 根据 hls.js context 和 range 构造 Request。
- 调用 hls.js `fetchSetup`。
- 使用 Transport 发出 GET/HEAD。
- 逐段读取 ReadableStream。
- 通过 Request AbortSignal 和 body cancel 表达取消。
- 将 response 转换为 Segment/Chunk 状态。

每个 attempt 有三类限制:

- 响应头超时: `fragLoadPolicy.maxTimeToFirstByteMs`。
- 完整请求超时: `fragLoadPolicy.maxLoadTimeMs`。
- body 连续无数据超时: 默认 5 秒。

body 每产生一段数据, Work 复制到 attempt 局部 parts, 同步更新共享 Chunk 的 loadedBytes, 并重置空闲计时器。只有完整 body 验证通过后才提交 Chunk data。

Proxy 为获得 CDN 缓存语义可能把上游 `206` 包装成物理 `200`; `ProxyHttpTransport` 在返回 HLS Loader 前恢复逻辑 status 和 `Content-Range`。因此浏览器 Network 面板可能显示 `200`, 诊断中仍显示逻辑 `206`。

## 错误、补救与重试

普通网络错误、HTTP 错误、Range 校验错误或资源 validator 变化会使 Segment 进入 failed outcome。Segment failure 会让同一 Segment 仍在 filling 的其他 Chunk 一起失败, 对应 Worker 收到通知后取消失效 Work。

Work 内部检测 timeout 并结束当前 attempt。默认允许补救 2 次, 即同一 Chunk 最多执行 3 个 Work:

```text
timeout
  -> rescueAttempts 未达上限
       -> generation 释放为 empty
       -> retryCount + 1
       -> 当前 Work 结束
       -> Worker 重新执行全局调度
  -> 已达上限
       -> Segment failed
```

预加载 Segment 失败后不会无限重试。后续 hls.js 正式读取到该 Segment 时, `startReading()` 清除 failed outcome 和 failed Chunk, 保留已经 ready 的 Chunk, 只重新加载缺失部分。正式读取再次失败后由 hls.js 原生错误恢复状态机决定是否创建新的 fLoader 重试。

fLoader timeout 和 Work attempt timeout 是不同层级:

- fLoader timeout 限制一次 hls.js 正式读取等待总时间。
- Work timeout 限制一次具体网络 attempt。

## Segment 存活与驱离

Segment 的唯一存活条件是:

```text
segment.key 在 VirtualStream.window 中
或者
segment.readerCount > 0
```

Worker 的 filling attempt 不构成独立存活依据。每次共享状态事务结束后, `ParallelSegmentLoaderState.reconcile()`:

1. 调用每个 Stream 的 prune。
2. 删除不在 window 且 readerCount 为 0 的 Segment。
3. 删除 window 为空且 segments 为空的 Stream。
4. Loader destroyed 时清空全部 Stream。

因此驱离不是 Controller、FragmentLoader 或 Worker 单独发出的命令:

- Controller 修改 window, 表达预加载需求是否存活。
- FragmentLoader 修改 readerCount, 表达正式读取是否存活。
- State reconcile 根据统一条件执行删除。
- Worker 在通知后发现 Segment/generation 消失, 调用 Work.cancel() 取消局部网络任务。

第一版不保留窗口之外的后向缓存。常驻媒体数据上界主要由每条活跃 Stream 的窗口、正式读取中的窗口外 Segment 和正在执行的 attempt 局部 body 决定。

## 诊断

`ParallelSegmentLoader.getDiagnostics()` 同步投影一份只读快照, 不返回内部可变对象或媒体数据。快照包含:

- timestamp、revision、destroyed。
- active request 数量和最大并发。
- 每个 Worker 的 idle/loading/stopped、当前 Stream/Segment/Chunk、Range 和 startedAt。
- 每条 VirtualStream 的 window。
- Segment 的 start、duration、windowIndex、readerCount、state、HTTP status、sequential 和字节数。
- Chunk 的范围、state、实时 loadedBytes、generation、attempt 和 failure。

诊断投影不修改 revision, 不参与调度, 也不暴露 ArrayBuffer、Uint8Array、Response、callback、listener 或 AbortController。example 定时轮询快照绘制 Segment 时间线和 Chunk 状态。

## 生命周期

### ParallelStreamController

Controller 由 hls.js 创建和销毁。一个 Hls session 中可能多次 start、stop、seek 和 level switch。`stopLoad()` 只清理该 Controller 当前维护的窗口, 不销毁 Loader、Worker 或 Transport。

### StoryaFragmentLoader

每个实例属于一次 hls.js Fragment 请求。success、failure、timeout、abort 或 destroy 后取消订阅并释放 reader。它不拥有 Loader 或 Transport。

### ChunkFillWorker

Worker 由 Loader 构造并启动, 持续到 Loader destroy。它一次只持有一个 active Work。destroy 时取消 listener 和 active Work, 不单独销毁共享 Transport。

### ChunkFillWork

Work 由 Worker 在成功 claim Chunk 后创建。`run()` 完成、失败或取消后生命周期结束。Work 拥有本次 attempt 的 AbortController 和 timer, 但只借用 Loader 与 Transport, 不负责销毁它们。

### ParallelSegmentLoader

Loader 由应用创建和销毁, 一个实例只服务一个 Hls session。destroy 后不可复用。Loader 拥有传入的 Transport, destroy 时会一起销毁。

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

应用拥有 Hls 和 Loader; Hls 拥有 Controller 和每次请求的 FragmentLoader; Loader 拥有 state、Worker 和 Transport; Worker 拥有当前 Work。Controller、FragmentLoader、Worker 和 Work 只协作使用 Loader, 不负责销毁它。

## 公开接口

```ts
import { ParallelSegmentLoader, ParallelStreamController } from 'storya-hls-loader'

const loader = new ParallelSegmentLoader({
  chunkSize: 2 * 1024 * 1024,
  maxConcurrency: 6,
})

const hls = new Hls({
  fLoader: loader.fLoader,
  progressive: false,
  streamController: ParallelStreamController,
})

const diagnostics = loader.getDiagnostics()
```

公开运行时类只有 `ParallelSegmentLoader` 和 `ParallelStreamController`。包同时导出默认配置常量、Loader options 和只读诊断 TypeScript 类型。

## 当前实现范围

已经实现:

- main Segment 向前窗口。
- 多 Segment 和 Segment 内多 Chunk 并发。
- reader 优先级和后台预加载抢占。
- 已知 byte range、discovery Range、HEAD 长度发现和 sequential fallback。
- response head、完整请求和 body idle 三层 timeout。
- timeout attempt 补救。
- ETag/Last-Modified 一致性检查。
- canonical Segment 缓存、窗口重叠保留和确定性驱离。
- Fetch、HTTP Proxy 和 WebSocket Transport 注入。
- Stream/Segment/Chunk/Worker 诊断。

尚未实现:

- LL-HLS Part 向前窗口。
- alternate audio/subtitle 向前窗口。
- 窗口外后向缓存或基于内存预算的 LRU。
- 基于历史吞吐的慢连接识别。
- 真正 Web Worker 化。

## 修改历史

- 2026-08-08: 将单次 Transport attempt 从 `ChunkFillWorker` 拆为独立 `ChunkFillWork`; Worker 只负责观察、选择、claim、抢占和衔接下一次 Work, Work 负责 AbortController、流式读取、Range 校验、状态提交和 timeout rescue。
- 2026-08-08: Loader 收敛为共享数据、同步事务、粗粒度通知和资源生命周期对象; Controller、StoryaFragmentLoader 和 ChunkFillWorker 直接操作数据模型; 删除角色专用 Loader 代理、Worker host adapter、Promise waiter 和旧 fillId 命名, 统一使用 generation。
- 2026-08-08: 将 VirtualStream、Segment、Chunk 和 LoaderState 拆为独立 class 文件; 数据方法只维护所属层级的不变量, 删除 WindowDescriptor、SegmentDescriptor、Token 和 Claim 等中间领域概念。
- 2026-08-08: Worker 改为流式读取 Transport body, 实时记录 Chunk 已接收字节; 增加响应头、连续无数据和完整请求三层 timeout, 停滞 attempt 默认补救 2 次。
- 2026-08-08: 完成 Controller 窗口、fLoader reader、Chunk 并行、Transport 注入、诊断和驱离的首个可运行实现。
- 2026-08-07: 删除旧 VirtualStreamRegistry、StreamFiller、frontier 和相关诊断, 验证 hls.js 自定义 StreamController 与 fLoader 替换入口。
