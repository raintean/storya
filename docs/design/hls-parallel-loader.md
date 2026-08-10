# HLS 并行加载器设计

本文描述 `storya-hls-loader` 当前采用的 HLS Segment/Chunk 并行加载设计。实现基于 hls.js `1.7.0-rc.3`, 对外提供 main/audio 并行 Controller 和 `ParallelSegmentLoader`。

## 设计目标

- 保留 hls.js 原生的选片、ABR、解密、transmux、错误恢复和 SourceBuffer append 流程。
- 由 Controller 根据 hls.js 已经选择的 Fragment 规划有序 Segment 预加载窗口。
- 同时加载多个 Segment, 并在单个 Segment 内使用 Range Chunk 并行加载。
- 让后台预加载和 hls.js 正式读取共享唯一的 Segment/Chunk 状态与媒体数据。
- 窗口推进时保留重叠 Segment, 只驱离既不在窗口中、也没有正式读取者的 Segment。
- 使用一份可直接检查的共享数据模型协调 Controller、fLoader 和 Worker, 不建立细粒度命令或回调网络。
- 所有共享状态修改保持同步和原子, 异步 Transport 结果不能覆盖已经失效的新状态。
- 保留 Transport 注入能力, 支持 Fetch 和 HTTP-over-WebSocket。
- 将多个并行 GET 的实际传输投影成 hls.js ABR 可消费的单 Fragment 等效时序。

## 非目标

当前设计不负责:

- 改写 hls.js 的解密、transmux、append 或 SourceBuffer 状态机。
- 让多个 Segment 同时进入 hls.js 的解析和 append 流程。
- 根据播放器当前时间直接创建、推进或取消窗口。
- LL-HLS Part 的向前预加载窗口。Part 的正式 fLoader 请求仍然可以进入共享模型并按 Chunk 加载。
- subtitle 的向前预加载窗口。字幕正式 fLoader 请求仍然可以进入共享模型并按 Chunk 加载。
- 根据绝对带宽阈值判断慢连接。当前慢速识别只比较同一 Loader 中同期 GET 的相对速率。
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
  +-- planning
  +-- rangeMode
  +-- outcome
  +-- chunks: VirtualStreamChunk[]

VirtualStreamChunk
  +-- range
  +-- attempt
  +-- phase
```

`VirtualStream`、`VirtualStreamSegment` 和 `VirtualStreamChunk` 是带有局部不变量方法的数据模型。它们不持有 callback、Promise resolver、listener、AbortController、Transport 或 Worker。

### 主动参与者直接协作

系统有三类长期参与者和两种按请求创建的执行对象:

- `ParallelStreamController` 和 `ParallelAudioStreamController` 分别把 main/audio Fragment 选择转换为独立 `window`。
- `StoryaFragmentLoader` 把一次 hls.js fLoader 调用转换为 `readerCount` 生命周期, 并观察 Segment outcome。
- `SegmentLoadWorker` 观察状态, 领取 HEAD 或 GET 任务, 创建对应 Work, 状态变化时取消已经失效的 Work。
- `SegmentPlanningWork` 负责一次 HEAD 长度探测。
- `SegmentFetchWork` 负责一次 GET attempt, 包括 response head 处理、流式读取、Range 校验、提交、正常超时和 rescue 检测。

不建立 Registry、Filler、Scheduler、Owner、Reader、Writer 或 Worker host adapter。参与者可以同步读取共享状态, 修改则必须经过 `ParallelSegmentLoader.update()`。

### 数据方法只维护局部不变量

方法挂在最了解不变量的数据类上:

- `ParallelSegmentLoaderState` 负责确保 Stream、定位 Segment、分配 generation 和全局 reconcile。
- `VirtualStream` 负责确保 Segment、替换窗口、GET 顺序门禁和删除不再存活的 Segment。
- `VirtualStreamSegment` 负责 reader 计数、长度规划、Range 模式、Chunk 规划、顺序 fallback、失败和最终组装。
- `VirtualStreamChunk` 负责 claim、进度、完成、释放、补救和失败等 phase 转换。

这些方法不负责跨对象调度, 也不产生新的 service 层。两种 Work 做成 class, 是因为它们拥有 generation、AbortController、timer 和明确的单次执行生命周期。Worker 内的 candidate 和 Work options interface 只是同步传值对象, 没有独立身份或行为。

## 总体结构

```text
hls.js
  |
  +-- ParallelStreamController
  |     +-- update(state => replace window)
  |     +-- super.loadFragment(current)
  |
  +-- ParallelAudioStreamController
  |     +-- update(state => replace audio window)
  |     +-- super.loadFragment(current)
  |
  +-- StoryaFragmentLoader
        +-- update(state => readerCount + 1)
        +-- subscribe(global change)
        +-- 按顺序复制连续 filling/ready 数据并渐进回调 hls.js
        +-- Segment ready 后结束正式读取
        +-- update(state => readerCount - 1)

ParallelSegmentLoader
  +-- state
  +-- TransferTracker
  +-- RescueTracker
  +-- update()
  +-- subscribe()
  +-- revision/listeners
  +-- SegmentLoadWorker[]
  |     +-- activeWork: SegmentPlanningWork | SegmentFetchWork
  +-- HttpTransport
  +-- fLoader constructor
  +-- getDiagnostics()
```

网络下载可以并行和乱序完成, 但只有 hls.js 原生 `StreamController` 选中的当前 Fragment 会通过 fLoader 渐进提交或最终返回, 进入后续解析和 append。并行发生在数据准备层, 不改变媒体时间线消费顺序。

## ParallelSegmentLoader

`ParallelSegmentLoader` 是一个 Hls session 级对象。它是共享状态和资源的所有者, 但不是承载所有业务操作的 facade。

它负责:

- 创建 `ParallelSegmentLoaderState`。
- 创建固定数量的 `SegmentLoadWorker`。
- 持有所有 Worker 共用的 `HttpTransport`。
- 生成与当前实例绑定的 hls.js `fLoader` 构造器。
- 绑定唯一的 hls.js `HlsConfig`。
- 提供同步事务入口 `update()`。
- 提供粗粒度全局订阅 `subscribe()`。
- 生成只读诊断快照。
- 跟踪进行中和最近完成的 GET Work, 统计全局聚合传输带宽并提供同期相对速率。
- 统计当前 session 的实际救援事件、丢弃字节和后续恢复结果。
- 销毁 Worker、Transport 和全部共享状态。

Loader 不提供 `replaceWindow()`、`startReading()`、`inspectSegment()`、`takeNextChunk()` 或 `waitForChange()` 等角色专用代理方法。Controller、FragmentLoader、Worker 和 Work 在事务中直接调用数据模型的方法。

`ParallelSegmentLoader.fLoader` 通过内部 WeakMap 与 Loader 实例关联。两个并行 Controller 都从 `HlsConfig.fLoader` 找回同一个 Loader, 从而保证 main/audio 窗口和正式读取一定操作同一份状态。一个 Loader 只能绑定一个 Hls 实例。

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
  -> 标记通知 dirty
  -> 按 8ms 窗口合并安排 listener 通知
```

约束如下:

- mutation 不能返回 Promise, 不能跨越 `await`。
- `update()` 不能嵌套调用。
- mutation 内不调用 hls.js callback。
- 一次窗口替换中的 Segment 创建、window 替换和旧 Segment 驱离对观察者原子可见。
- transaction 返回后才能执行 Transport、hls.js callback 或其他外部代码。

当前实现运行在同一个 JavaScript realm。同步代码具有 run-to-completion 语义, 所以读取共享状态不需要 mutex。若未来迁移到真正的 Web Worker, 必须改为单一状态 authority 加消息协议, 不能继续直接共享 Map 和 class 实例。

### 通知规则

Loader 持有实例级全局 listener 集合。listener 不携带 Segment、Chunk、结果或 diff, 只表示“共享状态可能发生变化”。通知采用固定 8ms 的 leading + trailing 调度: 首次变化通过 microtask 尽快通知; 距上次通知不足 8ms 的后续变化只标记 dirty, 在窗口结束时最多补发一次通知。持续变化时全局 listener 最多约每秒唤醒 125 轮, 但每次 `update()` 仍然同步完成事务并单独增加 `revision`。

订阅者总是读取通知发生时的最新状态, 不依赖逐个观察中间 revision。若 trailing timer 在后台页面被浏览器延迟, 后续 `update()` 发现 8ms 窗口已经结束时会取消旧 timer 并通过 microtask 立即补发, 避免持续更新完全受 timer clamp 限制。前台页面正常调度时, 状态就绪、失效取消和空闲 Worker 领取新任务相对原实现最多增加约 8ms 通知延迟; 后台页面仍受浏览器 timer clamp 影响。当前 Worker 完成 Work 后直接调度自己的下一次领取, 不等待全局通知。

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

当前由 `ParallelStreamController` 创建 `main:<level>` 窗口, `ParallelAudioStreamController` 创建 `audio:<identity>` 窗口。subtitle、init Segment 和 Part 可以由正式 fLoader 创建没有窗口、但具有 reader 的 Segment。

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
- s7 新建并进入长度规划阶段。
- s1 没有 reader 时立即驱离。
- s1 仍有 reader 时保留到最后一个 reader 结束。

窗口重叠不触发重建, 播放经过窗口首个 Segment 也不代表整个窗口失效。

## ParallelStreamController 与 ParallelAudioStreamController

两个 Controller 分别继承 hls.js 默认 main `StreamController` 和 `AudioStreamController`, 覆写调度和 seek 生命周期入口:

- `loadFragment()` 在调用原生实现前更新窗口。
- `stopLoad()` 清除当前 Controller 持有的窗口, 再调用原生实现。
- `onMediaAttached()` 在 hls.js 原生 `seeking` listener 后注册补充 listener; 原生逻辑因 seek 取消当前 Fragment、令 `fragCurrent` 为空时, 重置对应轨道的 transmuxer。
- `onMediaDetaching()` 移除补充 listener, 再调用原生实现。

渐进读取被 seek 取消后, 同一 Fragment 随后可能从字节 0 重新读取。hls.js 原生 seek 路径只重置加载状态, 不重置 transmuxer; 若 sequence number 相同, 解析器可能把新 attempt 误判为旧 attempt 的连续输入。补充重置会清除 partial parser 状态, 并在启用 demux Worker 时切换 instance number, 从而丢弃旧 attempt 尚未返回的解析结果。原生逻辑保留当前 Fragment 请求时不会重置, 避免从 Segment 中部继续到达的数据失去前置解析状态。

普通 VOD/传统 HLS 流程中, 窗口包含当前 Fragment 和它后面的 Fragment。窗口长度由 `ParallelSegmentLoaderOptions.windowSize` 配置, 默认为 6, 因而默认包含当前 Fragment 和后续最多 5 个 Fragment。gap 或没有 URL 的 Fragment 不进入窗口。窗口顺序来自 Level details 中的 Fragment 顺序。

以下情况清空对应窗口并交回 hls.js 原生行为:

- Level/track details 尚不存在。
- low-latency mode 下存在 Part 列表。

main Controller 启动 bandwidth test 时也会清空 main 窗口。Audio Controller 没有这条 main 专用分支。

Level、rendition 或 audio track 切换时, 对应 Controller 在同一次事务中清空旧 Stream 窗口并建立新 Stream 窗口。旧 Stream 中仍有正式 reader 的 Segment 继续存活。

Controller 不创建预加载专用 fLoader, 不发送请求, 不读取 Segment outcome, 也不根据播放时间驱离 Segment。

窗口建立时只同步写入规划意图, 不发起网络请求:

- hls.js 已声明 byte range 的 Segment 立即得到长度和全部 Chunk 范围。
- 窗口首个未知长度 Segment 使用 `range` 规划, 让首个 GET 同时探测长度和下载数据。
- 其余未知长度 Segment 使用 `head` 规划, 先通过 HEAD 获得 `Content-Length`。

如果 hls.js 在后续 Segment 的 HEAD 尚未开始时正式读取它, `startReading()` 会把规划方法提升为 `range`。已经发出的 HEAD 不因 reader 出现而取消, 其结果仍可直接用于后续 GET。

## StoryaFragmentLoader

`ParallelSegmentLoader.fLoader` 是 hls.js 要求的可实例化构造器。每个 `StoryaFragmentLoader` 实例只执行一次 `load()`。

### load

`load()` 执行以下步骤:

1. 保存本次 hls.js context、callbacks、highWaterMark 和 timeout 配置。
2. 在一个事务中确保 Stream/Segment 存在并调用 `segment.startReading()`。
3. `readerCount + 1`; 如果 Segment 曾失败, 清除失败 outcome 和 failed Chunk, 保留已经 ready 的 Chunk。
4. 注册全局 listener。
5. 设置本次正式读取的完整加载 timeout。
6. 立即检查一次 Segment outcome。

立即检查保证窗口已经预加载完成的 Segment 可以同步进入成功路径, 不必等待下一次 revision。

### 观察与回调

listener 被触发后直接定位 Segment:

- `pending`: 更新 LoaderStats; progressive 开启时复制并提交从上次游标开始的最长连续 filling/ready 数据, 然后继续订阅。
- `ready`: progressive 开启时从 canonical ArrayBuffer 补交尚未提交的尾部; 结束 reader 生命周期后调用 `onSuccess`。
- `failed`: 保存 failure 和 Response, 结束 reader 生命周期, 再调用 `onError`。
- Loader 已销毁或 reader 对应的 Segment 不存在: 作为内部加载错误结束。

hls.js callback 始终在 `loader.update()` 之外调用, 避免 callback 重入共享事务。

progressive 由 hls.js 的 `progressive: true` 开启。当前 hls.js `BaseStreamController` 使用该配置决定是否把 progress callback 传给 `FragmentLoader`; `enableStreamingMode()` 检查的是通用 `config.loader` 而不是 `fLoader`, 因此配置自定义 fLoader 不会自动开启 progressive。开启后 hls.js 向 fLoader 提供 `onProgress` 和有限的 `highWaterMark`, fLoader 为每个 reader 独立维护已提交字节游标。

GET response head 的状态、Range 边界和可用 validator 校验通过后, Work 把每段 body 数据同步追加到 filling phase 的可增长 data buffer, `loadedBytes` 表示其中可读取的有效前缀。fLoader 可以跨 ready Chunk 和当前 filling Chunk 复制最长连续数据; 后续 Chunk 乱序先完成时必须等待前缀。连续新增数据达到 highWaterMark 后可以合并一次提交, Segment 最终 ready 时无条件刷新剩余尾部。`Content-Range` 不可见等无法在读取 body 前证明边界的响应不暴露 filling data, 仍等待完整校验成为 ready。

stall/slow rescue 仍把当前 filling phase 释放为 empty 并从相同 Chunk 起点完整重下。fLoader 的已提交字节游标不回退; 新 attempt 的 filling data 尚未追上游标时不会重复提交, 超过游标后只提交新后缀。因此不跨 attempt 保留共享 data, 也不改变现有 Range 请求边界。

所有 `onProgress` 数据都复制后再交给 hls.js, 避免 hls.js 转移或修改 ArrayBuffer 破坏 canonical 数据。Segment 完整组装后会释放 Chunk 中的重复 data, 所以最终刷新从 canonical outcome 复制尚未提交的后缀。progressive `onSuccess` 返回空 ArrayBuffer, 防止 hls.js 重复解析已经通过 `onProgress` 提交的数据; 未开启 progressive 时仍在 ready 后复制并一次性返回完整 canonical ArrayBuffer。

success 时必须先复制最终返回所需的数据, 再减少 readerCount。最后一个 reader 结束后, 窗口外 Segment 可能立即被 reconcile 驱离。

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

每个 FragmentLoader 持有 hls.js 要求的 `LoaderStats` 对象。Segment/Chunk 仍提供字节数、Chunk 数和重试数, 但完成后的加载时序必须适配并行预加载模型:

- Segment 内部网络计时从首个媒体 GET Chunk claim 开始; HEAD 规划和等待前缀门禁的时间不计入。
- `loaded`: 所有 Chunk 当前唯一有效字节数。
- `total`: 已知 Segment 长度。
- `chunkCount`: 非 progressive 模式下为 ready Chunk 数量; progressive 模式下由 hls.js 按实际 progress 回调累计。
- `retry`: hls.js 初始 retry 加 Worker rescue 次数。
- `bwEstimate`: Loader 当前的聚合传输带宽; hls.js 采样后会用自身 EWMA 结果覆盖该字段。

当 Segment 仍在加载或失败时, `loading.start` 是当前 fLoader `load()` 的调用时间, `loading.first` 不早于该时间, `loading.end` 是失败时间或 0。这些 stats 表达当前正式 reader 的实际等待, 供 hls.js 的超时和紧急降档逻辑使用。它与 Segment 内部的预加载网络时序是两个视角。

当 Segment ready 并交付给 hls.js 时, FragmentLoader 不能直接上报预加载的绝对开始时间。否则 Segment 在缓存中等待播放的时间会被 hls.js 误认为下载时间, 持续压低 ABR 带宽估计。FragmentLoader 因此在交付时构造一条等效单 Fragment 时序:

```text
等效传输时长 = Segment 字节数 / Loader 聚合带宽
loading.end      = 当前交付时间
loading.first    = loading.end - 等效传输时长
loading.start    = loading.first - 原始 TTFB
```

这条时序是专门供 hls.js ABR 采样的适配值, 不再表示某个 Segment 实际发起所有并行 Chunk 请求的墙钟时间。它保留 TTFB 语义, 排除缓存驻留时间, 并让 hls.js 在 `FRAG_BUFFERED` 时根据聚合链路能力而不是单个并行 Chunk 的带宽份额进行 EWMA 采样。

### 传输跟踪与聚合带宽

Loader 内部的 `TransferTracker` 以 generation 标识进行中的 GET Work。Work 从 claim 开始登记, response head 通过后标记 body 开始, 每次收到非空 body 数据时同步追加累计字节与时间, body 完成、取消或失败时结束登记。Tracker 同时保留最近 64 个实际收到媒体数据的完成样本; HEAD 不进入带宽样本。

聚合带宽计算为:

```text
聚合带宽 = 样本实际接收总字节数 / 所有 GET 活跃时间区间的并集长度
```

并行 GET 的重叠时间只计一次, 但各请求接收的字节都计入总量; 两组请求之间没有 GET 活动的空闲间隙不计入分母。这使结果表达整个 Loader 在并行传输时实际获得的链路吞吐, 而不是某个 Chunk 的个别速率。

Work 因 slow rescue、timeout 或取消而中止时, 已经接收的字节仍消耗了真实链路带宽, 所以进入聚合样本; 这些字节不会计入 Segment `loaded` 的唯一有效字节数。完整 GET body 之后为 CORS `Content-Range` 降级而追加的 HEAD 不计入 GET 传输时间。

Tracker 还为尚未完成的 GET 提供最近 `rescue.stallTimeoutMs` 时间窗口的单请求速率。同期 peer 包括仍在读取 body 的其他 GET, 以及刚完成且传输时间与当前观察窗口重叠的 GET; 较快请求即使已经完成, 仍可作为剩余慢请求的比较基线。peer 必须已经收到 body 数据, 至少存在 2 个有效 peer 时才产生中位速率。

## SegmentLoadWorker

`ParallelSegmentLoader` 构造时创建固定数量的 `SegmentLoadWorker`。默认全局并发为 6。它们是同一 JavaScript realm 中的逻辑 Worker, 不是浏览器 Web Worker。

每个 Worker 持续订阅 Loader 的全局通知。空闲时, 通知只安排一次合并的调度 microtask:

```text
state changed
  -> schedule once
  -> scan best HEAD/GET candidate
  -> update(state => claim planning 或 Chunk with generation)
  -> create SegmentPlanningWork 或 SegmentFetchWork
  -> work.run()
```

每个 Work 独占一个 Worker 槽位。HEAD 和 GET 使用同一个固定 Worker 池, 因此所有网络请求都受 `maxConcurrency` 控制。没有可领取任务时, Worker 保持 idle, 不创建 Promise waiter。下一次全局通知会重新安排扫描。

Worker 根据 Segment 当前 planning phase 生产 Work:

```text
pending(head)       -> SegmentPlanningWork  -> HEAD
pending(range)      -> SegmentFetchWork     -> 首个 Range GET
pending(sequential) -> SegmentFetchWork     -> 完整 GET
planned + empty     -> SegmentFetchWork     -> 普通 Range GET
probing             -> 已有 Work, 不重复领取
```

`SegmentPlanningWork` 只读取 response head 和 `Content-Length`, 不创建 Chunk。HEAD 失败或缺少有效长度不会直接使 Segment failed, 而是把规划方法降级为 `range`, 等待首个 GET 探测。

### 调度优先级

候选任务按以下顺序比较:

1. `readerCount > 0` 的正式读取 Segment 优先。
2. windowIndex 更小的 Segment 优先。
3. Segment 内 index 更小的 Chunk 优先; planning task 等价于 index 0。
4. ChunkKey 或 SegmentKey 字典序作为稳定 tie-breaker。

所有 Worker 扫描同一份状态。claim 必须在同步事务中把 planning 从 `pending` 改为 `probing`, 或把 Chunk 从 `empty` 改为 `filling`, 因而同一 realm 内不会有两个 Worker 成功领取同一个 generation。

HEAD 可以乱序执行和完成, 但数据 GET 受窗口前缀门禁约束。某个 Segment 前面只要还有 `outcome=pending` 且 `rangeMode=unverified` 的 Segment, 它就不能领取 GET。这样后续 Segment 可以提前知道长度, 但不能因为 HEAD 较早返回而占满数据下载槽位。

每个 Segment 的 `rangeMode=unverified` 时只允许领取第 0 个 Chunk。首个 `206` 的 Range 和边界验证通过后切换为 `parallel`, 其余 Chunk 才能并行领取; Origin 忽略 Range 时切换为 `sequential`。

### 已发出请求不抢占

正在加载的 Worker 收到全局通知后只检查:

- 当前 Segment、planning 或 Chunk 是否仍存在。
- 当前 generation 是否仍匹配。

优先级只决定空闲 Worker 下一次领取哪个 Chunk。只要 active Work 仍属于存活 Segment 且 generation 有效, 新出现的正式 reader Chunk 或更高优先级候选都不会抢占它, 即使 response body 尚未产生数据。请求一旦发出, 数据可能已经在网络路径中, 因优先级变化取消会浪费请求和已传输数据。

窗口驱离、Stream 切换、失败传播或 Loader/Worker 销毁仍会使 Work 失效并取消请求。body 停滞或相对速率过慢触发的主动替换只属于 rescue, 不属于调度优先级抢占。

## Work 与 generation

Worker claim planning 或 Chunk 时从全局状态分配单调递增的 generation, 然后创建一个 `SegmentPlanningWork` 或 `SegmentFetchWork`。一个 Work 只代表一个 generation 和一次 Transport request, 不能重复执行。

Work 固定保存:

- streamId、segmentKey, GET Work 还保存 chunkKey
- generation
- FragmentLoaderContext 快照引用
- GET 的 request range 和已知资源长度

Work 拥有本次请求的 AbortController 和计时器。`SegmentFetchWork` 还持有 body 分片数组。共享 planning probing phase 与 Chunk filling phase 只保存 generation、workerId、startedAt 等可观察数据, 不保存回调或请求对象。

每次进度、完成、释放、补救或失败提交前, Work 都重新根据 key 定位 Chunk, 并验证 generation:

```text
Segment/planning/Chunk 不存在
  -> 丢弃迟到结果

generation 不匹配
  -> 丢弃迟到结果

generation 匹配
  -> 在同步事务中提交
```

窗口驱离、失败传播或重新 claim 都会使旧 generation 失效。网络请求即使稍后返回, 也不能覆盖新 attempt 或重新创建已驱离 Segment。

## Segment 与 Chunk 状态

Segment 同时保存三条互不混用的状态轴:

- `readerCount/windowIndex` 表达用途: 正式读取、预加载窗口或不活跃。
- `planning/rangeMode` 表达长度和请求方式是否已经确定。
- `outcome` 表达完整 Segment 最终是否可交付。

用途不是加载状态。一个 Segment 可以同时是 `READER`、处于 `filling`, 也可以同时在 window 中且已经 `ready`。

### Planning phase 与 Range mode

planning phase 为:

```text
pending { method, lastFailure }
probing { method, generation, workerId, startedAt }
planned { source }
```

`method` 为 `head`、`range` 或 `sequential`; `source` 为 `playlist`、`head`、`content-range` 或 `response`。planning 只描述 Segment 长度和 Chunk 边界的规划过程, 不表示最终加载成功。

Range mode 为:

```text
unverified -> 尚未验证 Origin 是否遵守 Range
parallel   -> 首个 Range 已通过状态、边界和长度校验
sequential -> 使用一个不带 Range 的完整 GET
```

### Segment outcome

Segment 持有显式 outcome:

```text
pending
ready  { data, response, code, url, completedAt }
failed { failure, completedAt }
```

诊断展示的生命周期 state 从 outcome、planning、rangeMode 和 Chunk phase 推导:

- ready outcome -> `ready`
- failed outcome -> `failed`
- planning 尚未完成 -> `planning`
- planning 已完成、首个 Range 正在请求且尚未验证 -> `verifying`
- 已有 filling Chunk -> `filling`
- 已规划但没有正在执行的 GET -> `queued`

Segment 还保存 URL/range context、媒体起止时间、长度、validator、readerCount 和统计时间。

### Chunk phase

Chunk phase 为:

```text
empty   { lastFailure }
filling { generation, workerId, startedAt, loadedBytes, data }
ready   { byteLength, data, response, url, firstByteAt, completedAt }
failed  { failure }
```

`attempt` 记录总 claim 次数, `rescueAttempts` 记录停滞或相对慢速触发的补救次数。filling data 使用按需扩容的 Uint8Array, 只有 `[0, loadedBytes)` 是当前 attempt 已发布的有效前缀。完整 body 验证后 data 收敛为精确长度并进入 ready phase; rescue、普通取消或失败会随 phase 转换释放当前 attempt 的 filling data。

## 长度发现、Chunk 规划与 Range 行为

默认 Chunk 大小为 2 MiB。尾部不足半个 Chunk 时合并到前一个 Chunk, 避免极小请求。

### m3u8 已知 HLS byte range

如果 fLoader context 声明 `[rangeStart, rangeEnd)`, Segment 长度立即确定。Chunk 使用 Segment 本地偏移保存, 发送请求时再加上 `resourceStart`:

```text
resource range: [10 MiB, 15 MiB)

chunk 0 local [0, 2 MiB) -> request [10 MiB, 12 MiB)
chunk 1 local [2, 4 MiB) -> request [12 MiB, 14 MiB)
chunk 2 local [4, 5 MiB) -> request [14 MiB, 15 MiB)
```

Origin 对声明 byte range 返回 `200` 时 Segment 失败, 因为不能把整个资源误当成子范围。

### 窗口首个未知长度 Segment

窗口首个 Segment 或 hls.js 正在正式读取的未知长度 Segment 使用首个 Range GET 同时探测和下载:

```text
Range: bytes=0-2097151
```

处理规则:

- 返回 `206` 且 `Content-Range` 可见: 收到 response head 时立即校验起止位置和总长度, 完成 planning, 切换为 `parallel`, 让其他 Worker 在首块 body 仍在传输时领取剩余 Chunk。
- 返回 `200`: Origin 忽略 Range。普通完整 Segment 直接复用本次完整响应并切换为 `sequential`, 不重复 GET; m3u8 声明 byte range 时失败。
- 返回 `206` 但浏览器因 CORS 看不到 `Content-Range`: 不能根据“body 比请求短”推断 EOF。必须使用已有长度或追加一次 HEAD 取得 `Content-Length`, 并验证 body 字节数恰好等于请求范围。
- `Content-Range` 不可见且仍无法安全得到长度: 丢弃这次 Range 数据, 回退为不带 Range 的 sequential GET。

首个 GET 也是正式媒体数据请求, 不存在单独的 discovery Chunk 类型。

### 窗口后续未知长度 Segment

后续 Segment 先由 `SegmentPlanningWork` 发送 HEAD。有效 `Content-Length` 会同步规划完整 Chunk 数组, 但 `rangeMode` 仍为 `unverified`; 第一个 GET 校验 Range 后才能打开其余 Chunk。

HEAD 结果允许乱序返回。后续 Segment 即使已经 planned, 只要前序 Segment 的 Range 模式尚未确定, 仍保持 `queued`, 不发起 GET。HEAD 失败只把方法切换为 `range`, 不立即产生 Segment failure。

### CORS 可见性要求

Fetch 路径的最佳性能要求 Origin 通过 CORS 向浏览器暴露 `Content-Range`; WebSocket Relay 不受浏览器 CORS response header 可见性限制。`206` 本身只能证明请求得到部分响应, 不能提供完整资源长度。m3u8 byte range 和 HEAD `Content-Length` 都可以作为独立可信长度来源, 但实际 GET 仍需校验状态、边界和接收字节数。

不同 Range response 在可用时比较 ETag 或 Last-Modified。validator 变化会使整个 Segment 失败, 避免拼接不同资源版本。

### Segment 组装

所有 Chunk ready 后按本地 start 顺序组装唯一 canonical ArrayBuffer。组装要求:

- Chunk 从 0 开始连续覆盖。
- 最终 cursor 等于 Segment length。
- 每个 Chunk 都存在完成数据和确定终点。

组装成功后, Chunk 保留 byteLength、Response、状态和诊断字段, 但释放各自的 Uint8Array, 避免长期同时持有完整 Segment 和全部 Chunk 两份媒体数据。

## Transport 与网络超时

`ParallelSegmentLoaderOptions.transport` 可以传入任意 `HttpTransport`; 未传入时创建 `FetchHttpTransport`。Loader 持有 Transport 生命周期, 所有 Worker 共享同一个实例。

Transport 只处理标准 `Request`、response head 和流式 body, 不理解 Stream、Segment、Chunk、窗口或优先级。

`SegmentPlanningWork` 负责:

- 根据 hls.js context 构造 HEAD Request。
- 调用 hls.js `fetchSetup`。
- 校验 HTTP 状态和 `Content-Length`。
- 提交 planning 结果或降级为 Range 规划。

`SegmentFetchWork` 负责:

- 根据 hls.js context 和 range 构造 Request。
- 调用 hls.js `fetchSetup`。
- 使用 Transport 发出 GET。
- 逐段读取 ReadableStream。
- 通过 Request AbortSignal 和 body cancel 表达取消。
- 将 response 转换为 Segment/Chunk 状态。

HEAD 和 GET 都占用 Worker 槽位并使用两类正常请求时限; GET 另有两类 rescue 检测:

- 响应头超时: `fragLoadPolicy.maxTimeToFirstByteMs`。
- 完整请求超时: `fragLoadPolicy.maxLoadTimeMs`。
- body 连续无数据的停滞检测: `rescue.stallTimeoutMs`, 默认 4 秒。
- 相对于同期 GET 中位速率的慢速检测: `rescue.slowRateThresholdRatio`, 默认 0.25。

GET body 每产生一段数据, Work 同步更新 TransferTracker。响应头已经证明状态、Range 边界和 validator 时, 数据同时追加到共享 Chunk 的 filling data 并可供正式 fLoader 渐进读取; 否则继续保存在 Work 局部直到完整验证。完整 body 验证通过后提交精确的 ready data。rescue 会丢弃当前 attempt 的 filling data 或局部 parts, 下一次从相同 Chunk 起点完整重下, 不跨 attempt 复用部分数据。

`rescue.maxAttempts > 0` 时每个 attempt 都安装停滞检测。当前 Chunk 的 `rescueAttempts < rescue.maxAttempts` 时还会安装相对慢速检测, 并允许停滞或慢速触发重新领取; 次数耗尽后不再判断相对慢速, 但停滞会快速终止 Segment, 避免单个 Chunk 等待正常完整请求时限。

## 错误、补救与重试

普通网络错误、HTTP 错误、Range 校验错误或资源 validator 变化会使 Segment 进入 failed outcome。Segment failure 会让同一 Segment 仍在 filling 的其他 Chunk 一起失败, 对应 Worker 收到通知后取消失效 Work。

rescue 是正常请求时限之外的额外补救。停滞是零吞吐的慢速极限状态, 两种信号共用同一次数和状态转换。默认允许补救 2 次, 即最多创建 3 个网络 attempt:

```text
body 连续无数据达到 stallTimeoutMs
或者
当前窗口速率低于同期 GET 中位速率的 slowRateThresholdRatio
且预计继续完成时间大于重新请求时间
  -> rescueAttempts < rescue.maxAttempts
       -> generation 释放为 empty
       -> retryCount + 1
       -> 当前 Work 结束
       -> Worker 重新执行全局调度
  -> 已达上限
       -> 不再安装相对慢速检测
       -> stall 检测仍然保留
       -> 再次停滞时取消 Work 并快速失败 Segment
```

相对慢速检测使用 `rescue.stallTimeoutMs` 作为滚动观察窗口。当前 GET 必须已经观察满一个窗口并收到过数据, 同期至少存在 2 个有效 peer。系统比较 `currentRate / peerMedianRate` 与配置阈值, 再计算:

```text
continueEta = remainingBytes / currentRate
retryEta    = expectedResponseBytes / peerMedianRate + peerMedianTtfb
```

只有当前速率低于阈值且 `continueEta > retryEta` 时才取消。判断没有绝对带宽下限; 所有同期请求一起变慢时不会把其中某一个误判为离群慢请求。`rescue.slowRateThresholdRatio = 0` 只关闭非零吞吐的相对慢速检测, 停滞检测仍然存在。

例如 `rescue.maxAttempts = 2` 时, 前两个 Work 都可以触发 rescue。第三个 Work 不再因相对慢速触发重试; 如果 body 连续无数据达到 `stallTimeoutMs`, 则以救援耗尽错误结束 Segment, 交给 hls.js 原生恢复流程处理。诊断中的 `exhaustedStallCount` 记录这种情况。

`rescue: false` 与 `rescue: { maxAttempts: 0 }` 都会从第一个 Work 起关闭全部 rescue 检测。

救援检测发生时, `SegmentFetchWork` 先在局部保存原因、速率比较、ETA、已接收字节和触发时间。只有取消完成后仍定位到相同 generation, 且 `chunk.rescue()` 成功把 Chunk 释放为 empty 时, Loader 的 `RescueTracker` 才记录事件。因此失效 generation、普通取消和正常 timeout 不会产生虚假的救援统计。

`RescueTracker` 为当前 Loader session 维护累计计数, 并只保留最近 64 个事件详情。事件初始 outcome 为 `pending`; 同一个 `VirtualStreamChunk` 后续完整校验并提交为 ready 时, 该 Chunk 关联的全部 pending 救援事件改为 `recovered`。累计统计包括总事件数、stall/slow 分类、已恢复数、未恢复数、救援耗尽后的停滞数和被取消 attempt 已接收但未提交的字节数。Tracker 使用 Chunk 对象的弱引用关联恢复结果, 不延长已驱离 Chunk 的生命周期。

响应头超时和完整请求超时属于正常 attempt failure, 不消耗 rescue 次数。外部窗口变化或 Worker destroy 使用普通取消, 只释放仍然有效的 generation。

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
- Loader 最近 GET Work 的聚合传输带宽。
- 当前 session 的救援累计统计和最近 64 个救援事件, 包括原因、Chunk identity、generation、attempt、触发耗时、丢弃字节、恢复结果和可用的 peer/ETA 判断数据。
- 每个 Worker 的 idle/loading/stopped、当前 HEAD/GET、任务类型、Stream/Segment/Chunk、Range 和 startedAt。
- 每条 VirtualStream 的 window。
- Segment 的 start、duration、windowIndex、readerCount、生命周期 state、planning phase/method/source、rangeMode、HTTP status 和字节数。
- Chunk 的范围、state、实时 loadedBytes、generation、attempt 和 failure。

诊断投影不修改 revision, 不参与调度, 也不暴露 ArrayBuffer、Uint8Array、Response、callback、listener 或 AbortController。example 定时轮询快照绘制 Segment 时间线和 Chunk 状态, 展示救援累计值并把新救援及其恢复写入事件日志。

## 生命周期

### ParallelStreamController 与 ParallelAudioStreamController

Controller 由 hls.js 创建和销毁。一个 Hls session 中可能多次 start、stop、seek、level switch 和 audio track switch。每个 Controller 的 `stopLoad()` 只清理自己维护的窗口, 不影响另一条轨道, 也不销毁 Loader、Worker 或 Transport。

### StoryaFragmentLoader

每个实例属于一次 hls.js Fragment 请求。success、failure、timeout、abort 或 destroy 后取消订阅并释放 reader。它不拥有 Loader 或 Transport。

### SegmentLoadWorker

Worker 由 Loader 构造并启动, 持续到 Loader destroy。它一次只持有一个 active Work。destroy 时取消 listener 和 active Work, 不单独销毁共享 Transport。

### SegmentPlanningWork 与 SegmentFetchWork

Work 由 Worker 在成功 claim planning 或 Chunk 后创建。`run()` 完成、失败或取消后生命周期结束。Work 拥有本次请求的 AbortController 和 timer, 但只借用 Loader 与 Transport, 不负责销毁它们。

### ParallelSegmentLoader

Loader 由应用创建和销毁, 一个实例只服务一个 Hls session。destroy 后不可复用。Loader 拥有传入的 Transport, destroy 时会一起销毁。

推荐顺序:

```ts
const loader = new ParallelSegmentLoader()
const hls = new Hls({
  audioStreamController: ParallelAudioStreamController,
  fLoader: loader.fLoader,
  progressive: true,
  streamController: ParallelStreamController,
})

// teardown
hls.destroy()
loader.destroy()
```

应用拥有 Hls 和 Loader; Hls 拥有 Controller 和每次请求的 FragmentLoader; Loader 拥有 state、Worker 和 Transport; Worker 拥有当前 Work。Controller、FragmentLoader、Worker 和 Work 只协作使用 Loader, 不负责销毁它。

## 公开接口

```ts
import {
  ParallelAudioStreamController,
  ParallelSegmentLoader,
  ParallelStreamController,
} from 'storya-hls-loader'

const loader = new ParallelSegmentLoader({
  chunkSize: 2 * 1024 * 1024,
  maxConcurrency: 6,
  rescue: {
    maxAttempts: 2,
    slowRateThresholdRatio: 0.25,
    stallTimeoutMs: 4_000,
  },
  windowSize: 6,
})

const hls = new Hls({
  audioStreamController: ParallelAudioStreamController,
  fLoader: loader.fLoader,
  progressive: true,
  streamController: ParallelStreamController,
})

const diagnostics = loader.getDiagnostics()
```

公开运行时类为 `ParallelSegmentLoader`、`ParallelStreamController` 和 `ParallelAudioStreamController`。包同时导出默认配置常量、Loader options 和只读诊断 TypeScript 类型。`ParallelSegmentLoaderOptions` 支持 `chunkSize`、`maxConcurrency`、`windowSize`、`rescue` 和 `transport`; `rescue` 接受 `false` 或包含 `maxAttempts`、`stallTimeoutMs`、`slowRateThresholdRatio` 的对象。

## 当前实现范围

已经实现:

- main 和 alternate audio Segment 向前窗口。
- 多 Segment 和 Segment 内多 Chunk 并发。
- reader、windowIndex 和 Chunk index 调度优先级; 已发出的有效请求不因优先级变化被抢占。
- m3u8 已知 byte range、HEAD 长度发现、首个 Range GET 规划和 sequential fallback。
- HEAD 乱序完成、后续 Segment GET 前缀门禁和每个 Segment 的 Range 验证门禁。
- `Content-Range` CORS 隐藏时基于已知长度与精确 body 长度的安全恢复。
- response head 和完整请求 timeout。
- 可配置或关闭的停滞与同期相对慢速 rescue。
- ETag/Last-Modified 一致性检查。
- canonical Segment 缓存、窗口重叠保留和确定性驱离。
- hls.js 正式 reader 对连续 filling/ready 数据的有序渐进提交。
- Fetch 和 WebSocket Transport 注入。
- 基于并行 GET 活跃区间的 Loader 聚合带宽估计, 以及排除缓存驻留时间的 hls.js LoaderStats 适配。
- Stream/Segment/Chunk/Worker 诊断。

尚未实现:

- LL-HLS Part 向前窗口。
- subtitle 向前窗口。
- 窗口外后向缓存或基于内存预算的 LRU。
- 真正 Web Worker 化。

## 修改历史

- 2026-08-10: 删除 HTTP Proxy Transport 注入路径, 保留 Browser Fetch 和 HTTP-over-WebSocket 两种网络实现。
- 2026-08-09: main/audio Controller 在 seek 取消渐进 Fragment 后重置对应 transmuxer, 隔离同一 Fragment 重试的旧 partial parser 状态和延迟 Worker 结果。
- 2026-08-09: fLoader 支持按 highWaterMark 有序提交已经通过响应头校验的连续 filling/ready 数据; rescue 重试依靠 reader 游标跳过已提交前缀, 最终从 canonical Segment 补齐尾部并以空 payload 完成; example 在并行模式下显式开启 hls.js progressive。
- 2026-08-09: 全局 listener 通知改为固定 8ms 的 leading + trailing 合并调度, 保持统一 `update()` 和逐事务 revision, 限制高频 body 进度更新引起的 Worker 与 FragmentLoader 唤醒。
- 2026-08-09: 默认救援次数从 1 调整为 2; 次数耗尽后继续检测 stall 并快速失败 Segment, 增加 `exhaustedStallCount` 诊断; 明确 rescue 不跨 attempt 复用部分数据。
- 2026-08-09: 将 `rescue.stallTimeoutMs` 默认值从 2 秒调整为 4 秒, 同时延长相对慢速检测的默认滚动观察窗口。
- 2026-08-09: 将 `idleTimeoutMs` 和 `maxRescueAttempts` 收敛为可设为 `false` 的 `rescue` 配置, 停滞阈值更名为 `stallTimeoutMs` 并默认改为 2 秒; TransferTracker 增加同期 GET 中位速率, 当前请求低于默认 25% 且取消重试预计更早完成时复用原有 rescue 流程。
- 2026-08-09: 增加 session 级 RescueTracker, 统计实际进入重试的 stall/slow 救援、丢弃字节、恢复结果和最近 64 个事件, 并通过 diagnostics 暴露给 example。
- 2026-08-08: 带宽估计改为统计最近并行 GET Work 的总字节数与活跃时间并集; HEAD、前缀等待和缓存驻留时间不再压低 hls.js ABR 采样, 诊断界面同时显示 Loader 聚合带宽和 hls.js EWMA 带宽。
- 2026-08-08: 增加 `ParallelAudioStreamController`, 为 alternate audio 维护独立预加载窗口; 音频 reader 结束后由 window 保持 Segment 和 VirtualStream 存活, 音轨切换和 stopLoad 时清理对应窗口。
- 2026-08-08: 未知长度 Segment 改为“窗口首段首个 Range GET、后续 Segment HEAD”的规划流程; HEAD 与 GET 共用固定 Worker 池, 后续数据受前序 Range 模式门禁; 删除根据短 body 推断 EOF 的行为。
- 2026-08-08: Segment 诊断拆分用途、planning phase、rangeMode、outcome 和组合生命周期状态; Worker/Work 重命名为 `SegmentLoadWorker`、`SegmentPlanningWork` 和 `SegmentFetchWork`。
- 2026-08-08: 调度优先级改为只影响空闲 Worker 的下一次领取; 已发出的有效请求不再因新出现的 Reader Chunk 被抢占, 只在窗口失效、失败、销毁或 slow rescue 时取消。
- 2026-08-08: rescue 次数只控制 Work 是否安装 body idle 慢速检测; 次数耗尽和 `maxRescueAttempts = 0` 统一表现为不安装检测, 不再因为 rescue 用尽而使 Segment 失败; 正常请求 timeout 与 rescue 分离。
- 2026-08-08: 增加实例级 `windowSize` 配置, 默认值保持 6; `ParallelStreamController` 从配对的 Loader 读取窗口长度。
- 2026-08-08: 将单次 Transport attempt 从 `SegmentLoadWorker` 拆为独立 `SegmentFetchWork`; Worker 只负责观察、选择、claim、失效取消和衔接下一次 Work, Work 负责 AbortController、流式读取、Range 校验、状态提交和 timeout rescue。
- 2026-08-08: Loader 收敛为共享数据、同步事务、粗粒度通知和资源生命周期对象; Controller、StoryaFragmentLoader 和 SegmentLoadWorker 直接操作数据模型; 删除角色专用 Loader 代理、Worker host adapter、Promise waiter 和旧 fillId 命名, 统一使用 generation。
- 2026-08-08: 将 VirtualStream、Segment、Chunk 和 LoaderState 拆为独立 class 文件; 数据方法只维护所属层级的不变量, 删除 WindowDescriptor、SegmentDescriptor、Token 和 Claim 等中间领域概念。
- 2026-08-08: Worker 改为流式读取 Transport body, 实时记录 Chunk 已接收字节; 增加响应头、连续无数据和完整请求三层 timeout, 停滞 attempt 默认补救 1 次。
- 2026-08-08: 完成 Controller 窗口、fLoader reader、Chunk 并行、Transport 注入、诊断和驱离的首个可运行实现。
- 2026-08-07: 删除旧 VirtualStreamRegistry、StreamFiller、frontier 和相关诊断, 验证 hls.js 自定义 StreamController 与 fLoader 替换入口。
