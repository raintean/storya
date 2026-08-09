import Hls from 'hls.js'
import type {
  Fragment,
  FragmentLoaderContext,
  HlsConfig,
  Loader as HlsLoader,
  LoaderConfiguration,
  LoaderResponse,
} from 'hls.js'
import { ParallelSegmentLoader } from './parallel-segment-loader.ts'

const originalFetch = globalThis.fetch
const resources = new Map([
  ['https://example.com/segment-1.ts', new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])],
  ['https://example.com/segment-2.ts', new Uint8Array([10, 11, 12, 13, 14, 15])],
  ['https://example.com/segment-3.ts', new Uint8Array([20, 21, 22, 23, 24])],
  ['https://example.com/segment-4.ts', new Uint8Array([30, 31, 32, 33, 34])],
  ['https://example.com/segment-5.ts', new Uint8Array([40, 41, 42, 43, 44, 45, 46, 47, 48, 49])],
  ['https://example.com/segment-6.ts', new Uint8Array([50, 51, 52, 53, 54, 55, 56, 57])],
  ['https://example.com/segment-7.ts', new Uint8Array([60, 61, 62, 63, 64, 65])],
  [
    'https://example.com/segment-8.ts',
    new Uint8Array([70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81]),
  ],
  ['https://example.com/segment-9.ts', new Uint8Array([80, 81, 82, 83])],
  ['https://example.com/segment-10.ts', new Uint8Array([90, 91, 92, 93, 94, 95, 96, 97])],
  ['https://example.com/segment-11.ts', new Uint8Array([100, 101, 102, 103, 104, 105])],
  ['https://example.com/segment-12.ts', new Uint8Array([110, 111, 112, 113, 114])],
  [
    'https://example.com/segment-13.ts',
    new Uint8Array([
      120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135,
    ]),
  ],
  ['https://example.com/segment-14.ts', new Uint8Array([140, 141, 142, 143])],
])
const fetchCounts = new Map<string, number>()
const requestMethods = new Map<string, string[]>()
const requestRanges = new Map<string, (string | null)[]>()
let activeFetches = 0
let maxActiveFetches = 0
let prefetchBodyCanceled = false
let slowBodyCanceled = false
let stalledBodyCanceled = false

globalThis.fetch = async input => {
  const request = input instanceof Request ? input : new Request(input)
  const payload = resources.get(request.url)
  if (payload === undefined) {
    return new Response(null, { status: 404 })
  }

  fetchCounts.set(request.url, (fetchCounts.get(request.url) ?? 0) + 1)
  requestMethods.set(request.url, [...(requestMethods.get(request.url) ?? []), request.method])
  if (request.method === 'GET') {
    requestRanges.set(request.url, [
      ...(requestRanges.get(request.url) ?? []),
      request.headers.get('range'),
    ])
  }
  activeFetches += 1
  maxActiveFetches = Math.max(maxActiveFetches, activeFetches)
  try {
    await abortableDelay(15, request.signal)
    if (request.method === 'HEAD') {
      return new Response(null, {
        headers: { 'content-length': String(payload.byteLength) },
        status: 200,
      })
    }
    if (request.url.endsWith('segment-10.ts')) {
      await abortableDelay(60, request.signal)
    }
    if (request.url.endsWith('segment-3.ts') || request.url.endsWith('segment-4.ts')) {
      return new Response(payload.slice().buffer, {
        headers: { age: '3', etag: '"stable"' },
        status: 200,
      })
    }
    const range = parseRange(request.headers.get('range'), payload.byteLength)
    if (request.url.endsWith('segment-12.ts') && range.start === 0) {
      let timer: ReturnType<typeof globalThis.setTimeout> | undefined
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          if (timer !== undefined) {
            globalThis.clearTimeout(timer)
          }
        },
        start(controller) {
          timer = globalThis.setTimeout(() => {
            controller.enqueue(payload.slice(range.start, range.endExclusive))
            controller.close()
          }, 80)
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (request.url.endsWith('segment-9.ts')) {
      let timer: ReturnType<typeof globalThis.setTimeout> | undefined
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          prefetchBodyCanceled = true
          if (timer !== undefined) {
            globalThis.clearTimeout(timer)
          }
        },
        start(controller) {
          timer = globalThis.setTimeout(() => {
            controller.enqueue(payload.slice(range.start, range.endExclusive))
            controller.close()
          }, 80)
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (
      request.url.endsWith('segment-6.ts') &&
      range.start === 4 &&
      rangeFetchCount(request.url, request.headers.get('range')) <= 2
    ) {
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          stalledBodyCanceled = true
        },
        start(controller) {
          controller.enqueue(payload.slice(range.start, range.start + 2))
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (
      (request.url.endsWith('segment-6.ts') &&
        range.start === 4 &&
        rangeFetchCount(request.url, request.headers.get('range')) === 3) ||
      (request.url.endsWith('segment-7.ts') && range.start === 0)
    ) {
      let timer: ReturnType<typeof globalThis.setTimeout> | undefined
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          if (timer !== undefined) {
            globalThis.clearTimeout(timer)
          }
        },
        start(controller) {
          const middle = Math.min(range.start + 2, range.endExclusive)
          controller.enqueue(payload.slice(range.start, middle))
          timer = globalThis.setTimeout(
            () => {
              controller.enqueue(payload.slice(middle, range.endExclusive))
              controller.close()
            },
            request.url.endsWith('segment-6.ts') ? 20 : 80,
          )
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (request.url.endsWith('segment-13.ts') && (range.start === 4 || range.start === 8)) {
      let timer: ReturnType<typeof globalThis.setTimeout> | undefined
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          if (timer !== undefined) {
            globalThis.clearTimeout(timer)
          }
        },
        start(controller) {
          controller.enqueue(payload.slice(range.start, range.start + 1))
          timer = globalThis.setTimeout(() => {
            controller.enqueue(payload.slice(range.start + 1, range.endExclusive))
            controller.close()
          }, 10)
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (
      request.url.endsWith('segment-13.ts') &&
      range.start === 12 &&
      rangeFetchCount(request.url, request.headers.get('range')) === 1
    ) {
      let timer: ReturnType<typeof globalThis.setTimeout> | undefined
      const body = new ReadableStream<Uint8Array>({
        cancel() {
          slowBodyCanceled = true
          if (timer !== undefined) {
            globalThis.clearTimeout(timer)
          }
        },
        start(controller) {
          controller.enqueue(payload.slice(range.start, range.start + 1))
          timer = globalThis.setTimeout(() => {
            controller.enqueue(payload.slice(range.start + 1, range.start + 2))
          }, 25)
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    if (request.url.endsWith('segment-14.ts')) {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(
            payload.slice(range.start, Math.min(range.start + 2, range.endExclusive)),
          )
        },
      })
      return new Response(body, {
        headers: {
          'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
          etag: '"stable"',
        },
        status: 206,
      })
    }
    return new Response(payload.slice(range.start, range.endExclusive).buffer, {
      headers: {
        age: '3',
        ...(request.url.endsWith('segment-5.ts')
          ? {}
          : {
              'content-range': `bytes ${range.start}-${range.endExclusive - 1}/${payload.byteLength}`,
            }),
        etag: '"stable"',
      },
      status: 206,
    })
  } finally {
    activeFetches -= 1
  }
}

const config = Hls.DefaultConfig as HlsConfig
const notificationOwner = new ParallelSegmentLoader({ maxConcurrency: 1 })
try {
  const observedRevisions: number[] = []
  const unsubscribe = notificationOwner.subscribe(() => {
    observedRevisions.push(notificationOwner.state.revision)
  })

  notificationOwner.update(() => undefined)
  notificationOwner.update(() => undefined)
  await Promise.resolve()
  if (observedRevisions.join(',') !== '2') {
    throw new Error('同一轮同步 update 应当合并成一次即时通知')
  }

  notificationOwner.update(() => undefined)
  await Promise.resolve()
  notificationOwner.update(() => undefined)
  await Promise.resolve()
  if (observedRevisions.join(',') !== '2') {
    throw new Error('通知间隔内的 update 不应反复唤醒 listener')
  }

  await new Promise(resolve => globalThis.setTimeout(resolve, 30))
  if (observedRevisions.join(',') !== '2,4') {
    throw new Error('通知间隔结束后应当补发一次通知并暴露最新 revision')
  }

  await new Promise(resolve => globalThis.setTimeout(resolve, 12))
  notificationOwner.update(() => undefined)
  await Promise.resolve()
  if (observedRevisions.join(',') !== '2,4,5') {
    throw new Error('空闲超过通知间隔后的首个 update 应当即时通知')
  }
  unsubscribe()
} finally {
  notificationOwner.destroy()
}

const estimatorOwner = new ParallelSegmentLoader({ maxConcurrency: 1 })
try {
  estimatorOwner.recordTransfer(100, 0, 100)
  estimatorOwner.recordTransfer(100, 0, 100)
  if (estimatorOwner.bandwidthEstimate !== 16_000) {
    throw new Error('并行 GET 应按重叠时间区间计算聚合带宽')
  }
  estimatorOwner.recordTransfer(100, 200, 300)
  if (
    Math.abs(estimatorOwner.bandwidthEstimate - 12_000) > 0.001 ||
    Math.abs(estimatorOwner.getDiagnostics().bandwidthEstimate - 12_000) > 0.001
  ) {
    throw new Error('不连续 GET 应排除请求之间的空闲时间')
  }
} finally {
  estimatorOwner.destroy()
}

const owner = new ParallelSegmentLoader({ chunkSize: 4, maxConcurrency: 3 })
const firstFragment = createFragment(1)
const firstContext = createContext(firstFragment)

try {
  if (
    owner.rescue.maxAttempts !== 2 ||
    owner.rescue.slowRateThresholdRatio !== 0.25 ||
    owner.rescue.stallTimeoutMs !== 4_000
  ) {
    throw new Error('默认救援配置错误')
  }
  replaceWindow(owner, [firstFragment], config)
  const first = startLoad(owner, firstContext, config)
  const second = startLoad(owner, firstContext, config)
  if (first.loader.stats.loading.start <= 0 || second.loader.stats.loading.start <= 0) {
    throw new Error('fLoader 必须从 load() 调用开始记录正式读取等待时间')
  }
  const firstResult = first.promise.then(
    () => 'success',
    () => 'aborted',
  )

  first.loader.abort()
  if ((await firstResult) !== 'aborted') {
    throw new Error('abort 应当只结束当前 fLoader 调用')
  }

  assertPayload(await second.promise, resources.get(firstContext.url))
  if (fetchCount(firstContext.url) !== 3) {
    throw new Error(`10 字节 Segment 应拆成 3 个请求, 实际 ${fetchCount(firstContext.url)}`)
  }
  if (maxActiveFetches < 2) {
    throw new Error('同一个 Segment 的后续 Chunk 应当并行加载')
  }

  const completedAt = owner.state.locateSegment(firstContext)?.outcome
  if (completedAt?.type !== 'ready') {
    throw new Error('首个 Segment 应当已经完成')
  }
  await new Promise(resolve => globalThis.setTimeout(resolve, 80))
  const cached = startLoad(owner, firstContext, config)
  assertPayload(await cached.promise, resources.get(firstContext.url))
  if (fetchCount(firstContext.url) !== 3) {
    throw new Error('窗口内 ready Segment 应复用 canonical 数据')
  }
  if (cached.loader.getCacheAge?.() !== 3) {
    throw new Error('fLoader 应当暴露缓存响应的 Age header')
  }
  if (
    cached.loader.stats.loading.start <= completedAt.completedAt ||
    cached.loader.stats.loading.end <= completedAt.completedAt ||
    cached.loader.stats.bwEstimate !== owner.bandwidthEstimate
  ) {
    throw new Error('缓存 Segment 的网络时间应重新锚定到 fLoader 交付时刻')
  }

  const readySegment = owner.getDiagnostics().streams[0]?.segments[0]
  if (readySegment?.state !== 'ready' || readySegment.chunks.length !== 3) {
    throw new Error('诊断快照应当包含 ready Segment 和 3 个 Chunk')
  }

  second.loader.destroy()
  cached.loader.destroy()

  const secondFragment = createFragment(2)
  replaceWindow(owner, [firstFragment, secondFragment], config)
  const overlapping = owner
    .getDiagnostics()
    .streams.flatMap(stream => stream.segments)
    .find(segment => segment.url === firstContext.url)
  if (overlapping?.state !== 'ready' || fetchCount(firstContext.url) !== 3) {
    throw new Error('窗口更新必须保留重叠 Segment 的 ready 数据和 Chunk 状态')
  }

  replaceWindow(owner, [secondFragment], config)
  if (
    owner
      .getDiagnostics()
      .streams.flatMap(stream => stream.segments)
      .some(segment => segment.url === firstContext.url)
  ) {
    throw new Error('离开窗口且没有 reader 的 Segment 应当立即驱离')
  }

  const reloaded = startLoad(owner, firstContext, config)
  assertPayload(await reloaded.promise, resources.get(firstContext.url))
  if (fetchCount(firstContext.url) !== 6) {
    throw new Error('驱离后的 Segment 再次读取应重新加载全部 Chunk')
  }
  if (
    owner
      .getDiagnostics()
      .streams.flatMap(stream => stream.segments)
      .some(segment => segment.url === firstContext.url)
  ) {
    throw new Error('窗口外 Segment 应在最后一个 reader 结束后驱离')
  }
  reloaded.loader.destroy()

  const sequentialFragment = createFragment(3)
  const sequentialContext = createContext(sequentialFragment)
  const sequential = startLoad(owner, sequentialContext, config)
  assertPayload(await sequential.promise, resources.get(sequentialContext.url))
  if (fetchCount(sequentialContext.url) !== 1) {
    throw new Error('Origin 忽略首个 Range GET 时应复用 200 响应, 不重复完整 GET')
  }
  sequential.loader.destroy()

  const declaredRangeFragment = createFragment(4)
  const declaredRangeContext = {
    ...createContext(declaredRangeFragment),
    rangeEnd: 4,
    rangeStart: 1,
  }
  const declaredRange = startLoad(owner, declaredRangeContext, config)
  const declaredRangeError = await declaredRange.promise.then(
    () => undefined,
    cause => cause,
  )
  if (!(declaredRangeError instanceof Error)) {
    throw new Error('Origin 忽略 HLS 声明的 byte range 时必须加载失败')
  }
  declaredRange.loader.destroy()

  const hiddenContentRangeFragment = createFragment(5)
  const hiddenContentRangeContext = createContext(hiddenContentRangeFragment)
  const hiddenContentRange = startLoad(owner, hiddenContentRangeContext, config)
  assertPayload(await hiddenContentRange.promise, resources.get(hiddenContentRangeContext.url))
  if (fetchCount(hiddenContentRangeContext.url) !== 4) {
    throw new Error('Content-Range 不可见时应使用一次 HEAD 规划 3 个 Chunk')
  }
  hiddenContentRange.loader.destroy()

  const rescueOwner = new ParallelSegmentLoader({
    chunkSize: 4,
    maxConcurrency: 2,
    rescue: {
      maxAttempts: 2,
      slowRateThresholdRatio: 0,
      stallTimeoutMs: 50,
    },
  })
  try {
    const rescueFragment = createFragment(6)
    const rescueContext = createContext(rescueFragment)
    replaceWindow(rescueOwner, [rescueFragment], config)
    const rescued = startLoad(rescueOwner, rescueContext, config)
    await waitForCondition(() => {
      const segment = rescueOwner.getDiagnostics().streams[0]?.segments[0]
      return segment?.chunks[1]?.state === 'filling' && segment.chunks[1].loadedBytes === 2
    })
    assertPayload(await rescued.promise, resources.get(rescueContext.url))
    if (fetchCount(rescueContext.url) !== 4) {
      throw new Error('连续两次无数据的 Fetch body 应分别取消并重新领取同一个 Chunk')
    }
    if (rangeFetchCount(rescueContext.url, 'bytes=4-7') !== 3) {
      throw new Error('救援后的请求应当从相同 Chunk 起点完整重下')
    }
    if (!stalledBodyCanceled) {
      throw new Error('Chunk 空闲超时后应取消原 Fetch body')
    }
    const rescuedSegment = rescueOwner.getDiagnostics().streams[0]?.segments[0]
    if (rescuedSegment?.chunks[1]?.attempt !== 3 || rescuedSegment.loadedBytes !== 8) {
      throw new Error('Chunk 补救完成后诊断状态错误')
    }
    const rescueStatistics = rescueOwner.getDiagnostics().rescue
    const rescueEvent = rescueStatistics.recentEvents[0]
    if (
      rescueStatistics.totalEvents !== 2 ||
      rescueStatistics.stallEvents !== 2 ||
      rescueStatistics.slowEvents !== 0 ||
      rescueStatistics.recoveredEvents !== 2 ||
      rescueStatistics.pendingEvents !== 0 ||
      rescueStatistics.exhaustedStallCount !== 0 ||
      rescueStatistics.discardedBytes !== 4 ||
      rescueEvent?.reason !== 'stall' ||
      rescueEvent.outcome !== 'recovered' ||
      rescueEvent.attempt !== 1 ||
      rescueEvent.discardedBytes !== 2
    ) {
      throw new Error('多次停滞救援应当记录原因、丢弃字节和恢复结果')
    }
    rescued.loader.destroy()
  } finally {
    rescueOwner.destroy()
  }

  const exhaustedRescueOwner = new ParallelSegmentLoader({
    chunkSize: 4,
    maxConcurrency: 1,
    rescue: {
      maxAttempts: 1,
      slowRateThresholdRatio: 0,
      stallTimeoutMs: 30,
    },
  })
  try {
    const exhaustedFragment = createFragment(14)
    const exhaustedContext = createContext(exhaustedFragment)
    replaceWindow(exhaustedRescueOwner, [exhaustedFragment], config)
    const exhausted = startLoad(exhaustedRescueOwner, exhaustedContext, config)
    const error = await settleWithin(
      exhausted.promise.then(
        () => undefined,
        cause => cause,
      ),
      500,
    )
    if (!(error instanceof Error) || !error.message.includes('救援次数已经耗尽')) {
      throw new Error('救援耗尽后的停滞 Chunk 应当快速失败')
    }
    if (fetchCount(exhaustedContext.url) !== 2) {
      throw new Error('救援耗尽测试应当只发出初始请求和一次重试')
    }
    const exhaustedStatistics = exhaustedRescueOwner.getDiagnostics().rescue
    if (
      exhaustedStatistics.totalEvents !== 1 ||
      exhaustedStatistics.stallEvents !== 1 ||
      exhaustedStatistics.exhaustedStallCount !== 1
    ) {
      throw new Error('救援耗尽后的停滞应当进入独立诊断统计')
    }
    exhausted.loader.destroy()
  } finally {
    exhaustedRescueOwner.destroy()
  }

  const rescueDisabledOwner = new ParallelSegmentLoader({
    chunkSize: 4,
    maxConcurrency: 1,
    rescue: false,
  })
  try {
    const rescueDisabledFragment = createFragment(7)
    const rescueDisabledContext = createContext(rescueDisabledFragment)
    const rescueDisabled = startLoad(rescueDisabledOwner, rescueDisabledContext, config)
    assertPayload(await rescueDisabled.promise, resources.get(rescueDisabledContext.url))
    if (fetchCount(rescueDisabledContext.url) !== 2) {
      throw new Error('禁用 rescue 时不应因为 body 停滞创建额外 Work')
    }
    if (rescueDisabledOwner.getDiagnostics().rescue.totalEvents !== 0) {
      throw new Error('禁用 rescue 时不应产生救援统计')
    }
    rescueDisabled.loader.destroy()
  } finally {
    rescueDisabledOwner.destroy()
  }

  const maxAttemptsDisabledOwner = new ParallelSegmentLoader({ rescue: { maxAttempts: 0 } })
  try {
    if (maxAttemptsDisabledOwner.rescue.maxAttempts !== rescueDisabledOwner.rescue.maxAttempts) {
      throw new Error('rescue: false 应等价于 maxAttempts 为 0')
    }
  } finally {
    maxAttemptsDisabledOwner.destroy()
  }

  const slowRescueOwner = new ParallelSegmentLoader({
    chunkSize: 4,
    maxConcurrency: 3,
    rescue: {
      maxAttempts: 1,
      slowRateThresholdRatio: 0.25,
      stallTimeoutMs: 40,
    },
  })
  try {
    const slowFragment = createFragment(13)
    const slowContext = createContext(slowFragment)
    replaceWindow(slowRescueOwner, [slowFragment], config)
    const slowLoad = startLoad(slowRescueOwner, slowContext, config)
    assertPayload(await slowLoad.promise, resources.get(slowContext.url))
    if (rangeFetchCount(slowContext.url, 'bytes=12-15') !== 2 || !slowBodyCanceled) {
      throw new Error('明显慢于同期 GET 且重试预计更快的 Chunk 应取消并重新领取')
    }
    const slowSegment = slowRescueOwner.getDiagnostics().streams[0]?.segments[0]
    if (slowSegment?.chunks[3]?.rescueAttempts !== 1) {
      throw new Error('慢速补救次数没有写入 Chunk 诊断')
    }
    const slowStatistics = slowRescueOwner.getDiagnostics().rescue
    const slowEvent = slowStatistics.recentEvents[0]
    if (
      slowStatistics.totalEvents !== 1 ||
      slowStatistics.slowEvents !== 1 ||
      slowStatistics.stallEvents !== 0 ||
      slowStatistics.recoveredEvents !== 1 ||
      slowEvent?.reason !== 'slow' ||
      slowEvent.outcome !== 'recovered' ||
      slowEvent.currentRate === undefined ||
      slowEvent.peerMedianRate === undefined ||
      slowEvent.peerCount !== 2 ||
      slowEvent.continueEtaMs === undefined ||
      slowEvent.retryEtaMs === undefined
    ) {
      throw new Error('慢速救援应当记录 peer 比较、ETA 和恢复结果')
    }
    slowLoad.loader.destroy()
  } finally {
    slowRescueOwner.destroy()
  }

  const nonPreemptiveOwner = new ParallelSegmentLoader({ chunkSize: 4, maxConcurrency: 2 })
  try {
    const readerFragment = createFragment(8)
    const prefetchFragment = createFragment(9)
    const readerContext = createContext(readerFragment)
    replaceWindow(nonPreemptiveOwner, [readerFragment, prefetchFragment], config)
    const reader = startLoad(nonPreemptiveOwner, readerContext, config)

    assertPayload(await reader.promise, resources.get(readerContext.url))
    await waitForCondition(() => {
      const prefetch = nonPreemptiveOwner
        .getDiagnostics()
        .streams[0]?.segments.find(segment => segment.url === prefetchFragment.url)
      return prefetch?.state === 'ready'
    })
    if (prefetchBodyCanceled || fetchCount(prefetchFragment.url) !== 2) {
      throw new Error('已经发出的 Prefetch Work 不应被新规划的 Reader Chunk 抢占')
    }
    reader.loader.destroy()
  } finally {
    nonPreemptiveOwner.destroy()
  }

  const orderedPlanningOwner = new ParallelSegmentLoader({ chunkSize: 4, maxConcurrency: 2 })
  try {
    const leadingFragment = createFragment(10)
    const followingFragment = createFragment(11)
    replaceWindow(orderedPlanningOwner, [leadingFragment, followingFragment], config)
    await waitForCondition(() => {
      const following = orderedPlanningOwner
        .getDiagnostics()
        .streams[0]?.segments.find(segment => segment.url === followingFragment.url)
      return following?.planningState === 'planned'
    })
    const followingWhileLeadingIsUnverified = orderedPlanningOwner
      .getDiagnostics()
      .streams[0]?.segments.find(segment => segment.url === followingFragment.url)
    if (
      followingWhileLeadingIsUnverified?.state !== 'queued' ||
      requestMethods.get(followingFragment.url)?.join(',') !== 'HEAD'
    ) {
      throw new Error('后续 Segment 的 HEAD 可以提前完成, 但不能越过前序 Segment 发起 GET')
    }
    const followingModel = [...orderedPlanningOwner.state.streams.values()]
      .flatMap(stream => [...stream.segments.values()])
      .find(segment => segment.context.url === followingFragment.url)
    if (followingModel?.startedAt !== undefined) {
      throw new Error('HEAD 和等待前序 Segment 的时间不能启动媒体下载计时')
    }

    await waitForCondition(() =>
      Boolean(
        orderedPlanningOwner
          .getDiagnostics()
          .streams[0]?.segments.every(segment => segment.state === 'ready'),
      ),
    )
    if (requestMethods.get(followingFragment.url)?.[0] !== 'HEAD') {
      throw new Error('后续 Segment 必须先通过 HEAD 规划长度')
    }
  } finally {
    orderedPlanningOwner.destroy()
  }

  const responseHeaderPlanningOwner = new ParallelSegmentLoader({
    chunkSize: 4,
    maxConcurrency: 2,
  })
  try {
    const fragment = createFragment(12)
    const context = createContext(fragment)
    replaceWindow(responseHeaderPlanningOwner, [fragment], config)
    const load = startLoad(responseHeaderPlanningOwner, context, config)
    await waitForCondition(() => {
      const segment = responseHeaderPlanningOwner.getDiagnostics().streams[0]?.segments[0]
      return segment?.planningState === 'planned' && segment.chunks.length === 2
    })
    const plannedFromHeaders = responseHeaderPlanningOwner.getDiagnostics().streams[0]?.segments[0]
    if (plannedFromHeaders?.rangeMode !== 'parallel' || plannedFromHeaders.totalBytes !== 5) {
      throw new Error('首个 Range GET 收到响应头后应立即规划剩余 Chunk')
    }

    assertPayload(await load.promise, resources.get(context.url))
    if (requestRanges.get(context.url)?.join(',') !== 'bytes=0-3,bytes=4-4') {
      throw new Error('Segment 尾部必须规划成独立且边界准确的 Chunk')
    }
    load.loader.destroy()
  } finally {
    responseHeaderPlanningOwner.destroy()
  }
} finally {
  owner.destroy()
  globalThis.fetch = originalFetch
}

interface StartedLoad {
  loader: HlsLoader<FragmentLoaderContext>
  promise: Promise<LoaderResponse>
}

function startLoad(
  owner: ParallelSegmentLoader,
  context: FragmentLoaderContext,
  config: HlsConfig,
): StartedLoad {
  const loader = new owner.fLoader(config)
  const promise = new Promise<LoaderResponse>((resolve, reject) => {
    loader.load(context, createLoaderConfiguration(), {
      onAbort: () => reject(new Error('Segment 加载已经取消')),
      onError: error => reject(new Error(error.text)),
      onSuccess: resolve,
      onTimeout: () => reject(new Error('Segment 加载超时')),
    })
  })
  return { loader, promise }
}

function createFragment(index: number): Fragment {
  return {
    cc: 0,
    duration: 2,
    gap: false,
    level: 0,
    sn: index,
    start: (index - 1) * 2,
    type: 'main',
    url: `https://example.com/segment-${index}.ts`,
  } as unknown as Fragment
}

function createContext(fragment: Fragment): FragmentLoaderContext {
  return {
    frag: fragment,
    headers: {},
    part: null,
    rangeEnd: 0,
    rangeStart: 0,
    responseType: 'arraybuffer',
    type: 'media-fragment' as FragmentLoaderContext['type'],
    url: fragment.url,
  }
}

function replaceWindow(
  owner: ParallelSegmentLoader,
  fragments: readonly Fragment[],
  config: HlsConfig,
): void {
  owner.configure(config)
  owner.update(state => {
    state.ensureStream('main:0').replaceWindow(fragments, owner.chunkSize)
    return undefined
  })
}

function createLoaderConfiguration(): LoaderConfiguration {
  return {
    highWaterMark: Number.POSITIVE_INFINITY,
    loadPolicy: {
      errorRetry: null,
      maxLoadTimeMs: 10_000,
      maxTimeToFirstByteMs: 5_000,
      timeoutRetry: null,
    },
    maxRetry: 0,
    maxRetryDelay: 0,
    retryDelay: 0,
    timeout: 10_000,
  }
}

function assertPayload(response: LoaderResponse, expected: Uint8Array | undefined): void {
  if (!(response.data instanceof ArrayBuffer) || expected === undefined) {
    throw new Error('fLoader 没有返回 ArrayBuffer')
  }
  const actual = new Uint8Array(response.data)
  if (
    actual.byteLength !== expected.byteLength ||
    actual.some((value, index) => value !== expected[index])
  ) {
    throw new Error('fLoader 返回了错误的 Segment 数据')
  }
}

function parseRange(
  value: string | null,
  length: number,
): {
  endExclusive: number
  start: number
} {
  const match = /^bytes=(\d+)-(\d+)$/.exec(value ?? '')
  if (match === null) {
    throw new Error(`测试请求缺少 Range: ${String(value)}`)
  }
  const start = Number.parseInt(match[1] ?? '', 10)
  const requestedEnd = Number.parseInt(match[2] ?? '', 10) + 1
  return { endExclusive: Math.min(requestedEnd, length), start }
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = globalThis.setTimeout(resolve, milliseconds)
    signal.addEventListener(
      'abort',
      () => {
        globalThis.clearTimeout(timer)
        reject(new DOMException('请求已经取消', 'AbortError'))
      },
      { once: true },
    )
  })
}

function fetchCount(url: string): number {
  return fetchCounts.get(url) ?? 0
}

function rangeFetchCount(url: string, range: string | null): number {
  return requestRanges.get(url)?.filter(value => value === range).length ?? 0
}

async function waitForCondition(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 1_000
  while (!predicate()) {
    if (performance.now() >= deadline) {
      throw new Error('等待测试状态超时')
    }
    await new Promise(resolve => globalThis.setTimeout(resolve, 1))
  }
}

function settleWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = globalThis.setTimeout(() => reject(new Error('等待 Promise 结束超时')), timeoutMs)
    void promise.then(
      value => {
        globalThis.clearTimeout(timer)
        resolve(value)
      },
      cause => {
        globalThis.clearTimeout(timer)
        reject(cause)
      },
    )
  })
}
