import Hls from 'hls.js'
import type {
  Fragment,
  FragmentLoaderContext,
  HlsConfig,
  LevelLoadedData,
  Loader,
  LoaderConfiguration,
  LoaderResponse,
} from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import { createHlsParallelLoader } from './index.ts'

class FakeHls {
  readonly config = {} as HlsConfig
  private readonly listeners = new Map<string, Set<(event: string, data: never) => void>>()

  on(event: string, listener: (event: string, data: never) => void): void {
    const listeners = this.listeners.get(event) ?? new Set()
    listeners.add(listener)
    this.listeners.set(event, listeners)
  }

  off(event: string, listener: (event: string, data: never) => void): void {
    this.listeners.get(event)?.delete(listener)
  }

  emit(event: string, data: unknown): void {
    for (const listener of this.listeners.get(event) ?? []) {
      listener(event, data as never)
    }
  }
}

const fragments = Array.from({ length: 9 }, (_, index) => createFragment(index))
const fetchCounts = new Map<string, number>()
const originalFetch = globalThis.fetch
let activeGets = 0
let headRequests = 0
let maxActiveGets = 0

globalThis.fetch = async input => {
  const request = input instanceof Request ? input : new Request(input)
  if (request.method === 'HEAD') {
    headRequests += 1
    return new Response(null, {
      headers: { 'accept-ranges': 'bytes', 'content-length': '4' },
      status: 200,
    })
  }
  if (request.headers.get('range') === null) {
    throw new Error('未知长度 Segment 的首个请求必须携带 Range')
  }

  fetchCounts.set(request.url, (fetchCounts.get(request.url) ?? 0) + 1)
  activeGets += 1
  maxActiveGets = Math.max(maxActiveGets, activeGets)
  await delay(20)
  activeGets -= 1
  const sn = Number.parseInt(request.url.split('/').at(-1)?.replace('.ts', '') ?? '0', 10)
  if (sn === 100) {
    return new Response('not found', { status: 404, statusText: 'Not Found' })
  }
  if (sn === 7) {
    return new Response(new Uint8Array([sn, sn, sn, sn]).buffer, {
      headers: { 'content-length': '4' },
      status: 200,
    })
  }
  const headers: Record<string, string> = { 'content-length': '4' }
  if (sn !== 8) {
    headers['content-range'] = 'bytes 0-3/4'
  }
  return new Response(new Uint8Array([sn, sn, sn, sn]).buffer, {
    headers,
    status: 206,
  })
}

const events: string[] = []
const parallel = createHlsParallelLoader({
  onEvent: event => {
    if (event.type === 'segment-state') {
      events.push(`${event.action}:${String(event.segmentSn)}`)
    }
  },
})
const hls = new FakeHls()
parallel.attach(hls as unknown as Hls)
hls.emit(Hls.Events.LEVEL_LOADED, {
  details: { fragments, url: 'https://example.com/main/index.m3u8' },
  level: 0,
} as LevelLoadedData)

try {
  const firstPromise = load(parallel.fragmentLoader, fragments[0] as Fragment)
  await delay(5)
  const loadingSnapshot = parallel.getDiagnostics()
  const loadingStream = loadingSnapshot.streams.find(stream => stream.id === 'main:0')
  if (loadingStream?.frontier === undefined || loadingStream.frontier.confirmed) {
    throw new Error('诊断快照应包含尚未确认的读取 frontier')
  }
  if (
    loadingSnapshot.activeRequests === 0 ||
    loadingSnapshot.activeRequests > loadingSnapshot.maxConcurrency
  ) {
    throw new Error('诊断快照中的活动请求数应处于全局并发范围内')
  }
  if (!loadingStream.segments.some(segment => segment.chunks.some(chunk => chunk.fillerId))) {
    throw new Error('诊断快照应暴露正在持有 Writer 的 Filler')
  }

  const first = await firstPromise
  assertPayload(first, 0)
  await delay(80)

  const readySnapshot = parallel.getDiagnostics()
  const readyStream = readySnapshot.streams.find(stream => stream.id === 'main:0')
  if (
    readyStream === undefined ||
    !readyStream.segments.some(segment => segment.prefetch && segment.state === 'ready')
  ) {
    throw new Error('诊断快照应暴露已经完成的预填充 Segment')
  }
  const deliveredSegment = readyStream.segments.find(segment => segment.start === 0)
  if (deliveredSegment?.state !== 'ready' || deliveredSegment.readerCount !== 0) {
    throw new Error('Segment 交付后 Reader 应自然结束, 数据继续由 Chunk 持有')
  }
  if (readyStream.frontier?.confirmed !== true || readyStream.frontier.barrier) {
    throw new Error('成功交付后 frontier 应被确认且不能是失败屏障')
  }

  for (let sn = 0; sn <= 6; sn += 1) {
    assertFetchCount(sn, 1, '首轮预填充')
  }
  if (maxActiveGets > 6) {
    throw new Error(`GET/Range 并发超过 6, 实际 ${maxActiveGets}`)
  }
  if (getHeadRequestCount() !== 0) {
    throw new Error('Content-Range 可读时不应发送 HEAD')
  }

  const second = await load(parallel.fragmentLoader, fragments[1] as Fragment)
  assertPayload(second, 1)
  await delay(50)
  assertFetchCount(1, 1, 'ready Segment 应复用预填充数据')
  assertFetchCount(7, 1, '窗口推进后应填充新的 Segment')

  const [sharedLeft, sharedRight] = await Promise.all([
    load(parallel.fragmentLoader, fragments[8] as Fragment),
    load(parallel.fragmentLoader, fragments[8] as Fragment),
  ])
  assertFetchCount(8, 1, '多个 Reader 应共享同一 Segment 的 Chunk')
  if (getHeadRequestCount() !== 1) {
    throw new Error(`Content-Range 缺失时应补发一次 HEAD, 实际 ${headRequests}`)
  }
  assertPayload(sharedLeft, 8)
  assertPayload(sharedRight, 8)
  if (sharedLeft.data === sharedRight.data) {
    throw new Error('不同 Reader 不应共享可能被 hls.js 转移或分离的 ArrayBuffer 实例')
  }

  if (!events.includes('reader-created:0') || !events.includes('reader-ready:1')) {
    throw new Error('Reader 生命周期事件没有正确发出')
  }

  const failingFragment = createFragment(100)
  hls.emit(Hls.Events.LEVEL_LOADED, {
    details: {
      fragments: [failingFragment],
      url: 'https://example.com/failure/index.m3u8',
    },
    level: 1,
  } as LevelLoadedData)
  await load(parallel.fragmentLoader, failingFragment).then(
    () => {
      throw new Error('失败 Segment 不应加载成功')
    },
    () => undefined,
  )
  await delay(5)
  const failedStream = parallel.getDiagnostics().streams.find(stream => stream.id === 'main:1')
  const failedSegment = failedStream?.segments.find(segment => segment.start === 200)
  if (
    failedSegment?.state !== 'failed' ||
    failedStream?.frontier?.barrier !== true ||
    failedSegment.readerCount !== 0
  ) {
    throw new Error('Segment 最终失败后应形成 frontier 屏障并结束 Reader')
  }

  const lateFragment = createFragment(50)
  lateFragment.level = 2
  const lateRead = load(parallel.fragmentLoader, lateFragment)
  await delay(1)
  hls.emit(Hls.Events.LEVEL_LOADED, {
    details: {
      fragments: [lateFragment],
      url: 'https://example.com/late/index.m3u8',
    },
    level: 2,
  } as LevelLoadedData)
  assertPayload(await lateRead, 50)
  const lateSnapshot = parallel.getDiagnostics()
  if (
    !lateSnapshot.streams.some(stream => stream.id === 'main:2') ||
    lateSnapshot.streams.some(stream => stream.id.startsWith('provisional:'))
  ) {
    throw new Error('迟到的 topology 应把 provisional VirtualStream 归并到 canonical Stream')
  }
} finally {
  parallel.destroy()
  globalThis.fetch = originalFetch
}

function getHeadRequestCount(): number {
  return headRequests
}

await testStableRangeRetry()

async function testStableRangeRetry(): Promise<void> {
  const requestedRanges: string[] = []
  let attempt = 0
  const transport: HttpTransport = {
    rangeRequestMode: 'stable',
    destroy: () => undefined,
    request: request => {
      requestedRanges.push(request.headers.get('range') ?? '')
      attempt += 1
      const headers = {
        'content-length': '4',
        'content-range': 'bytes 0-3/4',
      }
      if (attempt === 1) {
        let delivered = false
        const body = new ReadableStream<Uint8Array>({
          pull: controller => {
            if (!delivered) {
              delivered = true
              controller.enqueue(new Uint8Array([9, 9]))
              return
            }
            controller.error(new Error('模拟响应中断'))
          },
        })
        return Promise.resolve(
          new Response(body, { headers, status: 206 }) as HttpTransportResponse,
        )
      }
      return Promise.resolve(
        new Response(new Uint8Array([9, 9, 9, 9]), {
          headers,
          status: 206,
        }) as HttpTransportResponse,
      )
    },
  }
  const stableParallel = createHlsParallelLoader({ transport })
  const stableHls = new FakeHls()
  const fragment = createFragment(9)
  stableParallel.attach(stableHls as unknown as Hls)
  stableHls.emit(Hls.Events.LEVEL_LOADED, {
    details: { fragments: [fragment], url: 'https://example.com/stable/index.m3u8' },
    level: 0,
  } as LevelLoadedData)

  try {
    const response = await load(stableParallel.fragmentLoader, fragment)
    assertPayload(response, 9)
    if (
      requestedRanges.length !== 2 ||
      requestedRanges.some(range => range !== 'bytes=0-2097151')
    ) {
      throw new Error(`稳定 Range 重试改变了 Chunk 边界: ${requestedRanges.join(', ')}`)
    }
  } finally {
    stableParallel.destroy()
  }
}

async function load(
  LoaderConstructor: ReturnType<typeof createHlsParallelLoader>['fragmentLoader'],
  fragment: Fragment,
): Promise<LoaderResponse> {
  const loader: Loader<FragmentLoaderContext> = new LoaderConstructor({} as HlsConfig)
  try {
    return await new Promise<LoaderResponse>((resolve, reject) => {
      loader.load(createContext(fragment), createLoaderConfig(), {
        onError: error => reject(new Error(error.text)),
        onSuccess: resolve,
        onTimeout: () => reject(new Error('Segment 加载超时')),
      })
    })
  } finally {
    loader.destroy()
  }
}

function createFragment(sn: number): Fragment {
  return {
    baseurl: 'https://example.com/main/index.m3u8',
    byteRangeEndOffset: undefined,
    byteRangeStartOffset: undefined,
    cc: 0,
    duration: 2,
    gap: false,
    initSegment: null,
    level: 0,
    sn,
    start: sn * 2,
    type: 'main',
    url: `https://example.com/main/${sn}.ts`,
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
    url: fragment.url,
  }
}

function createLoaderConfig(): LoaderConfiguration {
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

function assertFetchCount(sn: number, expected: number, scene: string): void {
  const url = `https://example.com/main/${sn}.ts`
  const actual = fetchCounts.get(url) ?? 0
  if (actual !== expected) {
    throw new Error(`${scene}失败, Segment ${sn} 期望请求 ${expected} 次, 实际 ${actual} 次`)
  }
}

function assertPayload(response: LoaderResponse, sn: number): void {
  if (!(response.data instanceof ArrayBuffer)) {
    throw new Error(`Segment ${sn} 没有返回 ArrayBuffer`)
  }
  const payload = new Uint8Array(response.data)
  if (payload.length !== 4 || payload.some(value => value !== sn)) {
    throw new Error(`Segment ${sn} 返回了错误数据`)
  }
}

async function delay(ms: number): Promise<void> {
  await new Promise<void>(resolve => setTimeout(resolve, ms))
}
