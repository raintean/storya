import Hls from 'hls.js'
import type {
  Fragment,
  FragmentLoaderContext,
  HlsConfig,
  Loader as HlsLoader,
  LoaderConfiguration,
  LoaderResponse,
} from 'hls.js'
import { createWindowDescriptor, ParallelSegmentLoader } from './parallel-segment-loader.ts'

const originalFetch = globalThis.fetch
const resources = new Map([
  ['https://example.com/segment-1.ts', new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])],
  ['https://example.com/segment-2.ts', new Uint8Array([10, 11, 12, 13, 14, 15])],
  ['https://example.com/segment-3.ts', new Uint8Array([20, 21, 22, 23, 24])],
  ['https://example.com/segment-4.ts', new Uint8Array([30, 31, 32, 33, 34])],
  ['https://example.com/segment-5.ts', new Uint8Array([40, 41, 42, 43, 44, 45, 46, 47, 48, 49])],
  ['https://example.com/segment-6.ts', new Uint8Array([50, 51, 52, 53, 54, 55, 56, 57])],
])
const fetchCounts = new Map<string, number>()
let activeFetches = 0
let maxActiveFetches = 0
let stalledBodyCanceled = false

globalThis.fetch = async input => {
  const request = input instanceof Request ? input : new Request(input)
  const payload = resources.get(request.url)
  if (payload === undefined) {
    return new Response(null, { status: 404 })
  }

  fetchCounts.set(request.url, (fetchCounts.get(request.url) ?? 0) + 1)
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
    if (request.url.endsWith('segment-3.ts') || request.url.endsWith('segment-4.ts')) {
      return new Response(payload.slice().buffer, {
        headers: { age: '3', etag: '"stable"' },
        status: 200,
      })
    }
    const range = parseRange(request.headers.get('range'), payload.byteLength)
    if (
      request.url.endsWith('segment-6.ts') &&
      range.start === 4 &&
      fetchCount(request.url) === 2
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
const owner = new ParallelSegmentLoader({ chunkSize: 4, maxConcurrency: 3 })
const firstFragment = createFragment(1)
const firstContext = createContext(firstFragment)

try {
  owner.replaceWindow('main:0', [createWindowDescriptor(firstFragment)], config)
  const first = startLoad(owner, firstContext, config)
  const second = startLoad(owner, firstContext, config)
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

  const cached = startLoad(owner, firstContext, config)
  assertPayload(await cached.promise, resources.get(firstContext.url))
  if (fetchCount(firstContext.url) !== 3) {
    throw new Error('窗口内 ready Segment 应复用 canonical 数据')
  }
  if (cached.loader.getCacheAge?.() !== 3) {
    throw new Error('fLoader 应当暴露缓存响应的 Age header')
  }

  const readySegment = owner.getDiagnostics().streams[0]?.segments[0]
  if (readySegment?.state !== 'ready' || readySegment.chunks.length !== 3) {
    throw new Error('诊断快照应当包含 ready Segment 和 3 个 Chunk')
  }

  second.loader.destroy()
  cached.loader.destroy()

  const secondFragment = createFragment(2)
  owner.replaceWindow(
    'main:0',
    [createWindowDescriptor(firstFragment), createWindowDescriptor(secondFragment)],
    config,
  )
  const overlapping = owner
    .getDiagnostics()
    .streams.flatMap(stream => stream.segments)
    .find(segment => segment.url === firstContext.url)
  if (overlapping?.state !== 'ready' || fetchCount(firstContext.url) !== 3) {
    throw new Error('窗口更新必须保留重叠 Segment 的 ready 数据和 Chunk 状态')
  }

  owner.replaceWindow('main:0', [createWindowDescriptor(secondFragment)], config)
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
    throw new Error('Origin 忽略 discovery Range 时应复用 200 响应, 不重复完整 GET')
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
    idleTimeoutMs: 50,
    maxConcurrency: 2,
    maxRescueAttempts: 1,
  })
  try {
    const rescueFragment = createFragment(6)
    const rescueContext = createContext(rescueFragment)
    rescueOwner.replaceWindow('main:0', [createWindowDescriptor(rescueFragment)], config)
    const rescued = startLoad(rescueOwner, rescueContext, config)
    await waitForCondition(() => {
      const segment = rescueOwner.getDiagnostics().streams[0]?.segments[0]
      return segment?.chunks[1]?.state === 'filling' && segment.chunks[1].loadedBytes === 2
    })
    assertPayload(await rescued.promise, resources.get(rescueContext.url))
    if (fetchCount(rescueContext.url) !== 3) {
      throw new Error('无数据的 Fetch body 应取消并重新领取同一个 Chunk')
    }
    if (!stalledBodyCanceled) {
      throw new Error('Chunk 空闲超时后应取消原 Fetch body')
    }
    const rescuedSegment = rescueOwner.getDiagnostics().streams[0]?.segments[0]
    if (rescuedSegment?.chunks[1]?.attempt !== 2 || rescuedSegment.loadedBytes !== 8) {
      throw new Error('Chunk 补救完成后诊断状态错误')
    }
    rescued.loader.destroy()
  } finally {
    rescueOwner.destroy()
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

async function waitForCondition(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 1_000
  while (!predicate()) {
    if (performance.now() >= deadline) {
      throw new Error('等待测试状态超时')
    }
    await new Promise(resolve => globalThis.setTimeout(resolve, 1))
  }
}
