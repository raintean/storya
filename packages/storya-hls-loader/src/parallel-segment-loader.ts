import type {
  Fragment,
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  LoaderStats,
} from 'hls.js'
import {
  FetchHttpTransport,
  type HttpTransport,
  type HttpTransportResponse,
} from 'storya-transport'
import { splitByteRanges } from './byte-ranges'
import type {
  ChunkDiagnostics,
  DiagnosticChunkState,
  DiagnosticSegmentState,
  ParallelSegmentLoaderDiagnostics,
  SegmentDiagnostics,
  VirtualStreamDiagnostics,
  WorkerDiagnostics,
} from './diagnostics'
import {
  createStoryaFragmentLoader,
  type FragmentLoaderOwner,
  type SegmentLoadFailure,
  type SegmentObservation,
} from './fragment-loader'
import { cloneLoaderStats, createLoaderStats } from './stats'

export const DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024
export const DEFAULT_MAX_CONCURRENCY = 6
export const DEFAULT_WINDOW_SIZE = 6

export interface ParallelSegmentLoaderOptions {
  chunkSize?: number
  maxConcurrency?: number
  transport?: HttpTransport
}

export interface SegmentWindowDescriptor {
  context: FragmentLoaderContext
  duration: number
  key: string
  start: number
}

type SegmentState = 'empty' | 'failed' | 'filling' | 'ready'
type ChunkState = 'empty' | 'failed' | 'filling' | 'ready'

interface VirtualStream {
  id: string
  segments: Map<string, VirtualStreamSegment>
  window: string[]
}

interface VirtualStreamSegment {
  chunks: VirtualStreamChunk[]
  code: number
  context: FragmentLoaderContext
  declaredRange: boolean
  duration: number
  failure: SegmentLoadFailure | undefined
  fallbackAttempted: boolean
  finalUrl: string
  key: string
  lastResponse: Response | null
  length: number | undefined
  readerCount: number
  resourceStart: number
  result: ArrayBuffer | undefined
  sequential: boolean
  start: number
  state: SegmentState
  stats: LoaderStats
  streamId: string
  validator: string | null
  windowIndex: number | null
}

interface VirtualStreamChunk {
  attempt: number
  data: Uint8Array | undefined
  endExclusive: number | undefined
  failure: string | undefined
  fillId: number | undefined
  index: number
  key: string
  loadedBytes: number
  rangeEnabled: boolean
  start: number
  state: ChunkState
}

interface ChunkWork {
  chunkKey: string
  context: FragmentLoaderContext
  fillId: number
  rangeEnabled: boolean
  requestEnd: number | undefined
  requestStart: number
  resourceLength: number | undefined
  segmentKey: string
  streamId: string
}

interface WorkerRuntime {
  active: ChunkWork | undefined
  controller: AbortController
  done: Promise<void>
  id: number
  requestController: AbortController | undefined
  startedAt: number | undefined
  state: 'idle' | 'loading' | 'stopped'
}

interface ChunkFetchResult {
  data: Uint8Array
  firstByteAt: number
  response: Response
  url: string
}

const contentRangePattern = /^bytes (\d+)-(\d+)\/(\d+|\*)$/i

export class ParallelSegmentLoader implements FragmentLoaderOwner {
  private static readonly owners = new WeakMap<FragmentLoaderConstructor, ParallelSegmentLoader>()

  readonly fLoader: FragmentLoaderConstructor

  private readonly chunkSize: number
  private config: HlsConfig | undefined
  private destroyed = false
  private nextFillId = 0
  private notificationScheduled = false
  private readonly listeners = new Set<() => void>()
  private readonly maxConcurrency: number
  private readonly streams = new Map<string, VirtualStream>()
  private readonly transport: HttpTransport
  private readonly workers: WorkerRuntime[]
  revision = 0

  constructor(options: ParallelSegmentLoaderOptions = {}) {
    this.chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE
    this.maxConcurrency = options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY
    requirePositiveInteger(this.chunkSize, 'chunkSize')
    requirePositiveInteger(this.maxConcurrency, 'maxConcurrency')
    this.transport = options.transport ?? new FetchHttpTransport()

    this.fLoader = createStoryaFragmentLoader(this)
    ParallelSegmentLoader.owners.set(this.fLoader, this)
    this.workers = Array.from({ length: this.maxConcurrency }, (_, index) => {
      const controller = new AbortController()
      return {
        active: undefined,
        controller,
        done: Promise.resolve(),
        id: index + 1,
        requestController: undefined,
        startedAt: undefined,
        state: 'idle',
      }
    })
    for (const runtime of this.workers) {
      runtime.done = this.runWorker(runtime)
      void runtime.done.catch(() => undefined)
    }
  }

  static fromFragmentLoader(
    constructor: FragmentLoaderConstructor | undefined,
  ): ParallelSegmentLoader | undefined {
    return constructor === undefined ? undefined : this.owners.get(constructor)
  }

  configure(config: HlsConfig): void {
    if (this.destroyed) {
      throw new Error('ParallelSegmentLoader 已经销毁')
    }
    if (this.config !== undefined && this.config !== config) {
      throw new Error('一个 ParallelSegmentLoader 只能绑定一个 Hls 实例')
    }
    this.config = config
  }

  replaceWindow(
    streamId: string,
    descriptors: readonly SegmentWindowDescriptor[],
    config: HlsConfig,
    previousStreamId?: string,
  ): void {
    this.configure(config)
    if (previousStreamId !== undefined && previousStreamId !== streamId) {
      this.clearWindowState(previousStreamId)
    }

    const stream = this.requireStream(streamId)
    for (const segment of stream.segments.values()) {
      segment.windowIndex = null
    }

    const window: string[] = []
    for (const [windowIndex, descriptor] of descriptors.entries()) {
      const segment = this.ensureSegment(stream, descriptor.context, {
        duration: descriptor.duration,
        key: descriptor.key,
        start: descriptor.start,
      })
      segment.windowIndex = windowIndex
      window.push(segment.key)
    }
    stream.window = window
    this.pruneInactiveSegments(stream)
    this.deleteEmptyStream(previousStreamId)
    this.markChanged()
  }

  clearWindow(streamId: string | undefined): void {
    if (streamId === undefined || this.destroyed) {
      return
    }
    if (this.clearWindowState(streamId)) {
      this.deleteEmptyStream(streamId)
      this.markChanged()
    }
  }

  startReading(context: FragmentLoaderContext): void {
    if (this.destroyed) {
      throw new Error('ParallelSegmentLoader 已经销毁')
    }
    const stream = this.requireStream(createStreamId(context))
    const segment = this.ensureSegment(stream, context)
    segment.readerCount += 1
    if (segment.failure !== undefined) {
      segment.failure = undefined
      segment.state = 'empty'
      for (const chunk of segment.chunks) {
        if (chunk.state === 'failed') {
          chunk.failure = undefined
          chunk.state = 'empty'
        }
      }
    }
    this.markChanged()
  }

  stopReading(context: FragmentLoaderContext): void {
    const located = this.locateSegment(context)
    if (located === undefined) {
      return
    }
    const { segment, stream } = located
    segment.readerCount = Math.max(0, segment.readerCount - 1)
    this.pruneInactiveSegments(stream)
    this.deleteEmptyStream(stream.id)
    this.markChanged()
  }

  inspectSegment(context: FragmentLoaderContext): SegmentObservation {
    if (this.destroyed) {
      return {
        failure: createFailure('ParallelSegmentLoader 已经销毁'),
        state: 'failed',
        stats: createLoaderStats(),
      }
    }
    const located = this.locateSegment(context)
    if (located === undefined) {
      return {
        failure: createFailure('读取的 Segment 不存在'),
        state: 'failed',
        stats: createLoaderStats(),
      }
    }
    const { segment } = located
    const stats = cloneLoaderStats(segment.stats)
    if (segment.result !== undefined && segment.lastResponse !== null) {
      return {
        code: segment.code,
        data: segment.result,
        response: segment.lastResponse,
        state: 'ready',
        stats,
        url: segment.finalUrl,
      }
    }
    if (segment.failure !== undefined) {
      return { failure: segment.failure, state: 'failed', stats }
    }
    return { state: 'pending', stats }
  }

  waitForChange(afterRevision: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) {
      return Promise.reject(signal.reason)
    }
    if (this.destroyed || this.revision !== afterRevision) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      const cleanup = () => {
        this.listeners.delete(listener)
        signal.removeEventListener('abort', abort)
      }
      const listener = () => {
        cleanup()
        resolve()
      }
      const abort = () => {
        cleanup()
        reject(signal.reason)
      }
      this.listeners.add(listener)
      signal.addEventListener('abort', abort, { once: true })
      if (this.destroyed || this.revision !== afterRevision) {
        listener()
      }
    })
  }

  getDiagnostics(): ParallelSegmentLoaderDiagnostics {
    return {
      activeRequests: this.workers.filter(worker => worker.state === 'loading').length,
      destroyed: this.destroyed,
      maxConcurrency: this.maxConcurrency,
      revision: this.revision,
      streams: [...this.streams.values()]
        .sort((left, right) => left.id.localeCompare(right.id))
        .map(stream => this.createStreamDiagnostics(stream)),
      timestamp: Date.now(),
      workers: this.workers.map(worker => this.createWorkerDiagnostics(worker)),
    }
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    ParallelSegmentLoader.owners.delete(this.fLoader)
    for (const worker of this.workers) {
      worker.controller.abort()
      worker.requestController?.abort()
      worker.state = 'stopped'
    }
    this.streams.clear()
    this.transport.destroy()
    this.markChanged()
  }

  private async runWorker(runtime: WorkerRuntime): Promise<void> {
    while (!this.destroyed && !runtime.controller.signal.aborted) {
      const revision = this.revision
      const work = this.takeNextChunk()
      if (work === undefined) {
        runtime.state = 'idle'
        try {
          await this.waitForChange(revision, runtime.controller.signal)
        } catch {
          break
        }
        continue
      }

      runtime.active = work
      runtime.startedAt = performance.now()
      runtime.state = 'loading'
      await this.fillChunk(runtime, work)
      runtime.active = undefined
      runtime.requestController = undefined
      runtime.startedAt = undefined
    }
    runtime.state = 'stopped'
  }

  private takeNextChunk(): ChunkWork | undefined {
    const candidate = this.selectBestChunk()
    if (candidate === undefined) {
      return undefined
    }
    const { chunk, segment } = candidate
    const fillId = ++this.nextFillId
    chunk.attempt += 1
    chunk.failure = undefined
    chunk.fillId = fillId
    chunk.state = 'filling'
    segment.state = 'filling'
    if (segment.stats.loading.start === 0) {
      segment.stats.loading.start = performance.now()
    }

    const requestStart = chunk.rangeEnabled
      ? segment.resourceStart + chunk.start
      : segment.resourceStart
    const requestEnd = chunk.rangeEnabled
      ? chunk.endExclusive === undefined
        ? undefined
        : segment.resourceStart + chunk.endExclusive
      : undefined
    this.markChanged()
    return {
      chunkKey: chunk.key,
      context: segment.context,
      fillId,
      rangeEnabled: chunk.rangeEnabled,
      requestEnd,
      requestStart,
      resourceLength:
        segment.length === undefined ? undefined : segment.resourceStart + segment.length,
      segmentKey: segment.key,
      streamId: segment.streamId,
    }
  }

  private async fillChunk(runtime: WorkerRuntime, work: ChunkWork): Promise<void> {
    const requestController = new AbortController()
    runtime.requestController = requestController
    let preempted = false
    const onWorkerAbort = () => requestController.abort(runtime.controller.signal.reason)
    const onChange = () => {
      if (!this.isFillCurrent(work)) {
        requestController.abort()
        return
      }
      if (this.shouldPreempt(runtime)) {
        preempted = true
        requestController.abort()
      }
    }
    runtime.controller.signal.addEventListener('abort', onWorkerAbort, { once: true })
    this.listeners.add(onChange)

    try {
      const result = await this.fetchChunk(work, requestController)
      this.completeChunk(work, result)
    } catch (cause) {
      if (requestController.signal.aborted) {
        if (isTimeoutAbort(requestController.signal.reason)) {
          this.failChunk(work, createFailure('Chunk 请求超时'))
        } else {
          this.releaseChunk(work, preempted)
        }
      } else {
        this.failChunk(work, toFailure(cause))
      }
    } finally {
      runtime.controller.signal.removeEventListener('abort', onWorkerAbort)
      this.listeners.delete(onChange)
    }
  }

  private async fetchChunk(
    work: ChunkWork,
    controller: AbortController,
  ): Promise<ChunkFetchResult> {
    const config = this.config
    if (config === undefined) {
      throw new Error('ParallelSegmentLoader 尚未取得 hls.js 配置')
    }
    const headers = new Headers(work.context.headers)
    if (work.rangeEnabled && work.requestEnd !== undefined) {
      headers.set('range', `bytes=${work.requestStart}-${work.requestEnd - 1}`)
    } else {
      headers.delete('range')
    }
    const requestContext: FragmentLoaderContext = {
      ...work.context,
      headers: Object.fromEntries(headers.entries()),
      rangeEnd: work.rangeEnabled ? (work.requestEnd ?? 0) : 0,
      rangeStart: work.rangeEnabled ? work.requestStart : 0,
    }
    const init: RequestInit = {
      credentials: 'same-origin',
      headers,
      method: 'GET',
      mode: 'cors',
      signal: controller.signal,
    }
    const request =
      (await config.fetchSetup?.(requestContext, init)) ?? new Request(work.context.url, init)
    if (!this.isFillCurrent(work)) {
      throw new DOMException('Chunk Fill 已经失效', 'AbortError')
    }

    const timeoutMs = config.fragLoadPolicy.default.maxLoadTimeMs
    const timer =
      Number.isFinite(timeoutMs) && timeoutMs > 0
        ? globalThis.setTimeout(() => {
            if (!controller.signal.aborted) {
              controller.abort(new DOMException('Chunk 请求超时', 'TimeoutError'))
            }
          }, timeoutMs)
        : undefined
    try {
      const transportResponse = await this.transport.request(request)
      const firstByteAt = performance.now()
      if (!transportResponse.ok) {
        const response = createNetworkDetails(transportResponse)
        throw new ChunkRequestFailure(
          `HTTP ${transportResponse.status} ${transportResponse.statusText}`.trim(),
          transportResponse.status,
          response,
        )
      }
      const data = new Uint8Array(await transportResponse.arrayBuffer())
      const response = await this.createReadableRangeResponse(
        work,
        request,
        transportResponse,
        data.byteLength,
        controller.signal,
      )
      return { data, firstByteAt, response, url: transportResponse.url }
    } catch (cause) {
      if (cause instanceof ChunkRequestFailure || controller.signal.aborted) {
        throw cause
      }
      throw new ChunkRequestFailure(cause instanceof Error ? cause.message : 'Transport 请求失败')
    } finally {
      if (timer !== undefined) {
        globalThis.clearTimeout(timer)
      }
    }
  }

  private async createReadableRangeResponse(
    work: ChunkWork,
    request: Request,
    response: HttpTransportResponse,
    receivedBytes: number,
    signal: AbortSignal,
  ): Promise<Response> {
    if (
      response.status !== 206 ||
      response.headers.has('content-range') ||
      !work.rangeEnabled ||
      work.requestEnd === undefined
    ) {
      return createNetworkDetails(response)
    }

    const requestedBytes = work.requestEnd - work.requestStart
    let resourceLength: number | undefined
    if (receivedBytes < requestedBytes) {
      resourceLength = work.requestStart + receivedBytes
    } else if (work.resourceLength !== undefined) {
      resourceLength = work.resourceLength
    } else {
      resourceLength = await this.discoverResourceLength(request, signal)
    }
    if (resourceLength === undefined || receivedBytes <= 0) {
      return createNetworkDetails(response)
    }

    const endInclusive = work.requestStart + receivedBytes - 1
    if (resourceLength <= endInclusive) {
      resourceLength = endInclusive + 1
    }
    const headers = new Headers(response.headers)
    headers.set('content-range', `bytes ${work.requestStart}-${endInclusive}/${resourceLength}`)
    return new Response(null, {
      headers,
      status: response.status,
      statusText: response.statusText,
    })
  }

  private async discoverResourceLength(
    request: Request,
    signal: AbortSignal,
  ): Promise<number | undefined> {
    const headers = new Headers(request.headers)
    headers.delete('range')
    const headRequest = new Request(request.url, {
      cache: request.cache,
      credentials: request.credentials,
      headers,
      method: 'HEAD',
      mode: request.mode,
      redirect: request.redirect,
      signal,
    })
    try {
      const response = await this.transport.request(headRequest)
      const contentLength = Number.parseInt(response.headers.get('content-length') ?? '', 10)
      return response.ok && Number.isSafeInteger(contentLength) && contentLength > 0
        ? contentLength
        : undefined
    } catch (cause) {
      if (signal.aborted) {
        throw cause
      }
      return undefined
    }
  }

  private completeChunk(work: ChunkWork, result: ChunkFetchResult): void {
    const located = this.locateChunk(work)
    if (located === undefined) {
      return
    }
    const { chunk, segment } = located
    const response = result.response
    const validator = response.headers.get('etag') ?? response.headers.get('last-modified')
    if (segment.validator === null) {
      segment.validator = validator
    } else if (validator !== null && validator !== segment.validator) {
      this.setSegmentFailure(segment, createFailure('Segment 资源标识在 Range 请求之间发生变化'))
      this.markChanged()
      return
    }

    segment.code = response.status
    segment.finalUrl = result.url || response.url || segment.finalUrl
    segment.lastResponse = response
    if (segment.stats.loading.first === 0) {
      segment.stats.loading.first = result.firstByteAt
    }

    if (response.status === 200) {
      if (segment.declaredRange || (chunk.rangeEnabled && chunk.start !== 0)) {
        this.setSegmentFailure(
          segment,
          createFailure('服务器忽略了带边界的 Range 请求', 200, response),
        )
        this.markChanged()
        return
      }
      segment.sequential = true
      segment.length = result.data.byteLength
      segment.chunks = [chunk]
      chunk.start = 0
      chunk.endExclusive = result.data.byteLength
      chunk.rangeEnabled = false
      this.acceptChunkData(segment, chunk, result.data)
      this.finishSegmentIfReady(segment)
      this.markChanged()
      return
    }

    if (response.status !== 206 || !chunk.rangeEnabled) {
      this.setSegmentFailure(
        segment,
        createFailure(`Chunk 请求返回了 HTTP ${response.status}`, response.status, response),
      )
      this.markChanged()
      return
    }

    let contentRange: ParsedContentRange | undefined
    try {
      contentRange = parseContentRange(response.headers.get('content-range'))
    } catch (cause) {
      this.setSegmentFailure(segment, toFailure(cause))
      this.markChanged()
      return
    }
    if (
      contentRange === undefined ||
      (segment.length === undefined && contentRange.total === undefined)
    ) {
      if (!segment.declaredRange && !segment.fallbackAttempted) {
        this.fallbackToSequential(segment)
      } else {
        this.setSegmentFailure(segment, createFailure('Range 响应缺少可用的 Content-Range'))
      }
      this.markChanged()
      return
    }

    const expectedStart = segment.resourceStart + chunk.start
    if (contentRange.start !== expectedStart) {
      this.setSegmentFailure(
        segment,
        createFailure(`Content-Range 起点错误, 期望 ${expectedStart}, 实际 ${contentRange.start}`),
      )
      this.markChanged()
      return
    }
    const responseLength = contentRange.endExclusive - contentRange.start
    if (result.data.byteLength !== responseLength) {
      this.setSegmentFailure(
        segment,
        createFailure(`Chunk 长度错误, 期望 ${responseLength}, 实际 ${result.data.byteLength}`),
      )
      this.markChanged()
      return
    }
    const expectedEnd =
      work.requestEnd === undefined || contentRange.total === undefined || segment.declaredRange
        ? work.requestEnd
        : Math.min(work.requestEnd, contentRange.total)
    if (expectedEnd !== undefined && contentRange.endExclusive !== expectedEnd) {
      this.setSegmentFailure(
        segment,
        createFailure(
          `Content-Range 终点错误, 期望 ${expectedEnd}, 实际 ${contentRange.endExclusive}`,
        ),
      )
      this.markChanged()
      return
    }

    if (segment.length === undefined && contentRange.total !== undefined) {
      segment.length = contentRange.total - segment.resourceStart
    }
    const localEnd = contentRange.endExclusive - segment.resourceStart
    if (segment.length === undefined || localEnd > segment.length) {
      this.setSegmentFailure(segment, createFailure('Chunk 超出了 Segment 边界'))
      this.markChanged()
      return
    }
    chunk.endExclusive = localEnd
    this.acceptChunkData(segment, chunk, result.data)
    this.ensureChunks(segment)
    this.finishSegmentIfReady(segment)
    this.markChanged()
  }

  private failChunk(work: ChunkWork, failure: SegmentLoadFailure): void {
    const located = this.locateChunk(work)
    if (located === undefined) {
      return
    }
    this.setSegmentFailure(located.segment, failure)
    this.markChanged()
  }

  private releaseChunk(work: ChunkWork, preempted: boolean): void {
    const located = this.locateChunk(work)
    if (located === undefined) {
      return
    }
    const { chunk, segment } = located
    chunk.fillId = undefined
    chunk.state = 'empty'
    segment.state = segment.chunks.some(candidate => candidate.state === 'filling')
      ? 'filling'
      : 'empty'
    if (!preempted && !this.isSegmentAlive(segment)) {
      const stream = this.streams.get(segment.streamId)
      stream?.segments.delete(segment.key)
      this.deleteEmptyStream(segment.streamId)
    }
    this.markChanged()
  }

  private acceptChunkData(
    segment: VirtualStreamSegment,
    chunk: VirtualStreamChunk,
    data: Uint8Array,
  ): void {
    chunk.data = data
    chunk.failure = undefined
    chunk.fillId = undefined
    chunk.loadedBytes = data.byteLength
    chunk.state = 'ready'
    segment.failure = undefined
    this.updateSegmentStats(segment)
  }

  private finishSegmentIfReady(segment: VirtualStreamSegment): void {
    if (
      segment.length === undefined ||
      segment.chunks.length === 0 ||
      segment.chunks.some(chunk => chunk.state !== 'ready' || chunk.endExclusive === undefined)
    ) {
      segment.state = 'filling'
      return
    }

    const result = new Uint8Array(segment.length)
    let cursor = 0
    for (const chunk of [...segment.chunks].sort((left, right) => left.start - right.start)) {
      if (chunk.start !== cursor || chunk.data === undefined || chunk.endExclusive === undefined) {
        this.setSegmentFailure(segment, createFailure('Segment Chunk 存在空洞'))
        return
      }
      result.set(chunk.data, chunk.start)
      cursor = chunk.endExclusive
    }
    if (cursor !== segment.length) {
      this.setSegmentFailure(segment, createFailure('Segment Chunk 没有覆盖完整数据'))
      return
    }

    segment.result = result.buffer
    segment.state = 'ready'
    segment.stats.loading.end = performance.now()
    this.updateSegmentStats(segment)
    for (const chunk of segment.chunks) {
      chunk.data = undefined
    }
  }

  private fallbackToSequential(segment: VirtualStreamSegment): void {
    segment.fallbackAttempted = true
    segment.sequential = true
    segment.length = undefined
    segment.failure = undefined
    segment.state = 'empty'
    segment.chunks = [this.createChunk(segment, 0, undefined, false, 0)]
  }

  private setSegmentFailure(segment: VirtualStreamSegment, failure: SegmentLoadFailure): void {
    segment.failure = failure
    segment.state = 'failed'
    segment.stats.loading.end = performance.now()
    for (const chunk of segment.chunks) {
      if (chunk.state === 'filling') {
        chunk.failure = failure.message
        chunk.fillId = undefined
        chunk.state = 'failed'
      }
    }
  }

  private updateSegmentStats(segment: VirtualStreamSegment): void {
    const loaded = segment.chunks.reduce((total, chunk) => total + chunk.loadedBytes, 0)
    segment.stats.loaded = loaded
    segment.stats.total = segment.length ?? 0
    segment.stats.chunkCount = segment.chunks.filter(chunk => chunk.state === 'ready').length
    const end = segment.stats.loading.end || performance.now()
    const elapsed = end - segment.stats.loading.start
    segment.stats.bwEstimate = elapsed > 0 ? (loaded * 8 * 1000) / elapsed : 0
  }

  private selectBestChunk():
    | { chunk: VirtualStreamChunk; segment: VirtualStreamSegment }
    | undefined {
    const candidates: { chunk: VirtualStreamChunk; segment: VirtualStreamSegment }[] = []
    for (const stream of this.streams.values()) {
      for (const segment of stream.segments.values()) {
        if (
          !this.isSegmentAlive(segment) ||
          segment.failure !== undefined ||
          segment.result !== undefined
        ) {
          continue
        }
        for (const chunk of segment.chunks) {
          if (chunk.state === 'empty') {
            candidates.push({ chunk, segment })
          }
        }
      }
    }
    candidates.sort(compareChunkCandidates)
    return candidates[0]
  }

  private shouldPreempt(runtime: WorkerRuntime): boolean {
    const urgent = this.selectBestChunk()
    if (urgent === undefined || urgent.segment.readerCount === 0) {
      return false
    }
    const active = runtime.active === undefined ? undefined : this.locateChunk(runtime.active)
    if (active === undefined || active.segment.readerCount > 0) {
      return false
    }

    const victims = this.workers
      .map(worker => ({
        located: worker.active === undefined ? undefined : this.locateChunk(worker.active),
        worker,
      }))
      .filter(
        (
          entry,
        ): entry is {
          located: { chunk: VirtualStreamChunk; segment: VirtualStreamSegment }
          worker: WorkerRuntime
        } => entry.located !== undefined && entry.located.segment.readerCount === 0,
      )
      .sort((left, right) => compareChunkCandidates(right.located, left.located))
    const victim = victims[0]
    return victim?.worker === runtime && compareChunkCandidates(urgent, active) < 0
  }

  private isFillCurrent(work: ChunkWork): boolean {
    return this.locateChunk(work) !== undefined
  }

  private locateChunk(
    work: Pick<ChunkWork, 'chunkKey' | 'fillId' | 'segmentKey' | 'streamId'>,
  ): { chunk: VirtualStreamChunk; segment: VirtualStreamSegment } | undefined {
    const segment = this.streams.get(work.streamId)?.segments.get(work.segmentKey)
    const chunk = segment?.chunks.find(candidate => candidate.key === work.chunkKey)
    return chunk?.state === 'filling' && chunk.fillId === work.fillId && segment !== undefined
      ? { chunk, segment }
      : undefined
  }

  private ensureSegment(
    stream: VirtualStream,
    context: FragmentLoaderContext,
    overrides?: { duration: number; key: string; start: number },
  ): VirtualStreamSegment {
    const key = overrides?.key ?? createSegmentKey(context)
    let segment = stream.segments.get(key)
    if (segment !== undefined) {
      segment.context = cloneContext(context)
      if (overrides !== undefined) {
        segment.duration = overrides.duration
        segment.start = overrides.start
      }
      return segment
    }

    const rangeStart = context.rangeStart ?? 0
    const rangeEnd = context.rangeEnd ?? 0
    const declaredRange = rangeEnd > rangeStart
    segment = {
      chunks: [],
      code: 0,
      context: cloneContext(context),
      declaredRange,
      duration: overrides?.duration ?? context.part?.duration ?? context.frag.duration,
      failure: undefined,
      fallbackAttempted: false,
      finalUrl: context.url,
      key,
      lastResponse: null,
      length: declaredRange ? rangeEnd - rangeStart : undefined,
      readerCount: 0,
      resourceStart: declaredRange ? rangeStart : 0,
      result: undefined,
      sequential: false,
      start: overrides?.start ?? context.part?.start ?? context.frag.start,
      state: 'empty',
      stats: createLoaderStats(),
      streamId: stream.id,
      validator: null,
      windowIndex: null,
    }
    stream.segments.set(key, segment)
    this.ensureChunks(segment)
    return segment
  }

  private ensureChunks(segment: VirtualStreamSegment): void {
    const ranges =
      segment.length === undefined
        ? [{ endExclusive: this.chunkSize, start: 0 }]
        : splitByteRanges(0, segment.length, this.chunkSize)
    for (const [index, range] of ranges.entries()) {
      const existing = segment.chunks.find(chunk => chunk.start === range.start)
      if (existing === undefined) {
        segment.chunks.push(this.createChunk(segment, range.start, range.endExclusive, true, index))
      } else {
        existing.endExclusive = range.endExclusive
        existing.index = index
      }
    }
    segment.chunks.sort((left, right) => left.start - right.start)
  }

  private createChunk(
    segment: VirtualStreamSegment,
    start: number,
    endExclusive: number | undefined,
    rangeEnabled: boolean,
    index: number,
  ): VirtualStreamChunk {
    return {
      attempt: 0,
      data: undefined,
      endExclusive,
      failure: undefined,
      fillId: undefined,
      index,
      key: `${segment.key}\nchunk:${start}`,
      loadedBytes: 0,
      rangeEnabled,
      start,
      state: 'empty',
    }
  }

  private requireStream(streamId: string): VirtualStream {
    let stream = this.streams.get(streamId)
    if (stream === undefined) {
      stream = { id: streamId, segments: new Map(), window: [] }
      this.streams.set(streamId, stream)
    }
    return stream
  }

  private locateSegment(
    context: FragmentLoaderContext,
  ): { segment: VirtualStreamSegment; stream: VirtualStream } | undefined {
    const stream = this.streams.get(createStreamId(context))
    const segment = stream?.segments.get(createSegmentKey(context))
    return stream === undefined || segment === undefined ? undefined : { segment, stream }
  }

  private clearWindowState(streamId: string): boolean {
    const stream = this.streams.get(streamId)
    if (stream === undefined || stream.window.length === 0) {
      return false
    }
    stream.window = []
    for (const segment of stream.segments.values()) {
      segment.windowIndex = null
    }
    this.pruneInactiveSegments(stream)
    return true
  }

  private pruneInactiveSegments(stream: VirtualStream): void {
    for (const segment of stream.segments.values()) {
      if (!this.isSegmentAlive(segment)) {
        stream.segments.delete(segment.key)
      }
    }
  }

  private isSegmentAlive(segment: VirtualStreamSegment): boolean {
    return segment.windowIndex !== null || segment.readerCount > 0
  }

  private deleteEmptyStream(streamId: string | undefined): void {
    if (streamId === undefined) {
      return
    }
    const stream = this.streams.get(streamId)
    if (stream?.window.length === 0 && stream.segments.size === 0) {
      this.streams.delete(streamId)
    }
  }

  private markChanged(): void {
    this.revision += 1
    if (this.notificationScheduled) {
      return
    }
    this.notificationScheduled = true
    queueMicrotask(() => {
      this.notificationScheduled = false
      for (const listener of [...this.listeners]) {
        listener()
      }
    })
  }

  private createStreamDiagnostics(stream: VirtualStream): VirtualStreamDiagnostics {
    return {
      id: stream.id,
      segments: [...stream.segments.values()]
        .sort((left, right) => left.start - right.start || left.key.localeCompare(right.key))
        .map(segment => this.createSegmentDiagnostics(segment)),
      window: [...stream.window],
    }
  }

  private createSegmentDiagnostics(segment: VirtualStreamSegment): SegmentDiagnostics {
    return {
      chunks: segment.chunks.map(chunk => this.createChunkDiagnostics(chunk)),
      duration: segment.duration,
      httpStatus: segment.code,
      key: segment.key,
      loadedBytes: segment.stats.loaded,
      readerCount: segment.readerCount,
      sequential: segment.sequential,
      start: segment.start,
      state: segment.state as DiagnosticSegmentState,
      totalBytes: segment.length,
      url: segment.finalUrl,
      windowIndex: segment.windowIndex,
    }
  }

  private createChunkDiagnostics(chunk: VirtualStreamChunk): ChunkDiagnostics {
    return {
      attempt: chunk.attempt,
      endExclusive: chunk.endExclusive,
      failure: chunk.failure,
      fillId: chunk.fillId,
      key: chunk.key,
      loadedBytes: chunk.loadedBytes,
      start: chunk.start,
      state: chunk.state as DiagnosticChunkState,
    }
  }

  private createWorkerDiagnostics(worker: WorkerRuntime): WorkerDiagnostics {
    return {
      chunkKey: worker.active?.chunkKey,
      id: worker.id,
      requestEnd: worker.active?.requestEnd,
      requestStart: worker.active?.requestStart,
      segmentKey: worker.active?.segmentKey,
      startedAt: worker.startedAt,
      state: worker.state,
      streamId: worker.active?.streamId,
    }
  }
}

class ChunkRequestFailure extends Error {
  readonly code: number
  readonly response: Response | null

  constructor(message: string, code = 0, response: Response | null = null) {
    super(message)
    this.name = 'ChunkRequestFailure'
    this.code = code
    this.response = response
  }
}

interface ParsedContentRange {
  endExclusive: number
  start: number
  total: number | undefined
}

function parseContentRange(value: string | null): ParsedContentRange | undefined {
  if (value === null) {
    return undefined
  }
  const match = contentRangePattern.exec(value.trim())
  if (match === null) {
    throw new Error(`无效的 Content-Range: ${value}`)
  }
  const start = Number.parseInt(match[1] ?? '', 10)
  const end = Number.parseInt(match[2] ?? '', 10)
  const totalText = match[3]
  const total =
    totalText === undefined || totalText === '*' ? undefined : Number.parseInt(totalText, 10)
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    end < start ||
    (total !== undefined && (!Number.isSafeInteger(total) || total <= end))
  ) {
    throw new Error(`无效的 Content-Range: ${value}`)
  }
  return { endExclusive: end + 1, start, total }
}

function compareChunkCandidates(
  left: { chunk: VirtualStreamChunk; segment: VirtualStreamSegment },
  right: { chunk: VirtualStreamChunk; segment: VirtualStreamSegment },
): number {
  const leftDirect = left.segment.readerCount > 0
  const rightDirect = right.segment.readerCount > 0
  if (leftDirect !== rightDirect) {
    return leftDirect ? -1 : 1
  }
  const leftWindow = left.segment.windowIndex ?? Number.MAX_SAFE_INTEGER
  const rightWindow = right.segment.windowIndex ?? Number.MAX_SAFE_INTEGER
  return (
    leftWindow - rightWindow ||
    left.chunk.index - right.chunk.index ||
    left.chunk.key.localeCompare(right.chunk.key)
  )
}

function createStreamId(context: FragmentLoaderContext): string {
  return `${context.frag.type}:${context.frag.level}`
}

export function createSegmentKey(context: FragmentLoaderContext): string {
  return [
    context.frag.type,
    context.frag.level,
    context.frag.sn,
    context.frag.cc,
    context.part?.index ?? 'segment',
    context.url,
    context.rangeStart ?? 0,
    context.rangeEnd ?? 0,
  ].join('\n')
}

export function createWindowDescriptor(fragment: Fragment): SegmentWindowDescriptor {
  const context: FragmentLoaderContext = {
    frag: fragment,
    headers: {},
    part: null,
    rangeEnd: fragment.byteRangeEndOffset ?? 0,
    rangeStart: fragment.byteRangeStartOffset ?? 0,
    responseType: 'arraybuffer',
    type: 'media-fragment' as FragmentLoaderContext['type'],
    url: fragment.url,
  }
  return {
    context,
    duration: fragment.duration,
    key: createSegmentKey(context),
    start: fragment.start,
  }
}

function cloneContext(context: FragmentLoaderContext): FragmentLoaderContext {
  return {
    ...context,
    headers: { ...(context.headers ?? {}) },
  }
}

function createFailure(
  message: string,
  code = 0,
  response: Response | null = null,
): SegmentLoadFailure {
  return { code, message, response }
}

function toFailure(cause: unknown): SegmentLoadFailure {
  if (cause instanceof ChunkRequestFailure) {
    return createFailure(cause.message, cause.code, cause.response)
  }
  return createFailure(cause instanceof Error ? cause.message : '未知 Chunk 加载错误')
}

function requirePositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} 必须是正整数`)
  }
}

function isTimeoutAbort(reason: unknown): boolean {
  return reason instanceof DOMException && reason.name === 'TimeoutError'
}

function createNetworkDetails(response: HttpTransportResponse): Response {
  return new Response(null, {
    headers: response.headers,
    status: response.status,
    statusText: response.statusText,
  })
}
