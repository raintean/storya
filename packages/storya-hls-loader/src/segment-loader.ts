import type {
  FragmentLoaderContext,
  HlsConfig,
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderResponse,
  LoaderStats,
} from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import { splitByteRanges } from './byte-ranges'
import type { HlsLoaderDiagnosticChunk, HlsLoaderDiagnosticChunkState } from './diagnostics'
import type { HlsLoaderAbortEvent, HlsLoaderEventHandler } from './events'
import { RequestScheduler } from './scheduler'
import type { ScheduledRequest } from './scheduler'
import { createLoaderStats } from './stats'

export interface SegmentLoaderOptions {
  chunkSize: number
  finishingRatio: number
  finishingRemainingMs: number
  idleTimeoutMs: number
  maxLookAheadBytes: number
  maxRescueAttempts: number
  minSlowThroughputSamples: number
  minRequestLifetimeMs: number
  onEvent: HlsLoaderEventHandler
  slowThroughputRatio: number
  slowThroughputWindowMs: number
}

interface Attempt {
  badSince: number | undefined
  bytes: number
  controller: AbortController
  discardBytes: number
  id: number
  loadTimer: number | undefined
  requestEnd: number | undefined
  requestStart: number
  response: HttpTransportResponse | null
  startedAt: number
  trafficTimer: number | undefined
  ttfbTimer: number | undefined
}

interface ContentRange {
  endExclusive: number
  start: number
  total: number | undefined
}

interface SlowConnectionMetrics {
  baselineThroughputBytesPerSecond: number
  throughputBytesPerSecond: number
}

type AttemptStopReason = 'complete' | 'failure' | 'preempted' | 'slow'
type RangeMode = 'probing' | 'supported' | 'unsupported'

const contentRangePattern = /^bytes (\d+)-(\d+)\/(\d+|\*)$/i
const hardDemandPriorityBoostMs = 1_000_000_000
const progressiveHighWaterMark = 128 * 1024

let nextChunkId = 0

class TransportError extends Error {
  readonly code: number
  readonly timeout: boolean

  constructor(message: string, code = 0, timeout = false) {
    super(message)
    this.name = 'TransportError'
    this.code = code
    this.timeout = timeout
  }
}

export class SegmentLoader implements Loader<FragmentLoaderContext> {
  context: FragmentLoaderContext | null = null
  stats: LoaderStats = createLoaderStats()

  private coordinator: SegmentLoadCoordinator | null = null
  private readonly hlsConfig: HlsConfig
  private readonly isHardDemanded: () => boolean
  private readonly onStats: (stats: LoaderStats) => void
  private readonly options: SegmentLoaderOptions
  private readonly scheduler: RequestScheduler
  private readonly transport: HttpTransport

  constructor(
    hlsConfig: HlsConfig,
    scheduler: RequestScheduler,
    options: SegmentLoaderOptions,
    transport: HttpTransport,
    onStats: (stats: LoaderStats) => void = () => undefined,
    isHardDemanded: () => boolean = () => false,
  ) {
    this.hlsConfig = hlsConfig
    this.scheduler = scheduler
    this.options = options
    this.transport = transport
    this.onStats = onStats
    this.isHardDemanded = isHardDemanded
  }

  load(
    context: FragmentLoaderContext,
    config: LoaderConfiguration,
    callbacks: LoaderCallbacks<FragmentLoaderContext>,
  ): void {
    if (this.coordinator !== null) {
      throw new Error('SegmentLoader 的实例只能加载一次')
    }

    this.context = context
    this.coordinator = new SegmentLoadCoordinator(
      this.hlsConfig,
      this.scheduler,
      this.options,
      this.transport,
      context,
      config,
      callbacks,
      this.stats,
      this.onStats,
      this.isHardDemanded,
    )
    this.coordinator.start()
  }

  abort(): void {
    this.coordinator?.abort(true)
  }

  destroy(): void {
    this.coordinator?.abort(false)
    this.coordinator = null
    this.context = null
  }

  getCacheAge(): number | null {
    const age = this.coordinator?.getResponseHeader('age')
    return age === null || age === undefined ? null : Number.parseFloat(age)
  }

  getResponseHeader(name: string): string | null {
    return this.coordinator?.getResponseHeader(name) ?? null
  }

  getDiagnostics(): HlsLoaderDiagnosticChunk[] {
    return this.coordinator?.getDiagnostics() ?? []
  }
}

class SegmentLoadCoordinator {
  readonly callbacks: LoaderCallbacks<FragmentLoaderContext>
  readonly context: FragmentLoaderContext
  readonly loaderConfig: LoaderConfiguration
  readonly options: SegmentLoaderOptions
  readonly scheduler: RequestScheduler
  readonly stats: LoaderStats

  private aborted = false
  private completed = false
  private deliveredOffset = 0
  private finalUrl: string
  private firstTask: ChunkLoadTask | null = null
  private lastResponse: HttpTransportResponse | null = null
  private logicalTimer: number | undefined
  private pendingProgress: Uint8Array[] = []
  private pendingProgressBytes = 0
  private planned = false
  private rangeMode: RangeMode
  private readonly resultBuffers: Uint8Array[] = []
  private readonly resourceStart: number
  private segmentLength: number | undefined
  private readonly startTime: number
  private readonly tasks: ChunkLoadTask[] = []
  private validator: string | null = null
  private wireBytes = 0
  private readonly hlsConfig: HlsConfig
  private readonly isHardDemanded: () => boolean
  private readonly onStats: (stats: LoaderStats) => void
  private readonly transport: HttpTransport

  constructor(
    hlsConfig: HlsConfig,
    scheduler: RequestScheduler,
    options: SegmentLoaderOptions,
    transport: HttpTransport,
    context: FragmentLoaderContext,
    loaderConfig: LoaderConfiguration,
    callbacks: LoaderCallbacks<FragmentLoaderContext>,
    stats: LoaderStats,
    onStats: (stats: LoaderStats) => void,
    isHardDemanded: () => boolean,
  ) {
    this.hlsConfig = hlsConfig
    this.scheduler = scheduler
    this.options = options
    this.transport = transport
    this.context = context
    this.loaderConfig = loaderConfig
    this.callbacks = callbacks
    this.stats = stats
    this.onStats = onStats
    this.isHardDemanded = isHardDemanded
    this.finalUrl = context.url
    this.startTime = performance.now()
    this.stats.loading.start = this.startTime
    this.notifyStats()
    const rangeStart = context.rangeStart ?? 0
    const rangeEnd = context.rangeEnd ?? 0
    if (rangeEnd > rangeStart) {
      this.resourceStart = rangeStart
      this.segmentLength = rangeEnd - rangeStart
      this.stats.total = this.segmentLength
      this.rangeMode = 'probing'
    } else {
      this.resourceStart = 0
      this.rangeMode = this.isAtomicRequest() ? 'unsupported' : 'probing'
    }
  }

  start(): void {
    const maxLoadTimeMs = this.loaderConfig.loadPolicy.maxLoadTimeMs
    if (Number.isFinite(maxLoadTimeMs) && maxLoadTimeMs > 0) {
      this.logicalTimer = globalThis.setTimeout(() => {
        this.fail(new TransportError(`Segment 加载超过 ${maxLoadTimeMs}ms`, 0, true))
      }, maxLoadTimeMs)
    }

    this.initialize()
  }

  private initialize(): void {
    if (this.isAtomicRequest()) {
      this.startAtomicTask()
      return
    }
    this.startFirstRangeTask()
  }

  private startAtomicTask(): void {
    const task = this.createTask(0, this.segmentLength, this.segmentLength !== undefined)
    this.firstTask = task
    this.planned = true
    this.scheduler.add(task)
  }

  private startFirstRangeTask(): void {
    const firstEnd =
      this.segmentLength === undefined
        ? this.options.chunkSize
        : Math.min(this.options.chunkSize, this.segmentLength)
    const task = this.createTask(0, firstEnd, true)
    this.firstTask = task
    this.scheduler.add(task)
  }

  abort(notify: boolean): void {
    if (this.aborted || this.completed) {
      return
    }

    this.aborted = true
    this.clearLogicalTimer()
    for (const task of this.tasks) {
      task.cancel()
      this.scheduler.remove(task)
    }
    this.pendingProgress = []
    this.pendingProgressBytes = 0
    this.resultBuffers.length = 0
    this.stats.aborted = true
    this.stats.loading.end = performance.now()
    this.notifyStats()

    if (notify) {
      this.callbacks.onAbort?.(this.stats, this.context, this.lastResponse)
    }
  }

  canTaskRun(task: ChunkLoadTask): boolean {
    if (this.aborted || this.completed) {
      return false
    }
    if (task === this.firstTask || this.segmentLength === undefined) {
      return true
    }
    return task.startOffset < this.deliveredOffset + this.options.maxLookAheadBytes
  }

  getResponseHeader(name: string): string | null {
    return this.lastResponse?.headers.get(name) ?? null
  }

  getDiagnostics(): HlsLoaderDiagnosticChunk[] {
    return this.tasks.map(task => task.getDiagnostics())
  }

  getTaskPriority(task: ChunkLoadTask, playbackTime: number, playbackRate: number): number {
    if (this.context.frag.sn === 'initSegment') {
      return Number.NEGATIVE_INFINITY
    }

    const segmentStart = this.context.part?.start ?? this.context.frag.start
    const segmentDuration = this.context.part?.duration ?? this.context.frag.duration
    const fraction =
      this.segmentLength === undefined || this.segmentLength === 0
        ? 0
        : task.startOffset / this.segmentLength
    const estimatedMediaTime = segmentStart + segmentDuration * fraction
    const timeUntilPlaybackMs = ((estimatedMediaTime - playbackTime) * 1_000) / playbackRate
    const frontierBoost = task.containsOffset(this.deliveredOffset) ? 500 : 0
    const hardDemandBoost = this.isHardDemanded() ? hardDemandPriorityBoostMs : 0
    return timeUntilPlaybackMs - frontierBoost - hardDemandBoost
  }

  async createResponse(task: ChunkLoadTask, attempt: Attempt): Promise<HttpTransportResponse> {
    const headers = new Headers(this.context.headers)
    if (attempt.requestEnd !== undefined) {
      headers.set('Range', `bytes=${attempt.requestStart}-${attempt.requestEnd - 1}`)
    } else {
      headers.delete('Range')
    }

    const attemptContext: FragmentLoaderContext = {
      ...this.context,
      headers: Object.fromEntries(headers.entries()),
      rangeStart: attempt.requestStart,
      rangeEnd: attempt.requestEnd ?? 0,
    }
    const init: RequestInit = {
      method: 'GET',
      mode: 'cors',
      credentials: 'same-origin',
      headers,
      signal: attempt.controller.signal,
    }
    const configuredRequest = this.hlsConfig.fetchSetup?.(attemptContext, init)
    const request =
      configuredRequest === undefined
        ? new Request(this.context.url, init)
        : await configuredRequest
    return this.transport.request(
      request,
      attempt.requestEnd === undefined
        ? undefined
        : { maxResponseBytes: attempt.requestEnd - attempt.requestStart },
    )
  }

  async acceptResponse(
    task: ChunkLoadTask,
    attempt: Attempt,
    response: HttpTransportResponse,
  ): Promise<void> {
    if (!response.ok) {
      throw new TransportError(response.statusText || `HTTP ${response.status}`, response.status)
    }

    this.lastResponse = response
    this.finalUrl = response.url || this.finalUrl
    this.validateResourceIdentity(response)

    if (attempt.requestEnd === undefined) {
      this.acceptSequentialResponse(task, response)
      return
    }

    if (response.status === 200) {
      if (
        task === this.firstTask &&
        task.startOffset === 0 &&
        this.resourceStart === 0 &&
        (this.context.rangeEnd ?? 0) === 0
      ) {
        this.rangeMode = 'unsupported'
        this.planned = true
        task.disableRange()
        task.disablePreemption()
        const length = this.readContentLength(response)
        if (length !== undefined) {
          this.setSegmentLength(length)
          task.setEndOffset(length)
        } else {
          task.setEndOffset(undefined)
        }
        return
      }
      throw new TransportError('服务器忽略了 Range 请求', response.status)
    }

    if (response.status !== 206) {
      throw new TransportError(`Range 请求返回了 HTTP ${response.status}`, response.status)
    }

    let contentRange = this.parseContentRange(response)
    if (contentRange === undefined) {
      if (this.segmentLength === undefined) {
        const resourceLength = await this.probeResourceLength(attempt.controller.signal)
        this.setSegmentLength(resourceLength - this.resourceStart)
      }
      contentRange = this.inferContentRange(attempt, response)
    } else {
      if (contentRange.start !== attempt.requestStart) {
        throw new TransportError(
          `Content-Range 起点错误, 期望 ${attempt.requestStart}, 实际 ${contentRange.start}`,
          response.status,
        )
      }
      if (attempt.requestEnd !== undefined && contentRange.endExclusive > attempt.requestEnd) {
        throw new TransportError('Content-Range 超出了请求范围', response.status)
      }
      if (this.segmentLength === undefined && contentRange.total === undefined) {
        contentRange = {
          ...contentRange,
          total: await this.probeResourceLength(attempt.controller.signal),
        }
      }
    }

    if (this.segmentLength === undefined) {
      if (contentRange.total === undefined) {
        throw new TransportError('首个 Range 响应没有提供资源总长度', response.status)
      }
      this.setSegmentLength(contentRange.total - this.resourceStart)
    } else if (
      contentRange.total !== undefined &&
      this.resourceStart + this.segmentLength > contentRange.total
    ) {
      throw new TransportError('Segment 字节范围超出了资源总长度', response.status)
    }

    const responseLocalEnd = contentRange.endExclusive - this.resourceStart
    if (task.endOffset === undefined || responseLocalEnd < task.endOffset) {
      task.setEndOffset(responseLocalEnd)
    }
    this.rangeMode = 'supported'

    if (task === this.firstTask && !this.planned) {
      this.planRemainingTasks(task.endOffset ?? responseLocalEnd)
    }
  }

  acceptData(task: ChunkLoadTask, data: Uint8Array, response: HttpTransportResponse): void {
    if (this.aborted || this.completed || data.byteLength === 0) {
      return
    }

    const now = performance.now()
    if (this.stats.loading.first === 0) {
      this.stats.loading.first = now
    }
    this.stats.loaded += data.byteLength
    this.wireBytes += data.byteLength
    this.stats.bwEstimate =
      this.stats.loaded > 0 && now > this.startTime
        ? (this.stats.loaded * 8_000) / (now - this.startTime)
        : 0
    this.notifyStats()
    this.lastResponse = response
    task.append(data)
    this.flushContiguousData()
  }

  markResponseStarted(response: HttpTransportResponse): void {
    const now = performance.now()
    if (this.stats.loading.first === 0) {
      this.stats.loading.first = now
    }
    this.lastResponse = response
    this.notifyStats()
  }

  taskCompleted(task: ChunkLoadTask): void {
    if (this.aborted || this.completed) {
      return
    }

    if (task.endOffset === undefined) {
      const endOffset = task.startOffset + task.receivedBytes
      task.setEndOffset(endOffset)
      if (task === this.firstTask) {
        this.setSegmentLength(endOffset)
        this.planned = true
      }
    }

    this.flushContiguousData()
    this.maybeComplete()
  }

  taskFailed(error: TransportError): void {
    this.fail(error)
  }

  reportAttempt(bytes: number, durationMs: number): void {
    this.scheduler.reportThroughput(bytes, durationMs)
  }

  notifyStats(): void {
    this.onStats(this.stats)
  }

  emitEvent(event: HlsLoaderAbortEvent): void {
    try {
      this.options.onEvent(event)
    } catch {
      // 观测回调不能影响加载流程
    }
  }

  getSlowBaseline(): number | undefined {
    return this.scheduler.hasThroughputSamples(this.options.minSlowThroughputSamples)
      ? this.scheduler.getEstimatedThroughput()
      : undefined
  }

  usesStableRangeRequests(): boolean {
    return this.transport.rangeRequestMode === 'stable'
  }

  recordDiscardedWireBytes(bytes: number): void {
    this.wireBytes += bytes
  }

  private acceptSequentialResponse(task: ChunkLoadTask, response: HttpTransportResponse): void {
    if (response.status !== 200) {
      throw new TransportError(`顺序请求返回了 HTTP ${response.status}`, response.status)
    }

    this.rangeMode = 'unsupported'
    task.disablePreemption()
    const length = this.readContentLength(response)
    if (length !== undefined) {
      this.setSegmentLength(length)
      task.setEndOffset(length)
    }
  }

  private createTask(
    startOffset: number,
    endOffset: number | undefined,
    useRange: boolean,
  ): ChunkLoadTask {
    const task = new ChunkLoadTask(this, startOffset, endOffset, useRange)
    this.tasks.push(task)
    this.tasks.sort((left, right) => left.startOffset - right.startOffset)
    return task
  }

  private planRemainingTasks(startOffset: number): void {
    if (this.segmentLength === undefined) {
      throw new Error('规划 Chunk 前必须知道 Segment 长度')
    }

    this.planned = true
    for (const range of splitByteRanges(startOffset, this.segmentLength, this.options.chunkSize)) {
      const task = this.createTask(range.start, range.endExclusive, true)
      this.scheduler.add(task)
    }
    this.maybeComplete()
  }

  private setSegmentLength(length: number): void {
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new TransportError(`无效的 Segment 长度 ${length}`)
    }
    if (this.segmentLength !== undefined && this.segmentLength !== length) {
      throw new TransportError(
        `Segment 长度发生变化, 原长度 ${this.segmentLength}, 新长度 ${length}`,
      )
    }
    this.segmentLength = length
    this.stats.total = length
  }

  private flushContiguousData(): void {
    while (true) {
      const task = this.tasks.find(candidate => candidate.canDeliverAt(this.deliveredOffset))
      const data = task?.shiftBufferedData()
      if (data === undefined) {
        break
      }

      this.deliveredOffset += data.byteLength
      const highWaterMark = this.getProgressiveHighWaterMark()
      if (this.callbacks.onProgress === undefined || !Number.isFinite(highWaterMark)) {
        this.resultBuffers.push(data)
        continue
      }

      this.pendingProgress.push(data)
      this.pendingProgressBytes += data.byteLength
      if (this.pendingProgressBytes >= highWaterMark) {
        this.emitProgress()
      }
    }
  }

  private emitProgress(): void {
    if (this.callbacks.onProgress === undefined || this.pendingProgressBytes === 0) {
      return
    }

    const payload = joinBuffers(this.pendingProgress, this.pendingProgressBytes)
    this.pendingProgress = []
    this.pendingProgressBytes = 0
    this.callbacks.onProgress(this.stats, this.context, payload, this.lastResponse)
  }

  private maybeComplete(): void {
    if (
      this.completed ||
      this.aborted ||
      !this.planned ||
      this.segmentLength === undefined ||
      this.deliveredOffset !== this.segmentLength ||
      this.tasks.some(task => !task.isComplete())
    ) {
      return
    }

    this.completed = true
    this.clearLogicalTimer()
    this.emitProgress()
    const now = performance.now()
    this.stats.loading.end = now
    if (this.stats.loading.first === 0) {
      this.stats.loading.first = now
    }
    this.stats.bwEstimate =
      this.wireBytes > 0 && now > this.startTime
        ? (this.wireBytes * 8_000) / (now - this.startTime)
        : 0
    this.notifyStats()

    const progressive =
      this.callbacks.onProgress !== undefined && Number.isFinite(this.getProgressiveHighWaterMark())
    const data = progressive
      ? new ArrayBuffer(0)
      : joinBuffers(this.resultBuffers, this.segmentLength)
    if (!progressive && this.callbacks.onProgress !== undefined) {
      this.callbacks.onProgress(this.stats, this.context, data, this.lastResponse)
    }
    const response: LoaderResponse = {
      url: this.finalUrl,
      data,
      code: this.rangeMode === 'supported' ? 206 : 200,
    }
    this.callbacks.onSuccess(response, this.stats, this.context, this.lastResponse)
  }

  private getProgressiveHighWaterMark(): number {
    const configured = this.loaderConfig.highWaterMark
    if (configured === undefined) {
      return progressiveHighWaterMark
    }
    return Number.isFinite(configured) ? Math.max(configured, 1) : Number.POSITIVE_INFINITY
  }

  private fail(error: TransportError): void {
    if (this.aborted || this.completed) {
      return
    }

    this.completed = true
    this.clearLogicalTimer()
    for (const task of this.tasks) {
      task.cancel()
      this.scheduler.remove(task)
    }
    this.stats.loading.end = performance.now()
    this.notifyStats()

    if (error.timeout) {
      this.callbacks.onTimeout(this.stats, this.context, this.lastResponse)
      return
    }
    this.callbacks.onError(
      { code: error.code, text: error.message },
      this.context,
      this.lastResponse,
      this.stats,
    )
  }

  private validateResourceIdentity(response: HttpTransportResponse): void {
    const current = response.headers.get('etag') ?? response.headers.get('last-modified')
    if (this.validator === null) {
      this.validator = current
      return
    }
    if (current !== null && current !== this.validator) {
      throw new TransportError('Segment 资源标识在 Range 请求之间发生变化')
    }
  }

  private parseContentRange(response: HttpTransportResponse): ContentRange | undefined {
    const value = response.headers.get('content-range')
    if (value === null) {
      return undefined
    }
    const match = contentRangePattern.exec(value.trim())
    if (match === null) {
      throw new TransportError('Range 响应提供了无效的 Content-Range', response.status)
    }

    const start = Number.parseInt(match[1] ?? '', 10)
    const end = Number.parseInt(match[2] ?? '', 10)
    const totalText = match[3]
    const total =
      totalText === undefined || totalText === '*' ? undefined : Number.parseInt(totalText, 10)
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || end < start) {
      throw new TransportError(`无效的 Content-Range: ${value}`, response.status)
    }
    return { start, endExclusive: end + 1, total }
  }

  private inferContentRange(attempt: Attempt, response: HttpTransportResponse): ContentRange {
    if (this.segmentLength === undefined) {
      throw new TransportError('无法推断 Range 响应范围', response.status)
    }
    const responseLength = this.readContentLength(response)
    if (responseLength === undefined) {
      throw new TransportError(
        'Range 响应没有通过 CORS 暴露 Content-Range 或 Content-Length',
        response.status,
      )
    }
    const resourceEnd = this.resourceStart + this.segmentLength
    const expectedEnd = Math.min(attempt.requestEnd ?? resourceEnd, resourceEnd)
    if (attempt.requestStart + responseLength !== expectedEnd) {
      throw new TransportError(
        `Range 响应长度错误, 期望 ${expectedEnd - attempt.requestStart}, 实际 ${responseLength}`,
        response.status,
      )
    }
    return {
      start: attempt.requestStart,
      endExclusive: expectedEnd,
      total: resourceEnd,
    }
  }

  private async probeResourceLength(signal: AbortSignal): Promise<number> {
    const headers = new Headers(this.context.headers)
    headers.delete('Range')
    const probeContext: FragmentLoaderContext = {
      ...this.context,
      headers: Object.fromEntries(headers.entries()),
      rangeStart: 0,
      rangeEnd: 0,
    }
    const init: RequestInit = {
      method: 'HEAD',
      mode: 'cors',
      credentials: 'same-origin',
      headers,
      signal,
    }
    const configuredRequest = this.hlsConfig.fetchSetup?.(probeContext, init)
    const request =
      configuredRequest === undefined
        ? new Request(this.context.url, init)
        : await configuredRequest
    const response = await this.transport.request(request, { maxResponseBytes: 0 })
    if (!response.ok) {
      throw new TransportError(
        response.statusText || `HEAD 请求返回 HTTP ${response.status}`,
        response.status,
      )
    }
    this.validateResourceIdentity(response)
    const length = this.readContentLength(response)
    if (length === undefined) {
      throw new TransportError('HEAD 响应没有提供 Content-Length', response.status)
    }
    return length
  }

  private readContentLength(response: HttpTransportResponse): number | undefined {
    const value = response.headers.get('content-length')
    if (value === null) {
      return undefined
    }
    const length = Number.parseInt(value, 10)
    return Number.isSafeInteger(length) && length >= 0 ? length : undefined
  }

  private isAtomicRequest(): boolean {
    return (
      this.context.frag.sn === 'initSegment' ||
      this.context.part !== null ||
      this.context.frag.type === 'subtitle'
    )
  }

  private clearLogicalTimer(): void {
    if (this.logicalTimer !== undefined) {
      globalThis.clearTimeout(this.logicalTimer)
      this.logicalTimer = undefined
    }
  }
}

class ChunkLoadTask implements ScheduledRequest {
  readonly createdAt = performance.now()
  readonly id = ++nextChunkId
  readonly startOffset: number

  receivedBytes = 0

  private activeAttempt: Attempt | null = null
  private attemptSequence = 0
  private readonly buffers: Uint8Array[] = []
  private cancelled = false
  private completed = false
  private deliverOffset: number
  private end: number | undefined
  private readonly stableEnd: number | undefined
  private nextRunAt = 0
  private preemptible: boolean
  private preemptions = 0
  private rescueAttempts = 0
  private retryAttempts = 0
  private slowRetries = 0
  private lastStopReason: AttemptStopReason | undefined
  private rangeEnabled: boolean
  private readonly segment: SegmentLoadCoordinator

  constructor(
    segment: SegmentLoadCoordinator,
    startOffset: number,
    endOffset: number | undefined,
    useRange: boolean,
  ) {
    this.segment = segment
    this.startOffset = startOffset
    this.deliverOffset = startOffset
    this.end = endOffset
    this.stableEnd = endOffset
    this.rangeEnabled = useRange
    this.preemptible = useRange
  }

  get endOffset(): number | undefined {
    return this.end
  }

  canRun(now: number): boolean {
    return (
      !this.cancelled && !this.completed && now >= this.nextRunAt && this.segment.canTaskRun(this)
    )
  }

  getPriority(playbackTime: number, playbackRate: number): number {
    return this.segment.getTaskPriority(this, playbackTime, playbackRate)
  }

  isComplete(): boolean {
    return this.completed || this.cancelled
  }

  isProtected(now: number): boolean {
    const attempt = this.activeAttempt
    if (attempt === null) {
      return false
    }
    if (!this.preemptible) {
      return true
    }
    if (now - attempt.startedAt < this.segment.options.minRequestLifetimeMs) {
      return true
    }

    return this.isFinishing()
  }

  isRunning(): boolean {
    return this.activeAttempt !== null
  }

  start(): void {
    if (this.activeAttempt !== null || this.cancelled || this.completed) {
      return
    }

    const restartRange = this.rangeEnabled && this.segment.usesStableRangeRequests()
    const requestStart = this.segmentResourceOffset(
      this.startOffset + (restartRange ? 0 : this.receivedBytes),
    )
    const requestEndOffset = restartRange ? this.stableEnd : this.end
    const requestEnd =
      this.rangeEnabled && requestEndOffset !== undefined
        ? this.segmentResourceOffset(requestEndOffset)
        : undefined
    const attempt: Attempt = {
      badSince: undefined,
      bytes: 0,
      controller: new AbortController(),
      discardBytes: restartRange ? this.receivedBytes : 0,
      id: ++this.attemptSequence,
      loadTimer: undefined,
      requestEnd,
      requestStart,
      response: null,
      startedAt: performance.now(),
      trafficTimer: undefined,
      ttfbTimer: undefined,
    }
    this.activeAttempt = attempt
    this.armAttemptTimeouts(attempt)
    void this.performAttempt(attempt)
  }

  suspend(): void {
    if (!this.preemptible) {
      return
    }
    this.stopActiveAttempt('preempted')
  }

  cancel(): void {
    this.cancelled = true
    this.stopActiveAttempt('failure')
    this.buffers.length = 0
  }

  disablePreemption(): void {
    this.preemptible = false
  }

  disableRange(): void {
    this.rangeEnabled = false
  }

  setEndOffset(endOffset: number | undefined): void {
    if (endOffset !== undefined && endOffset < this.startOffset + this.receivedBytes) {
      throw new TransportError('响应数据超过了 Chunk 边界')
    }
    this.end = endOffset
  }

  append(data: Uint8Array): void {
    const total = this.getTotalBytes()
    if (total !== undefined && this.receivedBytes + data.byteLength > total) {
      throw new TransportError('Chunk 返回的数据超过了预期长度')
    }
    this.receivedBytes += data.byteLength
    this.buffers.push(data)
  }

  canDeliverAt(offset: number): boolean {
    return this.deliverOffset === offset && this.buffers.length > 0
  }

  shiftBufferedData(): Uint8Array | undefined {
    const data = this.buffers.shift()
    if (data !== undefined) {
      this.deliverOffset += data.byteLength
    }
    return data
  }

  containsOffset(offset: number): boolean {
    return this.startOffset <= offset && (this.end === undefined || offset < this.end)
  }

  getEstimatedRemainingMs(): number {
    const total = this.getTotalBytes()
    if (total === undefined) {
      return Number.POSITIVE_INFINITY
    }
    const remaining = Math.max(0, total - this.receivedBytes)
    return (remaining * 1_000) / this.segment.scheduler.getEstimatedThroughput()
  }

  getDiagnostics(): HlsLoaderDiagnosticChunk {
    const attempt = this.activeAttempt
    return {
      attempt: attempt?.id ?? this.attemptSequence,
      endOffset: this.end,
      id: this.id,
      networkRetries: this.retryAttempts,
      preemptions: this.preemptions,
      receivedBytes: this.receivedBytes,
      rescueAttempts: this.rescueAttempts,
      running: attempt !== null,
      slowRetries: this.slowRetries,
      startOffset: this.startOffset,
      state: this.getDiagnosticState(),
      throughputBytesPerSecond:
        attempt === null || attempt.bytes === 0
          ? 0
          : (attempt.bytes * 1_000) / Math.max(1, performance.now() - attempt.startedAt),
    }
  }

  private async performAttempt(attempt: Attempt): Promise<void> {
    try {
      const response = await this.segment.createResponse(this, attempt)
      if (!this.isCurrentAttempt(attempt)) {
        return
      }

      attempt.response = response
      this.markAttemptResponseStarted(attempt, response)
      await this.segment.acceptResponse(this, attempt, response)
      if (!this.isCurrentAttempt(attempt)) {
        return
      }
      await this.readResponseBody(attempt, response)
      if (!this.isCurrentAttempt(attempt)) {
        return
      }

      const total = this.getTotalBytes()
      if (total !== undefined && this.receivedBytes !== total) {
        throw new TransportError(
          `Chunk 提前结束, 期望 ${total} 字节, 实际 ${this.receivedBytes} 字节`,
        )
      }

      this.stopActiveAttempt('complete')
      this.completed = true
      this.segment.scheduler.remove(this)
      this.segment.taskCompleted(this)
    } catch (cause) {
      if (!this.isCurrentAttempt(attempt)) {
        return
      }
      const error = this.toTransportError(cause)
      this.handleAttemptFailure(error)
    }
  }

  private async readResponseBody(attempt: Attempt, response: HttpTransportResponse): Promise<void> {
    if (response.body === null) {
      const data = new Uint8Array(await response.arrayBuffer())
      this.acceptAttemptData(attempt, data, response)
      return
    }

    const reader = response.body.getReader()
    while (true) {
      const result = await reader.read()
      if (!this.isCurrentAttempt(attempt)) {
        await reader.cancel()
        return
      }
      if (result.done) {
        return
      }
      this.acceptAttemptData(attempt, result.value.slice(), response)
    }
  }

  private acceptAttemptData(
    attempt: Attempt,
    data: Uint8Array,
    response: HttpTransportResponse,
  ): void {
    if (!this.isCurrentAttempt(attempt) || data.byteLength === 0) {
      return
    }

    const now = performance.now()
    if (attempt.bytes === 0) {
      this.clearTimer(attempt.ttfbTimer)
      attempt.ttfbTimer = undefined
    }
    attempt.bytes += data.byteLength
    this.resetTrafficTimer(attempt)
    const discardedBytes = Math.min(attempt.discardBytes, data.byteLength)
    attempt.discardBytes -= discardedBytes
    this.segment.recordDiscardedWireBytes(discardedBytes)
    const freshData = discardedBytes === 0 ? data : data.subarray(discardedBytes)
    if (freshData.byteLength > 0) {
      this.segment.acceptData(this, freshData, response)
    }
    this.detectSlowAttempt(attempt, now)
  }

  private detectSlowAttempt(attempt: Attempt, now: number): void {
    if (
      !this.rangeEnabled ||
      this.rescueAttempts >= this.segment.options.maxRescueAttempts ||
      attempt.bytes < 256 * 1024 ||
      now - attempt.startedAt < this.segment.options.slowThroughputWindowMs ||
      this.isFinishing()
    ) {
      return
    }

    const baseline = this.segment.getSlowBaseline()
    if (baseline === undefined) {
      return
    }
    const speed = (attempt.bytes * 1_000) / (now - attempt.startedAt)
    if (speed >= baseline * this.segment.options.slowThroughputRatio) {
      attempt.badSince = undefined
      return
    }

    attempt.badSince ??= now
    if (now - attempt.badSince >= this.segment.options.slowThroughputWindowMs) {
      this.rescueAttempts += 1
      this.slowRetries += 1
      this.segment.stats.retry += 1
      this.segment.notifyStats()
      this.stopActiveAttempt('slow', {
        baselineThroughputBytesPerSecond: baseline,
        throughputBytesPerSecond: speed,
      })
      this.segment.scheduler.notify()
    }
  }

  private handleAttemptFailure(error: TransportError): void {
    const retryableStatus =
      error.code === 0 || error.code === 408 || error.code === 429 || error.code >= 500
    const canResume = this.rangeEnabled || this.receivedBytes === 0
    if (
      retryableStatus &&
      canResume &&
      this.rescueAttempts < this.segment.options.maxRescueAttempts
    ) {
      this.stopActiveAttempt('failure')
      this.rescueAttempts += 1
      this.segment.stats.retry += 1
      this.segment.notifyStats()
      const delay = error.timeout ? 0 : 500 * this.rescueAttempts
      this.nextRunAt = performance.now() + delay
      globalThis.setTimeout(() => this.segment.scheduler.notify(), delay)
      return
    }

    const retryConfig = error.timeout
      ? this.segment.loaderConfig.loadPolicy.timeoutRetry
      : this.segment.loaderConfig.loadPolicy.errorRetry
    const maxRetry = retryConfig?.maxNumRetry ?? 0
    const shouldRetry =
      retryConfig !== null &&
      retryableStatus &&
      (this.rangeEnabled || this.receivedBytes === 0) &&
      this.retryAttempts < maxRetry &&
      (retryConfig?.shouldRetry?.(
        retryConfig,
        this.retryAttempts,
        error.timeout,
        { url: this.segment.context.url, code: error.code, text: error.message },
        true,
      ) ??
        true)

    this.stopActiveAttempt('failure')
    if (!shouldRetry) {
      this.segment.taskFailed(error)
      return
    }

    this.retryAttempts += 1
    this.segment.stats.retry += 1
    this.segment.notifyStats()
    const backoff = retryConfig.backoff ?? 'exponential'
    const multiplier = backoff === 'linear' ? this.retryAttempts : 2 ** (this.retryAttempts - 1)
    const delay = Math.min(retryConfig.retryDelayMs * multiplier, retryConfig.maxRetryDelayMs)
    this.nextRunAt = performance.now() + delay
    globalThis.setTimeout(() => this.segment.scheduler.notify(), delay)
  }

  private armAttemptTimeouts(attempt: Attempt): void {
    const { maxLoadTimeMs, maxTimeToFirstByteMs } = this.segment.loaderConfig.loadPolicy
    if (Number.isFinite(maxTimeToFirstByteMs) && maxTimeToFirstByteMs > 0) {
      attempt.ttfbTimer = globalThis.setTimeout(() => {
        this.handleAttemptTimeout(attempt, `首字节等待超过 ${maxTimeToFirstByteMs}ms`)
      }, maxTimeToFirstByteMs)
    }
    if (Number.isFinite(maxLoadTimeMs) && maxLoadTimeMs > 0) {
      attempt.loadTimer = globalThis.setTimeout(() => {
        this.handleAttemptTimeout(attempt, `请求加载超过 ${maxLoadTimeMs}ms`)
      }, maxLoadTimeMs)
    }
  }

  private markAttemptResponseStarted(attempt: Attempt, response: HttpTransportResponse): void {
    this.clearTimer(attempt.ttfbTimer)
    attempt.ttfbTimer = undefined
    this.resetTrafficTimer(attempt)
    this.segment.markResponseStarted(response)
  }

  private resetTrafficTimer(attempt: Attempt): void {
    this.clearTimer(attempt.trafficTimer)
    attempt.trafficTimer = globalThis.setTimeout(() => {
      this.handleAttemptTimeout(
        attempt,
        `请求连续 ${this.segment.options.idleTimeoutMs}ms 没有收到数据`,
      )
    }, this.segment.options.idleTimeoutMs)
  }

  private handleAttemptTimeout(attempt: Attempt, message: string): void {
    if (!this.isCurrentAttempt(attempt)) {
      return
    }
    this.handleAttemptFailure(new TransportError(message, 0, true))
  }

  private stopActiveAttempt(
    reason: AttemptStopReason,
    slowConnectionMetrics?: SlowConnectionMetrics,
  ): void {
    const attempt = this.activeAttempt
    if (attempt === null) {
      return
    }

    this.activeAttempt = null
    this.lastStopReason = reason
    if (reason === 'preempted') {
      this.preemptions += 1
    }
    this.clearAttemptTimers(attempt)
    if (reason !== 'complete') {
      attempt.controller.abort()
    }
    const duration = performance.now() - attempt.startedAt
    if (reason === 'preempted' || reason === 'slow') {
      this.emitAbortEvent(attempt, reason, duration, slowConnectionMetrics)
    }
    if (attempt.bytes > 0 && (reason === 'complete' || reason === 'preempted')) {
      this.segment.reportAttempt(attempt.bytes, duration)
    }
    this.segment.scheduler.notify()
  }

  private emitAbortEvent(
    attempt: Attempt,
    reason: 'preempted' | 'slow',
    elapsedMs: number,
    slowConnectionMetrics?: SlowConnectionMetrics,
  ): void {
    const total = this.getTotalBytes()
    const segmentStart = this.segment.context.part?.start ?? this.segment.context.frag.start
    const segmentDuration =
      this.segment.context.part?.duration ?? this.segment.context.frag.duration
    this.segment.emitEvent({
      attempt: attempt.id,
      baselineThroughputBytesPerSecond: slowConnectionMetrics?.baselineThroughputBytesPerSecond,
      chunkEnd: this.end,
      chunkLoadedBytes: this.receivedBytes,
      chunkStart: this.startOffset,
      elapsedMs,
      loadedBytes: attempt.bytes,
      reason: reason === 'slow' ? 'slow-connection' : 'preempted',
      remainingBytes: total === undefined ? undefined : Math.max(0, total - this.receivedBytes),
      requestEnd: attempt.requestEnd,
      requestStart: attempt.requestStart,
      segmentDuration,
      segmentSn: this.segment.context.frag.sn,
      segmentStart,
      throughputBytesPerSecond:
        slowConnectionMetrics?.throughputBytesPerSecond ??
        (elapsedMs > 0 ? (attempt.bytes * 1_000) / elapsedMs : 0),
      timestamp: Date.now(),
      type: 'request-aborted',
      url: this.segment.context.url,
    })
  }

  private isCurrentAttempt(attempt: Attempt): boolean {
    return this.activeAttempt?.id === attempt.id && !this.cancelled && !this.completed
  }

  private clearAttemptTimers(attempt: Attempt): void {
    this.clearTimer(attempt.ttfbTimer)
    this.clearTimer(attempt.trafficTimer)
    this.clearTimer(attempt.loadTimer)
    attempt.ttfbTimer = undefined
    attempt.trafficTimer = undefined
    attempt.loadTimer = undefined
  }

  private clearTimer(timer: number | undefined): void {
    if (timer !== undefined) {
      globalThis.clearTimeout(timer)
    }
  }

  private getTotalBytes(): number | undefined {
    return this.end === undefined ? undefined : this.end - this.startOffset
  }

  private getDiagnosticState(): HlsLoaderDiagnosticChunkState {
    if (this.cancelled) {
      return 'cancelled'
    }
    if (this.completed) {
      return 'complete'
    }
    if (this.slowRetries > 0 && this.lastStopReason === 'slow') {
      return 'slow-retrying'
    }
    if (this.activeAttempt !== null) {
      return 'loading'
    }
    if (this.lastStopReason === 'preempted') {
      return 'preempted'
    }
    if (this.rescueAttempts > this.slowRetries || this.retryAttempts > 0) {
      return 'network-retrying'
    }
    return 'queued'
  }

  private isFinishing(): boolean {
    const total = this.getTotalBytes()
    const ratio = total === undefined || total === 0 ? 0 : this.receivedBytes / total
    return (
      ratio >= this.segment.options.finishingRatio ||
      this.getEstimatedRemainingMs() <= this.segment.options.finishingRemainingMs
    )
  }

  private segmentResourceOffset(localOffset: number): number {
    return (this.segment.context.rangeStart ?? 0) + localOffset
  }

  private toTransportError(cause: unknown): TransportError {
    if (cause instanceof TransportError) {
      return cause
    }
    if (cause instanceof Error) {
      return new TransportError(cause.message)
    }
    return new TransportError('未知网络错误')
  }
}

function joinBuffers(buffers: Uint8Array[], totalLength: number): ArrayBuffer {
  if (totalLength === 0) {
    return new ArrayBuffer(0)
  }
  if (buffers.length === 1) {
    const buffer = buffers[0]
    if (buffer !== undefined) {
      return buffer.slice().buffer
    }
  }

  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const buffer of buffers) {
    result.set(buffer, offset)
    offset += buffer.byteLength
  }
  return result.buffer
}

export const DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024
export const DEFAULT_MAX_CONCURRENCY = 6
export const DEFAULT_PROGRESSIVE_HIGH_WATER_MARK = progressiveHighWaterMark
