import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import type { HlsLoaderEventHandler } from './events'
import { VirtualStreamChunkWriter, VirtualStreamRegistry } from './virtual-stream'
import type {
  VirtualStreamChunkFillFailure,
  VirtualStreamChunkFillPlan,
  VirtualStreamChunkSnapshot,
  VirtualStreamRegistrySnapshot,
  VirtualStreamRetryPolicy,
  VirtualStreamSegmentSnapshot,
} from './virtual-stream'

export type StreamFillerStateName = 'destroyed' | 'filling' | 'preempting' | 'rescuing' | 'waiting'

export interface StreamFillerStateSnapshot {
  readonly attempt: number
  readonly bytes: number
  readonly chunkKey: string | undefined
  readonly fillerId: number
  readonly requestEnd: number | undefined
  readonly requestStart: number | undefined
  readonly startedAt: number | undefined
  readonly state: StreamFillerStateName
  readonly writerId: number | undefined
}

export interface StreamFillerOptions {
  readonly finishingRatio: number
  readonly finishingRemainingMs: number
  readonly getPlaybackRate: () => number
  readonly getPlaybackTime: () => number
  readonly idleTimeoutMs: number
  readonly maxRescueAttempts: number
  readonly minRequestLifetimeMs: number
  readonly minSlowThroughputSamples: number
  readonly onEvent: HlsLoaderEventHandler
  readonly slowThroughputRatio: number
  readonly slowThroughputWindowMs: number
}

interface ChunkCandidate {
  readonly chunk: VirtualStreamChunkSnapshot
  readonly direct: boolean
  readonly estimatedMediaTime: number
  readonly frontier: boolean
  readonly segment: VirtualStreamSegmentSnapshot
  readonly streamId: string
}

interface AttemptContext {
  bytes: number
  controller: AbortController
  firstByteReceived: boolean
  id: number
  idleTimer: number | undefined
  networkDetails: HttpTransportResponse | null
  outcome: AttemptOutcome | undefined
  startedAt: number
  ttfbTimer: number | undefined
}

type AttemptOutcome =
  | { readonly kind: 'complete' }
  | { readonly kind: 'destroyed' | 'preempted' | 'revoked' }
  | {
      readonly bytes: number
      readonly durationMs: number
      readonly error: TransportError
      readonly kind: 'failed'
      readonly networkDetails: HttpTransportResponse | null
    }
  | { readonly bytes: number; readonly durationMs: number; readonly kind: 'slow' }

const defaultThroughputBytesPerSecond = 2_000_000
const contentRangePattern = /^bytes (\d+)-(\d+)\/(\d+|\*)$/i
const maintenanceIntervalMs = 200
const minimumSlowAttemptBytes = 256 * 1024

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

export class StreamFiller {
  readonly id: number

  private activeAttempt: AttemptContext | undefined
  private activePlan: VirtualStreamChunkFillPlan | undefined
  private attemptSequence = 0
  private readonly destroyController = new AbortController()
  private destroyed = false
  private readonly onEvent: HlsLoaderEventHandler
  private readonly options: StreamFillerOptions
  private readonly registry: VirtualStreamRegistry
  private runPromise: Promise<void> | undefined
  private state: StreamFillerStateName = 'waiting'
  private readonly transport: HttpTransport
  private writer: VirtualStreamChunkWriter | undefined

  constructor(
    id: number,
    registry: VirtualStreamRegistry,
    transport: HttpTransport,
    options: StreamFillerOptions,
  ) {
    this.id = id
    this.registry = registry
    this.transport = transport
    this.options = options
    this.onEvent = options.onEvent
  }

  start(): void {
    if (this.runPromise !== undefined || this.destroyed) {
      return
    }
    this.runPromise = this.run()
    void this.runPromise.catch(() => undefined)
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.state = 'destroyed'
    this.destroyController.abort()
    this.activeAttempt?.controller.abort()
    this.writer?.release('destroyed')
    this.writer = undefined
  }

  getState(): StreamFillerStateSnapshot {
    const attempt = this.activeAttempt
    const plan = this.activePlan
    return {
      attempt: attempt?.id ?? this.attemptSequence,
      bytes: attempt?.bytes ?? 0,
      chunkKey: this.writer?.chunkKey,
      fillerId: this.id,
      requestEnd: plan?.requestEnd,
      requestStart: plan?.requestStart,
      startedAt: attempt?.startedAt,
      state: this.state,
      writerId: this.writer?.id,
    }
  }

  private async run(): Promise<void> {
    while (!this.destroyed) {
      this.registry.evictBefore(this.readPlaybackTime())
      if (this.writer === undefined) {
        const snapshot = this.registry.snapshot()
        const candidate = selectBestCandidate(
          snapshot,
          this.readPlaybackTime(),
          this.readPlaybackRate(),
        )
        if (candidate === undefined) {
          this.state = 'waiting'
          await this.waitForWork(snapshot)
          continue
        }
        this.writer = this.registry.tryAcquireChunkWriter({
          chunkKey: candidate.chunk.key,
          fillerId: this.id,
        })
        if (this.writer === undefined) {
          continue
        }
      }

      const writer = this.writer
      try {
        await this.fill(writer)
      } catch (cause) {
        if (writer.isCurrent()) {
          const error = toTransportError(cause)
          writer.fail(this.createFillFailure(error))
        }
      }
      if (this.writer === writer) {
        this.writer = undefined
      }
    }
  }

  private async fill(writer: VirtualStreamChunkWriter): Promise<void> {
    while (!this.destroyed && writer.isCurrent() && this.writer === writer) {
      this.state = 'filling'
      const plan = writer.getFillPlan(this.transport.rangeRequestMode)
      const outcome = await this.performAttempt(writer, plan)
      if (outcome.kind === 'complete') {
        writer.complete()
        return
      }
      if (
        outcome.kind === 'destroyed' ||
        outcome.kind === 'preempted' ||
        outcome.kind === 'revoked'
      ) {
        return
      }
      if (outcome.kind === 'slow') {
        if (!writer.isCurrent()) {
          return
        }
        this.state = 'rescuing'
        writer.recordAttempt({
          bytes: outcome.bytes,
          durationMs: outcome.durationMs,
          rescued: true,
          slowRetry: true,
        })
        continue
      }

      if (outcome.kind !== 'failed') {
        return
      }

      if (!writer.isCurrent()) {
        return
      }
      const failure = this.createFillFailure(outcome.error, outcome.networkDetails)
      const retryable = isRetryableStatus(outcome.error.code)
      if (retryable && writer.rescueAttempts < this.options.maxRescueAttempts) {
        this.state = 'rescuing'
        writer.recordAttempt({
          bytes: outcome.bytes,
          durationMs: outcome.durationMs,
          rescued: true,
        })
        const delayMs = outcome.error.timeout ? 0 : 500 * writer.rescueAttempts
        if (!(await this.wait(delayMs))) {
          return
        }
        continue
      }

      const retry = this.getPolicyRetry(plan, outcome.error, writer.networkRetries)
      if (retry !== undefined) {
        writer.recordAttempt({
          bytes: outcome.bytes,
          durationMs: outcome.durationMs,
          networkRetry: true,
        })
        writer.fail(failure, performance.now() + retry)
        return
      }

      writer.fail(failure)
      return
    }
  }

  private async performAttempt(
    writer: VirtualStreamChunkWriter,
    plan: VirtualStreamChunkFillPlan,
  ): Promise<AttemptOutcome> {
    const attempt: AttemptContext = {
      bytes: 0,
      controller: new AbortController(),
      firstByteReceived: false,
      id: ++this.attemptSequence,
      idleTimer: undefined,
      networkDetails: null,
      outcome: undefined,
      startedAt: performance.now(),
      ttfbTimer: undefined,
    }
    this.activeAttempt = attempt
    this.activePlan = plan

    const revoke = () => {
      attempt.outcome ??= this.destroyed ? { kind: 'destroyed' } : { kind: 'revoked' }
      attempt.controller.abort()
    }
    const destroy = () => {
      attempt.outcome = { kind: 'destroyed' }
      attempt.controller.abort()
    }
    writer.signal.addEventListener('abort', revoke, { once: true })
    this.destroyController.signal.addEventListener('abort', destroy, { once: true })

    const maxTimeToFirstByteMs = plan.fillPolicy.maxTimeToFirstByteMs
    if (Number.isFinite(maxTimeToFirstByteMs) && maxTimeToFirstByteMs > 0) {
      attempt.ttfbTimer = globalThis.setTimeout(() => {
        if (attempt.outcome === undefined && !attempt.firstByteReceived) {
          attempt.outcome = {
            bytes: attempt.bytes,
            durationMs: performance.now() - attempt.startedAt,
            error: new TransportError(`首字节等待超过 ${maxTimeToFirstByteMs}ms`, 0, true),
            kind: 'failed',
            networkDetails: attempt.networkDetails,
          }
          attempt.controller.abort()
        }
      }, maxTimeToFirstByteMs)
    }

    const monitor = globalThis.setInterval(() => {
      this.monitorAttempt(writer, plan, attempt)
    }, maintenanceIntervalMs)

    try {
      const request = await this.createRequest(plan, attempt.controller.signal)
      const response = await this.transport.request(
        request,
        plan.requestEnd === undefined
          ? undefined
          : { maxResponseBytes: plan.requestEnd - plan.requestStart },
      )
      if (attempt.outcome !== undefined) {
        return attempt.outcome
      }
      attempt.networkDetails = response
      attempt.firstByteReceived = true
      this.clearTimer(attempt.ttfbTimer)
      attempt.ttfbTimer = undefined
      this.resetIdleTimer(attempt)
      if (!response.ok) {
        throw new TransportError(response.statusText || `HTTP ${response.status}`, response.status)
      }

      const metadata = await this.createResponseMetadata(plan, response, attempt.controller.signal)
      if (attempt.outcome !== undefined) {
        return attempt.outcome
      }
      writer.acceptResponse(metadata)
      await this.readResponseBody(writer, plan, response, attempt)
      if (attempt.outcome !== undefined) {
        return attempt.outcome
      }
      const remainingLength = writer.remainingLength
      if (remainingLength !== undefined && remainingLength !== 0) {
        throw new TransportError(`Chunk 提前结束, 尚缺少 ${remainingLength} 字节`, response.status)
      }

      const durationMs = performance.now() - attempt.startedAt
      writer.recordAttempt({ bytes: attempt.bytes, durationMs })
      return { kind: 'complete' }
    } catch (cause) {
      if (attempt.outcome !== undefined) {
        return attempt.outcome
      }
      return {
        bytes: attempt.bytes,
        durationMs: performance.now() - attempt.startedAt,
        error: toTransportError(cause),
        kind: 'failed',
        networkDetails: attempt.networkDetails,
      }
    } finally {
      globalThis.clearInterval(monitor)
      this.clearTimer(attempt.ttfbTimer)
      this.clearTimer(attempt.idleTimer)
      writer.signal.removeEventListener('abort', revoke)
      this.destroyController.signal.removeEventListener('abort', destroy)
      if (this.activeAttempt === attempt) {
        this.activeAttempt = undefined
        this.activePlan = undefined
      }
    }
  }

  private async createRequest(
    plan: VirtualStreamChunkFillPlan,
    signal: AbortSignal,
  ): Promise<Request> {
    const headers = new Headers(plan.resource.headers)
    if (plan.rangeEnabled && plan.requestEnd !== undefined) {
      headers.set('Range', `bytes=${plan.requestStart}-${plan.requestEnd - 1}`)
    } else {
      headers.delete('Range')
    }
    return await plan.resource.createRequest({
      headers,
      method: 'GET',
      rangeEnd: plan.requestEnd ?? 0,
      rangeStart: plan.requestStart,
      signal,
    })
  }

  private async createResponseMetadata(
    plan: VirtualStreamChunkFillPlan,
    response: HttpTransportResponse,
    signal: AbortSignal,
  ) {
    const contentLength = readContentLength(response)
    if (response.status === 200) {
      return {
        contentLength,
        networkDetails: response,
        responseEnd: contentLength === undefined ? undefined : contentLength,
        responseStart: 0,
        resourceLength: contentLength,
        status: response.status,
      }
    }

    if (response.status !== 206) {
      return {
        contentLength,
        networkDetails: response,
        responseEnd: undefined,
        responseStart: undefined,
        resourceLength: undefined,
        status: response.status,
      }
    }

    const contentRange = parseContentRange(response.headers.get('content-range'))
    if (contentRange !== undefined) {
      const resourceLength = contentRange.total ?? (await this.probeResourceLength(plan, signal))
      return {
        contentLength,
        networkDetails: response,
        responseEnd: contentRange.endExclusive,
        responseStart: contentRange.start,
        resourceLength,
        status: response.status,
      }
    }

    if (contentLength === undefined) {
      throw new TransportError(
        'Range 响应没有通过 CORS 暴露 Content-Range 或 Content-Length',
        response.status,
      )
    }
    const resourceLength = await this.probeResourceLength(plan, signal)
    return {
      contentLength,
      networkDetails: response,
      responseEnd: plan.requestStart + contentLength,
      responseStart: plan.requestStart,
      resourceLength,
      status: response.status,
    }
  }

  private async probeResourceLength(
    plan: VirtualStreamChunkFillPlan,
    signal: AbortSignal,
  ): Promise<number> {
    const headers = new Headers(plan.resource.headers)
    headers.delete('Range')
    const request = await plan.resource.createRequest({
      headers,
      method: 'HEAD',
      rangeEnd: 0,
      rangeStart: 0,
      signal,
    })
    const response = await this.transport.request(request, { maxResponseBytes: 0 })
    if (!response.ok) {
      throw new TransportError(
        response.statusText || `HEAD 请求返回 HTTP ${response.status}`,
        response.status,
      )
    }
    const length = readContentLength(response)
    if (length === undefined) {
      throw new TransportError('HEAD 响应没有提供 Content-Length', response.status)
    }
    return length
  }

  private async readResponseBody(
    writer: VirtualStreamChunkWriter,
    plan: VirtualStreamChunkFillPlan,
    response: HttpTransportResponse,
    attempt: AttemptContext,
  ): Promise<void> {
    let discardBytes = plan.discardBytes
    const accept = (data: Uint8Array) => {
      if (data.byteLength === 0 || attempt.outcome !== undefined) {
        return
      }
      attempt.bytes += data.byteLength
      this.resetIdleTimer(attempt)
      const discarded = Math.min(discardBytes, data.byteLength)
      discardBytes -= discarded
      const fresh = discarded === 0 ? data : data.subarray(discarded)
      if (fresh.byteLength > 0) {
        writer.append(fresh)
      }
      this.detectSlowAttempt(writer, plan, attempt)
    }

    if (response.body === null) {
      accept(new Uint8Array(await response.arrayBuffer()))
      return
    }
    const reader = response.body.getReader()
    try {
      while (true) {
        const result = await reader.read()
        if (attempt.outcome !== undefined) {
          await reader.cancel()
          return
        }
        if (result.done) {
          return
        }
        accept(result.value.slice())
      }
    } finally {
      reader.releaseLock()
    }
  }

  private monitorAttempt(
    writer: VirtualStreamChunkWriter,
    plan: VirtualStreamChunkFillPlan,
    attempt: AttemptContext,
  ): void {
    if (attempt.outcome !== undefined || this.destroyed || !writer.isCurrent()) {
      return
    }
    const snapshot = this.registry.snapshot()
    const current = findCandidate(
      snapshot,
      writer.chunkKey,
      this.readPlaybackTime(),
      this.readPlaybackRate(),
    )
    const target = selectBestCandidate(snapshot, this.readPlaybackTime(), this.readPlaybackRate())
    if (
      current === undefined ||
      target === undefined ||
      compareCandidates(target, current) >= 0 ||
      !isLowestPriorityWriter(
        snapshot,
        current,
        this.readPlaybackTime(),
        this.readPlaybackRate(),
      ) ||
      this.isProtected(writer, attempt)
    ) {
      return
    }

    this.state = 'preempting'
    writer.recordAttempt({
      bytes: attempt.bytes,
      durationMs: performance.now() - attempt.startedAt,
    })
    const next = this.registry.trySwitchChunkWriter(writer, {
      chunkKey: target.chunk.key,
      fillerId: this.id,
    })
    if (next === undefined) {
      this.state = 'filling'
      return
    }

    this.writer = next
    attempt.outcome = { kind: 'preempted' }
    attempt.controller.abort()
    this.emitAbortEvent(writer, plan, attempt, 'preempted')
  }

  private detectSlowAttempt(
    writer: VirtualStreamChunkWriter,
    plan: VirtualStreamChunkFillPlan,
    attempt: AttemptContext,
  ): void {
    if (
      attempt.outcome !== undefined ||
      !plan.rangeEnabled ||
      writer.rescueAttempts >= this.options.maxRescueAttempts ||
      attempt.bytes < minimumSlowAttemptBytes ||
      this.isProtected(writer, attempt)
    ) {
      return
    }
    const elapsedMs = performance.now() - attempt.startedAt
    if (elapsedMs < this.options.slowThroughputWindowMs) {
      return
    }
    const snapshot = this.registry.snapshot()
    const samples = getThroughputSamples(snapshot)
    if (samples.length < this.options.minSlowThroughputSamples) {
      return
    }
    const baseline = estimateThroughput(snapshot)
    const throughput = (attempt.bytes * 1_000) / elapsedMs
    if (throughput >= baseline * this.options.slowThroughputRatio) {
      return
    }

    attempt.outcome = {
      bytes: attempt.bytes,
      durationMs: elapsedMs,
      kind: 'slow',
    }
    attempt.controller.abort()
    this.emitAbortEvent(writer, plan, attempt, 'slow-connection', baseline)
  }

  private isProtected(writer: VirtualStreamChunkWriter, attempt: AttemptContext): boolean {
    if (writer.sequential) {
      return true
    }
    const elapsedMs = performance.now() - attempt.startedAt
    if (elapsedMs < this.options.minRequestLifetimeMs) {
      return true
    }
    const total = writer.endOffset === undefined ? undefined : writer.endOffset - writer.startOffset
    if (total === undefined || total === 0) {
      return false
    }
    const ratio = writer.receivedLength / total
    const remainingMs =
      (Math.max(0, total - writer.receivedLength) * 1_000) /
      estimateThroughput(this.registry.snapshot())
    return ratio >= this.options.finishingRatio || remainingMs <= this.options.finishingRemainingMs
  }

  private getPolicyRetry(
    plan: VirtualStreamChunkFillPlan,
    error: TransportError,
    retryCount: number,
  ): number | undefined {
    const retryConfig = error.timeout ? plan.fillPolicy.timeoutRetry : plan.fillPolicy.errorRetry
    if (
      retryConfig === null ||
      retryConfig === undefined ||
      retryCount >= retryConfig.maxNumRetry
    ) {
      return undefined
    }
    const errorData = { code: error.code, message: error.message, url: plan.resource.url }
    if (retryConfig.shouldRetry?.(retryCount, error.timeout, errorData) === false) {
      return undefined
    }
    return getRetryDelay(retryConfig, retryCount + 1)
  }

  private createFillFailure(
    error: TransportError,
    networkDetails: HttpTransportResponse | null = null,
  ): VirtualStreamChunkFillFailure {
    return {
      code: error.code,
      kind: error.timeout ? 'timeout' : 'error',
      message: error.message,
      networkDetails,
    }
  }

  private emitAbortEvent(
    writer: VirtualStreamChunkWriter,
    plan: VirtualStreamChunkFillPlan,
    attempt: AttemptContext,
    reason: 'preempted' | 'slow-connection',
    baseline?: number,
  ): void {
    const elapsedMs = performance.now() - attempt.startedAt
    const endOffset = writer.endOffset
    try {
      this.onEvent({
        attempt: attempt.id,
        baselineThroughputBytesPerSecond: baseline,
        chunkEnd: endOffset,
        chunkLoadedBytes: writer.receivedLength,
        chunkStart: writer.startOffset,
        elapsedMs,
        loadedBytes: attempt.bytes,
        reason,
        remainingBytes:
          endOffset === undefined
            ? undefined
            : Math.max(0, endOffset - writer.startOffset - writer.receivedLength),
        requestEnd: plan.requestEnd,
        requestStart: plan.requestStart,
        segmentDuration: plan.segmentDuration,
        segmentKey: plan.segmentKey,
        segmentStart: plan.segmentStart,
        throughputBytesPerSecond: elapsedMs === 0 ? 0 : (attempt.bytes * 1_000) / elapsedMs,
        timestamp: Date.now(),
        type: 'request-aborted',
        streamId: plan.streamId,
        url: plan.resource.url,
      })
    } catch {
      // 观测回调不能影响填充流程
    }
  }

  private resetIdleTimer(attempt: AttemptContext): void {
    this.clearTimer(attempt.idleTimer)
    attempt.idleTimer = globalThis.setTimeout(() => {
      if (attempt.outcome !== undefined) {
        return
      }
      attempt.outcome = {
        bytes: attempt.bytes,
        durationMs: performance.now() - attempt.startedAt,
        error: new TransportError(`请求连续 ${this.options.idleTimeoutMs}ms 没有收到数据`, 0, true),
        kind: 'failed',
        networkDetails: attempt.networkDetails,
      }
      attempt.controller.abort()
    }, this.options.idleTimeoutMs)
  }

  private async waitForWork(snapshot: VirtualStreamRegistrySnapshot): Promise<void> {
    const nextRetryAt = snapshot.streams
      .flatMap(stream => stream.segments)
      .flatMap(segment => segment.chunks)
      .filter(chunk => chunk.contentState !== 'complete' && chunk.writer === undefined)
      .reduce<number | undefined>(
        (earliest, chunk) =>
          chunk.retryAt > performance.now() && (earliest === undefined || chunk.retryAt < earliest)
            ? chunk.retryAt
            : earliest,
        undefined,
      )
    const delayMs = Math.min(
      maintenanceIntervalMs,
      nextRetryAt === undefined
        ? maintenanceIntervalMs
        : Math.max(0, nextRetryAt - performance.now()),
    )
    const observer = new AbortController()
    const destroy = () => observer.abort()
    this.destroyController.signal.addEventListener('abort', destroy, { once: true })
    try {
      await Promise.race([
        this.registry.waitForChange(snapshot.revision, observer.signal),
        new Promise<void>(resolve => globalThis.setTimeout(resolve, delayMs)),
      ]).catch(() => undefined)
    } finally {
      observer.abort()
      this.destroyController.signal.removeEventListener('abort', destroy)
    }
  }

  private async wait(delayMs: number): Promise<boolean> {
    if (delayMs <= 0) {
      return !this.destroyed
    }
    return await new Promise<boolean>(resolve => {
      const timer = globalThis.setTimeout(() => {
        this.destroyController.signal.removeEventListener('abort', abort)
        resolve(true)
      }, delayMs)
      const abort = () => {
        globalThis.clearTimeout(timer)
        resolve(false)
      }
      this.destroyController.signal.addEventListener('abort', abort, { once: true })
    })
  }

  private readPlaybackTime(): number {
    const value = this.options.getPlaybackTime()
    return Number.isFinite(value) ? value : 0
  }

  private readPlaybackRate(): number {
    const value = this.options.getPlaybackRate()
    return Number.isFinite(value) ? Math.max(value, 0.1) : 1
  }

  private clearTimer(timer: number | undefined): void {
    if (timer !== undefined) {
      globalThis.clearTimeout(timer)
    }
  }
}

export function estimateThroughput(snapshot: VirtualStreamRegistrySnapshot): number {
  const samples = getThroughputSamples(snapshot).sort((left, right) => left - right)
  if (samples.length === 0) {
    return defaultThroughputBytesPerSecond
  }
  const middle = Math.floor(samples.length / 2)
  return samples.length % 2 === 0
    ? ((samples[middle - 1] ?? 0) + (samples[middle] ?? 0)) / 2
    : (samples[middle] ?? defaultThroughputBytesPerSecond)
}

function getThroughputSamples(snapshot: VirtualStreamRegistrySnapshot): number[] {
  return snapshot.streams
    .flatMap(stream => stream.segments)
    .flatMap(segment => segment.chunks)
    .map(chunk => chunk.throughputBytesPerSecond)
    .filter(sample => Number.isFinite(sample) && sample > 0)
}

function selectBestCandidate(
  snapshot: VirtualStreamRegistrySnapshot,
  playbackTime: number,
  playbackRate: number,
): ChunkCandidate | undefined {
  const candidates = createCandidates(snapshot, playbackTime, playbackRate).filter(
    candidate =>
      candidate.chunk.writer === undefined &&
      candidate.chunk.contentState !== 'complete' &&
      candidate.chunk.retryAt <= performance.now() &&
      candidate.segment.state !== 'failed',
  )
  candidates.sort(compareCandidates)
  return candidates[0]
}

function findCandidate(
  snapshot: VirtualStreamRegistrySnapshot,
  chunkKey: string,
  playbackTime: number,
  playbackRate: number,
): ChunkCandidate | undefined {
  return createCandidates(snapshot, playbackTime, playbackRate).find(
    candidate => candidate.chunk.key === chunkKey,
  )
}

function isLowestPriorityWriter(
  snapshot: VirtualStreamRegistrySnapshot,
  current: ChunkCandidate,
  playbackTime: number,
  playbackRate: number,
): boolean {
  return createCandidates(snapshot, playbackTime, playbackRate)
    .filter(candidate => candidate.chunk.writer !== undefined)
    .every(candidate => compareCandidates(current, candidate) >= 0)
}

function createCandidates(
  snapshot: VirtualStreamRegistrySnapshot,
  playbackTime: number,
  playbackRate: number,
): ChunkCandidate[] {
  const candidates: ChunkCandidate[] = []
  for (const stream of snapshot.streams) {
    for (const segment of stream.segments) {
      const incomplete = segment.chunks.filter(chunk => chunk.contentState !== 'complete')
      const deliveryFrontier = incomplete.reduce(
        (minimum, chunk) => Math.min(minimum, chunk.startOffset),
        Number.POSITIVE_INFINITY,
      )
      for (const chunk of incomplete) {
        const totalLength = segment.chunks.reduce(
          (maximum, candidate) => Math.max(maximum, candidate.endOffset ?? 0),
          0,
        )
        const fraction = totalLength === 0 ? 0 : chunk.startOffset / totalLength
        candidates.push({
          chunk,
          direct: segment.readerCount > 0,
          estimatedMediaTime:
            ((segment.start + segment.duration * fraction - playbackTime) * 1_000) / playbackRate,
          frontier: chunk.startOffset === deliveryFrontier,
          segment,
          streamId: stream.id,
        })
      }
    }
  }
  return candidates
}

function compareCandidates(left: ChunkCandidate, right: ChunkCandidate): number {
  if (left.direct !== right.direct) {
    return left.direct ? -1 : 1
  }
  const deadline = left.estimatedMediaTime - right.estimatedMediaTime
  if (deadline !== 0) {
    return deadline
  }
  if (left.frontier !== right.frontier) {
    return left.frontier ? -1 : 1
  }
  return (
    left.chunk.createdAt - right.chunk.createdAt || left.chunk.key.localeCompare(right.chunk.key)
  )
}

function readContentLength(response: HttpTransportResponse): number | undefined {
  const value = response.headers.get('content-length')
  if (value === null) {
    return undefined
  }
  const length = Number.parseInt(value, 10)
  return Number.isSafeInteger(length) && length >= 0 ? length : undefined
}

function parseContentRange(value: string | null):
  | {
      endExclusive: number
      start: number
      total: number | undefined
    }
  | undefined {
  if (value === null) {
    return undefined
  }
  const match = contentRangePattern.exec(value.trim())
  if (match === null) {
    throw new TransportError(`无效的 Content-Range: ${value}`)
  }
  const start = Number.parseInt(match[1] ?? '', 10)
  const end = Number.parseInt(match[2] ?? '', 10)
  const totalText = match[3]
  const total =
    totalText === undefined || totalText === '*' ? undefined : Number.parseInt(totalText, 10)
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || end < start) {
    throw new TransportError(`无效的 Content-Range: ${value}`)
  }
  return { endExclusive: end + 1, start, total }
}

function getRetryDelay(retry: VirtualStreamRetryPolicy, attempt: number): number {
  const multiplier = retry.backoff === 'linear' ? attempt : 2 ** (attempt - 1)
  return Math.min(retry.retryDelayMs * multiplier, retry.maxRetryDelayMs)
}

function isRetryableStatus(code: number): boolean {
  return code === 0 || code === 408 || code === 429 || code >= 500
}

function toTransportError(cause: unknown): TransportError {
  if (cause instanceof TransportError) {
    return cause
  }
  return new TransportError(cause instanceof Error ? cause.message : '未知网络错误')
}
