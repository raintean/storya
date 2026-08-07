import type { HttpTransportRangeRequestMode, HttpTransportResponse } from 'storya-transport'
import { splitByteRanges } from './byte-ranges'

export type VirtualStreamSegmentState = 'empty' | 'failed' | 'filling' | 'ready'
export type VirtualStreamChunkContentState = 'empty' | 'partial' | 'complete'
export type VirtualStreamChunkWriterReleaseReason =
  | 'destroyed'
  | 'evicted'
  | 'failed'
  | 'preempted'
  | 'released'
  | 'version-changed'

export interface VirtualStreamPosition {
  readonly duration: number
  readonly start: number
}

export interface VirtualStreamStatistics {
  readonly buffering: { readonly end: number; readonly first: number; readonly start: number }
  readonly bwEstimate: number
  readonly chunkCount: number
  readonly loaded: number
  readonly loading: { readonly end: number; readonly first: number; readonly start: number }
  readonly parsing: { readonly end: number; readonly start: number }
  readonly retry: number
  readonly total: number
}

export interface VirtualStreamRequestParameters {
  readonly headers: Headers
  readonly method: 'GET' | 'HEAD'
  readonly rangeEnd: number | undefined
  readonly rangeStart: number
  readonly signal: AbortSignal
}

export interface VirtualStreamResource {
  readonly createRequest: (parameters: VirtualStreamRequestParameters) => Promise<Request>
  readonly headers: Readonly<Record<string, string>>
  readonly rangeEnd: number | undefined
  readonly rangeStart: number
  readonly url: string
}

export interface VirtualStreamSegmentDescriptor {
  readonly key: string
  readonly position: VirtualStreamPosition
  readonly prefetch: boolean
  readonly resource: VirtualStreamResource
}

export interface VirtualStreamRetryPolicy {
  readonly backoff: 'exponential' | 'linear'
  readonly maxNumRetry: number
  readonly maxRetryDelayMs: number
  readonly retryDelayMs: number
  readonly shouldRetry?: (
    retryCount: number,
    timeout: boolean,
    error: { readonly code: number; readonly message: string; readonly url: string },
  ) => boolean
}

export interface VirtualStreamFillPolicy {
  readonly errorRetry: VirtualStreamRetryPolicy | undefined
  readonly maxTimeToFirstByteMs: number
  readonly timeoutRetry: VirtualStreamRetryPolicy | undefined
}

export interface VirtualStreamSegmentResult {
  readonly code: number
  readonly data: ArrayBuffer
  readonly networkDetails: HttpTransportResponse | null
  readonly statistics: VirtualStreamStatistics
  readonly url: string
}

export interface VirtualStreamReadRequest {
  readonly fillPolicy: VirtualStreamFillPolicy
  readonly onStatistics?: (statistics: VirtualStreamStatistics) => void
  readonly segment: VirtualStreamSegmentDescriptor
  readonly streamId: string
}

export interface VirtualStreamChunkWriteRequest {
  readonly chunkKey: string
  readonly fillerId: number
}

export interface VirtualStreamChunkFillPlan {
  readonly chunkEndOffset: number | undefined
  readonly chunkKey: string
  readonly chunkStartOffset: number
  readonly discardBytes: number
  readonly fillPolicy: VirtualStreamFillPolicy
  readonly rangeEnabled: boolean
  readonly requestEnd: number | undefined
  readonly requestStart: number
  readonly resource: VirtualStreamResource
  readonly segmentDuration: number
  readonly segmentKey: string
  readonly segmentStart: number
  readonly streamId: string
}

export interface VirtualStreamChunkResponseMetadata {
  readonly contentLength: number | undefined
  readonly networkDetails: HttpTransportResponse
  readonly responseEnd: number | undefined
  readonly responseStart: number | undefined
  readonly resourceLength: number | undefined
  readonly status: number
}

export interface VirtualStreamChunkAttemptMetadata {
  readonly bytes: number
  readonly durationMs: number
  readonly networkRetry?: boolean
  readonly preempted?: boolean
  readonly rescued?: boolean
  readonly slowRetry?: boolean
}

export interface VirtualStreamChunkFillFailure {
  readonly code: number
  readonly kind: 'error' | 'timeout'
  readonly message: string
  readonly networkDetails: HttpTransportResponse | null
}

export interface VirtualStreamFrontierSnapshot {
  readonly barrier: boolean
  readonly confirmed: boolean
  readonly generation: number
  readonly segmentKey: string
}

export interface VirtualStreamChunkWriterSnapshot {
  readonly fillerId: number
  readonly id: number
}

export interface VirtualStreamChunkSnapshot {
  readonly attemptCount: number
  readonly contentState: VirtualStreamChunkContentState
  readonly contentVersion: number
  readonly createdAt: number
  readonly endOffset: number | undefined
  readonly failure: string | undefined
  readonly key: string
  readonly networkRetries: number
  readonly preemptions: number
  readonly receivedLength: number
  readonly rescueAttempts: number
  readonly retryAt: number
  readonly slowRetries: number
  readonly startOffset: number
  readonly throughputBytesPerSecond: number
  readonly writer: VirtualStreamChunkWriterSnapshot | undefined
}

export interface VirtualStreamSegmentSnapshot {
  readonly chunks: readonly VirtualStreamChunkSnapshot[]
  readonly duration: number
  readonly key: string
  readonly prefetch: boolean
  readonly readerCount: number
  readonly start: number
  readonly state: VirtualStreamSegmentState
  readonly url: string
}

export interface VirtualStreamSnapshot {
  readonly frontier: VirtualStreamFrontierSnapshot | undefined
  readonly id: string
  readonly segments: readonly VirtualStreamSegmentSnapshot[]
}

export interface VirtualStreamRegistrySnapshot {
  readonly revision: number
  readonly streams: readonly VirtualStreamSnapshot[]
}

export interface VirtualStreamRegistryOptions {
  readonly chunkSize: number
  readonly prefetchAheadSegments: number
}

interface VirtualStreamFillConfiguration {
  readonly fillPolicy: VirtualStreamFillPolicy
}

interface MutableVirtualStreamStatistics {
  buffering: { end: number; first: number; start: number }
  bwEstimate: number
  chunkCount: number
  loaded: number
  loading: { end: number; first: number; start: number }
  parsing: { end: number; start: number }
  retry: number
  total: number
}

interface VirtualStreamFrontier {
  barrier: boolean
  confirmed: boolean
  generation: number
  segment: VirtualStreamSegment
}

interface VirtualStreamChunkWriterState {
  readonly contentVersion: number
  readonly controller: AbortController
  readonly fillerId: number
  readonly id: number
}

interface RegistryWaiter {
  readonly afterRevision: number
  readonly cleanup: () => void
  readonly resolve: (revision: number) => void
}

let nextReaderId = 0
let nextWriterId = 0
const writerChunk = Symbol('VirtualStreamChunkWriter.chunk')

export class VirtualStreamReadFailure extends Error {
  readonly code: number
  readonly kind: 'aborted' | 'error' | 'timeout'
  readonly networkDetails: HttpTransportResponse | null

  constructor(
    kind: 'aborted' | 'error' | 'timeout',
    message: string,
    code = 0,
    networkDetails: HttpTransportResponse | null = null,
  ) {
    super(message)
    this.name = 'VirtualStreamReadFailure'
    this.kind = kind
    this.code = code
    this.networkDetails = networkDetails
  }
}

export class VirtualStreamSegmentReader {
  readonly id = ++nextReaderId
  readonly onStatistics: ((statistics: VirtualStreamStatistics) => void) | undefined
  readonly result: Promise<VirtualStreamSegmentResult>
  readonly segment: VirtualStreamSegment

  private readonly registry: VirtualStreamRegistry
  private rejectResult: (failure: VirtualStreamReadFailure) => void = () => undefined
  private resolveResult: (result: VirtualStreamSegmentResult) => void = () => undefined
  private settled = false

  constructor(
    registry: VirtualStreamRegistry,
    segment: VirtualStreamSegment,
    onStatistics: ((statistics: VirtualStreamStatistics) => void) | undefined,
  ) {
    this.registry = registry
    this.segment = segment
    this.onStatistics = onStatistics
    this.result = new Promise<VirtualStreamSegmentResult>((resolve, reject) => {
      this.resolveResult = resolve
      this.rejectResult = reject
    })
    void this.result.catch(() => undefined)
  }

  cancel(): void {
    this.registry.cancelSegmentReader(this)
  }

  isSettled(): boolean {
    return this.settled
  }

  resolve(result: VirtualStreamSegmentResult): void {
    if (this.settled) {
      return
    }
    this.settled = true
    this.resolveResult(result)
  }

  reject(failure: VirtualStreamReadFailure): void {
    if (this.settled) {
      return
    }
    this.settled = true
    this.rejectResult(failure)
  }
}

export class VirtualStreamChunk {
  readonly createdAt = performance.now()
  readonly key: string
  readonly segment: VirtualStreamSegment
  readonly stableEndOffset: number | undefined
  readonly startOffset: number

  attemptCount = 0
  buffers: Uint8Array[] = []
  contentVersion: number
  endOffset: number | undefined
  failure: string | undefined
  networkRetries = 0
  preemptions = 0
  receivedLength = 0
  rescueAttempts = 0
  retryAt = 0
  slowRetries = 0
  throughputBytesPerSecond = 0
  writer: VirtualStreamChunkWriterState | undefined

  constructor(segment: VirtualStreamSegment, startOffset: number, endOffset: number | undefined) {
    this.segment = segment
    this.startOffset = startOffset
    this.endOffset = endOffset
    this.stableEndOffset = endOffset
    this.contentVersion = segment.contentVersion
    this.key = `${segment.stream.id}::${segment.key}::${startOffset}`
  }

  get contentState(): VirtualStreamChunkContentState {
    if (this.endOffset !== undefined && this.receivedLength === this.endOffset - this.startOffset) {
      return 'complete'
    }
    return this.receivedLength === 0 ? 'empty' : 'partial'
  }

  get remainingLength(): number | undefined {
    return this.endOffset === undefined
      ? undefined
      : Math.max(0, this.endOffset - this.startOffset - this.receivedLength)
  }
}

export class VirtualStreamSegment {
  readonly key: string
  readers = new Map<number, VirtualStreamSegmentReader>()
  stream: VirtualStream

  chunks: VirtualStreamChunk[] = []
  contentVersion = 1
  descriptor: VirtualStreamSegmentDescriptor
  failure: VirtualStreamReadFailure | undefined
  fillConfiguration: VirtualStreamFillConfiguration | undefined
  finalUrl: string
  lastNetworkDetails: HttpTransportResponse | null = null
  responseCode = 0
  result: VirtualStreamSegmentResult | undefined
  segmentLength: number | undefined
  sequential = false
  validator: string | null = null

  readonly statistics: MutableVirtualStreamStatistics = createStatistics()

  constructor(stream: VirtualStream, descriptor: VirtualStreamSegmentDescriptor) {
    this.stream = stream
    this.descriptor = descriptor
    this.key = descriptor.key
    this.finalUrl = descriptor.resource.url
    const { rangeEnd, rangeStart } = descriptor.resource
    if (rangeEnd !== undefined && rangeEnd > rangeStart) {
      this.segmentLength = rangeEnd - rangeStart
      this.statistics.total = this.segmentLength
    }
  }

  get duration(): number {
    return this.descriptor.position.duration
  }

  get position(): VirtualStreamPosition {
    return { duration: this.duration, start: this.start }
  }

  get start(): number {
    return this.descriptor.position.start
  }

  get state(): VirtualStreamSegmentState {
    if (this.result !== undefined) {
      return 'ready'
    }
    if (this.failure !== undefined) {
      return 'failed'
    }
    if (
      this.chunks.some(chunk => chunk.contentState !== 'empty' || chunk.writer !== undefined) ||
      this.readers.size > 0
    ) {
      return 'filling'
    }
    return 'empty'
  }

  get resourceStart(): number {
    return this.descriptor.resource.rangeStart
  }

  get isPrefetchSequenceSegment(): boolean {
    return this.descriptor.prefetch
  }

  updateDescriptor(descriptor: VirtualStreamSegmentDescriptor): void {
    this.descriptor = descriptor
    this.finalUrl = descriptor.resource.url
  }

  updateStatistics(): void {
    const loaded = this.chunks.reduce((total, chunk) => total + chunk.receivedLength, 0)
    this.statistics.loaded = loaded
    this.statistics.total = this.segmentLength ?? 0
    const now = performance.now()
    if (this.statistics.loading.start > 0 && loaded > 0) {
      this.statistics.bwEstimate =
        (loaded * 8_000) / Math.max(1, now - this.statistics.loading.start)
    }
    const snapshot = snapshotStatistics(this.statistics)
    for (const reader of this.readers.values()) {
      reader.onStatistics?.(snapshot)
    }
  }
}

export class VirtualStream {
  readonly id: string
  readonly segmentsByKey = new Map<string, VirtualStreamSegment>()

  fillConfiguration: VirtualStreamFillConfiguration | undefined
  frontier: VirtualStreamFrontier | undefined
  frontierGeneration = 0
  prefetchSequence: VirtualStreamSegment[] = []
  retainedFrontier: VirtualStreamSegment | undefined

  constructor(id: string) {
    this.id = id
  }

  upsertSegment(descriptor: VirtualStreamSegmentDescriptor): VirtualStreamSegment {
    const segment =
      this.segmentsByKey.get(descriptor.key) ?? new VirtualStreamSegment(this, descriptor)
    segment.updateDescriptor(descriptor)
    this.segmentsByKey.set(descriptor.key, segment)
    if (segment.isPrefetchSequenceSegment && !this.prefetchSequence.includes(segment)) {
      this.prefetchSequence.push(segment)
      this.prefetchSequence.sort(compareSegments)
    }
    return segment
  }

  removeSegment(segment: VirtualStreamSegment): void {
    this.segmentsByKey.delete(segment.key)
    this.prefetchSequence = this.prefetchSequence.filter(candidate => candidate !== segment)
  }

  updateTopology(descriptors: readonly VirtualStreamSegmentDescriptor[]): void {
    this.prefetchSequence = descriptors
      .map(descriptor => this.upsertSegment(descriptor))
      .filter(segment => segment.isPrefetchSequenceSegment)
      .sort(compareSegments)
  }
}

export class VirtualStreamChunkWriter {
  readonly contentVersion: number
  readonly fillerId: number
  readonly id: number
  readonly signal: AbortSignal
  readonly [writerChunk]: VirtualStreamChunk

  private readonly registry: VirtualStreamRegistry
  private readonly state: VirtualStreamChunkWriterState

  constructor(
    registry: VirtualStreamRegistry,
    chunk: VirtualStreamChunk,
    state: VirtualStreamChunkWriterState,
  ) {
    this.registry = registry
    this[writerChunk] = chunk
    this.state = state
    this.contentVersion = state.contentVersion
    this.fillerId = state.fillerId
    this.id = state.id
    this.signal = state.controller.signal
  }

  append(data: Uint8Array): void {
    this.registry.appendChunk(this, data)
  }

  get chunkKey(): string {
    return this[writerChunk].key
  }

  get endOffset(): number | undefined {
    return this[writerChunk].endOffset
  }

  get networkRetries(): number {
    return this[writerChunk].networkRetries
  }

  get receivedLength(): number {
    return this[writerChunk].receivedLength
  }

  get remainingLength(): number | undefined {
    return this[writerChunk].remainingLength
  }

  get rescueAttempts(): number {
    return this[writerChunk].rescueAttempts
  }

  get sequential(): boolean {
    return this[writerChunk].segment.sequential
  }

  get startOffset(): number {
    return this[writerChunk].startOffset
  }

  acceptResponse(metadata: VirtualStreamChunkResponseMetadata): void {
    this.registry.acceptChunkResponse(this, metadata)
  }

  complete(): void {
    this.registry.completeChunk(this)
  }

  fail(failure: VirtualStreamChunkFillFailure, retryAt?: number): void {
    this.registry.failChunk(this, failure, retryAt)
  }

  getFillPlan(rangeMode: HttpTransportRangeRequestMode | undefined): VirtualStreamChunkFillPlan {
    return this.registry.getChunkFillPlan(this, rangeMode)
  }

  isCurrent(): boolean {
    return this.registry.isCurrentWriter(this)
  }

  recordAttempt(metadata: VirtualStreamChunkAttemptMetadata): void {
    this.registry.recordChunkAttempt(this, metadata)
  }

  release(reason: VirtualStreamChunkWriterReleaseReason = 'released'): void {
    this.registry.releaseChunkWriter(this, reason)
  }
}

export class VirtualStreamRegistry {
  private readonly chunkSize: number
  private readonly chunksByKey = new Map<string, VirtualStreamChunk>()
  private destroyed = false
  private readonly prefetchAheadSegments: number
  private revisionValue = 0
  private readonly streams = new Map<string, VirtualStream>()
  private readonly waiters = new Set<RegistryWaiter>()

  constructor(options: VirtualStreamRegistryOptions) {
    this.chunkSize = options.chunkSize
    this.prefetchAheadSegments = options.prefetchAheadSegments
  }

  get revision(): number {
    return this.revisionValue
  }

  createSegmentReader(request: VirtualStreamReadRequest): VirtualStreamSegmentReader {
    if (this.destroyed) {
      throw new Error('VirtualStreamRegistry 已经销毁')
    }

    const segment = this.resolveSegment(request.segment, request.streamId)
    const stream = segment.stream
    const configuration = { fillPolicy: request.fillPolicy }
    stream.fillConfiguration = configuration
    segment.fillConfiguration = configuration
    if (segment.failure !== undefined) {
      segment.failure = undefined
      for (const chunk of segment.chunks) {
        chunk.failure = undefined
        chunk.retryAt = 0
      }
    }

    const reader = new VirtualStreamSegmentReader(this, segment, request.onStatistics)
    if (segment.result !== undefined) {
      request.onStatistics?.(segment.result.statistics)
      reader.resolve(segment.result)
      this.updateFrontier(stream, segment, true)
      this.reconcileStream(stream)
      this.markChanged()
      return reader
    }

    segment.readers.set(reader.id, reader)
    if (segment.statistics.loading.start === 0) {
      segment.statistics.loading.start = performance.now()
    }
    request.onStatistics?.(snapshotStatistics(segment.statistics))
    this.updateFrontier(stream, segment, false)
    this.ensureSegmentChunks(segment)
    this.reconcileStream(stream)
    this.markChanged()
    return reader
  }

  updateStream(
    streamId: string,
    descriptors: readonly VirtualStreamSegmentDescriptor[],
  ): VirtualStream {
    if (this.destroyed) {
      throw new Error('VirtualStreamRegistry 已经销毁')
    }

    let stream = this.streams.get(streamId)
    if (stream === undefined) {
      stream = new VirtualStream(streamId)
      this.streams.set(streamId, stream)
    }

    stream.updateTopology(descriptors)
    this.reconcileStream(stream)
    this.markChanged()
    return stream
  }

  mergeStream(sourceId: string, targetId: string): void {
    if (sourceId === targetId) {
      return
    }
    const source = this.streams.get(sourceId)
    if (source === undefined) {
      return
    }
    let target = this.streams.get(targetId)
    if (target === undefined) {
      target = new VirtualStream(targetId)
      this.streams.set(targetId, target)
    }
    this.mergeStreams(source, target)
    this.reconcileStream(target)
    this.markChanged()
  }

  tryAcquireChunkWriter(
    request: VirtualStreamChunkWriteRequest,
  ): VirtualStreamChunkWriter | undefined {
    const chunk = this.chunksByKey.get(request.chunkKey)
    if (!this.canAcquireChunk(chunk)) {
      return undefined
    }
    return this.acquireWriter(chunk, request.fillerId)
  }

  trySwitchChunkWriter(
    currentWriter: VirtualStreamChunkWriter,
    target: VirtualStreamChunkWriteRequest,
  ): VirtualStreamChunkWriter | undefined {
    if (!this.isCurrentWriter(currentWriter)) {
      return undefined
    }
    const targetChunk = this.chunksByKey.get(target.chunkKey)
    const currentChunk = currentWriter[writerChunk]
    if (!this.canAcquireChunk(targetChunk) || targetChunk === currentChunk) {
      return undefined
    }

    this.revokeWriter(currentChunk, 'preempted', false)
    currentChunk.preemptions += 1
    const next = this.acquireWriter(targetChunk, target.fillerId, false)
    this.markChanged()
    return next
  }

  snapshot(): VirtualStreamRegistrySnapshot {
    return {
      revision: this.revisionValue,
      streams: [...this.streams.values()].map(stream => ({
        frontier:
          stream.frontier === undefined
            ? undefined
            : {
                barrier: stream.frontier.barrier,
                confirmed: stream.frontier.confirmed,
                generation: stream.frontier.generation,
                segmentKey: stream.frontier.segment.key,
              },
        id: stream.id,
        segments: [...stream.segmentsByKey.values()].sort(compareSegments).map(segment => ({
          chunks: segment.chunks.map(chunk => ({
            attemptCount: chunk.attemptCount,
            contentState: chunk.contentState,
            contentVersion: chunk.contentVersion,
            createdAt: chunk.createdAt,
            endOffset: chunk.endOffset,
            failure: chunk.failure,
            key: chunk.key,
            networkRetries: chunk.networkRetries,
            preemptions: chunk.preemptions,
            receivedLength: chunk.receivedLength,
            rescueAttempts: chunk.rescueAttempts,
            retryAt: chunk.retryAt,
            slowRetries: chunk.slowRetries,
            startOffset: chunk.startOffset,
            throughputBytesPerSecond: chunk.throughputBytesPerSecond,
            writer:
              chunk.writer === undefined
                ? undefined
                : { fillerId: chunk.writer.fillerId, id: chunk.writer.id },
          })),
          duration: segment.duration,
          key: segment.key,
          prefetch: this.isPrefetchSegment(stream, segment),
          readerCount: segment.readers.size,
          start: segment.start,
          state: segment.state,
          url: segment.descriptor.resource.url,
        })),
      })),
    }
  }

  waitForChange(afterRevision: number, signal?: AbortSignal): Promise<number> {
    if (this.destroyed || this.revisionValue !== afterRevision) {
      return Promise.resolve(this.revisionValue)
    }
    if (signal?.aborted === true) {
      return Promise.reject(signal.reason)
    }

    return new Promise<number>((resolve, reject) => {
      let waiter: RegistryWaiter
      const handleAbort = () => {
        this.waiters.delete(waiter)
        reject(signal?.reason)
      }
      const cleanup = () => signal?.removeEventListener('abort', handleAbort)
      waiter = { afterRevision, cleanup, resolve }
      this.waiters.add(waiter)
      signal?.addEventListener('abort', handleAbort, { once: true })
    })
  }

  evictBefore(playbackTime: number): void {
    if (!Number.isFinite(playbackTime)) {
      return
    }
    let changed = false
    for (const stream of this.streams.values()) {
      const frontier = stream.frontier
      if (
        frontier === undefined ||
        frontier.segment.readers.size > 0 ||
        frontier.segment.start + frontier.segment.duration > playbackTime
      ) {
        continue
      }
      stream.frontier = undefined
      stream.retainedFrontier = undefined
      changed = this.reconcileStream(stream) || changed
    }
    if (changed) {
      this.markChanged()
    }
  }

  cancelSegmentReader(reader: VirtualStreamSegmentReader): void {
    if (reader.isSettled()) {
      return
    }
    const segment = reader.segment
    if (!segment.readers.delete(reader.id)) {
      return
    }
    reader.reject(new VirtualStreamReadFailure('aborted', 'Segment 读取已经取消'))

    const frontier = segment.stream.frontier
    if (
      frontier?.segment === segment &&
      !frontier.confirmed &&
      segment.readers.size === 0 &&
      segment.failure === undefined
    ) {
      const retained = segment.stream.retainedFrontier
      segment.stream.frontier =
        retained === undefined
          ? undefined
          : {
              barrier: false,
              confirmed: true,
              generation: ++segment.stream.frontierGeneration,
              segment: retained,
            }
    }
    this.reconcileStream(segment.stream)
    this.markChanged()
  }

  appendChunk(writer: VirtualStreamChunkWriter, data: Uint8Array): void {
    const chunk = this.requireCurrentWriter(writer)
    if (data.byteLength === 0) {
      return
    }
    const remaining = chunk.remainingLength
    if (remaining !== undefined && data.byteLength > remaining) {
      throw new Error('Chunk 返回的数据超过了目标边界')
    }
    const copy = data.slice()
    chunk.buffers.push(copy)
    chunk.receivedLength += copy.byteLength
    chunk.failure = undefined
    chunk.segment.statistics.chunkCount += 1
    if (chunk.segment.statistics.loading.first === 0) {
      chunk.segment.statistics.loading.first = performance.now()
    }
    chunk.segment.updateStatistics()
    this.markChanged()
  }

  acceptChunkResponse(
    writer: VirtualStreamChunkWriter,
    metadata: VirtualStreamChunkResponseMetadata,
  ): void {
    const chunk = this.requireCurrentWriter(writer)
    const segment = chunk.segment
    const validator =
      metadata.networkDetails.headers.get('etag') ??
      metadata.networkDetails.headers.get('last-modified')
    if (segment.validator === null) {
      segment.validator = validator
    } else if (validator !== null && validator !== segment.validator) {
      this.invalidateSegmentContent(segment)
      throw new Error('Segment 资源标识在 Range 请求之间发生变化')
    }

    segment.lastNetworkDetails = metadata.networkDetails
    segment.finalUrl = metadata.networkDetails.url || segment.finalUrl
    segment.responseCode = metadata.status

    if (metadata.status === 200) {
      const declaredRangeEnd = segment.descriptor.resource.rangeEnd
      if (
        (declaredRangeEnd !== undefined && declaredRangeEnd > segment.resourceStart) ||
        segment.resourceStart !== 0 ||
        chunk.startOffset !== 0
      ) {
        throw new Error('服务器忽略了带边界的 Range 请求')
      }
      segment.sequential = true
      if (metadata.contentLength !== undefined) {
        this.setSegmentLength(segment, metadata.contentLength)
        chunk.endOffset = metadata.contentLength
      } else {
        chunk.endOffset = undefined
      }
      this.removeOtherChunks(segment, chunk)
      this.markChanged()
      return
    }

    if (metadata.status !== 206) {
      throw new Error(`Range 请求返回了 HTTP ${metadata.status}`)
    }
    if (metadata.responseStart === undefined || metadata.responseEnd === undefined) {
      throw new Error('Range 响应缺少可验证的响应边界')
    }

    const expectedStart = segment.resourceStart + chunk.startOffset
    if (
      metadata.responseStart < expectedStart ||
      metadata.responseStart > expectedStart + chunk.receivedLength
    ) {
      throw new Error(
        `Content-Range 起点错误, Chunk 起点 ${expectedStart}, 实际 ${metadata.responseStart}`,
      )
    }
    if (
      metadata.responseEnd >
      segment.resourceStart + (chunk.endOffset ?? Number.MAX_SAFE_INTEGER)
    ) {
      throw new Error('Content-Range 超出了 Chunk 边界')
    }
    if (segment.segmentLength === undefined) {
      if (metadata.resourceLength === undefined) {
        throw new Error('首个 Range 响应没有提供资源总长度')
      }
      this.setSegmentLength(segment, metadata.resourceLength - segment.resourceStart)
    } else if (
      metadata.resourceLength !== undefined &&
      segment.resourceStart + segment.segmentLength > metadata.resourceLength
    ) {
      throw new Error('Segment 字节范围超出了资源总长度')
    }

    const responseLocalEnd = metadata.responseEnd - segment.resourceStart
    if (chunk.endOffset === undefined || responseLocalEnd < chunk.endOffset) {
      chunk.endOffset = responseLocalEnd
    }
    this.ensureSegmentChunks(segment)
    this.markChanged()
  }

  completeChunk(writer: VirtualStreamChunkWriter): void {
    const chunk = this.requireCurrentWriter(writer)
    const segment = chunk.segment
    if (chunk.endOffset === undefined) {
      chunk.endOffset = chunk.startOffset + chunk.receivedLength
      this.setSegmentLength(segment, chunk.endOffset)
    }
    if (chunk.receivedLength !== chunk.endOffset - chunk.startOffset) {
      throw new Error(
        `Chunk 提前结束, 期望 ${chunk.endOffset - chunk.startOffset} 字节, 实际 ${chunk.receivedLength} 字节`,
      )
    }
    this.revokeWriter(chunk, 'released', false)
    chunk.failure = undefined
    chunk.retryAt = 0
    segment.updateStatistics()
    this.completeSegmentIfReady(segment)
    this.markChanged()
  }

  failChunk(
    writer: VirtualStreamChunkWriter,
    failure: VirtualStreamChunkFillFailure,
    retryAt?: number,
  ): void {
    const chunk = this.requireCurrentWriter(writer)
    const segment = chunk.segment
    this.revokeWriter(chunk, retryAt === undefined ? 'failed' : 'released', false)
    chunk.failure = failure.message
    chunk.retryAt = retryAt ?? 0

    if (retryAt !== undefined) {
      this.markChanged()
      return
    }

    const readFailure = new VirtualStreamReadFailure(
      failure.kind,
      failure.message,
      failure.code,
      failure.networkDetails,
    )
    segment.failure = readFailure
    for (const other of segment.chunks) {
      if (other.writer !== undefined) {
        this.revokeWriter(other, 'failed', false)
      }
    }
    const frontier = segment.stream.frontier
    if (frontier?.segment === segment) {
      frontier.barrier = true
      frontier.confirmed = true
      segment.stream.retainedFrontier = segment
    }
    segment.statistics.loading.end = performance.now()
    segment.updateStatistics()
    this.rejectSegmentReaders(segment, readFailure)
    this.reconcileStream(segment.stream)
    this.markChanged()
  }

  recordChunkAttempt(
    writer: VirtualStreamChunkWriter,
    metadata: VirtualStreamChunkAttemptMetadata,
  ): void {
    const chunk = this.requireCurrentWriter(writer)
    chunk.attemptCount += 1
    if (metadata.networkRetry === true) {
      chunk.networkRetries += 1
    }
    if (metadata.rescued === true) {
      chunk.rescueAttempts += 1
    }
    if (metadata.slowRetry === true) {
      chunk.slowRetries += 1
    }
    if (metadata.preempted === true) {
      chunk.preemptions += 1
    }
    if (metadata.bytes > 0 && metadata.durationMs >= 100) {
      const sample = (metadata.bytes * 1_000) / metadata.durationMs
      chunk.throughputBytesPerSecond =
        chunk.throughputBytesPerSecond === 0
          ? sample
          : chunk.throughputBytesPerSecond * 0.7 + sample * 0.3
    }
    if (
      metadata.networkRetry === true ||
      metadata.rescued === true ||
      metadata.slowRetry === true
    ) {
      chunk.segment.statistics.retry += 1
      chunk.segment.updateStatistics()
    }
    this.markChanged()
  }

  getChunkFillPlan(
    writer: VirtualStreamChunkWriter,
    rangeMode: HttpTransportRangeRequestMode | undefined,
  ): VirtualStreamChunkFillPlan {
    const chunk = this.requireCurrentWriter(writer)
    const segment = chunk.segment
    const configuration = segment.fillConfiguration ?? segment.stream.fillConfiguration
    if (configuration === undefined) {
      throw new Error('Segment 不存在可用的 hls.js 加载配置')
    }

    const rangeEnabled = !segment.sequential
    const stable = rangeEnabled && rangeMode === 'stable'
    const localRequestStart = chunk.startOffset + (stable ? 0 : chunk.receivedLength)
    const requestEndOffset = stable ? chunk.stableEndOffset : chunk.endOffset
    return {
      chunkEndOffset: chunk.endOffset,
      chunkKey: chunk.key,
      chunkStartOffset: chunk.startOffset,
      discardBytes: stable ? chunk.receivedLength : 0,
      fillPolicy: configuration.fillPolicy,
      rangeEnabled,
      requestEnd:
        rangeEnabled && requestEndOffset !== undefined
          ? segment.resourceStart + requestEndOffset
          : undefined,
      requestStart: segment.resourceStart + localRequestStart,
      resource: segment.descriptor.resource,
      segmentDuration: segment.duration,
      segmentKey: segment.key,
      segmentStart: segment.start,
      streamId: segment.stream.id,
    }
  }

  isCurrentWriter(writer: VirtualStreamChunkWriter): boolean {
    const chunk = writer[writerChunk]
    const state = chunk.writer
    return (
      !this.destroyed &&
      state !== undefined &&
      state.id === writer.id &&
      state.contentVersion === writer.contentVersion &&
      chunk.contentVersion === writer.contentVersion
    )
  }

  releaseChunkWriter(
    writer: VirtualStreamChunkWriter,
    reason: VirtualStreamChunkWriterReleaseReason,
  ): void {
    if (!this.isCurrentWriter(writer)) {
      return
    }
    this.revokeWriter(writer[writerChunk], reason)
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    const failure = new VirtualStreamReadFailure('aborted', 'VirtualStreamRegistry 已经销毁')
    for (const stream of this.streams.values()) {
      for (const segment of stream.segmentsByKey.values()) {
        this.rejectSegmentReaders(segment, failure)
        for (const chunk of segment.chunks) {
          this.revokeWriter(chunk, 'destroyed', false)
        }
      }
    }
    this.markChanged()
    this.streams.clear()
    this.chunksByKey.clear()
  }

  values(): Iterable<VirtualStream> {
    return this.streams.values()
  }

  private acquireWriter(
    chunk: VirtualStreamChunk,
    fillerId: number,
    notify = true,
  ): VirtualStreamChunkWriter {
    const state: VirtualStreamChunkWriterState = {
      contentVersion: chunk.contentVersion,
      controller: new AbortController(),
      fillerId,
      id: ++nextWriterId,
    }
    chunk.writer = state
    chunk.failure = undefined
    if (chunk.segment.statistics.loading.start === 0) {
      chunk.segment.statistics.loading.start = performance.now()
      chunk.segment.updateStatistics()
    }
    const writer = new VirtualStreamChunkWriter(this, chunk, state)
    if (notify) {
      this.markChanged()
    }
    return writer
  }

  private canAcquireChunk(chunk: VirtualStreamChunk | undefined): chunk is VirtualStreamChunk {
    return (
      !this.destroyed &&
      chunk !== undefined &&
      chunk.writer === undefined &&
      chunk.contentState !== 'complete' &&
      chunk.retryAt <= performance.now() &&
      chunk.contentVersion === chunk.segment.contentVersion &&
      chunk.segment.failure === undefined &&
      this.hasFillIntent(chunk.segment)
    )
  }

  private requireCurrentWriter(writer: VirtualStreamChunkWriter): VirtualStreamChunk {
    if (!this.isCurrentWriter(writer)) {
      throw new Error('VirtualStreamChunkWriter 已经失效')
    }
    return writer[writerChunk]
  }

  private revokeWriter(
    chunk: VirtualStreamChunk,
    _reason: VirtualStreamChunkWriterReleaseReason,
    notify = true,
  ): void {
    const state = chunk.writer
    if (state === undefined) {
      return
    }
    chunk.writer = undefined
    state.controller.abort()
    if (notify) {
      this.markChanged()
    }
  }

  private resolveSegment(
    descriptor: VirtualStreamSegmentDescriptor,
    fallbackStreamId: string,
  ): VirtualStreamSegment {
    let stream = this.streams.get(fallbackStreamId)
    if (stream === undefined) {
      stream = new VirtualStream(fallbackStreamId)
      this.streams.set(fallbackStreamId, stream)
    }
    return stream.upsertSegment(descriptor)
  }

  private mergeStreams(source: VirtualStream, target: VirtualStream): void {
    if (source === target) {
      return
    }
    for (const segment of source.segmentsByKey.values()) {
      const existing = target.segmentsByKey.get(segment.key)
      if (existing === undefined) {
        source.removeSegment(segment)
        segment.stream = target
        target.segmentsByKey.set(segment.key, segment)
        if (segment.isPrefetchSequenceSegment) {
          target.prefetchSequence.push(segment)
        }
        continue
      }
      if (segment.readers.size > 0 || segment.chunks.length > 0) {
        target.removeSegment(existing)
        segment.stream = target
        target.segmentsByKey.set(segment.key, segment)
        if (segment.isPrefetchSequenceSegment) {
          target.prefetchSequence.push(segment)
        }
      }
    }
    target.prefetchSequence = [...new Set(target.prefetchSequence)].sort(compareSegments)
    target.fillConfiguration ??= source.fillConfiguration
    if (target.frontier === undefined && source.frontier !== undefined) {
      source.frontier.segment.stream = target
      target.frontier = source.frontier
      target.retainedFrontier = source.retainedFrontier
      target.frontierGeneration = source.frontierGeneration
    }
    this.streams.delete(source.id)
  }

  private updateFrontier(
    stream: VirtualStream,
    segment: VirtualStreamSegment,
    confirmed: boolean,
  ): void {
    if (!segment.isPrefetchSequenceSegment) {
      return
    }
    stream.frontier = {
      barrier: false,
      confirmed,
      generation: ++stream.frontierGeneration,
      segment,
    }
    if (confirmed) {
      stream.retainedFrontier = segment
    }
  }

  private reconcileStream(stream: VirtualStream, evictWriters = true): boolean {
    const desired = new Set<VirtualStreamSegment>()
    const frontier = stream.frontier
    if (frontier !== undefined) {
      const index = stream.prefetchSequence.indexOf(frontier.segment)
      if (index >= 0) {
        const count = frontier.barrier ? 1 : this.prefetchAheadSegments + 1
        for (const segment of stream.prefetchSequence.slice(index, index + count)) {
          desired.add(segment)
        }
      }
    }

    let changed = false
    for (const segment of stream.segmentsByKey.values()) {
      if (desired.has(segment) || segment.readers.size > 0) {
        segment.fillConfiguration ??= stream.fillConfiguration
        changed = this.ensureSegmentChunks(segment) || changed
        continue
      }
      if (!segment.isPrefetchSequenceSegment || segment.chunks.length === 0) {
        continue
      }
      if (!evictWriters && segment.chunks.some(chunk => chunk.writer !== undefined)) {
        continue
      }
      changed = this.clearSegmentContent(segment) || changed
    }
    return changed
  }

  private isPrefetchSegment(stream: VirtualStream, segment: VirtualStreamSegment): boolean {
    const frontier = stream.frontier
    if (frontier === undefined || frontier.barrier) {
      return false
    }
    const frontierIndex = stream.prefetchSequence.indexOf(frontier.segment)
    const segmentIndex = stream.prefetchSequence.indexOf(segment)
    return (
      frontierIndex >= 0 &&
      segmentIndex > frontierIndex &&
      segmentIndex <= frontierIndex + this.prefetchAheadSegments
    )
  }

  private hasFillIntent(segment: VirtualStreamSegment): boolean {
    if (segment.readers.size > 0) {
      return true
    }
    const frontier = segment.stream.frontier
    if (frontier?.barrier === true) {
      return frontier.segment === segment
    }
    if (frontier === undefined) {
      return false
    }
    const frontierIndex = segment.stream.prefetchSequence.indexOf(frontier.segment)
    const segmentIndex = segment.stream.prefetchSequence.indexOf(segment)
    return (
      frontierIndex >= 0 &&
      segmentIndex >= frontierIndex &&
      segmentIndex <= frontierIndex + this.prefetchAheadSegments
    )
  }

  private ensureSegmentChunks(segment: VirtualStreamSegment): boolean {
    if (segment.result !== undefined || segment.failure !== undefined) {
      return false
    }
    const ranges =
      segment.segmentLength === undefined
        ? [{ start: 0, endExclusive: this.chunkSize }]
        : splitByteRanges(0, segment.segmentLength, this.chunkSize)
    let changed = false
    for (const range of ranges) {
      let chunk = segment.chunks.find(candidate => candidate.startOffset === range.start)
      if (chunk === undefined) {
        chunk = new VirtualStreamChunk(segment, range.start, range.endExclusive)
        segment.chunks.push(chunk)
        segment.chunks.sort((left, right) => left.startOffset - right.startOffset)
        this.chunksByKey.set(chunk.key, chunk)
        changed = true
      } else if (chunk.endOffset !== range.endExclusive) {
        if (chunk.receivedLength > range.endExclusive - range.start) {
          throw new Error('已经接受的 Chunk 数据超过了新边界')
        }
        chunk.endOffset = range.endExclusive
        changed = true
      }
    }
    return changed
  }

  private clearSegmentContent(segment: VirtualStreamSegment): boolean {
    if (
      segment.chunks.length === 0 &&
      segment.result === undefined &&
      segment.failure === undefined
    ) {
      return false
    }
    for (const chunk of segment.chunks) {
      this.revokeWriter(chunk, 'evicted', false)
      this.chunksByKey.delete(chunk.key)
    }
    segment.chunks = []
    segment.result = undefined
    segment.failure = undefined
    segment.segmentLength = this.getDeclaredSegmentLength(segment.descriptor.resource)
    segment.sequential = false
    segment.validator = null
    segment.contentVersion += 1
    segment.statistics.loaded = 0
    segment.statistics.total = segment.segmentLength ?? 0
    return true
  }

  private invalidateSegmentContent(segment: VirtualStreamSegment): void {
    this.clearSegmentContent(segment)
    this.ensureSegmentChunks(segment)
    this.markChanged()
  }

  private removeOtherChunks(segment: VirtualStreamSegment, keep: VirtualStreamChunk): void {
    for (const chunk of segment.chunks) {
      if (chunk === keep) {
        continue
      }
      this.revokeWriter(chunk, 'version-changed', false)
      this.chunksByKey.delete(chunk.key)
    }
    segment.chunks = [keep]
  }

  private setSegmentLength(segment: VirtualStreamSegment, length: number): void {
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new Error(`无效的 Segment 长度 ${length}`)
    }
    if (segment.segmentLength !== undefined && segment.segmentLength !== length) {
      throw new Error(`Segment 长度发生变化, 原长度 ${segment.segmentLength}, 新长度 ${length}`)
    }
    segment.segmentLength = length
    segment.statistics.total = length
  }

  private completeSegmentIfReady(segment: VirtualStreamSegment): void {
    const length = segment.segmentLength
    if (
      length === undefined ||
      segment.chunks.length === 0 ||
      segment.chunks.some(chunk => chunk.contentState !== 'complete')
    ) {
      return
    }

    const sorted = [...segment.chunks].sort((left, right) => left.startOffset - right.startOffset)
    let offset = 0
    for (const chunk of sorted) {
      if (chunk.startOffset !== offset || chunk.endOffset === undefined) {
        return
      }
      offset = chunk.endOffset
    }
    if (offset !== length) {
      return
    }

    const data = new Uint8Array(length)
    for (const chunk of sorted) {
      let writeOffset = chunk.startOffset
      for (const buffer of chunk.buffers) {
        data.set(buffer, writeOffset)
        writeOffset += buffer.byteLength
      }
    }

    const now = performance.now()
    segment.statistics.loading.end = now
    if (segment.statistics.loading.first === 0) {
      segment.statistics.loading.first = now
    }
    segment.updateStatistics()
    const result: VirtualStreamSegmentResult = {
      code: segment.responseCode || (segment.sequential ? 200 : 206),
      data: data.buffer,
      networkDetails: segment.lastNetworkDetails,
      statistics: snapshotStatistics(segment.statistics),
      url: segment.finalUrl,
    }
    segment.result = result
    segment.failure = undefined

    const frontier = segment.stream.frontier
    if (frontier?.segment === segment) {
      frontier.barrier = false
      frontier.confirmed = true
      segment.stream.retainedFrontier = segment
    }
    this.resolveSegmentReaders(segment, result)
    this.reconcileStream(segment.stream)
  }

  private resolveSegmentReaders(
    segment: VirtualStreamSegment,
    result: VirtualStreamSegmentResult,
  ): void {
    for (const reader of segment.readers.values()) {
      reader.resolve(result)
    }
    segment.readers.clear()
  }

  private rejectSegmentReaders(
    segment: VirtualStreamSegment,
    failure: VirtualStreamReadFailure,
  ): void {
    for (const reader of segment.readers.values()) {
      reader.reject(failure)
    }
    segment.readers.clear()
  }

  private getDeclaredSegmentLength(resource: VirtualStreamResource): number | undefined {
    return resource.rangeEnd !== undefined && resource.rangeEnd > resource.rangeStart
      ? resource.rangeEnd - resource.rangeStart
      : undefined
  }

  private markChanged(): void {
    this.revisionValue += 1
    for (const waiter of [...this.waiters]) {
      if (this.revisionValue === waiter.afterRevision) {
        continue
      }
      this.waiters.delete(waiter)
      waiter.cleanup()
      waiter.resolve(this.revisionValue)
    }
  }
}

function createStatistics(): MutableVirtualStreamStatistics {
  return {
    buffering: { end: 0, first: 0, start: 0 },
    bwEstimate: 0,
    chunkCount: 0,
    loaded: 0,
    loading: { end: 0, first: 0, start: 0 },
    parsing: { end: 0, start: 0 },
    retry: 0,
    total: 0,
  }
}

function snapshotStatistics(statistics: MutableVirtualStreamStatistics): VirtualStreamStatistics {
  return {
    buffering: { ...statistics.buffering },
    bwEstimate: statistics.bwEstimate,
    chunkCount: statistics.chunkCount,
    loaded: statistics.loaded,
    loading: { ...statistics.loading },
    parsing: { ...statistics.parsing },
    retry: statistics.retry,
    total: statistics.total,
  }
}

function compareSegments(left: VirtualStreamSegment, right: VirtualStreamSegment): number {
  return left.start - right.start || left.key.localeCompare(right.key)
}
