import type { FragmentLoaderContext } from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import type { ParallelSegmentLoader } from './parallel-segment-loader'
import type { ParallelSegmentLoaderState } from './parallel-segment-loader-state'
import type { RescueEventRecord, RescueReason } from './rescue-tracker'
import type { VirtualStreamChunk } from './virtual-stream-chunk'
import type { SegmentLoadFailure, VirtualStreamSegment } from './virtual-stream-segment'

export interface SegmentFetchWorkOptions {
  chunkKey: string
  context: FragmentLoaderContext
  generation: number
  loader: ParallelSegmentLoader
  planning: boolean
  rangeEnabled: boolean
  requestEnd: number | undefined
  requestStart: number
  rescueAvailable: boolean
  resourceLength: number | undefined
  segmentKey: string
  stallDetectionEnabled: boolean
  startedAt: number
  streamId: string
  transport: HttpTransport
}

interface SegmentFetchResult {
  data: Uint8Array
  firstByteAt: number
  response: Response
  url: string
}

type DetectedRescue = Omit<
  RescueEventRecord,
  'attempt' | 'chunkKey' | 'generation' | 'segmentKey' | 'streamId'
>

export class SegmentFetchWork {
  readonly chunkKey: string
  readonly generation: number
  readonly method = 'GET'
  readonly requestEnd: number | undefined
  readonly requestStart: number
  readonly segmentKey: string
  readonly startedAt: number
  readonly streamId: string
  readonly task = 'chunk'

  private readonly context: FragmentLoaderContext
  private readonly controller = new AbortController()
  private readonly loader: ParallelSegmentLoader
  private readonly planning: boolean
  private readonly rangeEnabled: boolean
  private receivedBytes = 0
  private detectedRescue: DetectedRescue | undefined
  private readonly rescueAvailable: boolean
  private readonly resourceLength: number | undefined
  private started = false
  private readonly stallDetectionEnabled: boolean
  private readonly transport: HttpTransport
  private transferFinished = false

  constructor(options: SegmentFetchWorkOptions) {
    this.chunkKey = options.chunkKey
    this.context = options.context
    this.generation = options.generation
    this.loader = options.loader
    this.planning = options.planning
    this.rangeEnabled = options.rangeEnabled
    this.requestEnd = options.requestEnd
    this.requestStart = options.requestStart
    this.rescueAvailable = options.rescueAvailable
    this.resourceLength = options.resourceLength
    this.segmentKey = options.segmentKey
    this.stallDetectionEnabled = options.stallDetectionEnabled
    this.startedAt = options.startedAt
    this.streamId = options.streamId
    this.transport = options.transport
  }

  async run(): Promise<void> {
    if (this.started) {
      throw new Error('SegmentFetchWork 只能执行一次')
    }
    this.started = true
    this.loader.startTransfer(this.generation, this.startedAt)
    try {
      const result = await this.fetchChunk()
      this.completeChunk(result)
    } catch (cause) {
      this.finishFailedAttempt(cause)
    } finally {
      if (!this.transferFinished) {
        this.loader.finishTransfer(this.generation, performance.now(), false)
        this.transferFinished = true
      }
    }
  }

  cancel(reason: unknown): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort(reason)
    }
  }

  isCurrent(): boolean {
    return this.locateChunk(this.loader.state) !== undefined
  }

  private async fetchChunk(): Promise<SegmentFetchResult> {
    const config = this.loader.hlsConfig
    if (config === undefined) {
      throw new Error('ParallelSegmentLoader 尚未取得 hls.js 配置')
    }
    const headers = new Headers(this.context.headers)
    if (this.rangeEnabled && this.requestEnd !== undefined) {
      headers.set('range', `bytes=${this.requestStart}-${this.requestEnd - 1}`)
    } else {
      headers.delete('range')
    }
    const requestContext: FragmentLoaderContext = {
      ...this.context,
      headers: Object.fromEntries(headers.entries()),
      rangeEnd: this.rangeEnabled ? (this.requestEnd ?? 0) : 0,
      rangeStart: this.rangeEnabled ? this.requestStart : 0,
    }
    const init: RequestInit = {
      credentials: 'same-origin',
      headers,
      method: 'GET',
      mode: 'cors',
      signal: this.controller.signal,
    }
    const request =
      (await config.fetchSetup?.(requestContext, init)) ?? new Request(this.context.url, init)
    if (!this.isCurrent()) {
      throw new DOMException('Chunk Fill 已经失效', 'AbortError')
    }

    const loadPolicy = config.fragLoadPolicy.default
    const loadTimer =
      Number.isFinite(loadPolicy.maxLoadTimeMs) && loadPolicy.maxLoadTimeMs > 0
        ? globalThis.setTimeout(() => {
            if (!this.controller.signal.aborted) {
              this.controller.abort(new DOMException('Chunk 请求超过最大加载时间', 'TimeoutError'))
            }
          }, loadPolicy.maxLoadTimeMs)
        : undefined
    let firstByteTimer =
      Number.isFinite(loadPolicy.maxTimeToFirstByteMs) && loadPolicy.maxTimeToFirstByteMs > 0
        ? globalThis.setTimeout(() => {
            if (!this.controller.signal.aborted) {
              this.controller.abort(new DOMException('Chunk 等待响应头超时', 'TimeoutError'))
            }
          }, loadPolicy.maxTimeToFirstByteMs)
        : undefined
    try {
      const transportResponse = await this.transport.request(request)
      if (firstByteTimer !== undefined) {
        globalThis.clearTimeout(firstByteTimer)
        firstByteTimer = undefined
      }
      const firstByteAt = performance.now()
      if (!transportResponse.ok) {
        const response = createNetworkDetails(transportResponse)
        throw new ChunkRequestFailure(
          `HTTP ${transportResponse.status} ${transportResponse.statusText}`.trim(),
          transportResponse.status,
          response,
        )
      }
      this.prepareResponseHeaders(createNetworkDetails(transportResponse), firstByteAt)
      if (!this.isCurrent()) {
        await transportResponse.body?.cancel('Chunk response headers 校验失败')
        throw new ChunkRequestFailure('Chunk response headers 校验失败')
      }
      const data = await this.readResponseBody(transportResponse)
      const response = await this.createReadableRangeResponse(
        request,
        transportResponse,
        data.byteLength,
      )
      return { data, firstByteAt, response, url: transportResponse.url }
    } catch (cause) {
      if (cause instanceof ChunkRequestFailure || this.controller.signal.aborted) {
        throw cause
      }
      throw new ChunkRequestFailure(cause instanceof Error ? cause.message : 'Transport 请求失败')
    } finally {
      if (loadTimer !== undefined) {
        globalThis.clearTimeout(loadTimer)
      }
      if (firstByteTimer !== undefined) {
        globalThis.clearTimeout(firstByteTimer)
      }
    }
  }

  private async readResponseBody(response: HttpTransportResponse): Promise<Uint8Array> {
    const parts: Uint8Array[] = []
    const bodyStartedAt = performance.now()
    const expectedBytes = resolveExpectedResponseBytes(
      response.headers,
      this.requestStart,
      this.requestEnd,
      this.resourceLength,
    )
    this.loader.startTransferBody(this.generation, bodyStartedAt)
    let slowTimer: ReturnType<typeof globalThis.setInterval> | undefined
    let stallTimer: ReturnType<typeof globalThis.setTimeout> | undefined
    const resetStallTimer = () => {
      if (!this.stallDetectionEnabled) {
        return
      }
      if (stallTimer !== undefined) {
        globalThis.clearTimeout(stallTimer)
      }
      stallTimer = globalThis.setTimeout(() => {
        const message = `Chunk 连续 ${this.loader.rescue.stallTimeoutMs}ms 没有收到数据`
        if (this.rescueAvailable) {
          this.abortForRescue('stall', message)
        } else {
          this.abortForExhaustedStall(`${message}, 救援次数已经耗尽`)
        }
      }, this.loader.rescue.stallTimeoutMs)
    }
    const checkSlowTransfer = () => {
      if (
        this.controller.signal.aborted ||
        expectedBytes === undefined ||
        this.receivedBytes <= 0 ||
        this.receivedBytes >= expectedBytes
      ) {
        return
      }
      const comparison = this.loader.compareTransferRate(this.generation, performance.now())
      if (
        comparison === undefined ||
        comparison.currentRate >=
          comparison.peerMedianRate * this.loader.rescue.slowRateThresholdRatio
      ) {
        return
      }
      const remainingBytes = expectedBytes - this.receivedBytes
      const continueEtaMs = (remainingBytes * 1000) / comparison.currentRate
      const retryEtaMs =
        (expectedBytes * 1000) / comparison.peerMedianRate + comparison.peerMedianTtfbMs
      if (continueEtaMs <= retryEtaMs) {
        return
      }
      this.abortForRescue(
        'slow',
        `Chunk 当前速率为同期 GET 中位速率的 ${formatRateRatio(
          comparison.currentRate / comparison.peerMedianRate,
        )}, 预计重新请求更快`,
        {
          continueEtaMs,
          currentRate: comparison.currentRate,
          peerCount: comparison.peerCount,
          peerMedianRate: comparison.peerMedianRate,
          retryEtaMs,
        },
      )
    }
    const accept = (data: Uint8Array) => {
      if (data.byteLength === 0 || this.controller.signal.aborted) {
        return
      }
      const owned = data.slice()
      parts.push(owned)
      this.receivedBytes += owned.byteLength
      this.loader.recordTransferProgress(this.generation, owned.byteLength, performance.now())
      this.updateChunkProgress(this.receivedBytes)
      resetStallTimer()
    }

    resetStallTimer()
    if (
      this.rescueAvailable &&
      expectedBytes !== undefined &&
      this.loader.rescue.slowRateThresholdRatio > 0
    ) {
      const intervalMs = Math.max(1, Math.min(250, this.loader.rescue.stallTimeoutMs / 4))
      slowTimer = globalThis.setInterval(checkSlowTransfer, intervalMs)
    }
    try {
      if (response.body === null) {
        accept(new Uint8Array(await response.arrayBuffer()))
      } else {
        const reader = response.body.getReader()
        const abort = () => {
          void reader.cancel(this.controller.signal.reason).catch(() => undefined)
        }
        this.controller.signal.addEventListener('abort', abort, { once: true })
        try {
          while (true) {
            const result = await reader.read()
            if (this.controller.signal.aborted) {
              throw this.controller.signal.reason
            }
            if (result.done) {
              break
            }
            accept(result.value)
          }
        } finally {
          this.controller.signal.removeEventListener('abort', abort)
          reader.releaseLock()
        }
      }
    } finally {
      if (slowTimer !== undefined) {
        globalThis.clearInterval(slowTimer)
      }
      if (stallTimer !== undefined) {
        globalThis.clearTimeout(stallTimer)
      }
    }
    const transferCompletedAt = performance.now()
    this.loader.finishTransfer(this.generation, transferCompletedAt, true)
    this.transferFinished = true

    const data = new Uint8Array(this.receivedBytes)
    let offset = 0
    for (const part of parts) {
      data.set(part, offset)
      offset += part.byteLength
    }
    return data
  }

  private async createReadableRangeResponse(
    request: Request,
    response: HttpTransportResponse,
    receivedBytes: number,
  ): Promise<Response> {
    if (
      response.status !== 206 ||
      response.headers.has('content-range') ||
      !this.rangeEnabled ||
      this.requestEnd === undefined
    ) {
      return createNetworkDetails(response)
    }

    const resourceLength =
      this.resourceLength === undefined
        ? await this.discoverResourceLength(request)
        : this.resourceLength
    if (resourceLength === undefined || receivedBytes <= 0) {
      return createNetworkDetails(response)
    }

    const expectedEnd = Math.min(this.requestEnd, resourceLength)
    if (receivedBytes !== expectedEnd - this.requestStart) {
      return createNetworkDetails(response)
    }
    const endInclusive = expectedEnd - 1
    const headers = new Headers(response.headers)
    headers.set('content-range', `bytes ${this.requestStart}-${endInclusive}/${resourceLength}`)
    return new Response(null, {
      headers,
      status: response.status,
      statusText: response.statusText,
    })
  }

  private async discoverResourceLength(request: Request): Promise<number | undefined> {
    const headers = new Headers(request.headers)
    headers.delete('range')
    const headRequest = new Request(request.url, {
      cache: request.cache,
      credentials: request.credentials,
      headers,
      method: 'HEAD',
      mode: request.mode,
      redirect: request.redirect,
      signal: this.controller.signal,
    })
    try {
      const response = await this.transport.request(headRequest)
      const contentLength = Number.parseInt(response.headers.get('content-length') ?? '', 10)
      return response.ok && Number.isSafeInteger(contentLength) && contentLength > 0
        ? contentLength
        : undefined
    } catch (cause) {
      if (this.controller.signal.aborted) {
        throw cause
      }
      return undefined
    }
  }

  private updateChunkProgress(loadedBytes: number): void {
    if (this.loader.state.destroyed || !this.isCurrent()) {
      return
    }
    this.loader.update(state => {
      this.locateChunk(state)?.chunk.updateProgress(this.generation, loadedBytes)
      return undefined
    })
  }

  private prepareResponseHeaders(response: Response, firstByteAt: number): void {
    if (response.status === 200) {
      this.loader.update(state => {
        const located = this.locateChunk(state)
        if (located === undefined) {
          return undefined
        }
        const { chunk, segment } = located
        if (segment.declaredRange || (chunk.rangeEnabled && chunk.start !== 0)) {
          segment.fail(
            createFailure('服务器忽略了带边界的 Range 请求', response.status, response),
            firstByteAt,
          )
          return undefined
        }
        segment.firstByteAt ??= firstByteAt
        segment.useSequentialRange()
        return undefined
      })
      return
    }
    if (response.status !== 206 || !this.rangeEnabled) {
      return
    }

    let contentRange: ParsedContentRange | undefined
    try {
      contentRange = parseContentRange(response.headers.get('content-range'))
    } catch (cause) {
      this.loader.update(state => {
        this.locateChunk(state)?.segment.fail(toFailure(cause), firstByteAt)
        return undefined
      })
      return
    }
    if (contentRange === undefined) {
      return
    }

    this.loader.update(state => {
      const located = this.locateChunk(state)
      if (located === undefined) {
        return undefined
      }
      const { chunk, segment } = located
      const expectedStart = segment.resourceStart + chunk.start
      const expectedEnd =
        this.requestEnd === undefined || contentRange.total === undefined || segment.declaredRange
          ? this.requestEnd
          : Math.min(this.requestEnd, contentRange.total)
      if (
        contentRange.start !== expectedStart ||
        (expectedEnd !== undefined && contentRange.endExclusive !== expectedEnd)
      ) {
        segment.fail(createFailure('Content-Range 与请求范围不匹配', 206, response), firstByteAt)
        return undefined
      }

      const discoveredLength =
        segment.declaredRange || contentRange.total === undefined
          ? segment.length
          : contentRange.total - segment.resourceStart
      const localEnd = contentRange.endExclusive - segment.resourceStart
      if (discoveredLength === undefined || localEnd > discoveredLength) {
        segment.fail(createFailure('Content-Range 缺少可用的资源长度', 206, response), firstByteAt)
        return undefined
      }

      chunk.endExclusive = localEnd
      if (this.planning && segment.isPlanningCurrent(this.generation)) {
        segment.completePlanning(
          discoveredLength,
          'content-range',
          this.loader.chunkSize,
          this.generation,
        )
      } else if (segment.length !== discoveredLength) {
        segment.fail(createFailure('Segment 长度在 Range 请求之间发生变化'), firstByteAt)
        return undefined
      }
      const validator = response.headers.get('etag') ?? response.headers.get('last-modified')
      segment.validator ??= validator
      segment.firstByteAt ??= firstByteAt
      segment.verifyRange()
      return undefined
    })
  }

  private completeChunk(result: SegmentFetchResult): void {
    if (this.loader.state.destroyed || !this.isCurrent()) {
      return
    }

    const completedAt = performance.now()
    this.loader.update(state => {
      const located = this.locateChunk(state)
      if (located === undefined) {
        return undefined
      }
      const { chunk, segment } = located
      const response = result.response
      const validator = response.headers.get('etag') ?? response.headers.get('last-modified')
      if (segment.validator === null) {
        segment.validator = validator
      } else if (validator !== null && validator !== segment.validator) {
        segment.fail(createFailure('Segment 资源标识在 Range 请求之间发生变化'), completedAt)
        return undefined
      }
      segment.firstByteAt =
        segment.firstByteAt === undefined
          ? result.firstByteAt
          : Math.min(segment.firstByteAt, result.firstByteAt)
      const completion = {
        completedAt,
        ...result,
        url: result.url || response.url || segment.context.url,
      }

      if (response.status === 200) {
        if (segment.declaredRange || (chunk.rangeEnabled && chunk.start !== 0)) {
          segment.fail(createFailure('服务器忽略了带边界的 Range 请求', 200, response), completedAt)
          return undefined
        }
        segment.useSequentialRange()
        if (this.planning) {
          segment.completePlanning(
            result.data.byteLength,
            'response',
            this.loader.chunkSize,
            this.generation,
          )
        } else {
          segment.length = result.data.byteLength
        }
        segment.chunks.splice(0, segment.chunks.length, chunk)
        chunk.endExclusive = result.data.byteLength
        chunk.rangeEnabled = false
        if (chunk.complete(this.generation, completion)) {
          this.loader.markRescueRecovered(chunk)
        }
        if (!segment.assemble(completedAt)) {
          segment.fail(createFailure('Segment Chunk 没有覆盖完整数据'), completedAt)
        }
        return undefined
      }

      if (response.status !== 206 || !chunk.rangeEnabled) {
        segment.fail(
          createFailure(`Chunk 请求返回了 HTTP ${response.status}`, response.status, response),
          completedAt,
        )
        return undefined
      }

      let contentRange: ParsedContentRange | undefined
      try {
        contentRange = parseContentRange(response.headers.get('content-range'))
      } catch (cause) {
        segment.fail(toFailure(cause), completedAt)
        return undefined
      }
      if (
        contentRange === undefined ||
        (segment.length === undefined && contentRange.total === undefined)
      ) {
        if (!segment.declaredRange && !segment.fallbackAttempted) {
          segment.fallbackToSequential('Range 响应缺少可用的 Content-Range')
        } else {
          segment.fail(createFailure('Range 响应缺少可用的 Content-Range'), completedAt)
        }
        return undefined
      }

      const expectedStart = segment.resourceStart + chunk.start
      if (contentRange.start !== expectedStart) {
        segment.fail(
          createFailure(
            `Content-Range 起点错误, 期望 ${expectedStart}, 实际 ${contentRange.start}`,
          ),
          completedAt,
        )
        return undefined
      }
      const responseLength = contentRange.endExclusive - contentRange.start
      if (result.data.byteLength !== responseLength) {
        segment.fail(
          createFailure(`Chunk 长度错误, 期望 ${responseLength}, 实际 ${result.data.byteLength}`),
          completedAt,
        )
        return undefined
      }
      const expectedEnd =
        this.requestEnd === undefined || contentRange.total === undefined || segment.declaredRange
          ? this.requestEnd
          : Math.min(this.requestEnd, contentRange.total)
      if (expectedEnd !== undefined && contentRange.endExclusive !== expectedEnd) {
        segment.fail(
          createFailure(
            `Content-Range 终点错误, 期望 ${expectedEnd}, 实际 ${contentRange.endExclusive}`,
          ),
          completedAt,
        )
        return undefined
      }

      const localEnd = contentRange.endExclusive - segment.resourceStart
      const discoveredLength =
        segment.declaredRange || contentRange.total === undefined
          ? segment.length
          : contentRange.total - segment.resourceStart
      if (discoveredLength === undefined || localEnd > discoveredLength) {
        segment.fail(createFailure('Chunk 超出了 Segment 边界'), completedAt)
        return undefined
      }

      chunk.endExclusive = localEnd
      if (this.planning && segment.isPlanningCurrent(this.generation)) {
        if (
          !segment.completePlanning(
            discoveredLength,
            'content-range',
            this.loader.chunkSize,
            this.generation,
          )
        ) {
          segment.fail(createFailure('Segment 长度规划已经失效'), completedAt)
          return undefined
        }
      } else if (segment.length !== discoveredLength) {
        segment.fail(createFailure('Segment 长度在 Range 请求之间发生变化'), completedAt)
        return undefined
      }
      segment.verifyRange()
      if (chunk.complete(this.generation, completion)) {
        this.loader.markRescueRecovered(chunk)
      }
      if (
        !segment.assemble(completedAt) &&
        segment.chunks.every(item => item.phase.type === 'ready')
      ) {
        segment.fail(createFailure('Segment Chunk 没有覆盖完整数据'), completedAt)
      }
      return undefined
    })
  }

  private finishFailedAttempt(cause: unknown): void {
    if (this.loader.state.destroyed || !this.isCurrent()) {
      return
    }

    const completedAt = performance.now()
    this.loader.update(state => {
      const located = this.locateChunk(state)
      if (located === undefined) {
        return undefined
      }
      const { chunk, segment } = located
      if (!this.controller.signal.aborted) {
        segment.fail(toFailure(cause), completedAt)
        return undefined
      }
      if (!isRescueAbort(this.controller.signal.reason)) {
        if (isRescueExhaustedAbort(this.controller.signal.reason)) {
          const reason = this.controller.signal.reason
          this.loader.recordExhaustedStall()
          segment.fail(
            createFailure(reason instanceof Error ? reason.message : 'Chunk 停滞且救援次数已耗尽'),
            completedAt,
          )
          return undefined
        }
        if (isTimeoutAbort(this.controller.signal.reason)) {
          const reason = this.controller.signal.reason
          segment.fail(
            createFailure(reason instanceof Error ? reason.message : 'Chunk 请求超时'),
            completedAt,
          )
          return undefined
        }
        chunk.release(this.generation)
        if (this.planning) {
          segment.releasePlanning(this.generation)
        }
        return undefined
      }

      const reason = this.controller.signal.reason
      if (
        chunk.rescue(
          this.generation,
          reason instanceof Error ? reason.message : 'Chunk 请求需要补救',
        )
      ) {
        if (this.detectedRescue !== undefined) {
          this.loader.recordRescue(chunk, {
            ...this.detectedRescue,
            attempt: chunk.attempt,
            chunkKey: this.chunkKey,
            generation: this.generation,
            segmentKey: this.segmentKey,
            streamId: this.streamId,
          })
        }
        segment.retryCount += 1
        if (this.planning) {
          segment.releasePlanning(this.generation, 'Chunk 慢速补救')
        }
      }
      return undefined
    })
  }

  private abortForRescue(
    reason: RescueReason,
    message: string,
    details: Pick<
      DetectedRescue,
      'continueEtaMs' | 'currentRate' | 'peerCount' | 'peerMedianRate' | 'retryEtaMs'
    > = {},
  ): void {
    if (this.controller.signal.aborted) {
      return
    }
    const detectedAt = performance.now()
    this.detectedRescue = {
      ...details,
      discardedBytes: this.receivedBytes,
      elapsedMs: Math.max(0, detectedAt - this.startedAt),
      reason,
      timestamp: Date.now(),
    }
    this.controller.abort(new DOMException(message, 'RescueError'))
  }

  private abortForExhaustedStall(message: string): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort(new DOMException(message, 'RescueExhaustedError'))
    }
  }

  private locateChunk(
    state: ParallelSegmentLoaderState,
  ): { chunk: VirtualStreamChunk; segment: VirtualStreamSegment } | undefined {
    const segment = state.streams.get(this.streamId)?.segments.get(this.segmentKey)
    const chunk = segment?.chunks.find(item => item.key === this.chunkKey)
    return segment !== undefined && chunk?.isCurrent(this.generation)
      ? { chunk, segment }
      : undefined
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

const contentRangePattern = /^bytes (\d+)-(\d+)\/(\d+|\*)$/i

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

function isRescueAbort(reason: unknown): boolean {
  return reason instanceof DOMException && reason.name === 'RescueError'
}

function isRescueExhaustedAbort(reason: unknown): boolean {
  return reason instanceof DOMException && reason.name === 'RescueExhaustedError'
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

function resolveExpectedResponseBytes(
  headers: Headers,
  requestStart: number,
  requestEnd: number | undefined,
  resourceLength: number | undefined,
): number | undefined {
  const contentLength = Number.parseInt(headers.get('content-length') ?? '', 10)
  if (Number.isSafeInteger(contentLength) && contentLength > 0) {
    return contentLength
  }
  try {
    const contentRange = parseContentRange(headers.get('content-range'))
    if (contentRange !== undefined) {
      return contentRange.endExclusive - contentRange.start
    }
  } catch {
    return undefined
  }
  const boundedEnd =
    requestEnd === undefined
      ? resourceLength
      : resourceLength === undefined
        ? requestEnd
        : Math.min(requestEnd, resourceLength)
  return boundedEnd === undefined || boundedEnd <= requestStart
    ? undefined
    : boundedEnd - requestStart
}

function formatRateRatio(ratio: number): string {
  return `${(ratio * 100).toFixed(1)}%`
}
