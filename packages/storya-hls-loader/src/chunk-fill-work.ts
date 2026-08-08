import type { FragmentLoaderContext } from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import type { ParallelSegmentLoader } from './parallel-segment-loader'
import type { ParallelSegmentLoaderState } from './parallel-segment-loader-state'
import type { VirtualStreamChunk } from './virtual-stream-chunk'
import type { SegmentLoadFailure, VirtualStreamSegment } from './virtual-stream-segment'

export interface ChunkFillWorkOptions {
  chunkKey: string
  context: FragmentLoaderContext
  generation: number
  loader: ParallelSegmentLoader
  rangeEnabled: boolean
  requestEnd: number | undefined
  requestStart: number
  resourceLength: number | undefined
  segmentKey: string
  startedAt: number
  streamId: string
  transport: HttpTransport
}

interface ChunkFillResult {
  data: Uint8Array
  firstByteAt: number
  response: Response
  url: string
}

export class ChunkFillWork {
  readonly chunkKey: string
  readonly generation: number
  readonly requestEnd: number | undefined
  readonly requestStart: number
  readonly segmentKey: string
  readonly startedAt: number
  readonly streamId: string

  private readonly context: FragmentLoaderContext
  private readonly controller = new AbortController()
  private readonly loader: ParallelSegmentLoader
  private readonly rangeEnabled: boolean
  private readonly resourceLength: number | undefined
  private started = false
  private readonly transport: HttpTransport

  constructor(options: ChunkFillWorkOptions) {
    this.chunkKey = options.chunkKey
    this.context = options.context
    this.generation = options.generation
    this.loader = options.loader
    this.rangeEnabled = options.rangeEnabled
    this.requestEnd = options.requestEnd
    this.requestStart = options.requestStart
    this.resourceLength = options.resourceLength
    this.segmentKey = options.segmentKey
    this.startedAt = options.startedAt
    this.streamId = options.streamId
    this.transport = options.transport
  }

  async run(): Promise<void> {
    if (this.started) {
      throw new Error('ChunkFillWork 只能执行一次')
    }
    this.started = true
    try {
      const result = await this.fetchChunk()
      this.completeChunk(result)
    } catch (cause) {
      this.finishFailedAttempt(cause)
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

  private async fetchChunk(): Promise<ChunkFillResult> {
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
    let receivedBytes = 0
    let idleTimer: ReturnType<typeof globalThis.setTimeout> | undefined
    const resetIdleTimer = () => {
      if (idleTimer !== undefined) {
        globalThis.clearTimeout(idleTimer)
      }
      idleTimer = globalThis.setTimeout(() => {
        if (!this.controller.signal.aborted) {
          this.controller.abort(
            new DOMException(
              `Chunk 连续 ${this.loader.idleTimeoutMs}ms 没有收到数据`,
              'TimeoutError',
            ),
          )
        }
      }, this.loader.idleTimeoutMs)
    }
    const accept = (data: Uint8Array) => {
      if (data.byteLength === 0 || this.controller.signal.aborted) {
        return
      }
      const owned = data.slice()
      parts.push(owned)
      receivedBytes += owned.byteLength
      this.updateChunkProgress(receivedBytes)
      resetIdleTimer()
    }

    resetIdleTimer()
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
      if (idleTimer !== undefined) {
        globalThis.clearTimeout(idleTimer)
      }
    }

    const data = new Uint8Array(receivedBytes)
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

    const requestedBytes = this.requestEnd - this.requestStart
    let resourceLength: number | undefined
    if (receivedBytes < requestedBytes) {
      resourceLength = this.requestStart + receivedBytes
    } else if (this.resourceLength !== undefined) {
      resourceLength = this.resourceLength
    } else {
      resourceLength = await this.discoverResourceLength(request)
    }
    if (resourceLength === undefined || receivedBytes <= 0) {
      return createNetworkDetails(response)
    }

    const endInclusive = this.requestStart + receivedBytes - 1
    if (resourceLength <= endInclusive) {
      resourceLength = endInclusive + 1
    }
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

  private completeChunk(result: ChunkFillResult): void {
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
        segment.sequential = true
        segment.length = result.data.byteLength
        segment.chunks.splice(0, segment.chunks.length, chunk)
        chunk.endExclusive = result.data.byteLength
        chunk.rangeEnabled = false
        chunk.complete(this.generation, completion)
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
          segment.fallbackToSequential()
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

      if (segment.length === undefined && contentRange.total !== undefined) {
        segment.length = contentRange.total - segment.resourceStart
      }
      const localEnd = contentRange.endExclusive - segment.resourceStart
      if (segment.length === undefined || localEnd > segment.length) {
        segment.fail(createFailure('Chunk 超出了 Segment 边界'), completedAt)
        return undefined
      }

      chunk.endExclusive = localEnd
      chunk.complete(this.generation, completion)
      segment.planChunks(this.loader.chunkSize)
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
      if (!isTimeoutAbort(this.controller.signal.reason)) {
        chunk.release(this.generation)
        return undefined
      }
      if (chunk.rescueAttempts >= this.loader.maxRescueAttempts) {
        const reason = this.controller.signal.reason
        segment.fail(
          createFailure(reason instanceof Error ? reason.message : 'Chunk 请求超时'),
          completedAt,
        )
        return undefined
      }

      const reason = this.controller.signal.reason
      if (
        chunk.rescue(this.generation, reason instanceof Error ? reason.message : 'Chunk 请求超时')
      ) {
        segment.retryCount += 1
      }
      return undefined
    })
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
