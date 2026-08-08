import type { FragmentLoaderContext, HlsConfig } from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import type { WorkerDiagnostics } from './diagnostics'

export interface ChunkFillWork {
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

export interface ChunkFillResult {
  data: Uint8Array
  firstByteAt: number
  response: Response
  url: string
}

export interface ChunkFillWorkerHost {
  completeChunk(work: ChunkFillWork, result: ChunkFillResult): void
  failChunk(work: ChunkFillWork, cause: unknown): void
  getConfig(): HlsConfig | undefined
  getRevision(): number
  isFillCurrent(work: ChunkFillWork): boolean
  releaseChunk(work: ChunkFillWork, preempted: boolean): void
  rescueChunk(work: ChunkFillWork, reason: string): void
  shouldPreempt(workerId: number): boolean
  subscribe(listener: () => void): () => void
  takeNextChunk(): ChunkFillWork | undefined
  updateChunkProgress(work: ChunkFillWork, loadedBytes: number): void
  waitForChange(afterRevision: number, signal: AbortSignal): Promise<void>
}

interface ChunkFillWorkerOptions {
  host: ChunkFillWorkerHost
  id: number
  idleTimeoutMs: number
  transport: HttpTransport
}

export class ChunkFillWorker {
  readonly id: number

  private active: ChunkFillWork | undefined
  private readonly controller = new AbortController()
  private done: Promise<void> | undefined
  private readonly host: ChunkFillWorkerHost
  private readonly idleTimeoutMs: number
  private requestController: AbortController | undefined
  private startedAt: number | undefined
  private state: 'idle' | 'loading' | 'stopped' = 'idle'
  private readonly transport: HttpTransport

  constructor(options: ChunkFillWorkerOptions) {
    this.host = options.host
    this.id = options.id
    this.idleTimeoutMs = options.idleTimeoutMs
    this.transport = options.transport
  }

  get activeWork(): ChunkFillWork | undefined {
    return this.active
  }

  get loading(): boolean {
    return this.state === 'loading'
  }

  start(): void {
    if (this.done !== undefined || this.controller.signal.aborted) {
      return
    }
    this.done = this.run()
    void this.done.catch(() => undefined)
  }

  destroy(): void {
    if (this.controller.signal.aborted) {
      return
    }
    this.controller.abort()
    this.requestController?.abort()
    this.state = 'stopped'
  }

  getDiagnostics(): WorkerDiagnostics {
    return {
      chunkKey: this.active?.chunkKey,
      id: this.id,
      requestEnd: this.active?.requestEnd,
      requestStart: this.active?.requestStart,
      segmentKey: this.active?.segmentKey,
      startedAt: this.startedAt,
      state: this.state,
      streamId: this.active?.streamId,
    }
  }

  private async run(): Promise<void> {
    while (!this.controller.signal.aborted) {
      const revision = this.host.getRevision()
      const work = this.host.takeNextChunk()
      if (work === undefined) {
        this.state = 'idle'
        try {
          await this.host.waitForChange(revision, this.controller.signal)
        } catch {
          break
        }
        continue
      }

      this.active = work
      this.startedAt = performance.now()
      this.state = 'loading'
      await this.fillChunk(work)
      this.active = undefined
      this.requestController = undefined
      this.startedAt = undefined
    }
    this.state = 'stopped'
  }

  private async fillChunk(work: ChunkFillWork): Promise<void> {
    const requestController = new AbortController()
    this.requestController = requestController
    let preempted = false
    const onWorkerAbort = () => requestController.abort(this.controller.signal.reason)
    const onChange = () => {
      if (!this.host.isFillCurrent(work)) {
        requestController.abort()
        return
      }
      if (this.host.shouldPreempt(this.id)) {
        preempted = true
        requestController.abort()
      }
    }
    this.controller.signal.addEventListener('abort', onWorkerAbort, { once: true })
    const unsubscribe = this.host.subscribe(onChange)

    try {
      const result = await this.fetchChunk(work, requestController)
      this.host.completeChunk(work, result)
    } catch (cause) {
      if (requestController.signal.aborted) {
        if (isTimeoutAbort(requestController.signal.reason)) {
          this.host.rescueChunk(work, requestController.signal.reason.message)
        } else {
          this.host.releaseChunk(work, preempted)
        }
      } else {
        this.host.failChunk(work, cause)
      }
    } finally {
      this.controller.signal.removeEventListener('abort', onWorkerAbort)
      unsubscribe()
    }
  }

  private async fetchChunk(
    work: ChunkFillWork,
    controller: AbortController,
  ): Promise<ChunkFillResult> {
    const config = this.host.getConfig()
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
    if (!this.host.isFillCurrent(work)) {
      throw new DOMException('Chunk Fill 已经失效', 'AbortError')
    }

    const loadPolicy = config.fragLoadPolicy.default
    const loadTimer =
      Number.isFinite(loadPolicy.maxLoadTimeMs) && loadPolicy.maxLoadTimeMs > 0
        ? globalThis.setTimeout(() => {
            if (!controller.signal.aborted) {
              controller.abort(new DOMException('Chunk 请求超过最大加载时间', 'TimeoutError'))
            }
          }, loadPolicy.maxLoadTimeMs)
        : undefined
    let firstByteTimer =
      Number.isFinite(loadPolicy.maxTimeToFirstByteMs) && loadPolicy.maxTimeToFirstByteMs > 0
        ? globalThis.setTimeout(() => {
            if (!controller.signal.aborted) {
              controller.abort(new DOMException('Chunk 等待响应头超时', 'TimeoutError'))
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
      const data = await this.readResponseBody(work, transportResponse, controller)
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
      if (loadTimer !== undefined) {
        globalThis.clearTimeout(loadTimer)
      }
      if (firstByteTimer !== undefined) {
        globalThis.clearTimeout(firstByteTimer)
      }
    }
  }

  private async readResponseBody(
    work: ChunkFillWork,
    response: HttpTransportResponse,
    controller: AbortController,
  ): Promise<Uint8Array> {
    const parts: Uint8Array[] = []
    let receivedBytes = 0
    let idleTimer: ReturnType<typeof globalThis.setTimeout> | undefined
    const resetIdleTimer = () => {
      if (idleTimer !== undefined) {
        globalThis.clearTimeout(idleTimer)
      }
      idleTimer = globalThis.setTimeout(() => {
        if (!controller.signal.aborted) {
          controller.abort(
            new DOMException(`Chunk 连续 ${this.idleTimeoutMs}ms 没有收到数据`, 'TimeoutError'),
          )
        }
      }, this.idleTimeoutMs)
    }
    const accept = (data: Uint8Array) => {
      if (data.byteLength === 0 || controller.signal.aborted) {
        return
      }
      const owned = data.slice()
      parts.push(owned)
      receivedBytes += owned.byteLength
      this.host.updateChunkProgress(work, receivedBytes)
      resetIdleTimer()
    }

    resetIdleTimer()
    try {
      if (response.body === null) {
        accept(new Uint8Array(await response.arrayBuffer()))
      } else {
        const reader = response.body.getReader()
        const abort = () => {
          void reader.cancel(controller.signal.reason).catch(() => undefined)
        }
        controller.signal.addEventListener('abort', abort, { once: true })
        try {
          while (true) {
            const result = await reader.read()
            if (controller.signal.aborted) {
              throw controller.signal.reason
            }
            if (result.done) {
              break
            }
            accept(result.value)
          }
        } finally {
          controller.signal.removeEventListener('abort', abort)
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
    work: ChunkFillWork,
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
}

export class ChunkRequestFailure extends Error {
  readonly code: number
  readonly response: Response | null

  constructor(message: string, code = 0, response: Response | null = null) {
    super(message)
    this.name = 'ChunkRequestFailure'
    this.code = code
    this.response = response
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
