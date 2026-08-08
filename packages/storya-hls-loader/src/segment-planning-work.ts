import type { FragmentLoaderContext } from 'hls.js'
import type { HttpTransport } from 'storya-transport'
import type { ParallelSegmentLoader } from './parallel-segment-loader'

export interface SegmentPlanningWorkOptions {
  context: FragmentLoaderContext
  generation: number
  loader: ParallelSegmentLoader
  segmentKey: string
  startedAt: number
  streamId: string
  transport: HttpTransport
}

export class SegmentPlanningWork {
  readonly chunkKey = undefined
  readonly generation: number
  readonly method = 'HEAD'
  readonly requestEnd = undefined
  readonly requestStart = undefined
  readonly segmentKey: string
  readonly startedAt: number
  readonly streamId: string
  readonly task = 'planning'

  private readonly context: FragmentLoaderContext
  private readonly controller = new AbortController()
  private readonly loader: ParallelSegmentLoader
  private started = false
  private readonly transport: HttpTransport

  constructor(options: SegmentPlanningWorkOptions) {
    this.context = options.context
    this.generation = options.generation
    this.loader = options.loader
    this.segmentKey = options.segmentKey
    this.startedAt = options.startedAt
    this.streamId = options.streamId
    this.transport = options.transport
  }

  async run(): Promise<void> {
    if (this.started) {
      throw new Error('SegmentPlanningWork 只能执行一次')
    }
    this.started = true
    try {
      const length = await this.loadLength()
      if (!this.isCurrent()) {
        return
      }
      this.loader.update(state => {
        state.streams
          .get(this.streamId)
          ?.segments.get(this.segmentKey)
          ?.completePlanning(length, 'head', this.loader.chunkSize, this.generation)
        return undefined
      })
    } catch (cause) {
      if (!this.isCurrent()) {
        return
      }
      const message = cause instanceof Error ? cause.message : 'HEAD 探测失败'
      this.loader.update(state => {
        state.streams
          .get(this.streamId)
          ?.segments.get(this.segmentKey)
          ?.fallbackPlanning(this.generation, message)
        return undefined
      })
    }
  }

  cancel(reason: unknown): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort(reason)
    }
  }

  isCurrent(): boolean {
    return (
      this.loader.state.streams
        .get(this.streamId)
        ?.segments.get(this.segmentKey)
        ?.isPlanningCurrent(this.generation) ?? false
    )
  }

  private async loadLength(): Promise<number> {
    const config = this.loader.hlsConfig
    if (config === undefined) {
      throw new Error('ParallelSegmentLoader 尚未取得 hls.js 配置')
    }
    const headers = new Headers(this.context.headers)
    headers.delete('range')
    const requestContext: FragmentLoaderContext = {
      ...this.context,
      headers: Object.fromEntries(headers.entries()),
      rangeEnd: 0,
      rangeStart: 0,
    }
    const init: RequestInit = {
      credentials: 'same-origin',
      headers,
      method: 'HEAD',
      mode: 'cors',
      signal: this.controller.signal,
    }
    const request =
      (await config.fetchSetup?.(requestContext, init)) ?? new Request(this.context.url, init)
    if (!this.isCurrent()) {
      throw new DOMException('Segment Planning 已经失效', 'AbortError')
    }

    const loadPolicy = config.fragLoadPolicy.default
    const loadTimer =
      Number.isFinite(loadPolicy.maxLoadTimeMs) && loadPolicy.maxLoadTimeMs > 0
        ? globalThis.setTimeout(() => {
            if (!this.controller.signal.aborted) {
              this.controller.abort(new DOMException('HEAD 请求超过最大加载时间', 'TimeoutError'))
            }
          }, loadPolicy.maxLoadTimeMs)
        : undefined
    let firstByteTimer =
      Number.isFinite(loadPolicy.maxTimeToFirstByteMs) && loadPolicy.maxTimeToFirstByteMs > 0
        ? globalThis.setTimeout(() => {
            if (!this.controller.signal.aborted) {
              this.controller.abort(new DOMException('HEAD 等待响应头超时', 'TimeoutError'))
            }
          }, loadPolicy.maxTimeToFirstByteMs)
        : undefined
    try {
      const response = await this.transport.request(request)
      if (firstByteTimer !== undefined) {
        globalThis.clearTimeout(firstByteTimer)
        firstByteTimer = undefined
      }
      if (!response.ok) {
        throw new Error(`HEAD 返回 HTTP ${response.status}`)
      }
      await response.arrayBuffer()
      const length = Number.parseInt(response.headers.get('content-length') ?? '', 10)
      if (!Number.isSafeInteger(length) || length <= 0) {
        throw new Error('HEAD 响应缺少有效的 Content-Length')
      }
      return length
    } finally {
      if (loadTimer !== undefined) {
        globalThis.clearTimeout(loadTimer)
      }
      if (firstByteTimer !== undefined) {
        globalThis.clearTimeout(firstByteTimer)
      }
    }
  }
}
