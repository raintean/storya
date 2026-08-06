import type { HttpTransportResponse } from './http-transport'

const defaultLogIntervalMs = 5_000
const cacheHitStatuses = new Set(['HIT', 'REVALIDATED', 'STALE', 'UPDATING'])
const cacheMissStatuses = new Set(['EXPIRED', 'MISS'])
const cacheBypassStatuses = new Set(['BYPASS', 'DYNAMIC'])

export interface TransportStatisticsSnapshot {
  activeRequestCount: number
  ageMs: number
  cacheBypassCount: number
  cacheHitCount: number
  cacheHitRate: number | undefined
  cacheMissCount: number
  cacheStatusCounts: Record<string, number>
  cacheUnknownCount: number
  canceledCount: number
  failureCount: number
  requestCount: number
  responseBytes: number
  responseMiB: number
  successCount: number
  timestamp: number
  transport: string
  type: 'transport-statistics'
}

export interface TransportStatisticsOptions {
  intervalMs?: number
  logger?: (message: string, snapshot: TransportStatisticsSnapshot) => void
}

type RequestResult = 'canceled' | 'failure' | 'success'

export class TransportStatistics {
  private activeRequestCount = 0
  private cacheBypassCount = 0
  private cacheHitCount = 0
  private cacheMissCount = 0
  private readonly cacheStatusCounts = new Map<string, number>()
  private cacheUnknownCount = 0
  private canceledCount = 0
  private destroyed = false
  private failureCount = 0
  private readonly intervalMs: number
  private lastLoggedRevision = 0
  private readonly logger: (message: string, snapshot: TransportStatisticsSnapshot) => void
  private requestCount = 0
  private responseBytes = 0
  private revision = 0
  private readonly startedAt = performance.now()
  private successCount = 0
  private timer: ReturnType<typeof globalThis.setInterval> | undefined
  private readonly transport: string

  constructor(transport: string, options: TransportStatisticsOptions = {}) {
    const intervalMs = options.intervalMs ?? defaultLogIntervalMs
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      throw new Error('Transport 统计输出间隔必须大于 0')
    }
    this.intervalMs = intervalMs
    this.logger = options.logger ?? (message => console.info(message))
    this.transport = transport
  }

  startRequest(): TransportRequestStatistics {
    if (this.destroyed) {
      throw new Error('Transport 统计已经销毁')
    }
    this.requestCount += 1
    this.activeRequestCount += 1
    this.markChanged()
    this.ensureTimer()
    return new TransportRequestStatistics(this)
  }

  snapshot(): TransportStatisticsSnapshot {
    const cacheDecisions = this.cacheHitCount + this.cacheMissCount
    return {
      activeRequestCount: this.activeRequestCount,
      ageMs: Math.round(performance.now() - this.startedAt),
      cacheBypassCount: this.cacheBypassCount,
      cacheHitCount: this.cacheHitCount,
      cacheHitRate: cacheDecisions === 0 ? undefined : this.cacheHitCount / cacheDecisions,
      cacheMissCount: this.cacheMissCount,
      cacheStatusCounts: Object.fromEntries(
        [...this.cacheStatusCounts.entries()].sort(([left], [right]) => left.localeCompare(right)),
      ),
      cacheUnknownCount: this.cacheUnknownCount,
      canceledCount: this.canceledCount,
      failureCount: this.failureCount,
      requestCount: this.requestCount,
      responseBytes: this.responseBytes,
      responseMiB: Number((this.responseBytes / (1024 * 1024)).toFixed(2)),
      successCount: this.successCount,
      timestamp: Date.now(),
      transport: this.transport,
      type: 'transport-statistics',
    }
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    if (this.timer !== undefined) {
      globalThis.clearInterval(this.timer)
      this.timer = undefined
    }
  }

  recordCacheStatus(headers: Headers): void {
    const value = headers.get('cf-cache-status')?.trim().toUpperCase()
    if (value === undefined || value.length === 0) {
      this.cacheUnknownCount += 1
      this.markChanged()
      return
    }

    this.cacheStatusCounts.set(value, (this.cacheStatusCounts.get(value) ?? 0) + 1)
    if (cacheHitStatuses.has(value)) {
      this.cacheHitCount += 1
    } else if (cacheMissStatuses.has(value)) {
      this.cacheMissCount += 1
    } else if (cacheBypassStatuses.has(value)) {
      this.cacheBypassCount += 1
    } else {
      this.cacheUnknownCount += 1
    }
    this.markChanged()
  }

  recordBytes(bytes: number): void {
    this.responseBytes += bytes
    this.markChanged()
  }

  finishRequest(result: RequestResult): void {
    this.activeRequestCount = Math.max(0, this.activeRequestCount - 1)
    if (result === 'success') {
      this.successCount += 1
    } else if (result === 'canceled') {
      this.canceledCount += 1
    } else {
      this.failureCount += 1
    }
    this.markChanged()
  }

  private ensureTimer(): void {
    if (this.timer !== undefined) {
      return
    }
    this.timer = globalThis.setInterval(() => this.logIfChanged(), this.intervalMs)
  }

  private logIfChanged(): void {
    if (this.destroyed || this.revision === this.lastLoggedRevision) {
      return
    }
    this.lastLoggedRevision = this.revision
    const snapshot = this.snapshot()
    this.logger(formatTransportStatistics(snapshot), snapshot)
  }

  private markChanged(): void {
    this.revision += 1
  }
}

export function formatTransportStatistics(snapshot: TransportStatisticsSnapshot): string {
  const cacheHitRate =
    snapshot.cacheHitRate === undefined ? '—' : `${(snapshot.cacheHitRate * 100).toFixed(1)}%`
  const cacheStatuses = Object.entries(snapshot.cacheStatusCounts)
    .map(([status, count]) => `${status}=${count}`)
    .join(',')
  const fields = [
    `请求 ${snapshot.requestCount}`,
    `成功 ${snapshot.successCount}`,
    `失败 ${snapshot.failureCount}`,
    `取消 ${snapshot.canceledCount}`,
    `进行中 ${snapshot.activeRequestCount}`,
    `数据 ${formatBytes(snapshot.responseBytes)}`,
    `缓存 HIT ${snapshot.cacheHitCount} / MISS ${snapshot.cacheMissCount} / BYPASS ${snapshot.cacheBypassCount} / 未知 ${snapshot.cacheUnknownCount}`,
    `命中率 ${cacheHitRate}`,
    ...(cacheStatuses.length === 0 ? [] : [`CF ${cacheStatuses}`]),
  ]
  return `[storya-transport][${snapshot.transport}] ${fields.join(' | ')}`
}

export class TransportRequestStatistics {
  private finished = false
  private readonly statistics: TransportStatistics

  constructor(statistics: TransportStatistics) {
    this.statistics = statistics
  }

  trackResponse(response: HttpTransportResponse, bodyExpected = true): HttpTransportResponse {
    this.statistics.recordCacheStatus(response.headers)
    if (!response.ok) {
      this.finish('failure')
      return new StatisticsHttpTransportResponse(response, bytes => {
        this.statistics.recordBytes(bytes)
      })
    }
    if (!bodyExpected) {
      this.finish('success')
      return response
    }
    if (response.body === null) {
      this.finish('success')
      return response
    }

    return new StatisticsHttpTransportResponse(
      response,
      bytes => {
        this.statistics.recordBytes(bytes)
      },
      () => this.finish('success'),
      error => this.finish(isAbortError(error) ? 'canceled' : 'failure'),
      () => this.finish('canceled'),
    )
  }

  reject(error: unknown): void {
    this.finish(isAbortError(error) ? 'canceled' : 'failure')
  }

  private finish(result: RequestResult): void {
    if (this.finished) {
      return
    }
    this.finished = true
    this.statistics.finishRequest(result)
  }
}

class StatisticsHttpTransportResponse implements HttpTransportResponse {
  readonly body: ReadableStream<Uint8Array> | null
  readonly headers: Headers
  readonly ok: boolean
  readonly status: number
  readonly statusText: string
  readonly url: string

  private readonly original: HttpTransportResponse

  constructor(
    response: HttpTransportResponse,
    onBytes: (bytes: number) => void,
    onComplete?: () => void,
    onError?: (error: unknown) => void,
    onCancel?: () => void,
  ) {
    this.original = response
    this.headers = response.headers
    this.ok = response.ok
    this.status = response.status
    this.statusText = response.statusText
    this.url = response.url
    if (response.body === null) {
      this.body = null
      return
    }

    const reader = response.body.getReader()
    this.body = new ReadableStream<Uint8Array>({
      async cancel(reason) {
        onCancel?.()
        await reader.cancel(reason)
      },
      async pull(controller) {
        try {
          const result = await reader.read()
          if (result.done) {
            onComplete?.()
            controller.close()
            return
          }
          onBytes(result.value.byteLength)
          controller.enqueue(result.value)
        } catch (error) {
          onError?.(error)
          controller.error(error)
        }
      },
    })
  }

  arrayBuffer(): Promise<ArrayBuffer> {
    return this.body === null ? this.original.arrayBuffer() : new Response(this.body).arrayBuffer()
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError'
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KiB`
  }
  if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GiB`
}
