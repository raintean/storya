import { HTTP_RELAY_MAX_RESPONSE_BODY_BYTES } from 'storya-protocol'
import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from '../http-transport'
import { createAbortError, HttpTransportFailure } from '../http-transport'
import { TransportStatistics, type TransportStatisticsSnapshot } from '../transport-statistics'
import { WebSocketConnectionPool } from './connection-pool'
import type {
  ResolvedWebSocketHttpTransportOptions,
  WebSocketHttpTransportDebugEvent,
  WebSocketHttpTransportDebugLogger,
  WebSocketHttpTransportOptions,
} from './types'

export type {
  WebSocketChannelState,
  WebSocketFactory,
  WebSocketHttpTransportDebugEvent,
  WebSocketHttpTransportDebugEventType,
  WebSocketHttpTransportDebugLogger,
  WebSocketHttpTransportOptions,
  WebSocketLike,
} from './types'

export class WebSocketHttpTransport implements HttpTransport {
  private readonly defaultMaxResponseBytes: number
  private destroyed = false
  private readonly pool: WebSocketConnectionPool
  private readonly statistics = new TransportStatistics('WebSocketHttpTransport', {
    cacheLabel: 'Worker Fetch 缓存',
  })

  constructor(url: string, options: WebSocketHttpTransportOptions) {
    const resolvedOptions = resolveOptions(options)
    this.defaultMaxResponseBytes = resolvedOptions.defaultMaxResponseBytes
    this.pool = new WebSocketConnectionPool(normalizeWebSocketUrl(url), resolvedOptions)
  }

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      return Promise.reject(new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁'))
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return Promise.reject(
        new HttpTransportFailure(
          'protocol-error',
          `WebSocket transport 不支持 ${request.method} 请求`,
        ),
      )
    }
    if (request.signal.aborted) {
      return Promise.reject(createAbortError())
    }

    let maxResponseBytes: number
    try {
      maxResponseBytes = normalizeMaxResponseBytes(
        options?.maxResponseBytes ?? this.defaultMaxResponseBytes,
        request.method,
      )
    } catch (cause) {
      return Promise.reject(
        new HttpTransportFailure('protocol-error', 'maxResponseBytes 无效', { cause }),
      )
    }

    const statistics = this.statistics.startRequest()
    return this.pool.request(request, maxResponseBytes).then(
      response => statistics.trackResponse(response, request.method !== 'HEAD'),
      error => {
        statistics.reject(error)
        throw error
      },
    )
  }

  getStatistics(): TransportStatisticsSnapshot {
    return this.statistics.snapshot()
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.pool.destroy(new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁'))
    this.statistics.destroy()
  }
}

function normalizeMaxResponseBytes(value: number, method: string): number {
  if (
    !Number.isSafeInteger(value) ||
    value < 0 ||
    value > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES ||
    (method !== 'HEAD' && value === 0)
  ) {
    throw new Error(`无效的 maxResponseBytes: ${value}`)
  }
  return value
}

function normalizeWebSocketUrl(value: string): string {
  const url = new URL(value)
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`WebSocket transport URL 必须使用 ws 或 wss: ${value}`)
  }
  return url.toString()
}

function resolveDebugLogger(
  debug: boolean | WebSocketHttpTransportDebugLogger | undefined,
): WebSocketHttpTransportDebugLogger | undefined {
  if (typeof debug === 'function') {
    return debug
  }
  if (debug !== true) {
    return undefined
  }
  return event => {
    if (event.type === 'connection-closed') {
      console.info(formatConnectionClosed(event))
    }
  }
}

function formatConnectionClosed(event: WebSocketHttpTransportDebugEvent): string {
  const initiator =
    event.initiator === 'local' ? '本地' : event.initiator === 'remote' ? '远端' : '未知'
  const fields = [
    `连接 #${event.connectionId} 关闭`,
    `原因 ${event.reason || '—'}`,
    `关闭方 ${initiator}`,
    `Code ${event.code ?? '—'}`,
    `存活 ${Math.round(event.ageMs)} ms`,
    `请求 ${event.requestCount}`,
    `池内 ${event.poolSize}`,
    `排队 ${event.pendingRequestCount}`,
    ...(event.wasClean === undefined ? [] : [`正常关闭 ${event.wasClean ? '是' : '否'}`]),
    ...(event.error === undefined ? [] : [`错误 ${event.error}`]),
  ]
  return `[storya-transport][WebSocketHttpTransport] ${fields.join(' | ')}`
}

function resolveOptions(
  options: WebSocketHttpTransportOptions,
): ResolvedWebSocketHttpTransportOptions {
  positiveNumber(options.cancelTimeoutMs, 'cancelTimeoutMs')
  positiveNumber(options.connectTimeoutMs, 'connectTimeoutMs')
  positiveInteger(options.defaultMaxResponseBytes, 'defaultMaxResponseBytes')
  positiveNumber(options.idleConnectionTimeoutMs, 'idleConnectionTimeoutMs')
  positiveInteger(options.maxConnections, 'maxConnections')
  positiveInteger(options.maxRequestsPerConnection, 'maxRequestsPerConnection')
  nonNegativeInteger(options.retainedIdleConnections, 'retainedIdleConnections')
  if (options.defaultMaxResponseBytes > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES) {
    throw new Error(`defaultMaxResponseBytes 不能超过 ${HTTP_RELAY_MAX_RESPONSE_BODY_BYTES}`)
  }
  if (options.retainedIdleConnections > options.maxConnections) {
    throw new Error('retainedIdleConnections 不能超过 maxConnections')
  }
  return {
    cancelTimeoutMs: options.cancelTimeoutMs,
    connectTimeoutMs: options.connectTimeoutMs,
    defaultMaxResponseBytes: options.defaultMaxResponseBytes,
    debugLogger: resolveDebugLogger(options.debug),
    idleConnectionTimeoutMs: options.idleConnectionTimeoutMs,
    maxConnections: options.maxConnections,
    maxRequestsPerConnection: options.maxRequestsPerConnection,
    retainedIdleConnections: options.retainedIdleConnections,
    webSocketFactory: options.webSocketFactory ?? (url => new WebSocket(url)),
  }
}

function nonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} 必须是非负整数`)
  }
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} 必须是正整数`)
  }
}

function positiveNumber(value: number, name: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} 必须是正数`)
  }
}
