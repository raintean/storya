import {
  decodeHttpRelayResponse,
  encodeHttpRelayRequest,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
  HttpRelayResponseOutcome,
} from 'storya-protocol'
import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { HttpTransportFailure } from './http-transport'
import { WebSocketHttpResponse } from './websocket-http-response'

interface WebSocketEventMapLike {
  close: CloseEvent
  error: Event
  message: MessageEvent<unknown>
  open: Event
}

export interface WebSocketLike {
  binaryType: BinaryType
  readonly readyState: number

  addEventListener<K extends keyof WebSocketEventMapLike>(
    type: K,
    listener: (event: WebSocketEventMapLike[K]) => void,
  ): void
  close(code?: number, reason?: string): void
  removeEventListener<K extends keyof WebSocketEventMapLike>(
    type: K,
    listener: (event: WebSocketEventMapLike[K]) => void,
  ): void
  send(data: string | Blob | ArrayBuffer | ArrayBufferView<ArrayBuffer>): void
}

export type WebSocketFactory = (url: string) => WebSocketLike

export type WebSocketHttpTransportDebugEventType =
  | 'connection-created'
  | 'connection-opened'
  | 'connection-closed'

export interface WebSocketHttpTransportDebugEvent {
  ageMs: number
  code?: number
  connectionId: number
  error?: string
  initiator?: 'local' | 'remote'
  pendingRequestCount: number
  poolSize: number
  reason?: string
  requestCount: number
  state: WebSocketChannelState
  timestamp: number
  type: WebSocketHttpTransportDebugEventType
  wasClean?: boolean
}

export type WebSocketHttpTransportDebugLogger = (event: WebSocketHttpTransportDebugEvent) => void

export interface WebSocketHttpTransportOptions {
  connectTimeoutMs: number
  defaultMaxResponseBytes: number
  debug?: boolean | WebSocketHttpTransportDebugLogger
  idleConnectionTimeoutMs: number
  maxConnections: number
  maxRequestsPerConnection: number
  minIdleConnections: number
  transactionTimeoutMs: number
  webSocketFactory?: WebSocketFactory
}

interface ResolvedOptions extends Omit<
  WebSocketHttpTransportOptions,
  'debug' | 'webSocketFactory'
> {
  debugLogger: WebSocketHttpTransportDebugLogger | undefined
  webSocketFactory: WebSocketFactory
}

interface PendingRequest {
  maxResponseBytes: number
  reject: (reason: unknown) => void
  request: Request
  resolve: (response: HttpTransportResponse) => void
}

interface ActiveTransaction extends PendingRequest {
  timeout: number
}

interface ChannelCallbacks {
  onClosed(
    channel: WebSocketChannel,
    error: HttpTransportFailure,
    opened: boolean,
    details: ChannelCloseDetails,
  ): void
  onIdle(channel: WebSocketChannel): void
  onIdleTimeout(): void
  onOpen(channel: WebSocketChannel): void
}

export type WebSocketChannelState = 'busy' | 'closed' | 'connecting' | 'idle'
type ChannelRetirementReason = 'idle' | 'max-requests'

interface ChannelCloseDetails {
  code: number
  initiator: 'local' | 'remote'
  reason: string
  wasClean?: boolean
}

const webSocketConnecting = 0
const webSocketOpen = 1

export class WebSocketHttpTransport implements HttpTransport {
  readonly rangeRequestMode = 'stable' as const
  readonly responseMode = 'buffered' as const

  private readonly channels = new Set<WebSocketChannel>()
  private destroyed = false
  private nextChannelId = 0
  private readonly options: ResolvedOptions
  private readonly pending: PendingRequest[] = []
  private readonly url: string

  constructor(url: string, options: WebSocketHttpTransportOptions) {
    this.url = normalizeWebSocketUrl(url)
    this.options = resolveOptions(options)
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

    let maxResponseBytes: number
    try {
      maxResponseBytes = normalizeMaxResponseBytes(
        options?.maxResponseBytes ?? this.options.defaultMaxResponseBytes,
        request.method,
      )
    } catch (cause) {
      return Promise.reject(
        new HttpTransportFailure('protocol-error', 'maxResponseBytes 无效', { cause }),
      )
    }

    return new Promise<HttpTransportResponse>((resolve, reject) => {
      this.pending.push({ maxResponseBytes, reject, request, resolve })
      this.drain()
    })
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    const error = new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁')
    for (const pending of this.pending.splice(0)) {
      pending.reject(error)
    }
    for (const channel of [...this.channels]) {
      channel.destroy(error)
    }
    this.channels.clear()
  }

  private createChannel(): void {
    const connectionId = ++this.nextChannelId
    let channel: WebSocketChannel
    try {
      channel = new WebSocketChannel(connectionId, this.url, this.options, {
        onClosed: (closed, error, opened, details) => {
          this.channels.delete(closed)
          this.emitDebug(closed, 'connection-closed', {
            ...details,
            ...(details.code === 1000 ? {} : { error: error.message }),
          })
          if (!opened) {
            this.pending.shift()?.reject(error)
          }
          this.drain()
        },
        onIdle: () => this.drain(),
        onIdleTimeout: () => this.reclaimIdleConnections(),
        onOpen: opened => {
          this.emitDebug(opened, 'connection-opened')
          this.drain()
        },
      })
    } catch (cause) {
      this.pending
        .shift()
        ?.reject(new HttpTransportFailure('connection-failed', 'WebSocket 连接创建失败', { cause }))
      return
    }
    this.channels.add(channel)
    this.emitDebug(channel, 'connection-created', { reason: 'pending-request' })
  }

  private drain(): void {
    if (this.destroyed) {
      return
    }

    while (this.pending.length > 0) {
      const channel = this.findYoungestIdleChannel()
      if (channel === undefined) {
        break
      }
      const pending = this.pending.shift()
      if (pending === undefined) {
        break
      }
      channel.execute(pending)
    }

    let connecting = 0
    for (const channel of this.channels) {
      if (channel.isConnecting()) {
        connecting += 1
      }
    }
    const needed = Math.min(
      Math.max(0, this.pending.length - connecting),
      this.options.maxConnections - this.channels.size,
    )
    for (let index = 0; index < needed; index += 1) {
      this.createChannel()
    }
  }

  private emitDebug(
    channel: WebSocketChannel,
    type: WebSocketHttpTransportDebugEventType,
    details: Partial<
      Pick<WebSocketHttpTransportDebugEvent, 'code' | 'error' | 'initiator' | 'reason' | 'wasClean'>
    > = {},
  ): void {
    const logger = this.options.debugLogger
    if (logger === undefined) {
      return
    }
    try {
      logger({
        ageMs: channel.getAgeMs(),
        connectionId: channel.getId(),
        pendingRequestCount: this.pending.length,
        poolSize: this.channels.size,
        requestCount: channel.getRequestCount(),
        state: channel.getState(),
        timestamp: Date.now(),
        type,
        ...details,
      })
    } catch {
      // 调试日志不能影响 Transport 请求
    }
  }

  private findYoungestIdleChannel(): WebSocketChannel | undefined {
    let selected: WebSocketChannel | undefined
    for (const channel of this.channels) {
      if (
        channel.isAvailable() &&
        (selected === undefined ||
          channel.getCreatedAt() > selected.getCreatedAt() ||
          (channel.getCreatedAt() === selected.getCreatedAt() &&
            channel.getId() > selected.getId()))
      ) {
        selected = channel
      }
    }
    return selected
  }

  private reclaimIdleConnections(): void {
    if (this.destroyed) {
      return
    }
    const now = performance.now()
    const idle = [...this.channels]
      .filter(channel => channel.isAvailable())
      .sort((left, right) => left.getIdleSince() - right.getIdleSince())
    let retained = idle.length
    for (const channel of idle) {
      if (
        retained <= this.options.minIdleConnections ||
        now - channel.getIdleSince() < this.options.idleConnectionTimeoutMs
      ) {
        continue
      }
      retained -= 1
      channel.retire('idle')
    }
  }
}

class WebSocketChannel {
  private active: ActiveTransaction | undefined
  private readonly callbacks: ChannelCallbacks
  private readonly connectTimer: number
  private readonly createdAt = performance.now()
  private idleSince = performance.now()
  private idleTimer: number | undefined
  private opened = false
  private readingMessage = false
  private requests = 0
  private retirementReason: ChannelRetirementReason | undefined
  private retiring = false
  private readonly socket: WebSocketLike
  private state: WebSocketChannelState = 'connecting'

  constructor(
    private readonly connectionId: number,
    url: string,
    private readonly options: ResolvedOptions,
    callbacks: ChannelCallbacks,
  ) {
    this.callbacks = callbacks
    this.socket = options.webSocketFactory(url)
    this.socket.binaryType = 'arraybuffer'
    this.socket.addEventListener('open', this.handleOpen)
    this.socket.addEventListener('message', this.handleMessage)
    this.socket.addEventListener('error', this.handleError)
    this.socket.addEventListener('close', this.handleClose)
    this.connectTimer = globalThis.setTimeout(() => {
      if (this.state === 'connecting') {
        this.closeWithError(new HttpTransportFailure('connection-failed', 'WebSocket 连接建立超时'))
      }
    }, options.connectTimeoutMs)
  }

  destroy(error: HttpTransportFailure): void {
    this.close(1000, 'destroyed', error)
  }

  execute(pending: PendingRequest): void {
    if (!this.isAvailable()) {
      pending.reject(new HttpTransportFailure('connection-failed', 'WebSocket 连接当前不可用'))
      return
    }
    this.clearIdleTimer()
    this.state = 'busy'
    this.requests += 1
    if (this.requests >= this.options.maxRequestsPerConnection) {
      this.retiring = true
      this.retirementReason = 'max-requests'
    }
    const timeout = globalThis.setTimeout(() => {
      if (this.active !== undefined) {
        this.closeWithError(new HttpTransportFailure('connection-failed', 'WebSocket 请求事务超时'))
      }
    }, this.options.transactionTimeoutMs)
    this.active = { ...pending, timeout }

    try {
      const headers = []
      for (const [name, value] of pending.request.headers) {
        headers.push({ name, value })
      }
      this.socket.send(
        encodeHttpRelayRequest({
          headers,
          maxResponseBytes: pending.maxResponseBytes,
          method: pending.request.method as 'GET' | 'HEAD',
          url: pending.request.url,
        }),
      )
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('connection-failed', 'WebSocket 请求发送失败', { cause }),
      )
    }
  }

  getAgeMs(): number {
    return performance.now() - this.createdAt
  }

  getCreatedAt(): number {
    return this.createdAt
  }

  getId(): number {
    return this.connectionId
  }

  getIdleSince(): number {
    return this.idleSince
  }

  getRequestCount(): number {
    return this.requests
  }

  getState(): WebSocketChannelState {
    return this.state
  }

  isAvailable(): boolean {
    return this.state === 'idle' && !this.retiring
  }

  isConnecting(): boolean {
    return this.state === 'connecting'
  }

  retire(reason: ChannelRetirementReason): void {
    this.retiring = true
    this.retirementReason ??= reason
    if (this.state === 'idle') {
      this.close(1000, this.retirementReason)
    }
  }

  private readonly handleClose = (event: CloseEvent): void => {
    this.finalizeClose(
      new HttpTransportFailure(
        'connection-failed',
        event.reason || `WebSocket 连接已关闭 (${event.code})`,
      ),
      {
        code: event.code,
        initiator: 'remote',
        reason: event.reason,
        wasClean: event.wasClean,
      },
    )
  }

  private readonly handleError = (): void => {
    this.closeWithError(new HttpTransportFailure('connection-failed', 'WebSocket 连接发生错误'))
  }

  private readonly handleMessage = (event: MessageEvent<unknown>): void => {
    if (this.readingMessage) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '收到重叠的 WebSocket response'),
      )
      return
    }
    if (event.data instanceof ArrayBuffer || ArrayBuffer.isView(event.data)) {
      this.acceptResponse(event.data)
      return
    }
    if (!(event.data instanceof Blob)) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', 'Transport 只接受二进制 WebSocket message'),
      )
      return
    }

    this.readingMessage = true
    void event.data.arrayBuffer().then(
      data => {
        this.readingMessage = false
        if (this.state !== 'closed') {
          this.acceptResponse(data)
        }
      },
      cause => {
        this.readingMessage = false
        this.closeWithError(
          new HttpTransportFailure('protocol-error', '无法读取 WebSocket response', { cause }),
        )
      },
    )
  }

  private readonly handleOpen = (): void => {
    if (this.state !== 'connecting') {
      return
    }
    globalThis.clearTimeout(this.connectTimer)
    this.opened = true
    this.becomeIdle()
    this.callbacks.onOpen(this)
  }

  private acceptResponse(data: ArrayBuffer | ArrayBufferView): void {
    const active = this.active
    if (this.state !== 'busy' || active === undefined) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '空闲 WebSocket 收到意外 response'),
      )
      return
    }

    let response
    try {
      response = decodeHttpRelayResponse(data)
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', 'HTTP relay response 解码失败', { cause }),
      )
      return
    }
    if (response.body.byteLength > active.maxResponseBytes) {
      this.closeWithError(
        new HttpTransportFailure(
          'response-too-large',
          `HTTP 响应超过 ${active.maxResponseBytes} 字节限制`,
        ),
      )
      return
    }

    if (response.outcome === HttpRelayResponseOutcome.HTTP) {
      if (active.request.method === 'HEAD' && response.body.byteLength !== 0) {
        this.closeWithError(
          new HttpTransportFailure('invalid-response', 'HEAD response 不允许包含 body'),
        )
        return
      }
      const headers = new Headers()
      for (const header of response.headers) {
        headers.append(header.name, header.value)
      }
      active.resolve(
        new WebSocketHttpResponse(active.request.method === 'HEAD' ? null : response.body, {
          headers,
          status: response.status,
          url: response.url || active.request.url,
        }),
      )
    } else {
      active.reject(createRelayFailure(response.outcome, response.message))
    }
    this.finish(active)
  }

  private armIdleTimer(): void {
    this.clearIdleTimer()
    this.idleTimer = globalThis.setTimeout(
      () => this.callbacks.onIdleTimeout(),
      this.options.idleConnectionTimeoutMs,
    )
  }

  private becomeIdle(): void {
    this.active = undefined
    this.idleSince = performance.now()
    this.state = 'idle'
    this.armIdleTimer()
  }

  private clearIdleTimer(): void {
    if (this.idleTimer !== undefined) {
      globalThis.clearTimeout(this.idleTimer)
      this.idleTimer = undefined
    }
  }

  private close(
    code: number,
    reason: string,
    error = new HttpTransportFailure('connection-failed', `WebSocket 已关闭: ${reason}`),
  ): void {
    if (this.state === 'closed') {
      return
    }
    const details: ChannelCloseDetails = { code, initiator: 'local', reason }
    const shouldCloseSocket =
      this.socket.readyState === webSocketConnecting || this.socket.readyState === webSocketOpen
    this.finalizeClose(error, details)
    if (shouldCloseSocket) {
      try {
        this.socket.close(code, reason)
      } catch {
        // 本地状态仍然必须收敛
      }
    }
  }

  private closeWithError(error: HttpTransportFailure): void {
    this.close(1011, 'connection failed', error)
  }

  private failActive(error: HttpTransportFailure): void {
    const active = this.active
    if (active === undefined) {
      return
    }
    globalThis.clearTimeout(active.timeout)
    this.active = undefined
    active.reject(error)
  }

  private finalizeClose(error: HttpTransportFailure, details: ChannelCloseDetails): void {
    if (this.state === 'closed') {
      return
    }
    const opened = this.opened
    this.state = 'closed'
    globalThis.clearTimeout(this.connectTimer)
    this.clearIdleTimer()
    this.failActive(error)
    this.socket.removeEventListener('open', this.handleOpen)
    this.socket.removeEventListener('message', this.handleMessage)
    this.socket.removeEventListener('error', this.handleError)
    this.socket.removeEventListener('close', this.handleClose)
    this.callbacks.onClosed(this, error, opened, details)
  }

  private finish(active: ActiveTransaction): void {
    if (this.active !== active) {
      return
    }
    globalThis.clearTimeout(active.timeout)
    if (this.retiring) {
      this.active = undefined
      this.close(1000, this.retirementReason ?? 'max-requests')
      return
    }
    this.becomeIdle()
    this.callbacks.onIdle(this)
  }
}

function createRelayFailure(
  outcome: HttpRelayResponseOutcome,
  message: string,
): HttpTransportFailure {
  const code =
    outcome === HttpRelayResponseOutcome.RESPONSE_TOO_LARGE
      ? 'response-too-large'
      : outcome === HttpRelayResponseOutcome.UPSTREAM_FAILURE
        ? 'upstream-failure'
        : 'invalid-response'
  return new HttpTransportFailure(code, message || 'HTTP relay 请求失败')
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

function resolveOptions(options: WebSocketHttpTransportOptions): ResolvedOptions {
  positiveNumber(options.connectTimeoutMs, 'connectTimeoutMs')
  positiveInteger(options.defaultMaxResponseBytes, 'defaultMaxResponseBytes')
  positiveNumber(options.idleConnectionTimeoutMs, 'idleConnectionTimeoutMs')
  positiveInteger(options.maxConnections, 'maxConnections')
  positiveInteger(options.maxRequestsPerConnection, 'maxRequestsPerConnection')
  nonNegativeInteger(options.minIdleConnections, 'minIdleConnections')
  positiveNumber(options.transactionTimeoutMs, 'transactionTimeoutMs')
  if (options.defaultMaxResponseBytes > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES) {
    throw new Error(`defaultMaxResponseBytes 不能超过 ${HTTP_RELAY_MAX_RESPONSE_BODY_BYTES}`)
  }
  if (options.minIdleConnections > options.maxConnections) {
    throw new Error('minIdleConnections 不能超过 maxConnections')
  }
  return {
    connectTimeoutMs: options.connectTimeoutMs,
    defaultMaxResponseBytes: options.defaultMaxResponseBytes,
    debugLogger: resolveDebugLogger(options.debug),
    idleConnectionTimeoutMs: options.idleConnectionTimeoutMs,
    maxConnections: options.maxConnections,
    maxRequestsPerConnection: options.maxRequestsPerConnection,
    minIdleConnections: options.minIdleConnections,
    transactionTimeoutMs: options.transactionTimeoutMs,
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
