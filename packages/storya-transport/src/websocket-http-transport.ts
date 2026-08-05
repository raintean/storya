import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HttpRequestHeadSchema,
  HttpResponseHeadSchema,
  TransportErrorCode,
  TransportErrorSchema,
  TransportFrameKind,
} from 'storya-protocol'
import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { createAbortError, HttpTransportFailure } from './http-transport'
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
  state: ChannelState
  timestamp: number
  type: WebSocketHttpTransportDebugEventType
  wasClean?: boolean
}

export type WebSocketHttpTransportDebugLogger = (event: WebSocketHttpTransportDebugEvent) => void

export interface WebSocketHttpTransportOptions {
  connectTimeoutMs?: number
  defaultMaxResponseBytes?: number
  debug?: boolean | WebSocketHttpTransportDebugLogger
  heartbeatIntervalMs?: number
  heartbeatTimeoutMs?: number
  idleConnectionTimeoutMs?: number
  maxConnectionLifetimeMs?: number
  maxConnections?: number
  maxRequestsPerConnection?: number
  webSocketFactory?: WebSocketFactory
}

interface ResolvedOptions {
  connectTimeoutMs: number
  defaultMaxResponseBytes: number
  debugLogger: WebSocketHttpTransportDebugLogger | undefined
  heartbeatIntervalMs: number
  heartbeatTimeoutMs: number
  idleConnectionTimeoutMs: number
  maxConnectionLifetimeMs: number
  maxConnections: number
  maxRequestsPerConnection: number
  webSocketFactory: WebSocketFactory
}

interface PendingRequest {
  abortListener: () => void
  maxResponseBytes: number
  reject: (reason: unknown) => void
  request: Request
  resolve: (response: HttpTransportResponse) => void
}

interface ActiveTransaction {
  abortListener: () => void
  bodyCanceled: boolean
  bodyController: ReadableStreamDefaultController<Uint8Array> | undefined
  bytesReceived: number
  cancelTimer: number | undefined
  cancelRequested: boolean
  maxResponseBytes: number
  reject: (reason: unknown) => void
  request: Request
  resolve: (response: HttpTransportResponse) => void
  responseHeadReceived: boolean
  responseSettled: boolean
  sequence: number
}

interface ChannelCallbacks {
  onClosed(
    channel: WebSocketChannel,
    error: HttpTransportFailure,
    opened: boolean,
    details: ChannelCloseDetails,
  ): void
  onIdle(channel: WebSocketChannel): void
  onOpen(channel: WebSocketChannel): void
}

type ChannelState = 'closed' | 'connecting' | 'idle' | 'running'
type ChannelRetirementReason = 'idle' | 'max-lifetime' | 'max-requests'

interface ChannelCloseDetails {
  code: number
  initiator: 'local' | 'remote'
  reason: string
  wasClean?: boolean
}

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000
const DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000
const DEFAULT_HEARTBEAT_TIMEOUT_MS = 10_000
const DEFAULT_IDLE_CONNECTION_TIMEOUT_MS = 30_000
const DEFAULT_MAX_CONNECTION_LIFETIME_MS = 90_000
const DEFAULT_MAX_CONNECTIONS = 12
const DEFAULT_MAX_REQUESTS_PER_CONNECTION = 40
const MAINTENANCE_INTERVAL_MS = 1_000
const WEB_SOCKET_CONNECTING = 0
const WEB_SOCKET_OPEN = 1

export class WebSocketHttpTransport implements HttpTransport {
  private readonly channels = new Set<WebSocketChannel>()
  private destroyed = false
  private readonly maintenanceTimer: number
  private readonly options: ResolvedOptions
  private readonly pending: PendingRequest[] = []
  private nextChannelId = 0
  private readonly url: string

  constructor(url: string, options: WebSocketHttpTransportOptions = {}) {
    this.url = normalizeWebSocketUrl(url)
    this.options = resolveOptions(options)
    this.maintenanceTimer = globalThis.setInterval(() => this.maintain(), MAINTENANCE_INTERVAL_MS)
  }

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      return Promise.reject(new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁'))
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return Promise.reject(
        new HttpTransportFailure(
          'protocol-error',
          `WebSocket transport 暂不支持 ${request.method} 请求`,
        ),
      )
    }
    if (request.signal.aborted) {
      return Promise.reject(createAbortError())
    }

    let maxResponseBytes: number
    try {
      maxResponseBytes = normalizeMaxResponseBytes(
        options?.maxResponseBytes ?? this.options.defaultMaxResponseBytes,
      )
    } catch (cause) {
      return Promise.reject(
        new HttpTransportFailure('protocol-error', 'maxResponseBytes 无效', { cause }),
      )
    }

    return new Promise<HttpTransportResponse>((resolve, reject) => {
      const pending: PendingRequest = {
        abortListener: () => {
          const index = this.pending.indexOf(pending)
          if (index >= 0) {
            this.pending.splice(index, 1)
            reject(createAbortError())
          }
        },
        maxResponseBytes,
        reject,
        request,
        resolve,
      }
      request.signal.addEventListener('abort', pending.abortListener, { once: true })
      this.pending.push(pending)
      this.drain()
    })
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    globalThis.clearInterval(this.maintenanceTimer)
    const error = new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁')
    for (const pending of this.pending.splice(0)) {
      pending.request.signal.removeEventListener('abort', pending.abortListener)
      pending.reject(error)
    }
    for (const channel of [...this.channels]) {
      channel.destroy(error)
    }
    this.channels.clear()
  }

  private createChannel(): void {
    this.nextChannelId += 1
    const channelId = this.nextChannelId
    let channel: WebSocketChannel
    try {
      channel = new WebSocketChannel(channelId, this.url, this.options, {
        onClosed: (closed, error, opened, details) => {
          this.channels.delete(closed)
          this.emitDebug(closed, 'connection-closed', {
            ...details,
            error: error.message,
          })
          if (!opened) {
            const pending = this.pending.shift()
            if (pending !== undefined) {
              pending.request.signal.removeEventListener('abort', pending.abortListener)
              pending.reject(error)
            }
          }
          this.drain()
        },
        onIdle: () => this.drain(),
        onOpen: opened => {
          this.emitDebug(opened, 'connection-opened')
          this.drain()
        },
      })
    } catch (cause) {
      const pending = this.pending.shift()
      if (pending !== undefined) {
        pending.request.signal.removeEventListener('abort', pending.abortListener)
        pending.reject(
          new HttpTransportFailure('connection-failed', 'WebSocket 连接创建失败', { cause }),
        )
      }
      return
    }
    this.channels.add(channel)
    this.emitDebug(channel, 'connection-created', { reason: 'pending-request' })
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

  private drain(): void {
    if (this.destroyed) {
      return
    }

    while (this.pending.length > 0) {
      const channel = [...this.channels].find(candidate => candidate.isAvailable())
      if (channel === undefined) {
        break
      }
      const pending = this.pending.shift()
      if (pending === undefined) {
        break
      }
      pending.request.signal.removeEventListener('abort', pending.abortListener)
      channel
        .execute(pending.request, pending.maxResponseBytes)
        .then(pending.resolve, pending.reject)
    }

    const connecting = [...this.channels].filter(channel => channel.isConnecting()).length
    const needed = Math.min(
      Math.max(0, this.pending.length - connecting),
      this.options.maxConnections - this.channels.size,
    )
    for (let index = 0; index < needed; index += 1) {
      this.createChannel()
    }
  }

  private maintain(): void {
    if (this.destroyed) {
      return
    }
    const now = performance.now()
    for (const channel of [...this.channels]) {
      channel.maintain(now)
    }

    const idle = [...this.channels]
      .filter(channel => channel.isAvailable())
      .sort((left, right) => left.getIdleSince() - right.getIdleSince())
    let retainedIdleConnections = idle.length
    for (const channel of idle) {
      if (
        retainedIdleConnections <= 1 ||
        now - channel.getIdleSince() < this.options.idleConnectionTimeoutMs
      ) {
        continue
      }
      retainedIdleConnections -= 1
      channel.retire('idle')
    }
    this.drain()
  }
}

class WebSocketChannel {
  private active: ActiveTransaction | undefined
  private readonly callbacks: ChannelCallbacks
  private readonly connectionId: number
  private readonly connectTimer: number
  private readonly createdAt = performance.now()
  private heartbeatSequence = 0
  private idleSince = performance.now()
  private lastWireActivityAt = performance.now()
  private readonly lifetimeExpiresAt: number
  private localCloseDetails: ChannelCloseDetails | undefined
  private opened = false
  private readonly options: ResolvedOptions
  private pingSequence: number | undefined
  private pingSentAt: number | undefined
  private requests = 0
  private retirementReason: ChannelRetirementReason | undefined
  private retiring = false
  private sequence = 0
  private readonly socket: WebSocketLike
  private state: ChannelState = 'connecting'

  constructor(
    connectionId: number,
    url: string,
    options: ResolvedOptions,
    callbacks: ChannelCallbacks,
  ) {
    this.connectionId = connectionId
    this.options = options
    this.callbacks = callbacks
    this.lifetimeExpiresAt =
      performance.now() + options.maxConnectionLifetimeMs * (0.9 + Math.random() * 0.2)
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

  isAvailable(): boolean {
    return this.state === 'idle' && !this.retiring
  }

  isConnecting(): boolean {
    return this.state === 'connecting'
  }

  getIdleSince(): number {
    return this.idleSince
  }

  getAgeMs(): number {
    return performance.now() - this.createdAt
  }

  getId(): number {
    return this.connectionId
  }

  getRequestCount(): number {
    return this.requests
  }

  getState(): ChannelState {
    return this.state
  }

  execute(request: Request, maxResponseBytes: number): Promise<HttpTransportResponse> {
    if (!this.isAvailable()) {
      return Promise.reject(
        new HttpTransportFailure('connection-failed', 'WebSocket 连接当前不可用'),
      )
    }

    this.state = 'running'
    this.requests += 1
    if (this.requests >= this.options.maxRequestsPerConnection) {
      this.retiring = true
      this.retirementReason = 'max-requests'
    }
    const sequence = this.nextSequence()

    return new Promise<HttpTransportResponse>((resolve, reject) => {
      const active: ActiveTransaction = {
        abortListener: () => this.cancel(active, createAbortError()),
        bodyCanceled: false,
        bodyController: undefined,
        bytesReceived: 0,
        cancelTimer: undefined,
        cancelRequested: false,
        maxResponseBytes,
        reject,
        request,
        resolve,
        responseHeadReceived: false,
        responseSettled: false,
        sequence,
      }
      this.active = active
      request.signal.addEventListener('abort', active.abortListener, { once: true })

      const head = create(HttpRequestHeadSchema, {
        headers: [...request.headers].map(([name, value]) => ({ name, value })),
        maxResponseBytes: BigInt(maxResponseBytes),
        method: request.method,
        url: request.url,
      })
      try {
        this.send(TransportFrameKind.REQUEST_HEAD, sequence, toBinary(HttpRequestHeadSchema, head))
      } catch (cause) {
        this.closeWithError(
          new HttpTransportFailure('connection-failed', 'WebSocket 请求发送失败', { cause }),
        )
      }
    })
  }

  maintain(now: number): void {
    if (this.state === 'closed' || this.state === 'connecting') {
      return
    }
    if (now >= this.lifetimeExpiresAt) {
      this.retiring = true
      this.retirementReason ??= 'max-lifetime'
      if (this.state === 'idle') {
        this.retire('max-lifetime')
        return
      }
    }

    if (
      this.pingSequence !== undefined &&
      this.pingSentAt !== undefined &&
      now - this.pingSentAt >= this.options.heartbeatTimeoutMs
    ) {
      this.closeWithError(new HttpTransportFailure('connection-failed', 'WebSocket 心跳超时'))
      return
    }
    if (
      this.pingSequence === undefined &&
      now - this.lastWireActivityAt >= this.options.heartbeatIntervalMs
    ) {
      this.heartbeatSequence = nextUint32(this.heartbeatSequence)
      this.pingSequence = this.heartbeatSequence
      this.pingSentAt = now
      try {
        this.send(TransportFrameKind.PING, this.heartbeatSequence)
      } catch (cause) {
        this.closeWithError(
          new HttpTransportFailure('connection-failed', 'WebSocket 心跳发送失败', { cause }),
        )
      }
    }
  }

  retire(reason: ChannelRetirementReason): void {
    this.retiring = true
    this.retirementReason ??= reason
    if (this.state === 'idle') {
      this.close(1000, this.retirementReason)
    }
  }

  destroy(error: HttpTransportFailure): void {
    this.failActive(error)
    this.close(1000, 'destroyed')
  }

  private readonly handleOpen = (): void => {
    if (this.state !== 'connecting') {
      return
    }
    globalThis.clearTimeout(this.connectTimer)
    this.opened = true
    this.state = 'idle'
    this.idleSince = performance.now()
    this.lastWireActivityAt = this.idleSince
    this.callbacks.onOpen(this)
  }

  private readonly handleMessage = (event: MessageEvent<unknown>): void => {
    void this.readMessageData(event.data).then(
      data => this.acceptFrame(data),
      cause => {
        this.closeWithError(
          new HttpTransportFailure('protocol-error', '无法读取 WebSocket 消息', { cause }),
        )
      },
    )
  }

  private readonly handleError = (): void => {
    this.closeWithError(new HttpTransportFailure('connection-failed', 'WebSocket 连接发生错误'))
  }

  private readonly handleClose = (event: CloseEvent): void => {
    const details = this.localCloseDetails ?? {
      code: event.code,
      initiator: 'remote' as const,
      reason: event.reason,
      wasClean: event.wasClean,
    }
    this.finalizeClose(
      new HttpTransportFailure(
        'connection-failed',
        event.reason || `WebSocket 连接已关闭 (${event.code})`,
      ),
      details,
    )
  }

  private acceptFrame(data: ArrayBuffer | ArrayBufferView): void {
    if (this.state === 'closed') {
      return
    }
    this.lastWireActivityAt = performance.now()

    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '收到无效的 Transport frame', { cause }),
      )
      return
    }

    if (frame.kind === TransportFrameKind.PING) {
      this.send(TransportFrameKind.PONG, frame.sequence)
      return
    }
    if (frame.kind === TransportFrameKind.PONG) {
      if (frame.sequence === this.pingSequence) {
        this.pingSequence = undefined
        this.pingSentAt = undefined
      }
      return
    }

    const active = this.active
    if (active === undefined || frame.sequence !== active.sequence) {
      return
    }
    if (
      active.cancelRequested &&
      (frame.kind === TransportFrameKind.RESPONSE_HEAD ||
        frame.kind === TransportFrameKind.RESPONSE_BODY)
    ) {
      return
    }

    try {
      switch (frame.kind) {
        case TransportFrameKind.RESPONSE_HEAD:
          this.acceptResponseHead(active, frame.payload)
          break
        case TransportFrameKind.RESPONSE_BODY:
          this.acceptResponseBody(active, frame.payload)
          break
        case TransportFrameKind.RESPONSE_END:
          this.acceptResponseEnd(active)
          break
        case TransportFrameKind.CANCELED:
          this.acceptCanceled(active)
          break
        case TransportFrameKind.ERROR:
          this.acceptError(active, frame.payload)
          break
        default:
          throw new HttpTransportFailure(
            'protocol-error',
            `当前连接不接受 Transport frame ${frame.kind}`,
          )
      }
    } catch (cause) {
      const error =
        cause instanceof HttpTransportFailure
          ? cause
          : new HttpTransportFailure('protocol-error', 'Transport frame 处理失败', { cause })
      this.closeWithError(error)
    }
  }

  private acceptResponseHead(active: ActiveTransaction, payload: Uint8Array): void {
    if (active.responseHeadReceived) {
      throw new HttpTransportFailure('protocol-error', '重复收到 HTTP response head')
    }
    const head = fromBinary(HttpResponseHeadSchema, payload)
    if (head.status < 100 || head.status > 599) {
      throw new HttpTransportFailure('invalid-response', `无效的 HTTP status: ${head.status}`)
    }
    active.responseHeadReceived = true
    const headers = new Headers()
    for (const header of head.headers) {
      headers.append(header.name, header.value)
    }
    const declaredLength = parseContentLength(headers)
    if (
      active.request.method !== 'HEAD' &&
      declaredLength !== undefined &&
      declaredLength > active.maxResponseBytes
    ) {
      this.cancel(
        active,
        new HttpTransportFailure(
          'response-too-large',
          `HTTP 响应超过 ${active.maxResponseBytes} 字节限制`,
        ),
      )
      return
    }

    let body: ReadableStream<Uint8Array> | null = null
    if (active.request.method !== 'HEAD') {
      body = new ReadableStream<Uint8Array>({
        cancel: () => {
          active.bodyCanceled = true
          this.cancel(active, createAbortError('HTTP response body 已取消'))
        },
        start: controller => {
          active.bodyController = controller
        },
      })
    }
    active.responseSettled = true
    active.resolve(
      new WebSocketHttpResponse(body, {
        headers,
        status: head.status,
        statusText: head.statusText,
        url: head.url || active.request.url,
      }),
    )
  }

  private acceptResponseBody(active: ActiveTransaction, payload: Uint8Array): void {
    if (!active.responseHeadReceived || active.request.method === 'HEAD') {
      throw new HttpTransportFailure('protocol-error', 'HTTP response body 早于 response head')
    }
    active.bytesReceived += payload.byteLength
    if (active.bytesReceived > active.maxResponseBytes) {
      this.cancel(
        active,
        new HttpTransportFailure(
          'response-too-large',
          `HTTP 响应超过 ${active.maxResponseBytes} 字节限制`,
        ),
      )
      return
    }
    active.bodyController?.enqueue(payload.slice())
  }

  private acceptResponseEnd(active: ActiveTransaction): void {
    if (!active.responseHeadReceived && !active.cancelRequested) {
      throw new HttpTransportFailure('protocol-error', 'HTTP response 在 response head 前结束')
    }
    if (!active.cancelRequested && !active.bodyCanceled) {
      active.bodyController?.close()
    }
    this.finish(active)
  }

  private acceptCanceled(active: ActiveTransaction): void {
    if (!active.cancelRequested) {
      this.rejectActive(active, createAbortError('Relay 取消了 HTTP 请求'))
    }
    this.finish(active)
  }

  private acceptError(active: ActiveTransaction, payload: Uint8Array): void {
    const message = fromBinary(TransportErrorSchema, payload)
    const code =
      message.code === TransportErrorCode.RESPONSE_TOO_LARGE
        ? 'response-too-large'
        : message.code === TransportErrorCode.UPSTREAM_FAILURE
          ? 'upstream-failure'
          : 'invalid-response'
    this.rejectActive(active, new HttpTransportFailure(code, message.message || 'Relay 请求失败'))
    this.finish(active)
  }

  private cancel(active: ActiveTransaction, reason: Error): void {
    if (this.active !== active || active.cancelRequested) {
      return
    }
    active.cancelRequested = true
    this.rejectActive(active, reason)
    try {
      this.send(TransportFrameKind.CANCEL, active.sequence)
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('connection-failed', 'WebSocket CANCEL 发送失败', { cause }),
      )
      return
    }
    active.cancelTimer = globalThis.setTimeout(() => {
      if (this.active === active) {
        this.closeWithError(
          new HttpTransportFailure('connection-failed', 'WebSocket CANCEL 确认超时'),
        )
      }
    }, this.options.heartbeatTimeoutMs)
  }

  private rejectActive(active: ActiveTransaction, reason: unknown): void {
    if (!active.responseSettled) {
      active.responseSettled = true
      active.reject(reason)
    }
    if (!active.bodyCanceled) {
      try {
        active.bodyController?.error(reason)
      } catch {
        // 已经终止的 ReadableStream 不需要再次处理
      }
      active.bodyCanceled = true
    }
  }

  private finish(active: ActiveTransaction): void {
    if (this.active !== active) {
      return
    }
    active.request.signal.removeEventListener('abort', active.abortListener)
    if (active.cancelTimer !== undefined) {
      globalThis.clearTimeout(active.cancelTimer)
    }
    this.active = undefined
    this.idleSince = performance.now()
    this.state = 'idle'
    if (this.retiring) {
      this.close(1000, this.retirementReason ?? 'max-lifetime')
      return
    }
    this.callbacks.onIdle(this)
  }

  private failActive(error: HttpTransportFailure): void {
    const active = this.active
    if (active === undefined) {
      return
    }
    this.rejectActive(active, error)
    active.request.signal.removeEventListener('abort', active.abortListener)
    if (active.cancelTimer !== undefined) {
      globalThis.clearTimeout(active.cancelTimer)
    }
    this.active = undefined
  }

  private send(kind: TransportFrameKind, sequence: number, payload?: Uint8Array): void {
    if (this.socket.readyState !== WEB_SOCKET_OPEN) {
      throw new Error('WebSocket 尚未打开')
    }
    this.socket.send(encodeTransportFrame(kind, sequence, payload))
    this.lastWireActivityAt = performance.now()
  }

  private closeWithError(error: HttpTransportFailure): void {
    this.failActive(error)
    this.close(1011, 'connection failed', error)
  }

  private close(code: number, reason: string, error?: HttpTransportFailure): void {
    if (this.state === 'closed') {
      return
    }
    const details: ChannelCloseDetails = { code, initiator: 'local', reason }
    this.localCloseDetails = details
    if (
      this.socket.readyState === WEB_SOCKET_CONNECTING ||
      this.socket.readyState === WEB_SOCKET_OPEN
    ) {
      try {
        this.socket.close(code, reason)
      } catch {
        // 连接关闭失败时仍然需要释放本地状态
      }
    }
    this.finalizeClose(
      error ?? new HttpTransportFailure('connection-failed', `WebSocket 已关闭: ${reason}`),
      details,
    )
  }

  private finalizeClose(error: HttpTransportFailure, details: ChannelCloseDetails): void {
    if (this.state === 'closed') {
      return
    }
    const opened = this.opened
    this.state = 'closed'
    globalThis.clearTimeout(this.connectTimer)
    this.failActive(error)
    this.socket.removeEventListener('open', this.handleOpen)
    this.socket.removeEventListener('message', this.handleMessage)
    this.socket.removeEventListener('error', this.handleError)
    this.socket.removeEventListener('close', this.handleClose)
    this.callbacks.onClosed(this, error, opened, details)
  }

  private nextSequence(): number {
    this.sequence = nextUint32(this.sequence)
    return this.sequence
  }

  private async readMessageData(data: unknown): Promise<ArrayBuffer | ArrayBufferView> {
    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
      return data
    }
    if (data instanceof Blob) {
      return data.arrayBuffer()
    }
    throw new Error('Transport 只接受二进制 WebSocket 消息')
  }
}

function resolveOptions(options: WebSocketHttpTransportOptions): ResolvedOptions {
  return {
    connectTimeoutMs: positiveNumber(options.connectTimeoutMs, DEFAULT_CONNECT_TIMEOUT_MS),
    defaultMaxResponseBytes: positiveInteger(
      options.defaultMaxResponseBytes,
      DEFAULT_MAX_RESPONSE_BYTES,
    ),
    debugLogger: resolveDebugLogger(options.debug),
    heartbeatIntervalMs: positiveNumber(options.heartbeatIntervalMs, DEFAULT_HEARTBEAT_INTERVAL_MS),
    heartbeatTimeoutMs: positiveNumber(options.heartbeatTimeoutMs, DEFAULT_HEARTBEAT_TIMEOUT_MS),
    idleConnectionTimeoutMs: positiveNumber(
      options.idleConnectionTimeoutMs,
      DEFAULT_IDLE_CONNECTION_TIMEOUT_MS,
    ),
    maxConnectionLifetimeMs: positiveNumber(
      options.maxConnectionLifetimeMs,
      DEFAULT_MAX_CONNECTION_LIFETIME_MS,
    ),
    maxConnections: positiveInteger(options.maxConnections, DEFAULT_MAX_CONNECTIONS),
    maxRequestsPerConnection: positiveInteger(
      options.maxRequestsPerConnection,
      DEFAULT_MAX_REQUESTS_PER_CONNECTION,
    ),
    webSocketFactory: options.webSocketFactory ?? (url => new WebSocket(url)),
  }
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
    const labels: Record<WebSocketHttpTransportDebugEventType, string> = {
      'connection-closed': '连接关闭',
      'connection-created': '创建连接',
      'connection-opened': '连接建立',
    }
    console.info(`[storya-transport] WebSocket #${event.connectionId} ${labels[event.type]}`, event)
  }
}

function normalizeWebSocketUrl(value: string): string {
  const url = new URL(value)
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`WebSocket transport URL 必须使用 ws 或 wss: ${value}`)
  }
  return url.toString()
}

function normalizeMaxResponseBytes(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`无效的 maxResponseBytes: ${value}`)
  }
  return value
}

function parseContentLength(headers: Headers): number | undefined {
  const value = headers.get('content-length')
  if (value === null) {
    return undefined
  }
  const length = Number.parseInt(value, 10)
  return Number.isSafeInteger(length) && length >= 0 ? length : undefined
}

function nextUint32(value: number): number {
  return value >= 0xffff_ffff ? 1 : value + 1
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isSafeInteger(value) || value <= 0 ? fallback : value
}

function positiveNumber(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isFinite(value) || value <= 0 ? fallback : value
}

export const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS = DEFAULT_CONNECT_TIMEOUT_MS
export const DEFAULT_WEBSOCKET_HEARTBEAT_INTERVAL_MS = DEFAULT_HEARTBEAT_INTERVAL_MS
export const DEFAULT_WEBSOCKET_HEARTBEAT_TIMEOUT_MS = DEFAULT_HEARTBEAT_TIMEOUT_MS
export const DEFAULT_WEBSOCKET_IDLE_CONNECTION_TIMEOUT_MS = DEFAULT_IDLE_CONNECTION_TIMEOUT_MS
export const DEFAULT_WEBSOCKET_MAX_CONNECTION_LIFETIME_MS = DEFAULT_MAX_CONNECTION_LIFETIME_MS
export const DEFAULT_WEBSOCKET_MAX_CONNECTIONS = DEFAULT_MAX_CONNECTIONS
export const DEFAULT_WEBSOCKET_MAX_REQUESTS_PER_CONNECTION = DEFAULT_MAX_REQUESTS_PER_CONNECTION
