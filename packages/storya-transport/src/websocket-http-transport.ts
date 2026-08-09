import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
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
import { TransportStatistics, type TransportStatisticsSnapshot } from './transport-statistics'
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
  cancelTimeoutMs: number
  connectTimeoutMs: number
  defaultMaxResponseBytes: number
  debug?: boolean | WebSocketHttpTransportDebugLogger
  idleConnectionTimeoutMs: number
  maxConnections: number
  maxRequestsPerConnection: number
  minIdleConnections: number
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
  cancelRequested: boolean
  cancelTimer: number | undefined
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
  private readonly channels = new Set<WebSocketChannel>()
  private destroyed = false
  private nextChannelId = 0
  private readonly options: ResolvedOptions
  private readonly pending: PendingRequest[] = []
  private readonly statistics = new TransportStatistics('WebSocketHttpTransport', {
    cacheLabel: '上游缓存',
  })
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
    if (request.signal.aborted) {
      return Promise.reject(createAbortError())
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

    const statistics = this.statistics.startRequest()
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
    }).then(
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
    const error = new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁')
    for (const pending of this.pending.splice(0)) {
      pending.request.signal.removeEventListener('abort', pending.abortListener)
      pending.reject(error)
    }
    for (const channel of [...this.channels]) {
      channel.destroy(error)
    }
    this.channels.clear()
    this.statistics.destroy()
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
            const pending = this.pending.shift()
            if (pending !== undefined) {
              pending.request.signal.removeEventListener('abort', pending.abortListener)
              pending.reject(error)
            }
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
      pending.request.signal.removeEventListener('abort', pending.abortListener)
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
  private sequence = 0
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
    if (pending.request.signal.aborted) {
      pending.reject(createAbortError())
      this.callbacks.onIdle(this)
      return
    }

    this.clearIdleTimer()
    this.state = 'busy'
    this.requests += 1
    if (this.requests >= this.options.maxRequestsPerConnection) {
      this.retiring = true
      this.retirementReason = 'max-requests'
    }
    const sequence = this.nextSequence()
    const active: ActiveTransaction = {
      abortListener: () => this.cancel(active, createAbortError()),
      bodyCanceled: false,
      bodyController: undefined,
      bytesReceived: 0,
      cancelRequested: false,
      cancelTimer: undefined,
      maxResponseBytes: pending.maxResponseBytes,
      reject: pending.reject,
      request: pending.request,
      resolve: pending.resolve,
      responseHeadReceived: false,
      responseSettled: false,
      sequence,
    }
    this.active = active
    pending.request.signal.addEventListener('abort', active.abortListener, { once: true })

    const head = create(HttpRequestHeadSchema, {
      headers: [...pending.request.headers].map(([name, value]) => ({ name, value })),
      maxResponseBytes: BigInt(pending.maxResponseBytes),
      method: pending.request.method,
      url: pending.request.url,
    })
    try {
      this.send(TransportFrameKind.REQUEST_HEAD, sequence, toBinary(HttpRequestHeadSchema, head))
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
      this.closeWithError(new HttpTransportFailure('protocol-error', '收到重叠的 WebSocket 消息'))
      return
    }
    if (event.data instanceof ArrayBuffer || ArrayBuffer.isView(event.data)) {
      this.acceptFrame(event.data)
      return
    }
    if (!(event.data instanceof Blob)) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', 'Transport 只接受二进制 WebSocket 消息'),
      )
      return
    }

    this.readingMessage = true
    void event.data.arrayBuffer().then(
      data => {
        this.readingMessage = false
        if (this.state !== 'closed') {
          this.acceptFrame(data)
        }
      },
      cause => {
        this.readingMessage = false
        this.closeWithError(
          new HttpTransportFailure('protocol-error', '无法读取 WebSocket 消息', { cause }),
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

  private acceptFrame(data: ArrayBuffer | ArrayBufferView): void {
    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '收到无效的 Transport frame', { cause }),
      )
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
    active.bodyController?.enqueue(payload)
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
    }, this.options.cancelTimeoutMs)
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
    this.rejectActive(active, error)
    active.request.signal.removeEventListener('abort', active.abortListener)
    if (active.cancelTimer !== undefined) {
      globalThis.clearTimeout(active.cancelTimer)
    }
    this.active = undefined
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
    active.request.signal.removeEventListener('abort', active.abortListener)
    if (active.cancelTimer !== undefined) {
      globalThis.clearTimeout(active.cancelTimer)
    }
    if (this.retiring) {
      this.active = undefined
      this.close(1000, this.retirementReason ?? 'max-requests')
      return
    }
    this.becomeIdle()
    this.callbacks.onIdle(this)
  }

  private send(kind: TransportFrameKind, sequence: number, payload?: Uint8Array): void {
    if (this.socket.readyState !== webSocketOpen) {
      throw new Error('WebSocket 尚未打开')
    }
    this.socket.send(encodeTransportFrame(kind, sequence, payload))
  }

  private nextSequence(): number {
    this.sequence = this.sequence >= 0xffff_ffff ? 1 : this.sequence + 1
    return this.sequence
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

function parseContentLength(headers: Headers): number | undefined {
  const value = headers.get('content-length')
  if (value === null || !/^\d+$/u.test(value)) {
    return undefined
  }
  const length = Number(value)
  return Number.isSafeInteger(length) ? length : undefined
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
  positiveNumber(options.cancelTimeoutMs, 'cancelTimeoutMs')
  positiveNumber(options.connectTimeoutMs, 'connectTimeoutMs')
  positiveInteger(options.defaultMaxResponseBytes, 'defaultMaxResponseBytes')
  positiveNumber(options.idleConnectionTimeoutMs, 'idleConnectionTimeoutMs')
  positiveInteger(options.maxConnections, 'maxConnections')
  positiveInteger(options.maxRequestsPerConnection, 'maxRequestsPerConnection')
  nonNegativeInteger(options.minIdleConnections, 'minIdleConnections')
  if (options.defaultMaxResponseBytes > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES) {
    throw new Error(`defaultMaxResponseBytes 不能超过 ${HTTP_RELAY_MAX_RESPONSE_BODY_BYTES}`)
  }
  if (options.minIdleConnections > options.maxConnections) {
    throw new Error('minIdleConnections 不能超过 maxConnections')
  }
  return {
    cancelTimeoutMs: options.cancelTimeoutMs,
    connectTimeoutMs: options.connectTimeoutMs,
    defaultMaxResponseBytes: options.defaultMaxResponseBytes,
    debugLogger: resolveDebugLogger(options.debug),
    idleConnectionTimeoutMs: options.idleConnectionTimeoutMs,
    maxConnections: options.maxConnections,
    maxRequestsPerConnection: options.maxRequestsPerConnection,
    minIdleConnections: options.minIdleConnections,
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
