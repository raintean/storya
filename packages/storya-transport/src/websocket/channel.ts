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
import { createAbortError, HttpTransportFailure } from '../http-transport'
import { WebSocketHttpResponse } from './response'
import type {
  PendingWebSocketRequest,
  ResolvedWebSocketHttpTransportOptions,
  WebSocketChannelCloseDetails,
  WebSocketChannelRetirementReason,
  WebSocketChannelState,
  WebSocketLike,
} from './types'

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
  resolve: PendingWebSocketRequest['resolve']
  responseBodyPending: boolean
  responseHeadReceived: boolean
  responseSettled: boolean
}

export interface WebSocketChannelCallbacks {
  onClosed(
    channel: WebSocketChannel,
    error: HttpTransportFailure,
    opened: boolean,
    details: WebSocketChannelCloseDetails,
  ): void
  onIdle(channel: WebSocketChannel): void
  onOpen(channel: WebSocketChannel): void
}

const webSocketConnecting = 0
const webSocketOpen = 1

export class WebSocketChannel {
  private active: ActiveTransaction | undefined
  private readonly connectTimer: number
  private readonly createdAt = performance.now()
  private idleSince = performance.now()
  private opened = false
  private readingMessage = false
  private requests = 0
  private retirementReason: WebSocketChannelRetirementReason | undefined
  private readonly socket: WebSocketLike
  private state: WebSocketChannelState = 'connecting'

  constructor(
    private readonly connectionId: number,
    url: string,
    private readonly options: ResolvedWebSocketHttpTransportOptions,
    private readonly callbacks: WebSocketChannelCallbacks,
  ) {
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

  execute(pending: PendingWebSocketRequest): void {
    if (!this.isAvailable()) {
      pending.reject(new HttpTransportFailure('connection-failed', 'WebSocket 连接当前不可用'))
      return
    }
    if (pending.request.signal.aborted) {
      pending.reject(createAbortError())
      this.callbacks.onIdle(this)
      return
    }

    this.state = 'busy'
    this.requests += 1
    if (this.requests >= this.options.maxRequestsPerConnection) {
      this.retirementReason = 'max-requests'
    }
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
      responseBodyPending: false,
      responseHeadReceived: false,
      responseSettled: false,
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
      this.send(TransportFrameKind.REQUEST_HEAD, toBinary(HttpRequestHeadSchema, head))
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('connection-failed', 'WebSocket 请求发送失败', { cause }),
      )
    }
  }

  destroy(error: HttpTransportFailure): void {
    this.close(1000, 'destroyed', error)
  }

  getAgeMs(): number {
    return performance.now() - this.createdAt
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
    return this.state === 'idle' && this.retirementReason === undefined
  }

  retire(reason: WebSocketChannelRetirementReason): void {
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
    const active = this.active
    if (active?.responseBodyPending === true) {
      active.responseBodyPending = false
      if (!active.cancelRequested) {
        try {
          this.acceptResponseBody(active, toUint8Array(data))
        } catch (cause) {
          this.closeWithError(asProtocolError(cause, '原始 response body 处理失败'))
        }
      }
      return
    }

    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch (cause) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '收到无效的 Transport frame', { cause }),
      )
      return
    }

    if (active === undefined) {
      this.closeWithError(
        new HttpTransportFailure('protocol-error', '空闲 WebSocket 收到意外的 Transport frame'),
      )
      return
    }
    if (active.cancelRequested && frame.kind === TransportFrameKind.RESPONSE_HEAD) {
      return
    }

    try {
      switch (frame.kind) {
        case TransportFrameKind.RESPONSE_HEAD:
          this.acceptResponseHead(active, frame.payload)
          break
        case TransportFrameKind.RESPONSE_BODY:
          assertEmptyPayload(frame.payload, 'RESPONSE_BODY')
          this.acceptResponseBodyMarker(active)
          break
        case TransportFrameKind.RESPONSE_END:
          assertEmptyPayload(frame.payload, 'RESPONSE_END')
          this.acceptResponseEnd(active)
          break
        case TransportFrameKind.CANCELED:
          assertEmptyPayload(frame.payload, 'CANCELED')
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
      this.closeWithError(asProtocolError(cause, 'Transport frame 处理失败'))
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

  private acceptResponseBodyMarker(active: ActiveTransaction): void {
    if (active.cancelRequested) {
      active.responseBodyPending = true
      return
    }
    if (!active.responseHeadReceived || active.request.method === 'HEAD') {
      throw new HttpTransportFailure('protocol-error', 'HTTP response body 早于 response head')
    }
    active.responseBodyPending = true
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
    this.state = 'canceling'
    this.rejectActive(active, reason)
    try {
      this.send(TransportFrameKind.CANCEL)
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

  private becomeIdle(): void {
    this.active = undefined
    this.idleSince = performance.now()
    this.state = 'idle'
  }

  private close(
    code: number,
    reason: string,
    error = new HttpTransportFailure('connection-failed', `WebSocket 已关闭: ${reason}`),
  ): void {
    if (this.state === 'closed') {
      return
    }
    const details: WebSocketChannelCloseDetails = { code, initiator: 'local', reason }
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

  private finalizeClose(error: HttpTransportFailure, details: WebSocketChannelCloseDetails): void {
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

  private finish(active: ActiveTransaction): void {
    if (this.active !== active) {
      return
    }
    active.request.signal.removeEventListener('abort', active.abortListener)
    if (active.cancelTimer !== undefined) {
      globalThis.clearTimeout(active.cancelTimer)
    }
    if (this.retirementReason !== undefined) {
      this.active = undefined
      this.close(1000, this.retirementReason)
      return
    }
    this.becomeIdle()
    this.callbacks.onIdle(this)
  }

  private send(kind: TransportFrameKind, payload?: Uint8Array): void {
    if (this.socket.readyState !== webSocketOpen) {
      throw new Error('WebSocket 尚未打开')
    }
    this.socket.send(encodeTransportFrame(kind, payload))
  }
}

function parseContentLength(headers: Headers): number | undefined {
  const value = headers.get('content-length')
  if (value === null || !/^\d+$/u.test(value)) {
    return undefined
  }
  const length = Number(value)
  return Number.isSafeInteger(length) ? length : undefined
}

function assertEmptyPayload(payload: Uint8Array, kind: string): void {
  if (payload.byteLength !== 0) {
    throw new HttpTransportFailure('protocol-error', `${kind} payload 必须为空`)
  }
}

function asProtocolError(cause: unknown, message: string): HttpTransportFailure {
  return cause instanceof HttpTransportFailure
    ? cause
    : new HttpTransportFailure('protocol-error', message, { cause })
}

function toUint8Array(data: ArrayBuffer | ArrayBufferView): Uint8Array {
  return data instanceof ArrayBuffer
    ? new Uint8Array(data)
    : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}
