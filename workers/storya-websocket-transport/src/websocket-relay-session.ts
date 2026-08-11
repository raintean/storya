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
import type { HttpRequestHead } from 'storya-protocol'
import {
  createClientHeaders,
  createUpstreamHeaders,
  InvalidRelayRequest,
  parseContentLength,
  upstreamCacheTtlSeconds,
  validateRelayRequest,
} from './relay-http'

interface RelayTransaction {
  readonly controller: AbortController
  reader: ReadableStreamBYOBReader | undefined
  transferredBytes: number
}

const responseBodyFrameBytes = 4 * 1024
const responseBodyMaxReadBytes = 256 * 1024
const responseBodyMarker = encodeTransportFrame(TransportFrameKind.RESPONSE_BODY)

export function createWebSocketRelayResponse(request: Request, ctx: ExecutionContext): Response {
  const pair = new WebSocketPair()
  const client = pair[0]
  const session = new WebSocketRelaySession(pair[1], createClientHeaders(request), ctx)
  session.accept()
  return new Response(null, {
    headers: { 'sec-websocket-extensions': '' },
    status: 101,
    webSocket: client,
  })
}

class WebSocketRelaySession {
  private active: RelayTransaction | undefined

  constructor(
    private readonly socket: WebSocket,
    private readonly clientHeaders: Headers,
    private readonly ctx: ExecutionContext,
  ) {}

  accept(): void {
    this.socket.binaryType = 'arraybuffer'
    this.socket.accept()
    this.socket.addEventListener('message', this.handleMessage)
    this.socket.addEventListener('close', this.handleClose)
    this.socket.addEventListener('error', this.handleError)
  }

  private readonly handleMessage = (event: MessageEvent): void => {
    this.acceptMessage(event.data)
  }

  private readonly handleClose = (): void => {
    this.stopActive()
  }

  private readonly handleError = (): void => {
    this.stopActive()
    this.close(1011, 'websocket error')
  }

  private acceptMessage(data: string | ArrayBuffer): void {
    if (typeof data === 'string') {
      this.stopActive()
      this.close(1003, 'binary frames required')
      return
    }

    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch {
      this.stopActive()
      this.close(1002, 'invalid transport frame')
      return
    }

    if (frame.kind === TransportFrameKind.CANCEL) {
      if (frame.payload.byteLength !== 0) {
        this.stopActive()
        this.close(1002, 'CANCEL payload must be empty')
        return
      }
      this.cancel()
      return
    }
    if (frame.kind !== TransportFrameKind.REQUEST_HEAD) {
      this.stopActive()
      this.close(1002, 'unexpected transport frame')
      return
    }
    if (this.active !== undefined) {
      this.stopActive()
      this.close(1008, 'request already active')
      return
    }

    let head
    try {
      head = fromBinary(HttpRequestHeadSchema, frame.payload)
    } catch {
      this.sendError(TransportErrorCode.INVALID_REQUEST, 'HTTP request head 无效')
      return
    }

    const transaction: RelayTransaction = {
      controller: new AbortController(),
      reader: undefined,
      transferredBytes: 0,
    }
    this.active = transaction
    this.track(this.relay(transaction, head))
  }

  private async relay(transaction: RelayTransaction, head: HttpRequestHead): Promise<void> {
    try {
      const { maxResponseBytes, target } = validateRelayRequest(head)
      const response = await fetch(
        new Request(target, {
          cf: {
            cacheEverything: true,
            cacheTtlByStatus: {
              '200-299': upstreamCacheTtlSeconds,
              '300-399': 0,
              '400-599': -1,
            },
          },
          headers: createUpstreamHeaders(head, this.clientHeaders),
          method: head.method,
          redirect: 'follow',
          signal: transaction.controller.signal,
        }),
      )
      if (!this.isActive(transaction)) {
        void response.body?.cancel()
        return
      }

      const contentLength = parseContentLength(response.headers)
      if (
        head.method !== 'HEAD' &&
        contentLength !== undefined &&
        contentLength > maxResponseBytes
      ) {
        this.fail(
          transaction,
          TransportErrorCode.RESPONSE_TOO_LARGE,
          `HTTP 响应超过 ${maxResponseBytes} 字节限制`,
        )
        return
      }

      const responseHead = create(HttpResponseHeadSchema, {
        headers: [...response.headers].map(([name, value]) => ({ name, value })),
        status: response.status,
        statusText: response.statusText,
        url: response.url || target.toString(),
      })
      this.send(TransportFrameKind.RESPONSE_HEAD, toBinary(HttpResponseHeadSchema, responseHead))

      if (head.method === 'HEAD' || response.body === null) {
        this.finish(transaction)
        return
      }
      await this.relayBody(transaction, response.body, maxResponseBytes)
    } catch (cause) {
      if (!this.isActive(transaction)) {
        return
      }
      this.fail(
        transaction,
        cause instanceof InvalidRelayRequest
          ? TransportErrorCode.INVALID_REQUEST
          : TransportErrorCode.UPSTREAM_FAILURE,
        cause instanceof Error ? cause.message : '未知上游请求错误',
      )
    }
  }

  private async relayBody(
    transaction: RelayTransaction,
    body: ReadableStream<Uint8Array>,
    maxResponseBytes: number,
  ): Promise<void> {
    const reader = body.getReader({ mode: 'byob' })
    transaction.reader = reader
    let readBytes = responseBodyFrameBytes
    while (this.isActive(transaction)) {
      const remainingWithOverflowByte = maxResponseBytes - transaction.transferredBytes + 1
      const readSize = Math.min(readBytes, remainingWithOverflowByte)
      const result = await reader.readAtLeast(readSize, new Uint8Array(readSize))
      if (!this.isActive(transaction)) {
        return
      }

      const payload = result.value
      if (payload !== undefined && payload.byteLength !== 0) {
        transaction.transferredBytes += payload.byteLength
        if (transaction.transferredBytes > maxResponseBytes) {
          this.fail(
            transaction,
            TransportErrorCode.RESPONSE_TOO_LARGE,
            `HTTP 响应超过 ${maxResponseBytes} 字节限制`,
          )
          return
        }
        this.sendResponseBody(payload)
      }
      if (result.done) {
        this.finish(transaction)
        return
      }
      readBytes = Math.min(readBytes * 2, responseBodyMaxReadBytes)
    }
  }

  private cancel(): void {
    const transaction = this.active
    if (transaction === undefined) {
      return
    }
    if (this.stopTransaction(transaction)) {
      this.send(TransportFrameKind.CANCELED)
    }
  }

  private finish(transaction: RelayTransaction): void {
    if (!this.isActive(transaction)) {
      return
    }
    this.send(TransportFrameKind.RESPONSE_END)
    transaction.reader = undefined
    this.active = undefined
  }

  private fail(transaction: RelayTransaction, code: TransportErrorCode, message: string): void {
    if (!this.isActive(transaction)) {
      return
    }
    this.sendError(code, message)
    this.stopTransaction(transaction)
  }

  private stopActive(): void {
    const transaction = this.active
    if (transaction !== undefined) {
      this.stopTransaction(transaction)
    }
  }

  private stopTransaction(transaction: RelayTransaction): boolean {
    if (!this.isActive(transaction)) {
      return false
    }
    this.active = undefined
    const reader = transaction.reader
    transaction.reader = undefined
    if (reader !== undefined) {
      void reader.cancel().catch(() => {
        // AbortController 会继续终止上游请求
      })
    }
    transaction.controller.abort()
    return true
  }

  private isActive(transaction: RelayTransaction): boolean {
    return this.active === transaction
  }

  private send(kind: TransportFrameKind, payload?: Uint8Array): void {
    this.socket.send(encodeTransportFrame(kind, payload))
  }

  private sendResponseBody(payload: Uint8Array): void {
    for (let offset = 0; offset < payload.byteLength; offset += responseBodyFrameBytes) {
      this.socket.send(responseBodyMarker)
      this.socket.send(
        payload.subarray(offset, Math.min(offset + responseBodyFrameBytes, payload.byteLength)),
      )
    }
  }

  private sendError(code: TransportErrorCode, message: string): void {
    const error = create(TransportErrorSchema, { code, message })
    this.send(TransportFrameKind.ERROR, toBinary(TransportErrorSchema, error))
  }

  private close(code: number, reason: string): void {
    try {
      this.socket.close(code, reason)
    } catch {
      // 连接丢失或已经关闭时只需收敛本地事务
    }
  }

  private track(task: Promise<void>): void {
    this.ctx.waitUntil(
      task.catch(cause => {
        console.error({
          message: cause instanceof Error ? cause.message : String(cause),
          type: 'http-relay-failure',
        })
        this.stopActive()
        if (this.socket.readyState === WebSocket.OPEN) {
          this.close(1011, 'relay failure')
        }
      }),
    )
  }
}
