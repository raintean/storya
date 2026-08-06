import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HttpResponseHeadSchema,
  HttpRequestHeadSchema,
  TRANSPORT_FRAME_HEADER_SIZE,
  TransportErrorCode,
  TransportErrorSchema,
  TransportFrameKind,
} from 'storya-protocol'
import type { HttpRequestHead } from 'storya-protocol'

interface RelayTransaction {
  readonly controller: AbortController
  reader: ReadableStreamBYOBReader | undefined
  readonly sequence: number
  readonly startedAt: number
  terminal: boolean
  transferredBytes: number
}

const MAX_RESPONSE_BYTES = 64 * 1024 * 1024
const RESPONSE_FRAME_SIZE = 256 * 1024
const blockedRequestHeaders = new Set([
  'connection',
  'content-length',
  'host',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
])

function json(body: unknown, init?: ResponseInit): Response {
  const headers = new Headers(init?.headers)
  headers.set('content-type', 'application/json; charset=utf-8')
  return new Response(JSON.stringify(body), { ...init, headers })
}

function createTransportResponse(request: Request, ctx: ExecutionContext): Response {
  const pair = new WebSocketPair()
  const client = pair[0]
  const server = pair[1]
  const clientHeaders = createClientHeaders(request)
  server.binaryType = 'arraybuffer'
  server.accept()

  let active: RelayTransaction | undefined

  const send = (kind: TransportFrameKind, sequence: number, payload?: Uint8Array): void => {
    server.send(encodeTransportFrame(kind, sequence, payload))
  }

  const sendResponseBody = (sequence: number, payload: Uint8Array<ArrayBuffer>): void => {
    const frame = new Uint8Array(
      payload.buffer,
      payload.byteOffset - TRANSPORT_FRAME_HEADER_SIZE,
      payload.byteLength + TRANSPORT_FRAME_HEADER_SIZE,
    )
    frame[0] = TransportFrameKind.RESPONSE_BODY
    new DataView(frame.buffer, frame.byteOffset, TRANSPORT_FRAME_HEADER_SIZE).setUint32(1, sequence)
    server.send(frame)
  }

  const sendError = (sequence: number, code: TransportErrorCode, message: string): void => {
    const error = create(TransportErrorSchema, { code, message })
    send(TransportFrameKind.ERROR, sequence, toBinary(TransportErrorSchema, error))
  }

  const stopActive = async (): Promise<void> => {
    const transaction = active
    active = undefined
    if (transaction === undefined) {
      return
    }
    transaction.terminal = true
    transaction.controller.abort()
    try {
      await transaction.reader?.cancel()
    } catch {
      // 上游流可能已经因 WebSocket 关闭而终止
    }
  }

  const trackTask = (name: string, task: Promise<void>): void => {
    ctx.waitUntil(
      task.catch(async cause => {
        console.error({
          activeSequence: active?.sequence,
          message: cause instanceof Error ? cause.message : String(cause),
          name,
          type: 'transport-async-task-error',
        })
        await stopActive()
        if (server.readyState === WebSocket.OPEN) {
          try {
            server.close(1011, 'transport task failed')
          } catch (closeCause) {
            console.error({
              message: closeCause instanceof Error ? closeCause.message : String(closeCause),
              name,
              type: 'transport-websocket-close-error',
            })
          }
        }
      }),
    )
  }

  const finish = (transaction: RelayTransaction, kind: TransportFrameKind): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    send(kind, transaction.sequence)
    transaction.terminal = true
    active = undefined
  }

  const fail = (transaction: RelayTransaction, code: TransportErrorCode, message: string): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    sendError(transaction.sequence, code, message)
    transaction.terminal = true
    active = undefined
  }

  const cancel = async (sequence: number): Promise<void> => {
    const transaction = active
    if (transaction === undefined || transaction.sequence !== sequence || transaction.terminal) {
      return
    }
    transaction.terminal = true
    transaction.controller.abort()
    try {
      await transaction.reader?.cancel()
    } catch {
      // 上游流已经结束时不需要再次处理取消错误
    }
    if (active !== transaction) {
      return
    }
    send(TransportFrameKind.CANCELED, sequence)
    active = undefined
  }

  const proxy = async (transaction: RelayTransaction, head: HttpRequestHead): Promise<void> => {
    try {
      const target = new URL(head.url)
      if (target.protocol !== 'http:' && target.protocol !== 'https:') {
        fail(
          transaction,
          TransportErrorCode.INVALID_REQUEST,
          `不支持的 HTTP URL protocol: ${target.protocol}`,
        )
        return
      }
      if (head.method !== 'GET' && head.method !== 'HEAD') {
        fail(transaction, TransportErrorCode.UNSUPPORTED_METHOD, `暂不支持 ${head.method} 请求`)
        return
      }

      const requestedLimit = Number(head.maxResponseBytes)
      if (
        !Number.isSafeInteger(requestedLimit) ||
        requestedLimit < 0 ||
        (head.method !== 'HEAD' && requestedLimit === 0)
      ) {
        fail(transaction, TransportErrorCode.INVALID_REQUEST, 'max_response_bytes 无效')
        return
      }
      const maxResponseBytes = Math.min(requestedLimit, MAX_RESPONSE_BYTES)
      const headers = new Headers()
      for (const header of head.headers) {
        const name = header.name.toLowerCase()
        if (!blockedRequestHeaders.has(name) && !name.startsWith('sec-websocket-')) {
          headers.append(header.name, header.value)
        }
      }
      for (const [name, value] of clientHeaders) {
        if (!headers.has(name)) {
          headers.set(name, value)
        }
      }

      const response = await fetch(
        new Request(target, {
          headers,
          method: head.method,
          redirect: 'follow',
          signal: transaction.controller.signal,
        }),
      )
      if (active !== transaction || transaction.terminal) {
        await response.body?.cancel()
        return
      }

      const contentLength = parseContentLength(response.headers)
      if (
        head.method !== 'HEAD' &&
        contentLength !== undefined &&
        contentLength > maxResponseBytes
      ) {
        await response.body?.cancel()
        fail(
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
      send(
        TransportFrameKind.RESPONSE_HEAD,
        transaction.sequence,
        toBinary(HttpResponseHeadSchema, responseHead),
      )

      if (head.method === 'HEAD' || response.body === null) {
        finish(transaction, TransportFrameKind.RESPONSE_END)
        return
      }

      const reader = response.body.getReader({ mode: 'byob' })
      transaction.reader = reader
      const responseFrameSize = Math.min(RESPONSE_FRAME_SIZE, maxResponseBytes)
      while (true) {
        const readBuffer = new Uint8Array(
          new ArrayBuffer(TRANSPORT_FRAME_HEADER_SIZE + responseFrameSize),
          TRANSPORT_FRAME_HEADER_SIZE,
        )
        const result = await reader.readAtLeast(responseFrameSize, readBuffer)
        if (active !== transaction || transaction.terminal) {
          await reader.cancel()
          return
        }

        const payload = result.value
        transaction.transferredBytes += payload?.byteLength ?? 0
        if (transaction.transferredBytes > maxResponseBytes) {
          transaction.controller.abort()
          await reader.cancel()
          fail(
            transaction,
            TransportErrorCode.RESPONSE_TOO_LARGE,
            `HTTP 响应超过 ${maxResponseBytes} 字节限制`,
          )
          return
        }
        if (payload !== undefined && payload.byteLength !== 0) {
          sendResponseBody(transaction.sequence, payload)
        }
        if (result.done) {
          finish(transaction, TransportFrameKind.RESPONSE_END)
          return
        }
      }
    } catch (cause) {
      if (active !== transaction || transaction.terminal) {
        return
      }
      const message = cause instanceof Error ? cause.message : '未知上游请求错误'
      fail(transaction, TransportErrorCode.UPSTREAM_FAILURE, message)
    }
  }

  const acceptMessage = async (data: string | ArrayBuffer): Promise<void> => {
    if (typeof data === 'string') {
      server.close(1003, 'binary frames required')
      return
    }

    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch {
      server.close(1002, 'invalid transport frame')
      return
    }

    if (frame.kind === TransportFrameKind.PING) {
      send(TransportFrameKind.PONG, frame.sequence)
      return
    }
    if (frame.kind === TransportFrameKind.CANCEL) {
      await cancel(frame.sequence)
      return
    }
    if (frame.kind !== TransportFrameKind.REQUEST_HEAD) {
      sendError(frame.sequence, TransportErrorCode.INVALID_REQUEST, '当前连接不接受该消息')
      return
    }
    if (active !== undefined) {
      sendError(frame.sequence, TransportErrorCode.INVALID_REQUEST, '当前连接已有活动请求')
      return
    }

    let head
    try {
      head = fromBinary(HttpRequestHeadSchema, frame.payload)
    } catch {
      sendError(frame.sequence, TransportErrorCode.INVALID_REQUEST, 'HTTP request head 无效')
      return
    }
    const transaction: RelayTransaction = {
      controller: new AbortController(),
      reader: undefined,
      sequence: frame.sequence,
      startedAt: Date.now(),
      terminal: false,
      transferredBytes: 0,
    }
    active = transaction
    await proxy(transaction, head)
  }

  server.addEventListener('message', event => {
    trackTask('message', acceptMessage(event.data))
  })
  server.addEventListener('close', event => {
    if (event.code !== 1000 || active !== undefined) {
      console.info({
        activeAgeMs: active === undefined ? undefined : Date.now() - active.startedAt,
        activeSequence: active?.sequence,
        activeTransferredBytes: active?.transferredBytes,
        code: event.code,
        readyState: server.readyState,
        reason: event.reason,
        type: 'transport-websocket-close',
        wasClean: event.wasClean,
      })
    }
    trackTask('close-cleanup', stopActive())
  })
  server.addEventListener('error', event => {
    console.error({
      activeSequence: active?.sequence,
      message: event.message,
      type: 'transport-websocket-error',
    })
    trackTask('error-cleanup', stopActive())
    if (server.readyState === WebSocket.OPEN) {
      try {
        server.close(1011, 'transport error')
      } catch (cause) {
        console.error({
          message: cause instanceof Error ? cause.message : String(cause),
          type: 'transport-websocket-close-error',
        })
      }
    }
  })

  return new Response(null, {
    headers: { 'sec-websocket-extensions': '' },
    status: 101,
    webSocket: client,
  })
}

function createClientHeaders(request: Request): Headers {
  const headers = new Headers()
  const origin = parseHttpOrigin(request.headers.get('origin'))
  if (origin !== undefined) {
    headers.set('referer', `${origin}/`)
  }
  const userAgent = request.headers.get('user-agent')
  if (userAgent !== null) {
    headers.set('user-agent', userAgent)
  }
  return headers
}

function parseHttpOrigin(value: string | null): string | undefined {
  if (value === null) {
    return undefined
  }
  try {
    const url = new URL(value)
    return url.protocol === 'http:' || url.protocol === 'https:' ? url.origin : undefined
  } catch {
    return undefined
  }
}

function parseContentLength(headers: Headers): number | undefined {
  const value = headers.get('content-length')
  if (value === null) {
    return undefined
  }
  const length = Number.parseInt(value, 10)
  return Number.isSafeInteger(length) && length >= 0 ? length : undefined
}

export default {
  fetch(request, _env, ctx): Response {
    const url = new URL(request.url)

    if (url.pathname === '/health') {
      return json({ service: 'storya-edge-worker', status: 'ok' })
    }
    if (url.pathname === '/transport') {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return json(
          { error: 'upgrade_required', message: 'Transport endpoint requires WebSocket.' },
          { status: 426 },
        )
      }
      return createTransportResponse(request, ctx)
    }

    return json({ error: 'not_found', message: 'Edge capability was not found.' }, { status: 404 })
  },
} satisfies ExportedHandler<Env>
