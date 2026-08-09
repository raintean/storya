import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
  HttpRequestHeadSchema,
  HttpResponseHeadSchema,
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
  terminal: boolean
  transferredBytes: number
}

const responseBodyFrameBytes = 128 * 1024
const upstreamCacheTtlSeconds = 365 * 24 * 60 * 60
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
  let active: RelayTransaction | undefined

  server.binaryType = 'arraybuffer'
  server.accept()

  const closeServer = (code?: number, reason?: string): void => {
    try {
      if (code === undefined) {
        server.close()
      } else {
        server.close(code, reason)
      }
    } catch {
      // 连接丢失或已经关闭时只需收敛本地事务
    }
  }

  const send = (kind: TransportFrameKind, sequence: number, payload?: Uint8Array): void => {
    server.send(encodeTransportFrame(kind, sequence, payload))
  }

  const sendResponseBody = (sequence: number, payload: Uint8Array<ArrayBuffer>): void => {
    if (payload.byteOffset < TRANSPORT_FRAME_HEADER_SIZE) {
      send(TransportFrameKind.RESPONSE_BODY, sequence, payload)
      return
    }
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

  const stopTransaction = (transaction: RelayTransaction): boolean => {
    if (active !== transaction || transaction.terminal) {
      return false
    }
    transaction.terminal = true
    active = undefined
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

  const stopActive = (): void => {
    const transaction = active
    if (transaction !== undefined) {
      stopTransaction(transaction)
    }
  }

  const finish = (transaction: RelayTransaction): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    send(TransportFrameKind.RESPONSE_END, transaction.sequence)
    transaction.terminal = true
    transaction.reader = undefined
    active = undefined
  }

  const fail = (transaction: RelayTransaction, code: TransportErrorCode, message: string): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    sendError(transaction.sequence, code, message)
    stopTransaction(transaction)
  }

  const cancel = (sequence: number): void => {
    const transaction = active
    if (transaction === undefined || transaction.sequence !== sequence || transaction.terminal) {
      return
    }
    if (stopTransaction(transaction)) {
      send(TransportFrameKind.CANCELED, sequence)
    }
  }

  const proxy = async (transaction: RelayTransaction, head: HttpRequestHead): Promise<void> => {
    try {
      const { maxResponseBytes, target } = validateRequestHead(head)
      const headers = createUpstreamHeaders(head, clientHeaders)
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
          headers,
          method: head.method,
          redirect: 'follow',
          signal: transaction.controller.signal,
        }),
      )
      if (active !== transaction || transaction.terminal) {
        void response.body?.cancel()
        return
      }

      const contentLength = parseContentLength(response.headers)
      if (
        head.method !== 'HEAD' &&
        contentLength !== undefined &&
        contentLength > maxResponseBytes
      ) {
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
        finish(transaction)
        return
      }

      const reader = response.body.getReader({ mode: 'byob' })
      transaction.reader = reader
      while (active === transaction && !transaction.terminal) {
        const remainingWithOverflowByte = maxResponseBytes - transaction.transferredBytes + 1
        const readSize = Math.min(responseBodyFrameBytes, remainingWithOverflowByte)
        const readBuffer = new Uint8Array(
          new ArrayBuffer(TRANSPORT_FRAME_HEADER_SIZE + readSize),
          TRANSPORT_FRAME_HEADER_SIZE,
        )
        const result = await reader.readAtLeast(readSize, readBuffer)
        if (active !== transaction || transaction.terminal) {
          return
        }

        const payload = result.value
        if (payload !== undefined && payload.byteLength !== 0) {
          transaction.transferredBytes += payload.byteLength
          if (transaction.transferredBytes > maxResponseBytes) {
            fail(
              transaction,
              TransportErrorCode.RESPONSE_TOO_LARGE,
              `HTTP 响应超过 ${maxResponseBytes} 字节限制`,
            )
            return
          }
          sendResponseBody(transaction.sequence, payload)
        }
        if (result.done) {
          finish(transaction)
          return
        }
      }
    } catch (cause) {
      if (active !== transaction || transaction.terminal) {
        return
      }
      fail(
        transaction,
        cause instanceof InvalidRelayRequest
          ? TransportErrorCode.INVALID_REQUEST
          : TransportErrorCode.UPSTREAM_FAILURE,
        cause instanceof Error ? cause.message : '未知上游请求错误',
      )
    }
  }

  const acceptMessage = (data: string | ArrayBuffer): void => {
    if (typeof data === 'string') {
      stopActive()
      closeServer(1003, 'binary frames required')
      return
    }

    let frame
    try {
      frame = decodeTransportFrame(data)
    } catch {
      stopActive()
      closeServer(1002, 'invalid transport frame')
      return
    }

    if (frame.kind === TransportFrameKind.CANCEL) {
      cancel(frame.sequence)
      return
    }
    if (frame.kind !== TransportFrameKind.REQUEST_HEAD) {
      stopActive()
      closeServer(1002, 'unexpected transport frame')
      return
    }
    if (active !== undefined) {
      stopActive()
      closeServer(1008, 'request already active')
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
      terminal: false,
      transferredBytes: 0,
    }
    active = transaction
    track(proxy(transaction, head))
  }

  const track = (task: Promise<void>): void => {
    ctx.waitUntil(
      task.catch(cause => {
        console.error({
          message: cause instanceof Error ? cause.message : String(cause),
          type: 'http-relay-failure',
        })
        stopActive()
        if (server.readyState === WebSocket.OPEN) {
          closeServer(1011, 'relay failure')
        }
      }),
    )
  }

  server.addEventListener('message', event => {
    acceptMessage(event.data)
  })
  server.addEventListener('close', () => {
    stopActive()
  })
  server.addEventListener('error', () => {
    stopActive()
    closeServer(1011, 'websocket error')
  })

  return new Response(null, {
    headers: { 'sec-websocket-extensions': '' },
    status: 101,
    webSocket: client,
  })
}

function validateRequestHead(head: HttpRequestHead): {
  maxResponseBytes: number
  target: URL
} {
  let target: URL
  try {
    target = new URL(head.url)
  } catch {
    throw new InvalidRelayRequest('HTTP relay URL 无效')
  }
  if (target.protocol !== 'http:' && target.protocol !== 'https:') {
    throw new InvalidRelayRequest(`不支持的 HTTP URL protocol: ${target.protocol}`)
  }
  if (head.method !== 'GET' && head.method !== 'HEAD') {
    throw new InvalidRelayRequest(`不支持的 HTTP method: ${head.method}`)
  }
  const maxResponseBytes = Number(head.maxResponseBytes)
  if (
    !Number.isSafeInteger(maxResponseBytes) ||
    maxResponseBytes < 0 ||
    maxResponseBytes > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES ||
    (head.method === 'GET' && maxResponseBytes === 0)
  ) {
    throw new InvalidRelayRequest('max_response_bytes 无效')
  }
  return { maxResponseBytes, target }
}

function createUpstreamHeaders(head: HttpRequestHead, clientHeaders: Headers): Headers {
  const upstream = new Headers()
  for (const header of head.headers) {
    const name = header.name.toLowerCase()
    if (!blockedRequestHeaders.has(name) && !name.startsWith('sec-websocket-')) {
      upstream.append(name, header.value)
    }
  }
  for (const [name, value] of clientHeaders) {
    if (!upstream.has(name)) {
      upstream.set(name, value)
    }
  }
  return upstream
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
  if (value === null || !/^\d+$/u.test(value)) {
    return undefined
  }
  const length = Number(value)
  return Number.isSafeInteger(length) ? length : undefined
}

class InvalidRelayRequest extends Error {}

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
