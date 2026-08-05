import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HttpResponseHeadSchema,
  HttpRequestHeadSchema,
  TransportErrorCode,
  TransportErrorSchema,
  TransportFrameKind,
} from 'storya-protocol'
import type { HttpRequestHead } from 'storya-protocol'

interface RelayTransaction {
  readonly controller: AbortController
  reader: ReadableStreamDefaultReader<Uint8Array> | undefined
  readonly sequence: number
  terminal: boolean
}

const MAX_RESPONSE_BYTES = 64 * 1024 * 1024
const RESPONSE_FRAME_SIZE = 64 * 1024
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

function createTransportResponse(request: Request): Response {
  const pair = new WebSocketPair()
  const client = pair[0]
  const server = pair[1]
  const clientHeaders = createClientHeaders(request)
  server.accept()
  server.binaryType = 'arraybuffer'

  let active: RelayTransaction | undefined

  const send = (kind: TransportFrameKind, sequence: number, payload?: Uint8Array): void => {
    server.send(encodeTransportFrame(kind, sequence, payload))
  }

  const sendError = (sequence: number, code: TransportErrorCode, message: string): void => {
    const error = create(TransportErrorSchema, { code, message })
    send(TransportFrameKind.ERROR, sequence, toBinary(TransportErrorSchema, error))
  }

  const finish = (transaction: RelayTransaction, kind: TransportFrameKind): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    transaction.terminal = true
    send(kind, transaction.sequence)
    active = undefined
  }

  const fail = (transaction: RelayTransaction, code: TransportErrorCode, message: string): void => {
    if (active !== transaction || transaction.terminal) {
      return
    }
    transaction.terminal = true
    sendError(transaction.sequence, code, message)
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
    send(TransportFrameKind.CANCELED, sequence)
    if (active === transaction) {
      active = undefined
    }
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

      const reader = response.body.getReader()
      transaction.reader = reader
      let transferred = 0
      while (true) {
        const result = await reader.read()
        if (active !== transaction || transaction.terminal) {
          await reader.cancel()
          return
        }
        if (result.done) {
          finish(transaction, TransportFrameKind.RESPONSE_END)
          return
        }

        transferred += result.value.byteLength
        if (transferred > maxResponseBytes) {
          transaction.controller.abort()
          await reader.cancel()
          fail(
            transaction,
            TransportErrorCode.RESPONSE_TOO_LARGE,
            `HTTP 响应超过 ${maxResponseBytes} 字节限制`,
          )
          return
        }
        for (let offset = 0; offset < result.value.byteLength; offset += RESPONSE_FRAME_SIZE) {
          if (active !== transaction || transaction.terminal) {
            await reader.cancel()
            return
          }
          send(
            TransportFrameKind.RESPONSE_BODY,
            transaction.sequence,
            result.value.subarray(offset, offset + RESPONSE_FRAME_SIZE),
          )
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
      terminal: false,
    }
    active = transaction
    void proxy(transaction, head)
  }

  server.addEventListener('message', event => {
    void acceptMessage(event.data)
  })
  server.addEventListener('close', () => {
    active?.controller.abort()
    void active?.reader?.cancel()
    active = undefined
  })

  return new Response(null, { status: 101, webSocket: client })
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
  fetch(request): Response {
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
      return createTransportResponse(request)
    }

    return json({ error: 'not_found', message: 'Edge capability was not found.' }, { status: 404 })
  },
} satisfies ExportedHandler<Env>
