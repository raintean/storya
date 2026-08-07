import {
  createHttpRelayResponseBuffer,
  decodeHttpRelayRequest,
  encodeHttpRelayError,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
  HTTP_RELAY_RESPONSE_HEADER_NAMES,
  HttpRelayResponseOutcome,
} from 'storya-protocol'
import type { HttpRelayHeader, HttpRelayRequest } from 'storya-protocol'

interface RelayTransaction {
  readonly controller: AbortController
  reader: ReadableStreamBYOBReader | undefined
}

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

  const sendError = (outcome: HttpRelayResponseOutcome, message: string): void => {
    server.send(encodeHttpRelayError(outcome, message))
  }

  const stop = (): void => {
    const transaction = active
    active = undefined
    if (transaction === undefined) {
      return
    }
    const reader = transaction.reader
    transaction.reader = undefined
    if (reader !== undefined) {
      void reader.cancel().catch(() => {
        // AbortController 会继续终止上游请求
      })
    }
    transaction.controller.abort()
  }

  const sendUpstreamResponse = async (
    relayRequest: HttpRelayRequest,
    transaction: RelayTransaction,
    response: Response,
  ): Promise<void> => {
    const responseHead = {
      headerValues: collectResponseHeaderValues(response.headers),
      status: response.status,
      url: response.url === '' || response.url === relayRequest.url ? '' : response.url,
    }
    if (relayRequest.method === 'HEAD' || response.body === null) {
      server.send(createHttpRelayResponseBuffer(responseHead, 0).finishEmpty())
      return
    }

    const declaredLength = parseContentLength(response.headers)
    if (declaredLength !== undefined && declaredLength > relayRequest.maxResponseBytes) {
      transaction.controller.abort()
      sendError(
        HttpRelayResponseOutcome.RESPONSE_TOO_LARGE,
        `HTTP 响应超过 ${relayRequest.maxResponseBytes} 字节限制`,
      )
      return
    }

    const capacity = declaredLength ?? relayRequest.maxResponseBytes + 1
    const output = createHttpRelayResponseBuffer(responseHead, capacity)
    if (capacity === 0) {
      transaction.controller.abort()
      server.send(output.finishEmpty())
      return
    }

    const reader = response.body.getReader({ mode: 'byob' })
    transaction.reader = reader
    const result = await reader.readAtLeast(capacity, output.body).finally(() => {
      if (transaction.reader === reader) {
        transaction.reader = undefined
      }
    })
    const body = result.value
    if (active !== transaction) {
      return
    }
    if (body === undefined) {
      server.send(createHttpRelayResponseBuffer(responseHead, 0).finishEmpty())
      return
    }
    if (body.byteLength === 0) {
      server.send(output.finish(body))
      return
    }
    if (body.byteLength > relayRequest.maxResponseBytes) {
      transaction.controller.abort()
      sendError(
        HttpRelayResponseOutcome.RESPONSE_TOO_LARGE,
        `HTTP 响应超过 ${relayRequest.maxResponseBytes} 字节限制`,
      )
      return
    }
    if (declaredLength !== undefined && body.byteLength !== declaredLength) {
      transaction.controller.abort()
      sendError(HttpRelayResponseOutcome.UPSTREAM_FAILURE, '上游响应长度与 Content-Length 不一致')
      return
    }
    server.send(output.finish(body))
  }

  const proxy = async (
    relayRequest: HttpRelayRequest,
    transaction: RelayTransaction,
  ): Promise<void> => {
    try {
      const target = validateRequest(relayRequest)
      const headers = createUpstreamHeaders(relayRequest.headers, clientHeaders)
      const response = await fetch(
        new Request(target, {
          headers,
          method: relayRequest.method,
          redirect: 'follow',
          signal: transaction.controller.signal,
        }),
      )
      if (active !== transaction) {
        await response.body?.cancel()
        return
      }
      await sendUpstreamResponse(relayRequest, transaction, response)
    } catch (cause) {
      if (active !== transaction) {
        return
      }
      sendError(
        cause instanceof InvalidRelayRequest
          ? HttpRelayResponseOutcome.INVALID_REQUEST
          : HttpRelayResponseOutcome.UPSTREAM_FAILURE,
        cause instanceof Error ? cause.message : '未知上游请求错误',
      )
    } finally {
      if (active === transaction) {
        active = undefined
      }
    }
  }

  const acceptMessage = async (data: string | ArrayBuffer): Promise<void> => {
    if (typeof data === 'string') {
      closeServer(1003, 'binary message required')
      return
    }
    if (active !== undefined) {
      closeServer(1008, 'request already active')
      return
    }

    let relayRequest: HttpRelayRequest
    try {
      relayRequest = decodeHttpRelayRequest(data)
    } catch (cause) {
      sendError(
        HttpRelayResponseOutcome.INVALID_REQUEST,
        cause instanceof Error ? cause.message : 'HTTP relay request 无效',
      )
      return
    }

    const transaction: RelayTransaction = {
      controller: new AbortController(),
      reader: undefined,
    }
    active = transaction
    await proxy(relayRequest, transaction)
  }

  const track = (task: Promise<void>): void => {
    ctx.waitUntil(
      task.catch(cause => {
        console.error({
          message: cause instanceof Error ? cause.message : String(cause),
          type: 'http-relay-failure',
        })
        stop()
        if (server.readyState === WebSocket.OPEN) {
          closeServer(1011, 'relay failure')
        }
      }),
    )
  }

  server.addEventListener('message', event => {
    track(acceptMessage(event.data))
  })
  server.addEventListener('close', () => {
    stop()
    closeServer()
  })
  server.addEventListener('error', () => {
    stop()
    closeServer(1011, 'websocket error')
  })

  return new Response(null, {
    headers: { 'sec-websocket-extensions': '' },
    status: 101,
    webSocket: client,
  })
}

function validateRequest(request: HttpRelayRequest): URL {
  let target: URL
  try {
    target = new URL(request.url)
  } catch {
    throw new InvalidRelayRequest('HTTP relay URL 无效')
  }
  if (target.protocol !== 'http:' && target.protocol !== 'https:') {
    throw new InvalidRelayRequest(`不支持的 HTTP URL protocol: ${target.protocol}`)
  }
  if (
    request.maxResponseBytes > HTTP_RELAY_MAX_RESPONSE_BODY_BYTES ||
    (request.method === 'GET' && request.maxResponseBytes === 0)
  ) {
    throw new InvalidRelayRequest('maxResponseBytes 无效')
  }
  return target
}

function createUpstreamHeaders(
  headers: readonly HttpRelayHeader[],
  clientHeaders: Headers,
): Headers {
  const upstream = new Headers()
  for (const header of headers) {
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

function collectResponseHeaderValues(headers: Headers): (string | null)[] {
  const values: (string | null)[] = []
  for (const name of HTTP_RELAY_RESPONSE_HEADER_NAMES) {
    values.push(headers.get(name))
  }
  return values
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
