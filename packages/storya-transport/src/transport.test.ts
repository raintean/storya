import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
  HttpRequestHeadSchema,
  HttpResponseHeadSchema,
  TransportFrameKind,
} from 'storya-protocol'
import type { HttpRequestHead } from 'storya-protocol'
import { FetchHttpTransport } from './fetch-http-transport'
import {
  formatTransportStatistics,
  TransportStatistics,
  type TransportStatisticsSnapshot,
} from './transport-statistics'
import { WebSocketHttpTransport } from './websocket-http-transport'
import type {
  WebSocketFactory,
  WebSocketHttpTransportDebugEvent,
  WebSocketHttpTransportOptions,
  WebSocketLike,
} from './websocket-http-transport'

interface RelayRequest {
  readonly request: HttpRequestHead
  readonly sequence: number
  readonly socket: FakeWebSocket
}

class FakeRelay {
  readonly canceledSequences: number[] = []
  readonly clients: FakeWebSocket[] = []
  readonly requests: RelayRequest[] = []
  ignoreCancel = false

  readonly factory: WebSocketFactory = () => {
    const socket = new FakeWebSocket(this)
    this.clients.push(socket)
    return socket
  }

  accept(socket: FakeWebSocket, data: ArrayBuffer | ArrayBufferView): void {
    const frame = decodeTransportFrame(data)
    if (frame.kind === TransportFrameKind.CANCEL) {
      this.canceledSequences.push(frame.sequence)
      if (!this.ignoreCancel) {
        const index = this.requests.findIndex(
          entry => entry.socket === socket && entry.sequence === frame.sequence,
        )
        if (index >= 0) {
          this.requests.splice(index, 1)
        }
        socket.receive(encodeTransportFrame(TransportFrameKind.CANCELED, frame.sequence))
      }
      return
    }
    if (frame.kind !== TransportFrameKind.REQUEST_HEAD) {
      throw new Error(`测试 Relay 不接受 frame ${frame.kind}`)
    }

    const request = fromBinary(HttpRequestHeadSchema, frame.payload)
    this.requests.push({ request, sequence: frame.sequence, socket })
    if (!new URL(request.url).pathname.startsWith('/hold')) {
      this.respond(request.url)
    }
  }

  respond(url: string): void {
    this.respondHead(url)
    const entry = this.findRequest(url)
    const path = new URL(entry.request.url).pathname
    if (entry.request.method !== 'HEAD') {
      const body = new TextEncoder().encode(path)
      const middle = Math.floor(body.byteLength / 2)
      if (middle > 0) {
        this.respondBody(url, body.subarray(0, middle))
      }
      this.respondBody(url, body.subarray(middle))
    }
    this.respondEnd(url)
  }

  respondBody(url: string, body: Uint8Array): void {
    const entry = this.findRequest(url)
    entry.socket.receive(
      encodeTransportFrame(TransportFrameKind.RESPONSE_BODY, entry.sequence, body),
    )
  }

  respondEnd(url: string): void {
    const index = this.requests.findIndex(entry => entry.request.url === url)
    const entry = index < 0 ? undefined : this.requests.splice(index, 1)[0]
    if (entry === undefined) {
      throw new Error(`测试 Relay 没有等待中的请求: ${url}`)
    }
    entry.socket.receive(encodeTransportFrame(TransportFrameKind.RESPONSE_END, entry.sequence))
  }

  respondHead(
    url: string,
    contentLength?: number,
    responseHeaders: Record<string, string> = {},
  ): void {
    const entry = this.findRequest(url)
    const path = new URL(entry.request.url).pathname
    const head = create(HttpResponseHeadSchema, {
      headers: [
        { name: 'content-length', value: String(contentLength ?? path.length) },
        ...Object.entries(responseHeaders).map(([name, value]) => ({ name, value })),
      ],
      status: 200,
      statusText: 'OK',
      url: entry.request.url,
    })
    entry.socket.receive(
      encodeTransportFrame(
        TransportFrameKind.RESPONSE_HEAD,
        entry.sequence,
        toBinary(HttpResponseHeadSchema, head),
      ),
    )
  }

  requestedUrls(socket: FakeWebSocket): string[] {
    return this.requests.filter(entry => entry.socket === socket).map(entry => entry.request.url)
  }

  private findRequest(url: string): RelayRequest {
    const entry = this.requests.find(candidate => candidate.request.url === url)
    if (entry === undefined) {
      throw new Error(`测试 Relay 没有等待中的请求: ${url}`)
    }
    return entry
  }
}

class FakeWebSocket implements WebSocketLike {
  binaryType: BinaryType = 'blob'
  readyState = 0

  private readonly listeners = new Map<string, Set<(event: never) => void>>()

  constructor(private readonly relay: FakeRelay) {
    queueMicrotask(() => {
      if (this.readyState !== 0) {
        return
      }
      this.readyState = 1
      this.emit('open', new Event('open'))
    })
  }

  addEventListener(
    type: 'close' | 'error' | 'message' | 'open',
    listener: (event: never) => void,
  ): void {
    const listeners = this.listeners.get(type) ?? new Set()
    listeners.add(listener)
    this.listeners.set(type, listeners)
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === 3) {
      return
    }
    this.readyState = 3
    this.emit('close', { code, reason, wasClean: code === 1000 } as CloseEvent)
  }

  removeEventListener(
    type: 'close' | 'error' | 'message' | 'open',
    listener: (event: never) => void,
  ): void {
    this.listeners.get(type)?.delete(listener)
  }

  send(data: string | Blob | ArrayBuffer | ArrayBufferView<ArrayBuffer>): void {
    if (typeof data === 'string' || data instanceof Blob) {
      throw new Error('测试 WebSocket 只支持二进制 message')
    }
    this.relay.accept(this, data)
  }

  receive(data: Uint8Array<ArrayBuffer>): void {
    this.emit('message', { data: data.buffer } as MessageEvent<ArrayBuffer>)
  }

  private emit(type: string, event: Event): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event as never)
    }
  }
}

function testTransportFrameBodyView(): void {
  const payload = new Uint8Array([1, 2, 3])
  const message = encodeTransportFrame(TransportFrameKind.RESPONSE_BODY, 7, payload)
  const frame = decodeTransportFrame(message)
  assert(frame.payload.buffer === message.buffer, 'Transport frame 解码不应复制 body')
  assert(frame.payload.byteLength === 3, 'Transport frame body 长度错误')
  assert(frame.payload[2] === 3, 'Transport frame body 内容错误')
  assert(frame.sequence === 7, 'Transport frame sequence 错误')
}

async function testTransportStatistics(): Promise<void> {
  const logs: TransportStatisticsSnapshot[] = []
  const logMessages: string[] = []
  const statistics = new TransportStatistics('TestTransport', {
    intervalMs: 5,
    logger: (message, snapshot) => {
      logMessages.push(message)
      logs.push(snapshot)
    },
  })

  const hit = statistics
    .startRequest()
    .trackResponse(new Response('fetch', { headers: { 'cf-cache-status': 'HIT' } }))
  await hit.arrayBuffer()
  const miss = statistics
    .startRequest()
    .trackResponse(new Response('data', { headers: { 'cf-cache-status': 'MISS' } }))
  await miss.arrayBuffer()

  const pendingBody = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array([1, 2, 3]))
    },
  })
  const bypass = statistics
    .startRequest()
    .trackResponse(new Response(pendingBody, { headers: { 'cf-cache-status': 'BYPASS' } }))
  const bypassReader = bypass.body?.getReader()
  await bypassReader?.read()
  await bypassReader?.cancel()

  statistics.startRequest().reject(new Error('请求失败'))
  statistics.startRequest().trackResponse(new Response(null), false)

  const snapshot = statistics.snapshot()
  assert(snapshot.requestCount === 5, 'Transport 统计请求数量错误')
  assert(snapshot.successCount === 3, 'Transport 统计成功数量错误')
  assert(snapshot.failureCount === 1, 'Transport 统计失败数量错误')
  assert(snapshot.canceledCount === 1, 'Transport 统计取消数量错误')
  assert(snapshot.activeRequestCount === 0, 'Transport 统计活动请求数量错误')
  assert(snapshot.responseBytes === 12, 'Transport 统计实际响应字节错误')
  assert(snapshot.cacheHitCount === 1, 'Transport 统计缓存命中数量错误')
  assert(snapshot.cacheLabel === '缓存', 'Transport 统计默认缓存标签错误')
  assert(snapshot.cacheMissCount === 1, 'Transport 统计缓存未命中数量错误')
  assert(snapshot.cacheBypassCount === 1, 'Transport 统计缓存绕过数量错误')
  assert(snapshot.cacheUnknownCount === 1, 'Transport 统计未知缓存数量错误')
  const formatted = formatTransportStatistics(snapshot)
  assert(formatted.includes('请求 5'), 'Transport 统计摘要缺少请求数量')
  assert(formatted.includes('数据 12 B'), 'Transport 统计摘要缺少数据量')
  assert(formatted.includes('命中率 50.0%'), 'Transport 统计摘要缺少缓存命中率')

  await waitForWithin(() => logs.length > 0, 100)
  assert(logMessages[0] === formatted, 'Transport 定时日志没有使用可读摘要')
  const logCount = logs.length
  statistics.destroy()
  await delay(10)
  assert(logs.length === logCount, 'Transport 统计销毁后仍在输出日志')
}

async function testFetchTransport(): Promise<void> {
  let requestedUrl = ''
  const transport = new FetchHttpTransport(async request => {
    requestedUrl = request.url
    return new Response('fetch')
  })
  const response = await transport.request(new Request('https://example.com/fetch'))
  assert(requestedUrl === 'https://example.com/fetch', 'Fetch transport 没有转发请求')
  assert(decode(await response.arrayBuffer()) === 'fetch', 'Fetch 响应错误')
  const statistics = transport.getStatistics()
  assert(statistics.successCount === 1, 'Fetch transport 没有记录成功请求')
  assert(statistics.responseBytes === 5, 'Fetch transport 没有记录响应字节')
  transport.destroy()
}

async function testWebSocketTransportStatistics(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const url = 'https://example.com/hold/statistics'
  const responsePromise = transport.request(new Request(url))
  await waitFor(() => relay.requests.length === 1)
  relay.respondHead(url, 4, { 'cf-cache-status': 'HIT' })
  const response = await responsePromise
  relay.respondBody(url, new Uint8Array([1, 2]))
  relay.respondBody(url, new Uint8Array([3, 4]))
  relay.respondEnd(url)
  assert((await response.arrayBuffer()).byteLength === 4, 'WebSocket 统计测试响应错误')

  const statistics = transport.getStatistics()
  assert(statistics.requestCount === 1, 'WebSocket transport 没有记录请求数量')
  assert(statistics.successCount === 1, 'WebSocket transport 没有记录成功请求')
  assert(statistics.activeRequestCount === 0, 'WebSocket transport 成功后仍有活动统计')
  assert(statistics.responseBytes === 4, 'WebSocket transport 没有记录消费的响应字节')
  assert(statistics.cacheHitCount === 1, 'WebSocket transport 没有记录上游缓存命中')
  assert(statistics.cacheLabel === '上游缓存', 'WebSocket transport 缓存标签不明确')
  assert(
    formatTransportStatistics(statistics).includes('上游缓存 HIT 1'),
    'WebSocket transport 统计摘要没有标记上游缓存',
  )
  transport.destroy()
}

async function testStreamingResponse(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const responsePromise = transport.request(new Request('https://example.com/hold/stream'))
  await waitFor(() => relay.requests.length === 1)
  relay.respondHead('https://example.com/hold/stream', 4)
  const response = await responsePromise
  const reader = response.body?.getReader()
  assert(reader !== undefined, 'GET response 没有流式 body')

  relay.respondBody('https://example.com/hold/stream', new Uint8Array([1, 2]))
  const first = await reader.read()
  assert(!first.done && first.value.byteLength === 2, '首个流式 body frame 没有立即到达')
  relay.respondBody('https://example.com/hold/stream', new Uint8Array([3, 4]))
  relay.respondEnd('https://example.com/hold/stream')
  const second = await reader.read()
  const end = await reader.read()
  assert(!second.done && second.value[1] === 4, '第二个流式 body frame 错误')
  assert(end.done, 'RESPONSE_END 没有关闭 response body')
  transport.destroy()
}

async function testSequentialReuse(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const first = await transport.request(new Request('https://example.com/first'))
  assert(decode(await first.arrayBuffer()) === '/first', '第一个响应数据错误')
  const second = await transport.request(new Request('https://example.com/second'))
  assert(decode(await second.arrayBuffer()) === '/second', '第二个响应数据错误')
  assert(relay.clients.length === 1, '串行请求没有复用同一条 WebSocket')
  transport.destroy()
}

async function testConcurrentGrowth(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const first = transport.request(new Request('https://example.com/hold/one'))
  const second = transport.request(new Request('https://example.com/hold/two'))
  await waitFor(() => relay.requests.length === 2)
  assert(relay.clients.length === 2, '并发请求没有扩容 WebSocket 连接池')
  relay.respond('https://example.com/hold/one')
  relay.respond('https://example.com/hold/two')
  await Promise.all([(await first).arrayBuffer(), (await second).arrayBuffer()])
  transport.destroy()
}

async function testHeadContentLength(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const response = await transport.request(
    new Request('https://example.com/head', { method: 'HEAD' }),
    { maxResponseBytes: 0 },
  )
  assert(response.status === 200, 'HEAD 响应没有成功返回')
  assert(response.headers.get('content-length') === '5', 'HEAD 响应丢失 Content-Length')
  assert((await response.arrayBuffer()).byteLength === 0, 'HEAD 响应不应包含 body')
  const statistics = transport.getStatistics()
  assert(statistics.successCount === 1, 'WebSocket HEAD 请求没有记录成功统计')
  assert(statistics.cacheUnknownCount === 1, '无缓存头的 WebSocket 请求没有归入 UNKNOWN')
  transport.destroy()
}

async function testAbortSendsCancelAndReusesConnection(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const controller = new AbortController()
  const response = transport.request(
    new Request('https://example.com/hold/cancel', { signal: controller.signal }),
  )
  await waitFor(() => relay.requests.length === 1)
  controller.abort()
  await assertRejectsAbort(response, '请求 Abort 没有结束 WebSocket 事务')
  assert(relay.canceledSequences.length === 1, '请求 Abort 没有发送 CANCEL')
  assert(transport.getStatistics().canceledCount === 1, '请求 Abort 没有计入取消统计')

  const next = await transport.request(new Request('https://example.com/next'))
  assert(decode(await next.arrayBuffer()) === '/next', 'CANCELED 后连接没有恢复复用')
  assert(relay.clients.length === 1, 'CANCELED 后不应创建新连接')
  transport.destroy()
}

async function testBodyCancelSendsCancel(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const responsePromise = transport.request(new Request('https://example.com/hold/body-cancel'))
  await waitFor(() => relay.requests.length === 1)
  relay.respondHead('https://example.com/hold/body-cancel', 10)
  const response = await responsePromise
  await response.body?.cancel()
  assert(relay.canceledSequences.length === 1, 'response body cancel 没有发送 CANCEL')
  assert(transport.getStatistics().canceledCount === 1, 'body cancel 没有计入取消统计')

  await (await transport.request(new Request('https://example.com/reused'))).arrayBuffer()
  assert(relay.clients.length === 1, 'body cancel 确认后连接没有恢复复用')
  transport.destroy()
}

async function testStreamingResponseLimit(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const responsePromise = transport.request(new Request('https://example.com/hold/too-large'), {
    maxResponseBytes: 4,
  })
  await waitFor(() => relay.requests.length === 1)
  relay.respondHead('https://example.com/hold/too-large', 4)
  const response = await responsePromise
  const reader = response.body?.getReader()
  assert(reader !== undefined, '超限测试没有 response body')
  relay.respondBody('https://example.com/hold/too-large', new Uint8Array([1, 2, 3, 4, 5]))
  await assertRejectsCode(reader.read(), 'response-too-large', '流式累计上限没有生效')
  assert(relay.canceledSequences.length === 1, '流式响应超限没有发送 CANCEL')
  transport.destroy()
}

async function testCancelTimeoutClosesConnection(): Promise<void> {
  const relay = new FakeRelay()
  relay.ignoreCancel = true
  const transport = createTransport(relay, { cancelTimeoutMs: 5 })
  const controller = new AbortController()
  const response = transport.request(
    new Request('https://example.com/hold/cancel-timeout', { signal: controller.signal }),
  )
  await waitFor(() => relay.requests.length === 1)
  controller.abort()
  await assertRejectsAbort(response, 'CANCEL 超时测试没有先取消请求')
  await waitForWithin(() => relay.clients[0]?.readyState === 3, 100)
  transport.destroy()
}

async function testMaximumReuse(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay, { maxRequestsPerConnection: 1 })
  await (await transport.request(new Request('https://example.com/first'))).arrayBuffer()
  await (await transport.request(new Request('https://example.com/second'))).arrayBuffer()
  assert(relay.clients.length === 2, '达到最大复用次数后没有创建新连接')
  transport.destroy()
}

async function testYoungestConnectionFirst(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const first = transport.request(new Request('https://example.com/hold/old'))
  const second = transport.request(new Request('https://example.com/hold/young'))
  await waitFor(() => relay.requests.length === 2)
  relay.respond('https://example.com/hold/old')
  relay.respond('https://example.com/hold/young')
  await Promise.all([first, second])

  const next = transport.request(new Request('https://example.com/hold/next'))
  await waitFor(() => relay.requests.some(entry => entry.request.url.endsWith('/hold/next')))
  const nextEntry = relay.requests.find(entry => entry.request.url.endsWith('/hold/next'))
  assert(nextEntry?.socket === relay.clients[1], '新请求没有优先使用年龄最小的连接')
  relay.respond('https://example.com/hold/next')
  await next
  transport.destroy()
}

async function testIdleRetentionFloor(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay, {
    idleConnectionTimeoutMs: 5,
    minIdleConnections: 2,
  })
  const requests = [
    transport.request(new Request('https://example.com/hold/1')),
    transport.request(new Request('https://example.com/hold/2')),
    transport.request(new Request('https://example.com/hold/3')),
  ]
  await waitFor(() => relay.requests.length === 3)
  for (let index = 1; index <= 3; index += 1) {
    relay.respond(`https://example.com/hold/${index}`)
  }
  await Promise.all(requests)
  await delay(50)
  const openConnections = relay.clients.filter(socket => socket.readyState === 1).length
  assert(openConnections === 2, `空闲连接没有回收到 minIdleConnections, 当前 ${openConnections}`)
  transport.destroy()
}

async function testMinimumIdleDoesNotPreconnect(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay, { minIdleConnections: 6 })
  await delay(5)
  assert(countClients(relay) === 0, 'minIdleConnections 不应主动创建连接')
  await (await transport.request(new Request('https://example.com/one'))).arrayBuffer()
  assert(countClients(relay) === 1, '单个请求不应创建最低空闲数量的连接')
  transport.destroy()
}

async function testConnectionDiagnostics(): Promise<void> {
  const relay = new FakeRelay()
  const events: WebSocketHttpTransportDebugEvent[] = []
  const transport = createTransport(relay, {
    debug: event => events.push(event),
    maxRequestsPerConnection: 1,
  })
  await (await transport.request(new Request('https://example.com/diagnostics'))).arrayBuffer()

  assert(
    events.some(event => event.type === 'connection-created'),
    '没有记录连接创建事件',
  )
  assert(
    events.some(event => event.type === 'connection-opened'),
    '没有记录连接建立事件',
  )
  const closed = events.find(event => event.type === 'connection-closed')
  assert(closed?.reason === 'max-requests', '最大复用次数关闭原因错误')
  assert(closed.requestCount === 1, '连接关闭时请求次数错误')
  transport.destroy()
}

async function testDefaultConnectionLog(): Promise<void> {
  const relay = new FakeRelay()
  const logs: unknown[][] = []
  const originalConsoleInfo = console.info
  console.info = (...data: unknown[]) => logs.push(data)
  const transport = createTransport(relay, {
    debug: true,
    maxRequestsPerConnection: 1,
  })
  try {
    await (await transport.request(new Request('https://example.com/log'))).arrayBuffer()
  } finally {
    transport.destroy()
    console.info = originalConsoleInfo
  }

  assert(logs.length === 1, `默认 debug 应只输出连接关闭日志, 实际 ${logs.length} 条`)
  assert(logs[0]?.length === 1, '连接关闭日志应输出为单个字符串')
  const message = logs[0][0]
  assert(typeof message === 'string', '连接关闭日志不是字符串')
  assert(
    message.startsWith('[storya-transport][WebSocketHttpTransport] 连接 #1 关闭'),
    '连接关闭日志前缀错误',
  )
  assert(message.includes('原因 max-requests'), '连接关闭日志缺少原因')
  assert(message.includes('关闭方 本地'), '连接关闭日志缺少关闭方')
  assert(message.includes('Code 1000'), '连接关闭日志缺少 Code')
  assert(message.includes('请求 1'), '连接关闭日志缺少复用次数')
  assert(message.includes('池内 0'), '连接关闭日志缺少池大小')
  assert(message.includes('排队 0'), '连接关闭日志缺少排队数')
}

async function testConnectionFactoryFailure(): Promise<void> {
  const transport = new WebSocketHttpTransport('wss://relay.example.com/transport', {
    ...createOptions(),
    webSocketFactory: () => {
      throw new Error('连接创建失败')
    },
  })
  let failed = false
  try {
    await transport.request(new Request('https://example.com/failure'))
  } catch (cause) {
    failed = cause instanceof Error && cause.message === 'WebSocket 连接创建失败'
  }
  assert(failed, 'WebSocket 工厂同步失败时请求没有结束')
  const statistics = transport.getStatistics()
  assert(statistics.failureCount === 1, 'WebSocket 连接失败没有计入失败统计')
  assert(statistics.activeRequestCount === 0, 'WebSocket 连接失败后仍有活动统计')
  transport.destroy()
}

async function testInvalidResponseLimit(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  let failed = false
  try {
    await transport.request(new Request('https://example.com/invalid'), {
      maxResponseBytes: Number.NaN,
    })
  } catch (cause) {
    failed = cause instanceof Error && cause.message === 'maxResponseBytes 无效'
  }
  assert(failed, '无效响应上限没有被拒绝')
  assert(relay.clients.length === 0, '无效响应上限不应创建 WebSocket')
  transport.destroy()
}

function createOptions(
  overrides: Partial<WebSocketHttpTransportOptions> = {},
): WebSocketHttpTransportOptions {
  return {
    cancelTimeoutMs: 1_000,
    connectTimeoutMs: 1_000,
    defaultMaxResponseBytes: HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
    idleConnectionTimeoutMs: 60_000,
    maxConnections: 12,
    maxRequestsPerConnection: 50,
    minIdleConnections: 0,
    ...overrides,
  }
}

function createTransport(
  relay: FakeRelay,
  overrides: Partial<WebSocketHttpTransportOptions> = {},
): WebSocketHttpTransport {
  return new WebSocketHttpTransport('wss://relay.example.com/transport', {
    ...createOptions(overrides),
    webSocketFactory: relay.factory,
  })
}

async function assertRejectsAbort(value: Promise<unknown>, message: string): Promise<void> {
  try {
    await value
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === 'AbortError') {
      return
    }
  }
  throw new Error(message)
}

async function assertRejectsCode(
  value: Promise<unknown>,
  code: string,
  message: string,
): Promise<void> {
  try {
    await value
  } catch (cause) {
    if (cause instanceof Error && 'code' in cause && cause.code === code) {
      return
    }
  }
  throw new Error(message)
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) {
      return
    }
    await delay(0)
  }
  throw new Error('等待测试条件超时')
}

async function waitForWithin(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = performance.now() + timeoutMs
  while (performance.now() < deadline) {
    if (predicate()) {
      return
    }
    await delay(1)
  }
  throw new Error('等待定时测试条件超时')
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => globalThis.setTimeout(resolve, ms))
}

function decode(buffer: ArrayBuffer): string {
  return new TextDecoder().decode(buffer)
}

function countClients(relay: FakeRelay): number {
  return relay.clients.length
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

testTransportFrameBodyView()
await testTransportStatistics()
await testFetchTransport()
await testWebSocketTransportStatistics()
await testStreamingResponse()
await testSequentialReuse()
await testConcurrentGrowth()
await testHeadContentLength()
await testAbortSendsCancelAndReusesConnection()
await testBodyCancelSendsCancel()
await testStreamingResponseLimit()
await testCancelTimeoutClosesConnection()
await testMaximumReuse()
await testYoungestConnectionFirst()
await testIdleRetentionFloor()
await testMinimumIdleDoesNotPreconnect()
await testConnectionDiagnostics()
await testDefaultConnectionLog()
await testConnectionFactoryFailure()
await testInvalidResponseLimit()
