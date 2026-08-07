import {
  createHttpRelayResponseBuffer,
  decodeHttpRelayRequest,
  decodeHttpRelayResponse,
  encodeHttpRelayError,
  encodeHttpRelayResponse,
  HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
  HTTP_RELAY_RESPONSE_HEADER_NAMES,
  HttpRelayResponseOutcome,
} from 'storya-protocol'
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
  readonly request: ReturnType<typeof decodeHttpRelayRequest>
  readonly socket: FakeWebSocket
}

class FakeRelay {
  readonly clients: FakeWebSocket[] = []
  readonly requests: RelayRequest[] = []

  readonly factory: WebSocketFactory = () => {
    const socket = new FakeWebSocket(this)
    this.clients.push(socket)
    return socket
  }

  accept(socket: FakeWebSocket, data: ArrayBuffer | ArrayBufferView): void {
    const request = decodeHttpRelayRequest(data)
    this.requests.push({ request, socket })
    if (!new URL(request.url).pathname.startsWith('/hold')) {
      this.respond(request.url)
    }
  }

  respond(url: string): void {
    const index = this.requests.findIndex(entry => entry.request.url === url)
    const entry = index < 0 ? undefined : this.requests.splice(index, 1)[0]
    if (entry === undefined) {
      throw new Error(`测试 Relay 没有等待中的请求: ${url}`)
    }
    const path = new URL(entry.request.url).pathname
    const body = entry.request.method === 'HEAD' ? new Uint8Array() : new TextEncoder().encode(path)
    entry.socket.receive(
      encodeHttpRelayResponse({
        body,
        headers: [{ name: 'content-length', value: String(path.length) }],
        message: '',
        outcome: HttpRelayResponseOutcome.HTTP,
        status: 200,
        url: entry.request.url,
      }),
    )
  }

  requestedUrls(socket: FakeWebSocket): string[] {
    return this.requests.filter(entry => entry.socket === socket).map(entry => entry.request.url)
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

function testRelayCodecBodyView(): void {
  const output = createHttpRelayResponseBuffer(
    {
      headerValues: HTTP_RELAY_RESPONSE_HEADER_NAMES.map(name =>
        name === 'content-type' ? 'video/mp4' : null,
      ),
      status: 206,
      url: 'https://example.com/video',
    },
    4,
  )
  output.body.set([1, 2, 3])
  const message = output.finish(output.body.subarray(0, 3))
  const response = decodeHttpRelayResponse(message)
  assert(response.body.buffer === message.buffer, 'Response codec 不应复制 body')
  assert(response.body.byteLength === 3, 'Response codec body 长度错误')
  assert(response.body[2] === 3, 'Response codec body 内容错误')
  assert(response.headers[0]?.name === 'content-type', 'Response codec header ID 解码错误')
  assert(response.headers[0]?.value === 'video/mp4', 'Response codec header value 解码错误')

  const error = decodeHttpRelayResponse(
    encodeHttpRelayError(HttpRelayResponseOutcome.RESPONSE_TOO_LARGE, 'response too large'),
  )
  assert(error.body.byteLength === 0, '错误 Response 不应暴露 body')
  assert(error.message === 'response too large', '错误 Response message 解码错误')
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
  transport.destroy()
}

async function testAbortIsIgnored(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const controller = new AbortController()
  const response = transport.request(
    new Request('https://example.com/hold/ignored-abort', { signal: controller.signal }),
  )
  await waitFor(() => relay.requests.length === 1)
  controller.abort()

  const next = transport.request(new Request('https://example.com/next'))
  await waitFor(() => relay.clients.length === 2)
  relay.respond('https://example.com/hold/ignored-abort')
  assert(decode(await (await response).arrayBuffer()) === '/hold/ignored-abort', 'Abort 被错误转发')
  assert(decode(await (await next).arrayBuffer()) === '/next', '补充连接请求失败')
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
  assert(relay.clients.length === 0, 'minIdleConnections 不应主动创建连接')
  await (await transport.request(new Request('https://example.com/one'))).arrayBuffer()
  assert(countClients(relay) === 1, '单个请求不应创建最低空闲数量的连接')
  transport.destroy()
}

async function testTransactionTimeout(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay, { transactionTimeoutMs: 5 })
  let failed = false
  try {
    await transport.request(new Request('https://example.com/hold/timeout'))
  } catch (cause) {
    failed = cause instanceof Error && cause.message === 'WebSocket 请求事务超时'
  }
  assert(failed, '事务超时没有结束请求')
  assert(relay.clients[0]?.readyState === 3, '事务超时没有关闭连接')
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
    connectTimeoutMs: 1_000,
    defaultMaxResponseBytes: HTTP_RELAY_MAX_RESPONSE_BODY_BYTES,
    idleConnectionTimeoutMs: 60_000,
    maxConnections: 12,
    maxRequestsPerConnection: 50,
    minIdleConnections: 0,
    transactionTimeoutMs: 1_000,
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

testRelayCodecBodyView()
await testTransportStatistics()
await testFetchTransport()
await testSequentialReuse()
await testConcurrentGrowth()
await testHeadContentLength()
await testAbortIsIgnored()
await testMaximumReuse()
await testYoungestConnectionFirst()
await testIdleRetentionFloor()
await testMinimumIdleDoesNotPreconnect()
await testTransactionTimeout()
await testConnectionDiagnostics()
await testDefaultConnectionLog()
await testConnectionFactoryFailure()
await testInvalidResponseLimit()
