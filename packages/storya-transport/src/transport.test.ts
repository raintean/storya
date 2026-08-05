import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import {
  decodeTransportFrame,
  encodeTransportFrame,
  HttpRequestHeadSchema,
  HttpResponseHeadSchema,
  TransportFrameKind,
} from 'storya-protocol'
import { FetchHttpTransport } from './fetch-http-transport'
import { WebSocketHttpTransport } from './websocket-http-transport'
import type { WebSocketFactory, WebSocketLike } from './websocket-http-transport'

class FakeRelay {
  readonly clients: FakeWebSocket[] = []
  requestCount = 0
  respondToPing = true

  readonly factory: WebSocketFactory = () => {
    const socket = new FakeWebSocket(this)
    this.clients.push(socket)
    return socket
  }

  accept(socket: FakeWebSocket, data: ArrayBuffer | ArrayBufferView): void {
    const frame = decodeTransportFrame(data)
    if (frame.kind === TransportFrameKind.PING) {
      if (this.respondToPing) {
        socket.receive(encodeTransportFrame(TransportFrameKind.PONG, frame.sequence))
      }
      return
    }
    if (frame.kind === TransportFrameKind.CANCEL) {
      socket.receive(encodeTransportFrame(TransportFrameKind.CANCELED, frame.sequence))
      return
    }
    if (frame.kind !== TransportFrameKind.REQUEST_HEAD) {
      throw new Error(`测试 Relay 收到意外 frame: ${frame.kind}`)
    }

    this.requestCount += 1
    const request = fromBinary(HttpRequestHeadSchema, frame.payload)
    if (new URL(request.url).pathname === '/hang') {
      return
    }

    const payload = new TextEncoder().encode(new URL(request.url).pathname)
    const response = create(HttpResponseHeadSchema, {
      headers: [{ name: 'content-length', value: String(payload.byteLength) }],
      status: 200,
      statusText: 'OK',
      url: request.url,
    })
    socket.receive(
      encodeTransportFrame(
        TransportFrameKind.RESPONSE_HEAD,
        frame.sequence,
        toBinary(HttpResponseHeadSchema, response),
      ),
    )
    if (request.method !== 'HEAD') {
      socket.receive(
        encodeTransportFrame(TransportFrameKind.RESPONSE_BODY, frame.sequence, payload),
      )
    }
    socket.receive(encodeTransportFrame(TransportFrameKind.RESPONSE_END, frame.sequence))
  }
}

class FakeWebSocket implements WebSocketLike {
  binaryType: BinaryType = 'blob'
  readyState = 0

  private readonly listeners = new Map<string, Set<(event: never) => void>>()
  private readonly relay: FakeRelay

  constructor(relay: FakeRelay) {
    this.relay = relay
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

  removeEventListener(
    type: 'close' | 'error' | 'message' | 'open',
    listener: (event: never) => void,
  ): void {
    this.listeners.get(type)?.delete(listener)
  }

  send(data: string | Blob | ArrayBuffer | ArrayBufferView<ArrayBuffer>): void {
    if (typeof data === 'string' || data instanceof Blob) {
      throw new Error('测试 WebSocket 只支持二进制消息')
    }
    this.relay.accept(this, data)
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === 3) {
      return
    }
    this.readyState = 3
    this.emit('close', { code, reason } as CloseEvent)
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

async function testFetchTransport(): Promise<void> {
  let requestedUrl = ''
  const transport = new FetchHttpTransport(async request => {
    requestedUrl = request.url
    return new Response('fetch')
  })
  const response = await transport.request(new Request('https://example.com/fetch'))
  assert(requestedUrl === 'https://example.com/fetch', 'Fetch transport 没有转发请求')
  assert(new TextDecoder().decode(await response.arrayBuffer()) === 'fetch', 'Fetch 响应错误')
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
  const [first, second] = await Promise.all([
    transport.request(new Request('https://example.com/one')),
    transport.request(new Request('https://example.com/two')),
  ])
  await Promise.all([first.arrayBuffer(), second.arrayBuffer()])
  assert(relay.clients.length === 2, '并发请求没有扩容 WebSocket 连接池')
  transport.destroy()
}

async function testHeadContentLength(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const response = await transport.request(
    new Request('https://example.com/head', { method: 'HEAD' }),
    {
      maxResponseBytes: 0,
    },
  )
  assert(response.status === 200, 'HEAD 响应没有成功返回')
  assert(response.headers.get('content-length') === '5', 'HEAD 响应丢失 Content-Length')
  assert((await response.arrayBuffer()).byteLength === 0, 'HEAD 响应不应包含 body')
  transport.destroy()
}

async function testCancellation(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay)
  const controller = new AbortController()
  const response = transport.request(
    new Request('https://example.com/hang', { signal: controller.signal }),
  )
  await waitFor(() => relay.requestCount === 1)
  controller.abort()

  let aborted = false
  try {
    await response
  } catch (error) {
    aborted = error instanceof DOMException && error.name === 'AbortError'
  }
  assert(aborted, '取消请求没有返回 AbortError')
  transport.destroy()
}

async function testRequestAging(): Promise<void> {
  const relay = new FakeRelay()
  const transport = createTransport(relay, 1)
  const first = await transport.request(new Request('https://example.com/first'))
  await first.arrayBuffer()
  const second = await transport.request(new Request('https://example.com/second'))
  await second.arrayBuffer()
  assert(relay.clients.length === 2, '达到请求次数后没有退休旧连接')
  transport.destroy()
}

async function testHeartbeatTimeout(): Promise<void> {
  const relay = new FakeRelay()
  relay.respondToPing = false
  const transport = new WebSocketHttpTransport('wss://relay.example.com/transport', {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 1,
    heartbeatTimeoutMs: 1,
    idleConnectionTimeoutMs: 60_000,
    maxConnectionLifetimeMs: 60_000,
    maxConnections: 12,
    maxRequestsPerConnection: 40,
    webSocketFactory: relay.factory,
  })
  const response = await transport.request(new Request('https://example.com/heartbeat'))
  await response.arrayBuffer()

  await waitForWithin(() => relay.clients[0]?.readyState === 3, 2_500)
  assert(relay.clients[0]?.readyState === 3, '没有响应心跳的 WebSocket 未被关闭')
  transport.destroy()
}

async function testConnectionFactoryFailure(): Promise<void> {
  const transport = new WebSocketHttpTransport('wss://relay.example.com/transport', {
    webSocketFactory: () => {
      throw new Error('连接创建失败')
    },
  })
  let failed = false
  try {
    await transport.request(new Request('https://example.com/failure'))
  } catch (error) {
    failed = error instanceof Error && error.message === 'WebSocket 连接创建失败'
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
  } catch (error) {
    failed = error instanceof Error && error.message === 'maxResponseBytes 无效'
  }
  assert(failed, '无效响应上限没有被拒绝')
  assert(relay.clients.length === 0, '无效响应上限不应创建 WebSocket')
  transport.destroy()
}

function createTransport(relay: FakeRelay, maxRequestsPerConnection = 40): WebSocketHttpTransport {
  return new WebSocketHttpTransport('wss://relay.example.com/transport', {
    connectTimeoutMs: 1_000,
    heartbeatIntervalMs: 60_000,
    idleConnectionTimeoutMs: 60_000,
    maxConnectionLifetimeMs: 60_000,
    maxConnections: 12,
    maxRequestsPerConnection,
    webSocketFactory: relay.factory,
  })
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (predicate()) {
      return
    }
    await new Promise(resolve => globalThis.setTimeout(resolve, 0))
  }
  throw new Error('等待测试条件超时')
}

async function waitForWithin(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = performance.now() + timeoutMs
  while (performance.now() < deadline) {
    if (predicate()) {
      return
    }
    await new Promise(resolve => globalThis.setTimeout(resolve, 10))
  }
  throw new Error('等待定时测试条件超时')
}

function decode(buffer: ArrayBuffer): string {
  return new TextDecoder().decode(buffer)
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

await testFetchTransport()
await testSequentialReuse()
await testConcurrentGrowth()
await testHeadContentLength()
await testCancellation()
await testRequestAging()
await testHeartbeatTimeout()
await testConnectionFactoryFailure()
await testInvalidResponseLimit()
