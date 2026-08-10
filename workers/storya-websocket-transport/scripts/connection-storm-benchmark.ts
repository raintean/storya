import { WebSocketHttpTransport } from 'storya-transport'
import type { WebSocketHttpTransportDebugEvent } from 'storya-transport'

declare const process: {
  argv: string[]
  exitCode: number | undefined
}

interface BenchmarkOptions {
  concurrency: number
  durationMs: number
  endpoint: string
  maxConnections: number
  rangeBytes: number
  reportIntervalMs: number
  retryDelayMs: number
  url: string
}

interface RequestCounters {
  completedBytes: number
  completedRequests: number
  failedRequests: number
  failures: Map<string, number>
}

interface ConnectionCounters {
  closed: WebSocketHttpTransportDebugEvent[]
  created: number
  currentPoolSize: number
  opened: number
  peakPoolSize: number
}

const DEFAULT_DURATION_MS = 60_000
const DEFAULT_ENDPOINT = 'ws://127.0.0.1:8787/transport'
const DEFAULT_RANGE_BYTES = 1024 * 1024
const DEFAULT_REPORT_INTERVAL_MS = 5_000
const DEFAULT_RETRY_DELAY_MS = 250
const DEFAULT_MAX_CONNECTIONS = 12
const CANCEL_TIMEOUT_MS = 10_000
const CONNECT_TIMEOUT_MS = 10_000
const DEFAULT_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
const IDLE_CONNECTION_TIMEOUT_MS = 30_000
const MAX_REQUESTS_PER_CONNECTION = 50
const RETAINED_IDLE_CONNECTIONS = 6
const DEFAULT_URL =
  'https://cdn.radiantmediatechs.com/rmp/media/samples-for-rmp-site/04052024-lac-de-bimont/hls/avc_2160p/1.m4s'

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2))
  const requests: RequestCounters = {
    completedBytes: 0,
    completedRequests: 0,
    failedRequests: 0,
    failures: new Map(),
  }
  const connections: ConnectionCounters = {
    closed: [],
    created: 0,
    currentPoolSize: 0,
    opened: 0,
    peakPoolSize: 0,
  }
  let measuring = true
  const transport = new WebSocketHttpTransport(options.endpoint, {
    cancelTimeoutMs: CANCEL_TIMEOUT_MS,
    connectTimeoutMs: CONNECT_TIMEOUT_MS,
    defaultMaxResponseBytes: DEFAULT_MAX_RESPONSE_BYTES,
    debug: event => {
      if (!measuring) {
        return
      }
      observeConnection(event, connections)
    },
    idleConnectionTimeoutMs: IDLE_CONNECTION_TIMEOUT_MS,
    maxConnections: options.maxConnections,
    maxRequestsPerConnection: MAX_REQUESTS_PER_CONNECTION,
    retainedIdleConnections: Math.min(RETAINED_IDLE_CONNECTIONS, options.maxConnections),
  })

  console.info('开始 WebSocket 连接风暴 benchmark', options)
  const startedAt = performance.now()
  const deadline = startedAt + options.durationMs
  const reportTimer = globalThis.setInterval(() => {
    console.info('连接池周期统计', createSnapshot(requests, connections, startedAt))
  }, options.reportIntervalMs)

  try {
    await Promise.all(
      Array.from({ length: options.concurrency }, (_, lane) =>
        runLane(lane, deadline, transport, options, requests),
      ),
    )
  } finally {
    globalThis.clearInterval(reportTimer)
    measuring = false
    transport.destroy()
  }

  const expectedRotations = connections.closed.filter(isExpectedRotation)
  const unexpectedClosures = connections.closed.filter(event => !isExpectedRotation(event))
  console.info('WebSocket 连接风暴 benchmark 完成', {
    ...createSnapshot(requests, connections, startedAt),
    closeCodes: countBy(connections.closed, event => String(event.code ?? 'unknown')),
    closeReasons: countBy(
      connections.closed,
      event => `${event.initiator ?? 'unknown'}:${event.reason || event.error || 'empty-reason'}`,
    ),
    expectedRotations: expectedRotations.length,
    lowUseClosedConnections: connections.closed.filter(event => event.requestCount <= 1).length,
    meanClosedConnectionAgeMs: mean(connections.closed.map(event => event.ageMs)),
    meanRequestsPerClosedConnection: mean(connections.closed.map(event => event.requestCount)),
    remoteClosedConnections: connections.closed.filter(event => event.initiator === 'remote')
      .length,
    replacementConnections: Math.max(0, connections.created - connections.peakPoolSize),
    shortLivedClosedConnections: connections.closed.filter(event => event.ageMs < 10_000).length,
    unexpectedClosures: unexpectedClosures.length,
    unexpectedShortLivedClosures: unexpectedClosures.filter(event => event.ageMs < 10_000).length,
  })
}

async function runLane(
  lane: number,
  deadline: number,
  transport: WebSocketHttpTransport,
  options: BenchmarkOptions,
  counters: RequestCounters,
): Promise<void> {
  while (performance.now() < deadline) {
    try {
      const response = await transport.request(
        new Request(options.url, {
          headers: {
            range: `bytes=0-${options.rangeBytes - 1}`,
          },
        }),
        { maxResponseBytes: options.rangeBytes },
      )
      if (response.status !== 206) {
        await response.body?.cancel()
        throw new Error(`目标没有返回 Range 响应: HTTP ${response.status}`)
      }
      const data = await response.arrayBuffer()
      if (data.byteLength !== options.rangeBytes) {
        throw new Error(
          `Lane ${lane + 1} Range 响应长度错误: 期望 ${options.rangeBytes}, 实际 ${data.byteLength}`,
        )
      }
      counters.completedBytes += data.byteLength
      counters.completedRequests += 1
    } catch (cause) {
      counters.failedRequests += 1
      const key = describeError(cause)
      counters.failures.set(key, (counters.failures.get(key) ?? 0) + 1)
      if (performance.now() < deadline && options.retryDelayMs !== 0) {
        await delay(options.retryDelayMs)
      }
    }
  }
}

function observeConnection(
  event: WebSocketHttpTransportDebugEvent,
  counters: ConnectionCounters,
): void {
  counters.currentPoolSize = event.poolSize
  counters.peakPoolSize = Math.max(counters.peakPoolSize, event.poolSize)
  if (event.type === 'connection-created') {
    counters.created += 1
    return
  }
  if (event.type === 'connection-opened') {
    counters.opened += 1
    return
  }
  counters.closed.push(event)
  console.info('连接关闭', {
    ageMs: event.ageMs,
    code: event.code,
    connectionId: event.connectionId,
    error: event.error,
    initiator: event.initiator,
    poolSize: event.poolSize,
    reason: event.reason,
    requestCount: event.requestCount,
    wasClean: event.wasClean,
  })
}

function createSnapshot(
  requests: RequestCounters,
  connections: ConnectionCounters,
  startedAt: number,
): Record<string, unknown> {
  const elapsedMs = performance.now() - startedAt
  return {
    closedConnections: connections.closed.length,
    completedBytes: requests.completedBytes,
    completedRequests: requests.completedRequests,
    connectionCloseRatePerMinute:
      elapsedMs === 0 ? 0 : (connections.closed.length * 60_000) / elapsedMs,
    createdConnections: connections.created,
    currentPoolSize: connections.currentPoolSize,
    elapsedMs,
    failedRequests: requests.failedRequests,
    failures: Object.fromEntries(requests.failures),
    openedConnections: connections.opened,
    peakPoolSize: connections.peakPoolSize,
    throughputBytesPerSecond: elapsedMs === 0 ? 0 : (requests.completedBytes * 1_000) / elapsedMs,
  }
}

function countBy(
  events: WebSocketHttpTransportDebugEvent[],
  keyOf: (event: WebSocketHttpTransportDebugEvent) => string,
): Record<string, number> {
  const counts = new Map<string, number>()
  for (const event of events) {
    const key = keyOf(event)
    counts.set(key, (counts.get(key) ?? 0) + 1)
  }
  return Object.fromEntries(counts)
}

function isExpectedRotation(event: WebSocketHttpTransportDebugEvent): boolean {
  return (
    event.code === 1000 &&
    event.initiator === 'local' &&
    (event.reason === 'idle' || event.reason === 'max-requests')
  )
}

function mean(values: number[]): number {
  return values.length === 0 ? 0 : values.reduce((total, value) => total + value, 0) / values.length
}

function describeError(cause: unknown): string {
  if (!(cause instanceof Error)) {
    return String(cause)
  }
  const code = 'code' in cause && typeof cause.code === 'string' ? cause.code : cause.name
  return `${code}:${cause.message}`
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => globalThis.setTimeout(resolve, ms))
}

function parseOptions(args: string[]): BenchmarkOptions {
  const normalizedArgs = args[0] === '--' ? args.slice(1) : args
  const values = new Map<string, string>()
  for (let index = 0; index < normalizedArgs.length; index += 1) {
    const name = normalizedArgs[index]
    if (name === '--help') {
      printHelp()
      throw new BenchmarkHelpRequested()
    }
    if (name === undefined || !name.startsWith('--')) {
      throw new Error(`无效参数: ${normalizedArgs.slice(index).join(' ')}`)
    }
    const value = normalizedArgs[index + 1]
    if (value === undefined || value.startsWith('--')) {
      throw new Error(`${name} 缺少参数值`)
    }
    values.set(name, value)
    index += 1
  }

  const url = values.get('--url') ?? DEFAULT_URL
  assertHttpUrl(url, '--url')
  const endpoint = values.get('--endpoint') ?? DEFAULT_ENDPOINT
  assertWebSocketUrl(endpoint)
  const maxConnections = parsePositiveInteger(
    values.get('--max-connections'),
    DEFAULT_MAX_CONNECTIONS,
    '--max-connections',
  )
  return {
    concurrency: parsePositiveInteger(values.get('--concurrency'), maxConnections, '--concurrency'),
    durationMs: parsePositiveInteger(
      values.get('--duration-ms'),
      DEFAULT_DURATION_MS,
      '--duration-ms',
    ),
    endpoint,
    maxConnections,
    rangeBytes: parsePositiveInteger(
      values.get('--range-bytes'),
      DEFAULT_RANGE_BYTES,
      '--range-bytes',
    ),
    reportIntervalMs: parsePositiveInteger(
      values.get('--report-interval-ms'),
      DEFAULT_REPORT_INTERVAL_MS,
      '--report-interval-ms',
    ),
    retryDelayMs: parseNonNegativeInteger(
      values.get('--retry-delay-ms'),
      DEFAULT_RETRY_DELAY_MS,
      '--retry-delay-ms',
    ),
    url,
  }
}

function parsePositiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined) {
    return fallback
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} 必须是正整数`)
  }
  return parsed
}

function parseNonNegativeInteger(
  value: string | undefined,
  fallback: number,
  name: string,
): number {
  if (value === undefined) {
    return fallback
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${name} 必须是非负整数`)
  }
  return parsed
}

function assertHttpUrl(value: string, name: string): void {
  const url = new URL(value)
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${name} 必须使用 http 或 https`)
  }
}

function assertWebSocketUrl(value: string): void {
  const url = new URL(value)
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('--endpoint 必须使用 ws 或 wss')
  }
}

function printHelp(): void {
  console.info(`用法:
  pnpm --filter storya-websocket-transport run benchmark:connections -- [options]

使用 Transport 的生产默认连接轮换参数持续发送 Range 请求，并汇总连接创建、关闭、
关闭原因、连接寿命和每连接请求数。

选项:
  --url <url>                  Range 请求地址, 默认使用测试 HLS Segment
  --endpoint <url>             Worker WebSocket 地址, 默认 ${DEFAULT_ENDPOINT}
  --concurrency <count>        持续请求并发数, 默认等于 max-connections
  --max-connections <count>    连接池上限, 默认 ${DEFAULT_MAX_CONNECTIONS}
  --range-bytes <bytes>        每个请求的字节数, 默认 ${DEFAULT_RANGE_BYTES}
  --duration-ms <ms>           发起请求的持续时间, 默认 ${DEFAULT_DURATION_MS}
  --report-interval-ms <ms>    周期统计间隔, 默认 ${DEFAULT_REPORT_INTERVAL_MS}
  --retry-delay-ms <ms>        请求失败后的重试间隔, 默认 ${DEFAULT_RETRY_DELAY_MS}
  --help                       显示帮助`)
}

class BenchmarkHelpRequested extends Error {}

try {
  await main()
} catch (cause) {
  if (!(cause instanceof BenchmarkHelpRequested)) {
    console.error(cause)
    process.exitCode = 1
  }
}
