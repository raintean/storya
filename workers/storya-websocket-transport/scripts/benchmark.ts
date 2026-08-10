import { WebSocketHttpTransport } from 'storya-transport'

declare const process: {
  argv: string[]
  exitCode: number | undefined
  stdin: {
    readonly isTTY?: boolean
    once(event: 'data', listener: () => void): void
    pause(): void
    resume(): void
  }
}

interface BenchmarkOptions {
  concurrency: number
  endpoint: string
  interactive: boolean
  rangeBytes: number
  requestTimeoutMs: number
  rounds: number
  url: string
  warmup: boolean
}

interface RequestResult {
  bytes: number
  elapsedMs: number
}

const DEFAULT_CONCURRENCY = 6
const DEFAULT_ENDPOINT = 'ws://127.0.0.1:8787/transport'
const DEFAULT_RANGE_BYTES = 1024 * 1024
const DEFAULT_REQUEST_TIMEOUT_MS = 0
const DEFAULT_ROUNDS = 4
const DEFAULT_URL =
  'https://cdn.radiantmediatechs.com/rmp/media/samples-for-rmp-site/04052024-lac-de-bimont/hls/avc_2160p/1.m4s'
const LONG_CONNECTION_TIMEOUT_MS = 30 * 60 * 1000
const MAX_REQUESTS_PER_CONNECTION = 1_000_000
const CONNECT_TIMEOUT_MS = 10_000
const CANCEL_TIMEOUT_MS = 10_000
const MAX_RESPONSE_BYTES = 32 * 1024 * 1024

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2))
  const transport = new WebSocketHttpTransport(options.endpoint, {
    cancelTimeoutMs: CANCEL_TIMEOUT_MS,
    connectTimeoutMs: CONNECT_TIMEOUT_MS,
    defaultMaxResponseBytes: MAX_RESPONSE_BYTES,
    idleConnectionTimeoutMs: LONG_CONNECTION_TIMEOUT_MS,
    maxConnections: options.concurrency,
    maxRequestsPerConnection: MAX_REQUESTS_PER_CONNECTION,
    retainedIdleConnections: options.concurrency,
  })

  console.info('准备 WebSocket Worker CPU benchmark', {
    concurrency: options.concurrency,
    endpoint: options.endpoint,
    interactive: options.interactive,
    rangeBytes: options.rangeBytes,
    requestTimeoutMs: options.requestTimeoutMs,
    rounds: options.rounds,
    url: options.url,
    warmup: options.warmup,
  })

  try {
    if (options.warmup) {
      console.info(`正在预建 ${options.concurrency} 条 WebSocket 连接...`)
      await runRound(transport, options, 1)
      console.info('连接预热完成')
    }

    if (options.interactive) {
      await waitForEnter('请开始 Profiler, 然后按回车执行正式负载')
      console.info('已收到回车, 正式负载开始')
    }

    const results: RequestResult[] = []
    const startedAt = performance.now()
    for (let round = 1; round <= options.rounds; round += 1) {
      console.info(`Round ${round}/${options.rounds} 开始`)
      const roundResults = await runRound(transport, options, options.rangeBytes)
      results.push(...roundResults)
      console.info(`Round ${round}/${options.rounds} 完成`, summarize(roundResults))
    }
    const elapsedMs = performance.now() - startedAt
    console.info('WebSocket Worker CPU benchmark 完成', {
      ...summarize(results),
      elapsedMs,
      throughputBytesPerSecond:
        elapsedMs === 0
          ? 0
          : (results.reduce((total, result) => total + result.bytes, 0) * 1_000) / elapsedMs,
    })

    if (options.interactive) {
      await waitForEnter('请先停止 Profiler, 然后按回车关闭 benchmark 连接')
      console.info('已收到回车, 正在关闭 benchmark 连接')
    }
  } finally {
    transport.destroy()
  }
}

async function runRound(
  transport: WebSocketHttpTransport,
  options: BenchmarkOptions,
  rangeBytes: number,
): Promise<RequestResult[]> {
  return Promise.all(
    Array.from({ length: options.concurrency }, async (_, lane) => {
      const startedAt = performance.now()
      const controller = new AbortController()
      const timeout =
        options.requestTimeoutMs === 0
          ? undefined
          : globalThis.setTimeout(() => controller.abort(), options.requestTimeoutMs)
      try {
        const response = await transport.request(
          new Request(options.url, {
            headers: {
              range: `bytes=0-${rangeBytes - 1}`,
            },
            signal: controller.signal,
          }),
          { maxResponseBytes: rangeBytes },
        )
        if (response.status !== 206) {
          await response.body?.cancel()
          throw new Error(`目标没有返回 Range 响应: HTTP ${response.status}`)
        }
        if (response.headers.get('content-length') !== String(rangeBytes)) {
          await response.body?.cancel()
          throw new Error('Range 响应没有通过 relay 保留正确的 Content-Length')
        }
        if (!response.headers.get('content-range')?.startsWith(`bytes 0-${rangeBytes - 1}/`)) {
          await response.body?.cancel()
          throw new Error('Range 响应没有通过 relay 保留正确的 Content-Range')
        }
        const data = await response.arrayBuffer()
        if (data.byteLength !== rangeBytes) {
          throw new Error(`Range 响应长度错误: 期望 ${rangeBytes}, 实际 ${data.byteLength}`)
        }
        return {
          bytes: data.byteLength,
          elapsedMs: performance.now() - startedAt,
        }
      } catch (cause) {
        if (
          cause instanceof DOMException &&
          cause.name === 'AbortError' &&
          options.requestTimeoutMs !== 0
        ) {
          throw new Error(`Lane ${lane + 1} 请求超过 ${options.requestTimeoutMs}ms`, { cause })
        }
        throw cause
      } finally {
        if (timeout !== undefined) {
          globalThis.clearTimeout(timeout)
        }
      }
    }),
  )
}

function summarize(results: RequestResult[]): {
  loadedBytes: number
  maxRequestMs: number
  meanRequestMs: number
  minRequestMs: number
  requestCount: number
} {
  const elapsedValues = results.map(result => result.elapsedMs)
  return {
    loadedBytes: results.reduce((total, result) => total + result.bytes, 0),
    maxRequestMs: elapsedValues.length === 0 ? 0 : Math.max(...elapsedValues),
    meanRequestMs:
      elapsedValues.length === 0
        ? 0
        : elapsedValues.reduce((total, elapsedMs) => total + elapsedMs, 0) / elapsedValues.length,
    minRequestMs: elapsedValues.length === 0 ? 0 : Math.min(...elapsedValues),
    requestCount: results.length,
  }
}

function parseOptions(args: string[]): BenchmarkOptions {
  const normalizedArgs = args[0] === '--' ? args.slice(1) : args
  const values = new Map<string, string>()
  let interactive = process.stdin.isTTY === true
  let warmup = true

  for (let index = 0; index < normalizedArgs.length; index += 1) {
    const name = normalizedArgs[index]
    if (name === '--help') {
      printHelp()
      throw new BenchmarkHelpRequested()
    }
    if (name === '--non-interactive') {
      interactive = false
      continue
    }
    if (name === '--skip-warmup') {
      warmup = false
      continue
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

  return {
    concurrency: parsePositiveInteger(
      values.get('--concurrency'),
      DEFAULT_CONCURRENCY,
      '--concurrency',
    ),
    endpoint,
    interactive,
    rangeBytes: parsePositiveInteger(
      values.get('--range-bytes'),
      DEFAULT_RANGE_BYTES,
      '--range-bytes',
    ),
    requestTimeoutMs: parseNonNegativeInteger(
      values.get('--request-timeout-ms'),
      DEFAULT_REQUEST_TIMEOUT_MS,
      '--request-timeout-ms',
    ),
    rounds: parsePositiveInteger(values.get('--rounds'), DEFAULT_ROUNDS, '--rounds'),
    url,
    warmup,
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

function waitForEnter(message: string): Promise<void> {
  console.info(`${message}...`)
  return new Promise(resolve => {
    process.stdin.resume()
    process.stdin.once('data', () => {
      process.stdin.pause()
      resolve()
    })
  })
}

function printHelp(): void {
  console.info(`用法:
  pnpm --filter storya-websocket-transport run benchmark -- [options]

交互模式会先预建连接并暂停。开始 Profiler 后按回车执行负载；负载结束后先停止
Profiler, 再按回车关闭连接。stdin 不是 TTY 时自动使用非交互模式。

选项:
  --url <url>                  Range 请求地址, 默认使用测试 HLS Segment
  --endpoint <url>             Worker WebSocket 地址, 默认 ${DEFAULT_ENDPOINT}
  --concurrency <count>        并发连接数, 默认 ${DEFAULT_CONCURRENCY}
  --range-bytes <bytes>        每个请求的字节数, 默认 ${DEFAULT_RANGE_BYTES}
  --rounds <count>             每条连接的请求轮数, 默认 ${DEFAULT_ROUNDS}
  --request-timeout-ms <ms>    单请求超时, 默认 0 (不限制)
  --skip-warmup                不预建连接
  --non-interactive            不等待 Profiler 的开始和停止操作
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
