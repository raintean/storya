import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { HttpTransportFailure } from './http-transport'
import type { FetchFunction } from './fetch-http-transport'
import { TransportStatistics, type TransportStatisticsSnapshot } from './transport-statistics'

const proxyPathPattern = /^\/proxy\/([A-Za-z0-9_-]+)\.jpg$/
const headDescriptorPrefix = 'storya-proxy-head-v1\n'
const rangeDescriptorPrefix = 'storya-proxy-range-v1\n'
const rangeHeaderPattern = /^bytes=(\d+)-(\d+)$/i
const proxyContentLengthHeader = 'x-storya-proxy-content-length'
const proxyContentRangeHeader = 'x-storya-proxy-content-range'
const proxyStatusHeader = 'x-storya-proxy-status'
const proxyContentTypeHeader = 'x-storya-proxy-content-type'
const proxyShardSize = 2 * 1024 * 1024

export interface ProxyByteRange {
  endInclusive: number
  start: number
}

interface ProxyTargetDescriptor {
  method: 'GET' | 'HEAD'
  range: ProxyByteRange | undefined
  url: string
}

export class ProxyHttpTransport implements HttpTransport {
  private destroyed = false
  private readonly fetcher: FetchFunction
  private readonly proxyOrigins: string[]
  private readonly statistics: TransportStatistics

  constructor(proxyOrigins: readonly string[], fetcher: FetchFunction = request => fetch(request)) {
    if (proxyOrigins.length === 0) {
      throw new Error('Proxy transport 至少需要一个 Proxy Origin')
    }
    this.proxyOrigins = proxyOrigins.map(normalizeProxyOrigin)
    this.fetcher = fetcher
    this.statistics = new TransportStatistics('ProxyHttpTransport')
  }

  async request(
    request: Request,
    _options?: HttpTransportRequestOptions,
  ): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      throw new HttpTransportFailure('destroyed', 'Proxy transport 已经销毁')
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      throw new HttpTransportFailure(
        'protocol-error',
        `Proxy transport 不支持 ${request.method} 请求`,
      )
    }

    const range = parseRequestRange(request)
    const proxyOrigin = this.selectProxyOrigin(request.url, range)
    const logicalMethod = request.method === 'HEAD' ? 'HEAD' : 'GET'
    const proxyUrl = createProxyUrl(proxyOrigin, request.url, range, logicalMethod)
    const headers = new Headers(request.headers)
    headers.delete('Range')
    const proxyRequest = new Request(proxyUrl, {
      cache: request.cache,
      credentials: 'omit',
      headers,
      // Cloudflare 会把可缓存 URL 的 HEAD 转换成 GET 再访问 Origin
      // 始终使用物理 GET, 由 descriptor 告诉 Rust proxy 是否执行上游 HEAD
      method: 'GET',
      mode: 'cors',
      redirect: 'follow',
      signal: request.signal,
    })
    const statistics = this.statistics.startRequest()
    try {
      const response = await this.fetcher(proxyRequest)
      const proxyResponse = new ProxyHttpResponse(
        response,
        decodeProxyResponseUrl(response.url) ?? request.url,
        range !== undefined || logicalMethod === 'HEAD',
      )
      return statistics.trackResponse(proxyResponse, logicalMethod !== 'HEAD')
    } catch (error) {
      statistics.reject(error)
      throw error
    }
  }

  getStatistics(): TransportStatisticsSnapshot {
    return this.statistics.snapshot()
  }

  destroy(): void {
    this.destroyed = true
    this.statistics.destroy()
  }

  private selectProxyOrigin(targetUrl: string, range: ProxyByteRange | undefined): string {
    const baseIndex = stableHash(targetUrl) % this.proxyOrigins.length
    const rangeOffset =
      range === undefined ? 0 : Math.floor(range.start / proxyShardSize) % this.proxyOrigins.length
    const origin = this.proxyOrigins[(baseIndex + rangeOffset) % this.proxyOrigins.length]
    if (origin === undefined) {
      throw new Error('Proxy Origin 列表为空')
    }
    return origin
  }
}

class ProxyHttpResponse implements HttpTransportResponse {
  readonly body: ReadableStream<Uint8Array> | null
  readonly headers: Headers
  readonly ok: boolean
  readonly status: number
  readonly statusText: string
  readonly url: string

  private readonly response: Response

  constructor(response: Response, url: string, expectsProxyStatus: boolean) {
    this.response = response
    this.body = response.body
    this.headers = new Headers(response.headers)
    const proxyStatus = this.headers.get(proxyStatusHeader)
    this.headers.delete(proxyStatusHeader)
    const proxyContentLength = this.headers.get(proxyContentLengthHeader)
    this.headers.delete(proxyContentLengthHeader)
    const proxyContentRange = this.headers.get(proxyContentRangeHeader)
    this.headers.delete(proxyContentRangeHeader)
    const proxyContentType = this.headers.get(proxyContentTypeHeader)
    if (proxyContentType !== null) {
      this.headers.delete(proxyContentTypeHeader)
      this.headers.set('Content-Type', proxyContentType)
    }

    if (proxyStatus === null && expectsProxyStatus) {
      throw new HttpTransportFailure('invalid-response', 'Proxy 响应缺少原始状态码')
    }
    if (proxyStatus === null) {
      this.status = response.status
      this.statusText = response.statusText
    } else {
      const status = Number.parseInt(proxyStatus, 10)
      if (!/^\d{3}$/u.test(proxyStatus) || status < 100 || status > 599) {
        throw new HttpTransportFailure(
          'invalid-response',
          `Proxy 返回了无效的原始状态码: ${proxyStatus}`,
        )
      }
      if (status !== 206 && status !== response.status) {
        throw new HttpTransportFailure(
          'invalid-response',
          `Proxy 响应状态不一致: 原始 ${status}, 实际 ${response.status}`,
        )
      }
      this.status = status
      this.statusText = status === 206 ? 'Partial Content' : response.statusText
      if (status === 206 && proxyContentRange !== null) {
        this.headers.set('Content-Range', proxyContentRange)
      }
      if (proxyContentLength !== null) {
        this.headers.set('Content-Length', proxyContentLength)
      }
    }

    this.ok = this.status >= 200 && this.status < 300
    this.url = url
  }

  arrayBuffer(): Promise<ArrayBuffer> {
    return this.response.arrayBuffer()
  }
}

export function createProxyUrl(
  proxyOrigin: string,
  targetUrl: string,
  range?: ProxyByteRange,
  method: 'GET' | 'HEAD' = 'GET',
): string {
  const normalizedOrigin = normalizeProxyOrigin(proxyOrigin)
  const encodedTarget = encodeProxyTargetDescriptor({ method, range, url: targetUrl })
  return new URL(`/proxy/${encodedTarget}.jpg`, normalizedOrigin).toString()
}

export function encodeProxyTargetUrl(targetUrl: string): string {
  return encodeBase64Url(targetUrl)
}

export function decodeProxyTargetUrl(encodedTarget: string): string {
  return decodeProxyTargetDescriptor(encodedTarget).url
}

function encodeProxyTargetDescriptor(descriptor: ProxyTargetDescriptor): string {
  if (descriptor.method === 'HEAD') {
    if (descriptor.range !== undefined) {
      throw new Error('Proxy HEAD descriptor 不能包含 Range')
    }
    return encodeBase64Url(`${headDescriptorPrefix}${descriptor.url}`)
  }
  if (descriptor.range === undefined) {
    return encodeProxyTargetUrl(descriptor.url)
  }
  validateByteRange(descriptor.range)
  return encodeBase64Url(
    `${rangeDescriptorPrefix}${descriptor.range.start}\n${descriptor.range.endInclusive}\n${descriptor.url}`,
  )
}

function decodeProxyTargetDescriptor(encodedTarget: string): ProxyTargetDescriptor {
  const value = decodeBase64Url(encodedTarget)
  if (value.startsWith(headDescriptorPrefix)) {
    return {
      method: 'HEAD',
      range: undefined,
      url: value.slice(headDescriptorPrefix.length),
    }
  }
  if (!value.startsWith(rangeDescriptorPrefix)) {
    return { method: 'GET', range: undefined, url: value }
  }

  const fields = value.slice(rangeDescriptorPrefix.length).split('\n')
  const startText = fields.shift()
  const endText = fields.shift()
  if (startText === undefined || endText === undefined || fields.length === 0) {
    throw new Error('Proxy Range descriptor 格式无效')
  }
  if (!/^\d+$/u.test(startText) || !/^\d+$/u.test(endText)) {
    throw new Error('Proxy Range descriptor 包含无效的字节位置')
  }
  const range = {
    endInclusive: Number.parseInt(endText, 10),
    start: Number.parseInt(startText, 10),
  }
  validateByteRange(range)
  return { method: 'GET', range, url: fields.join('\n') }
}

function encodeBase64Url(value: string): string {
  const bytes = new TextEncoder().encode(value)
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}

function decodeBase64Url(value: string): string {
  const remainder = value.length % 4
  if (remainder === 1 || !/^[A-Za-z0-9_-]+$/u.test(value)) {
    throw new Error('Proxy URL 包含无效的 Base64URL target')
  }
  const padding = remainder === 0 ? '' : '='.repeat(4 - remainder)
  const binary = atob(value.replaceAll('-', '+').replaceAll('_', '/') + padding)
  const bytes = Uint8Array.from(binary, character => character.charCodeAt(0))
  return new TextDecoder('utf-8', { fatal: true }).decode(bytes)
}

function parseRequestRange(request: Request): ProxyByteRange | undefined {
  const value = request.headers.get('Range')
  if (value === null) {
    return undefined
  }
  if (request.method !== 'GET') {
    throw new HttpTransportFailure('protocol-error', 'Proxy transport 只允许 GET 使用 Range')
  }
  const match = rangeHeaderPattern.exec(value.trim())
  if (match === null) {
    throw new HttpTransportFailure(
      'protocol-error',
      `Proxy transport 只支持单个闭区间 Range: ${value}`,
    )
  }
  const range = {
    endInclusive: Number.parseInt(match[2] ?? '', 10),
    start: Number.parseInt(match[1] ?? '', 10),
  }
  try {
    validateByteRange(range)
  } catch (cause) {
    throw new HttpTransportFailure('protocol-error', 'Proxy transport 收到无效 Range', {
      cause,
    })
  }
  return range
}

function validateByteRange(range: ProxyByteRange): void {
  if (
    !Number.isSafeInteger(range.start) ||
    !Number.isSafeInteger(range.endInclusive) ||
    range.start < 0 ||
    range.endInclusive < range.start
  ) {
    throw new Error('Proxy Range 必须是有效的非负闭区间')
  }
}

function decodeProxyResponseUrl(responseUrl: string): string | undefined {
  if (responseUrl.length === 0) {
    return undefined
  }
  try {
    const match = proxyPathPattern.exec(new URL(responseUrl).pathname)
    return match?.[1] === undefined ? undefined : decodeProxyTargetUrl(match[1])
  } catch {
    return undefined
  }
}

function stableHash(value: string): number {
  let hash = 0x811c9dc5
  for (const byte of new TextEncoder().encode(value)) {
    hash ^= byte
    hash = Math.imul(hash, 0x01000193)
  }
  return hash >>> 0
}

function normalizeProxyOrigin(value: string): string {
  let url: URL
  try {
    url = new URL(value)
  } catch {
    throw new Error(`Proxy Origin 格式无效: ${value}`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`Proxy Origin 必须使用 HTTP 或 HTTPS: ${value}`)
  }
  if (url.username.length > 0 || url.password.length > 0) {
    throw new Error(`Proxy Origin 不能包含用户信息: ${value}`)
  }
  return url.origin
}
