import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { HttpTransportFailure } from './http-transport'
import type { FetchFunction } from './fetch-http-transport'

const proxyPathPattern = /^\/proxy\/([A-Za-z0-9_-]+)\.bin$/

export class ProxyHttpTransport implements HttpTransport {
  private destroyed = false
  private nextOriginIndex = 0
  private readonly fetcher: FetchFunction
  private readonly proxyOrigins: string[]

  constructor(proxyOrigins: readonly string[], fetcher: FetchFunction = request => fetch(request)) {
    if (proxyOrigins.length === 0) {
      throw new Error('Proxy transport 至少需要一个 Proxy Origin')
    }
    this.proxyOrigins = proxyOrigins.map(normalizeProxyOrigin)
    this.fetcher = fetcher
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

    const proxyOrigin = this.selectProxyOrigin()
    const proxyUrl = createProxyUrl(proxyOrigin, request.url)
    const proxyRequest = new Request(proxyUrl, {
      cache: request.cache,
      credentials: 'omit',
      headers: request.headers,
      method: request.method,
      mode: 'cors',
      redirect: 'follow',
      signal: request.signal,
    })
    const response = await this.fetcher(proxyRequest)
    return new ProxyHttpResponse(response, decodeProxyResponseUrl(response.url) ?? request.url)
  }

  destroy(): void {
    this.destroyed = true
  }

  private selectProxyOrigin(): string {
    const origin = this.proxyOrigins[this.nextOriginIndex]
    if (origin === undefined) {
      throw new Error('Proxy Origin 列表为空')
    }
    this.nextOriginIndex = (this.nextOriginIndex + 1) % this.proxyOrigins.length
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

  constructor(response: Response, url: string) {
    this.response = response
    this.body = response.body
    this.headers = response.headers
    this.ok = response.ok
    this.status = response.status
    this.statusText = response.statusText
    this.url = url
  }

  arrayBuffer(): Promise<ArrayBuffer> {
    return this.response.arrayBuffer()
  }
}

export function createProxyUrl(proxyOrigin: string, targetUrl: string): string {
  const normalizedOrigin = normalizeProxyOrigin(proxyOrigin)
  const encodedTarget = encodeProxyTargetUrl(targetUrl)
  return new URL(`/proxy/${encodedTarget}.bin`, normalizedOrigin).toString()
}

export function encodeProxyTargetUrl(targetUrl: string): string {
  const bytes = new TextEncoder().encode(targetUrl)
  let binary = ''
  for (const byte of bytes) {
    binary += String.fromCharCode(byte)
  }
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}

export function decodeProxyTargetUrl(encodedTarget: string): string {
  const remainder = encodedTarget.length % 4
  if (remainder === 1 || !/^[A-Za-z0-9_-]+$/u.test(encodedTarget)) {
    throw new Error('Proxy URL 包含无效的 Base64URL target')
  }
  const padding = remainder === 0 ? '' : '='.repeat(4 - remainder)
  const binary = atob(encodedTarget.replaceAll('-', '+').replaceAll('_', '/') + padding)
  const bytes = Uint8Array.from(binary, character => character.charCodeAt(0))
  return new TextDecoder('utf-8', { fatal: true }).decode(bytes)
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
