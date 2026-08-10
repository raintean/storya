import { HTTP_RELAY_MAX_RESPONSE_BODY_BYTES } from 'storya-protocol'
import type { HttpRequestHead } from 'storya-protocol'

export const upstreamCacheTtlSeconds = 365 * 24 * 60 * 60

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

export interface ValidatedRelayRequest {
  maxResponseBytes: number
  target: URL
}

export function validateRelayRequest(head: HttpRequestHead): ValidatedRelayRequest {
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

export function createUpstreamHeaders(head: HttpRequestHead, clientHeaders: Headers): Headers {
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

export function createClientHeaders(request: Request): Headers {
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

export function parseContentLength(headers: Headers): number | undefined {
  const value = headers.get('content-length')
  if (value === null || !/^\d+$/u.test(value)) {
    return undefined
  }
  const length = Number(value)
  return Number.isSafeInteger(length) ? length : undefined
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

export class InvalidRelayRequest extends Error {}
