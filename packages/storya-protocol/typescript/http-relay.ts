export interface HttpRelayHeader {
  name: string
  value: string
}

export interface HttpRelayRequest {
  headers: readonly HttpRelayHeader[]
  maxResponseBytes: number
  method: 'GET' | 'HEAD'
  url: string
}

export enum HttpRelayResponseOutcome {
  HTTP = 0,
  INVALID_REQUEST = 1,
  RESPONSE_TOO_LARGE = 2,
  UPSTREAM_FAILURE = 3,
  INTERNAL_FAILURE = 4,
}

export interface HttpRelayResponseHead {
  headerValues: readonly (string | null)[]
  status: number
  url: string
}

export interface HttpRelayResponse {
  body: Uint8Array
  headers: HttpRelayHeader[]
  message: string
  outcome: HttpRelayResponseOutcome
  status: number
  url: string
}

export interface HttpRelayResponseBuffer {
  readonly body: Uint8Array<ArrayBuffer>
  finish(value: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer>
  finishEmpty(): Uint8Array<ArrayBuffer>
}

interface EncodedRequestHeader {
  readonly name: Uint8Array
  readonly value: Uint8Array
}

interface EncodedRequestHeaders {
  readonly byteLength: number
  readonly values: EncodedRequestHeader[]
}

interface EncodedResponseHeader {
  readonly id: number
  readonly value: Uint8Array
}

interface EncodedResponseHeaders {
  readonly byteLength: number
  readonly values: EncodedResponseHeader[]
}

export const HTTP_RELAY_MAX_RESPONSE_BODY_BYTES = 32 * 1024 * 1024

export const HTTP_RELAY_RESPONSE_HEADER_NAMES = [
  'accept-ranges',
  'age',
  'cache-control',
  'cf-cache-status',
  'content-length',
  'content-range',
  'content-type',
  'etag',
  'expires',
  'last-modified',
] as const

const protocolVersion = 2
const requestHeaderSize = 11
const responseHeaderSize = 13
const requestHeaderPrefixSize = 3
const responseHeaderPrefixSize = 3
const responsePayloadLengthOffset = 9
const textDecoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })
const textEncoder = new TextEncoder()
const emptyText = new Uint8Array()
const responseHeaderIdByName = new Map<string, number>(
  HTTP_RELAY_RESPONSE_HEADER_NAMES.map((name, index) => [name, index]),
)

export function encodeHttpRelayRequest(request: HttpRelayRequest): Uint8Array<ArrayBuffer> {
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    throw new Error(`不支持的 HTTP method: ${String(request.method)}`)
  }

  const url = encodeText(request.url, 0xffff_ffff, 'URL UTF-8 长度')
  const headers = prepareRequestHeaders(request.headers)
  assertUint32(request.maxResponseBytes, 'maxResponseBytes')
  const bytes = new Uint8Array(requestHeaderSize + url.byteLength + headers.byteLength)
  const view = new DataView(bytes.buffer)
  bytes[0] = protocolVersion
  bytes[1] = request.method === 'GET' ? 0 : 1
  bytes[2] = headers.values.length
  view.setUint32(3, request.maxResponseBytes)
  view.setUint32(7, url.byteLength)
  bytes.set(url, requestHeaderSize)
  writeRequestHeaders(bytes, view, requestHeaderSize + url.byteLength, headers.values)
  return bytes
}

export function decodeHttpRelayRequest(data: ArrayBuffer | ArrayBufferView): HttpRelayRequest {
  const reader = new BinaryReader(data)
  reader.expectVersion()
  const methodCode = reader.readUint8()
  if (methodCode > 1) {
    throw new Error(`未知的 HTTP method 编码: ${methodCode}`)
  }
  const headerCount = reader.readUint8()
  const maxResponseBytes = reader.readUint32()
  const url = reader.readString(reader.readUint32())
  const headers = reader.readRequestHeaders(headerCount)
  reader.expectEnd()
  return {
    headers,
    maxResponseBytes,
    method: methodCode === 0 ? 'GET' : 'HEAD',
    url,
  }
}

export function createHttpRelayResponseBuffer(
  response: HttpRelayResponseHead,
  bodyCapacity: number,
): HttpRelayResponseBuffer {
  assertHttpStatus(response.status)
  assertUint32(bodyCapacity, 'bodyCapacity')
  const url = encodeText(response.url, 0xffff_ffff, 'URL UTF-8 长度')
  const headers = prepareResponseHeaderValues(response.headerValues)
  const bodyOffset = responseHeaderSize + url.byteLength + headers.byteLength
  const bytes = new Uint8Array(bodyOffset + bodyCapacity)
  const view = new DataView(bytes.buffer)
  bytes[0] = protocolVersion
  bytes[1] = HttpRelayResponseOutcome.HTTP
  view.setUint16(2, response.status)
  bytes[4] = headers.values.length
  view.setUint32(5, url.byteLength)
  view.setUint32(responsePayloadLengthOffset, 0)
  bytes.set(url, responseHeaderSize)
  writeResponseHeaders(bytes, view, responseHeaderSize + url.byteLength, headers.values)

  return {
    body: new Uint8Array(bytes.buffer, bodyOffset, bodyCapacity),
    finish(value) {
      const messageOffset = value.byteOffset - bodyOffset
      const messageLength = bodyOffset + value.byteLength
      if (
        value.byteLength > bodyCapacity ||
        messageOffset < 0 ||
        messageOffset + messageLength > value.buffer.byteLength
      ) {
        throw new Error('HTTP relay response body 超出预分配空间')
      }
      const messageView = new DataView(value.buffer, messageOffset, messageLength)
      messageView.setUint32(responsePayloadLengthOffset, value.byteLength)
      return new Uint8Array(value.buffer, messageOffset, messageLength)
    },
    finishEmpty() {
      return new Uint8Array(bytes.buffer, 0, bodyOffset)
    },
  }
}

export function encodeHttpRelayResponse(response: HttpRelayResponse): Uint8Array<ArrayBuffer> {
  if (response.outcome !== HttpRelayResponseOutcome.HTTP) {
    return encodeHttpRelayError(response.outcome, response.message)
  }
  const buffer = createHttpRelayResponseBuffer(
    {
      headerValues: collectResponseHeaderValues(response.headers),
      status: response.status,
      url: response.url,
    },
    response.body.byteLength,
  )
  buffer.body.set(response.body)
  return buffer.finish(buffer.body)
}

export function encodeHttpRelayError(
  outcome: HttpRelayResponseOutcome,
  message: string,
): Uint8Array<ArrayBuffer> {
  if (
    outcome < HttpRelayResponseOutcome.INVALID_REQUEST ||
    outcome > HttpRelayResponseOutcome.INTERNAL_FAILURE
  ) {
    throw new Error(`无效的 HTTP relay error outcome: ${outcome}`)
  }
  const payload = encodeText(message, 0xffff_ffff, 'error message UTF-8 长度')
  const bytes = new Uint8Array(responseHeaderSize + payload.byteLength)
  const view = new DataView(bytes.buffer)
  bytes[0] = protocolVersion
  bytes[1] = outcome
  view.setUint32(responsePayloadLengthOffset, payload.byteLength)
  bytes.set(payload, responseHeaderSize)
  return bytes
}

export function decodeHttpRelayResponse(data: ArrayBuffer | ArrayBufferView): HttpRelayResponse {
  const reader = new BinaryReader(data)
  reader.expectVersion()
  const outcome = reader.readUint8()
  if (outcome > HttpRelayResponseOutcome.INTERNAL_FAILURE) {
    throw new Error(`未知的 HTTP relay response outcome: ${outcome}`)
  }
  const status = reader.readUint16()
  const headerCount = reader.readUint8()
  const urlLength = reader.readUint32()
  const payloadLength = reader.readUint32()
  const url = reader.readString(urlLength)
  const headers = reader.readResponseHeaders(headerCount)
  const payload = reader.readBytes(payloadLength)
  reader.expectEnd()

  if (outcome === HttpRelayResponseOutcome.HTTP) {
    assertHttpStatus(status)
    return { body: payload, headers, message: '', outcome, status, url }
  }
  if (status !== 0 || headerCount !== 0 || urlLength !== 0) {
    throw new Error('HTTP relay error response 包含无效 metadata')
  }
  return {
    body: payload.subarray(0, 0),
    headers,
    message: decodeText(payload),
    outcome,
    status,
    url,
  }
}

function prepareRequestHeaders(headers: readonly HttpRelayHeader[]): EncodedRequestHeaders {
  assertUint8(headers.length, 'request header 数量')
  const values = headers.map(header => ({
    name: encodeText(header.name, 0xff, 'request header name UTF-8 长度'),
    value: encodeText(header.value, 0xffff, 'request header value UTF-8 长度'),
  }))
  return {
    byteLength: values.reduce(
      (total, header) =>
        total + requestHeaderPrefixSize + header.name.byteLength + header.value.byteLength,
      0,
    ),
    values,
  }
}

function prepareResponseHeaderValues(
  headerValues: readonly (string | null)[],
): EncodedResponseHeaders {
  if (headerValues.length > HTTP_RELAY_RESPONSE_HEADER_NAMES.length) {
    throw new Error(`HTTP relay response header value 数量无效: ${headerValues.length}`)
  }
  const values: EncodedResponseHeader[] = []
  for (let id = 0; id < headerValues.length; id += 1) {
    const value = headerValues[id]
    if (value !== null && value !== undefined) {
      values.push({
        id,
        value: encodeText(value, 0xffff, 'response header value UTF-8 长度'),
      })
    }
  }
  return {
    byteLength: values.reduce(
      (total, header) => total + responseHeaderPrefixSize + header.value.byteLength,
      0,
    ),
    values,
  }
}

function collectResponseHeaderValues(headers: readonly HttpRelayHeader[]): (string | null)[] {
  const values = new Array<string | null>(HTTP_RELAY_RESPONSE_HEADER_NAMES.length).fill(null)
  for (const header of headers) {
    const id = responseHeaderIdByName.get(header.name)
    if (id === undefined) {
      throw new Error(`HTTP relay response header 不受支持: ${header.name}`)
    }
    values[id] = header.value
  }
  return values
}

function writeRequestHeaders(
  bytes: Uint8Array<ArrayBuffer>,
  view: DataView<ArrayBuffer>,
  initialOffset: number,
  headers: readonly EncodedRequestHeader[],
): void {
  let offset = initialOffset
  for (const header of headers) {
    bytes[offset] = header.name.byteLength
    view.setUint16(offset + 1, header.value.byteLength)
    offset += requestHeaderPrefixSize
    bytes.set(header.name, offset)
    offset += header.name.byteLength
    bytes.set(header.value, offset)
    offset += header.value.byteLength
  }
}

function writeResponseHeaders(
  bytes: Uint8Array<ArrayBuffer>,
  view: DataView<ArrayBuffer>,
  initialOffset: number,
  headers: readonly EncodedResponseHeader[],
): void {
  let offset = initialOffset
  for (const header of headers) {
    bytes[offset] = header.id
    view.setUint16(offset + 1, header.value.byteLength)
    offset += responseHeaderPrefixSize
    bytes.set(header.value, offset)
    offset += header.value.byteLength
  }
}

function encodeText(value: string, maxBytes: number, name: string): Uint8Array {
  if (value.length === 0) {
    return emptyText
  }
  const bytes = textEncoder.encode(value)
  if (bytes.byteLength > maxBytes) {
    throw new Error(`${name} 超出编码范围: ${bytes.byteLength}`)
  }
  return bytes
}

function decodeText(bytes: Uint8Array): string {
  return bytes.byteLength === 0 ? '' : textDecoder.decode(bytes)
}

function assertHttpStatus(status: number): void {
  if (!Number.isSafeInteger(status) || status < 100 || status > 599) {
    throw new Error(`HTTP response status 无效: ${status}`)
  }
}

function assertUint8(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xff) {
    throw new Error(`${name} 超出 uint8 范围: ${value}`)
  }
}

function assertUint32(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new Error(`${name} 超出 uint32 范围: ${value}`)
  }
}

class BinaryReader {
  private readonly bytes: Uint8Array
  private offset = 0
  private readonly view: DataView

  constructor(data: ArrayBuffer | ArrayBufferView) {
    this.bytes =
      data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    this.view = new DataView(this.bytes.buffer, this.bytes.byteOffset, this.bytes.byteLength)
  }

  expectVersion(): void {
    const version = this.readUint8()
    if (version !== protocolVersion) {
      throw new Error(`不支持的 HTTP relay protocol version: ${version}`)
    }
  }

  expectEnd(): void {
    if (this.offset !== this.bytes.byteLength) {
      throw new Error('HTTP relay message 包含多余数据')
    }
  }

  readBytes(length: number): Uint8Array {
    this.ensureAvailable(length)
    const value = this.bytes.subarray(this.offset, this.offset + length)
    this.offset += length
    return value
  }

  readRequestHeaders(count: number): HttpRelayHeader[] {
    const headers: HttpRelayHeader[] = []
    for (let index = 0; index < count; index += 1) {
      const nameLength = this.readUint8()
      const valueLength = this.readUint16()
      headers.push({
        name: this.readString(nameLength),
        value: this.readString(valueLength),
      })
    }
    return headers
  }

  readResponseHeaders(count: number): HttpRelayHeader[] {
    const headers: HttpRelayHeader[] = []
    for (let index = 0; index < count; index += 1) {
      const id = this.readUint8()
      const name = HTTP_RELAY_RESPONSE_HEADER_NAMES[id]
      if (name === undefined) {
        throw new Error(`未知的 HTTP relay response header ID: ${id}`)
      }
      headers.push({
        name,
        value: this.readString(this.readUint16()),
      })
    }
    return headers
  }

  readString(length: number): string {
    return decodeText(this.readBytes(length))
  }

  readUint8(): number {
    this.ensureAvailable(1)
    return this.bytes[this.offset++] ?? 0
  }

  readUint16(): number {
    this.ensureAvailable(2)
    const value = this.view.getUint16(this.offset)
    this.offset += 2
    return value
  }

  readUint32(): number {
    this.ensureAvailable(4)
    const value = this.view.getUint32(this.offset)
    this.offset += 4
    return value
  }

  private ensureAvailable(length: number): void {
    if (length > this.bytes.byteLength - this.offset) {
      throw new Error('HTTP relay message 长度无效')
    }
  }
}
