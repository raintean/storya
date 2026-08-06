export interface HttpTransportRequestOptions {
  maxResponseBytes?: number
}

export interface HttpTransportResponse {
  readonly body: ReadableStream<Uint8Array> | null
  readonly headers: Headers
  readonly ok: boolean
  readonly status: number
  readonly statusText: string
  readonly url: string

  arrayBuffer(): Promise<ArrayBuffer>
}

export type HttpTransportRangeRequestMode = 'resumable' | 'stable'

export interface HttpTransport {
  readonly rangeRequestMode?: HttpTransportRangeRequestMode

  request(request: Request, options?: HttpTransportRequestOptions): Promise<HttpTransportResponse>
  destroy(): void
}

export type HttpTransportFailureCode =
  | 'aborted'
  | 'connection-failed'
  | 'destroyed'
  | 'invalid-response'
  | 'protocol-error'
  | 'response-too-large'
  | 'upstream-failure'

export class HttpTransportFailure extends Error {
  readonly code: HttpTransportFailureCode

  constructor(code: HttpTransportFailureCode, message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'HttpTransportFailure'
    this.code = code
  }
}

export function createAbortError(message = 'HTTP transport 请求已取消'): DOMException {
  return new DOMException(message, 'AbortError')
}
