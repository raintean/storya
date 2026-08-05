import type { HttpTransportResponse } from './http-transport'

export class WebSocketHttpResponse implements HttpTransportResponse {
  readonly body: ReadableStream<Uint8Array> | null
  readonly headers: Headers
  readonly status: number
  readonly statusText: string
  readonly url: string

  constructor(
    body: ReadableStream<Uint8Array> | null,
    init: {
      headers: Headers
      status: number
      statusText: string
      url: string
    },
  ) {
    this.body = body
    this.headers = init.headers
    this.status = init.status
    this.statusText = init.statusText
    this.url = init.url
  }

  get ok(): boolean {
    return this.status >= 200 && this.status < 300
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    if (this.body === null) {
      return new ArrayBuffer(0)
    }

    const reader = this.body.getReader()
    const chunks: Uint8Array[] = []
    let length = 0
    while (true) {
      const result = await reader.read()
      if (result.done) {
        break
      }
      const chunk = result.value.slice()
      chunks.push(chunk)
      length += chunk.byteLength
    }

    const data = new Uint8Array(length)
    let offset = 0
    for (const chunk of chunks) {
      data.set(chunk, offset)
      offset += chunk.byteLength
    }
    return data.buffer
  }
}
