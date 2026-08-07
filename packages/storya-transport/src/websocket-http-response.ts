import type { HttpTransportResponse } from './http-transport'

export class WebSocketHttpResponse implements HttpTransportResponse {
  readonly body: ReadableStream<Uint8Array> | null
  readonly headers: Headers
  readonly status: number
  readonly statusText = ''
  readonly url: string

  constructor(
    body: Uint8Array | null,
    init: {
      headers: Headers
      status: number
      url: string
    },
  ) {
    this.headers = init.headers
    this.status = init.status
    this.url = init.url
    this.body =
      body === null
        ? null
        : new ReadableStream<Uint8Array>({
            start(controller) {
              if (body.byteLength !== 0) {
                controller.enqueue(body)
              }
              controller.close()
            },
          })
  }

  get ok(): boolean {
    return this.status >= 200 && this.status < 300
  }

  arrayBuffer(): Promise<ArrayBuffer> {
    return this.body === null
      ? Promise.resolve(new ArrayBuffer(0))
      : new Response(this.body).arrayBuffer()
  }
}
