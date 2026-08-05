import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { HttpTransportFailure } from './http-transport'

export type FetchFunction = (request: Request) => Promise<Response>

export class FetchHttpTransport implements HttpTransport {
  private destroyed = false
  private readonly fetcher: FetchFunction

  constructor(fetcher: FetchFunction = request => fetch(request)) {
    this.fetcher = fetcher
  }

  request(
    request: Request,
    _options?: HttpTransportRequestOptions,
  ): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      return Promise.reject(new HttpTransportFailure('destroyed', 'Fetch transport 已经销毁'))
    }
    return this.fetcher(request)
  }

  destroy(): void {
    this.destroyed = true
  }
}
