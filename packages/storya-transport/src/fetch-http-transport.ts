import type {
  HttpTransport,
  HttpTransportRequestOptions,
  HttpTransportResponse,
} from './http-transport'
import { HttpTransportFailure } from './http-transport'
import { TransportStatistics } from './transport-statistics'

export type FetchFunction = (request: Request) => Promise<Response>

export class FetchHttpTransport implements HttpTransport {
  private destroyed = false
  private readonly fetcher: FetchFunction
  private readonly statistics: TransportStatistics

  constructor(fetcher: FetchFunction = request => fetch(request)) {
    this.fetcher = fetcher
    this.statistics = new TransportStatistics('FetchHttpTransport')
  }

  async request(
    request: Request,
    _options?: HttpTransportRequestOptions,
  ): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      throw new HttpTransportFailure('destroyed', 'Fetch transport 已经销毁')
    }
    const statistics = this.statistics.startRequest()
    try {
      const response = await this.fetcher(request)
      return statistics.trackResponse(response, request.method !== 'HEAD')
    } catch (error) {
      statistics.reject(error)
      throw error
    }
  }

  destroy(): void {
    this.destroyed = true
    this.statistics.destroy()
  }
}
