import type { HlsConfig, LoaderConfiguration, LoaderStats } from 'hls.js'
import type { HttpTransport, HttpTransportResponse } from 'storya-transport'
import type { HlsLoaderDiagnosticChunk } from './diagnostics'
import type { HlsLoaderEventHandler } from './events'
import { SegmentLoader } from './segment-loader'
import type { SegmentLoaderOptions } from './segment-loader'
import { RequestScheduler } from './scheduler'
import type { VirtualSegment, VirtualSegmentResult } from './virtual-stream'

export type SegmentFillFailureKind = 'aborted' | 'error' | 'timeout'

export class SegmentFillFailure extends Error {
  readonly code: number
  readonly kind: SegmentFillFailureKind
  readonly networkDetails: HttpTransportResponse | null
  readonly stats: LoaderStats

  constructor(
    kind: SegmentFillFailureKind,
    message: string,
    code: number,
    stats: LoaderStats,
    networkDetails: HttpTransportResponse | null,
  ) {
    super(message)
    this.name = 'SegmentFillFailure'
    this.kind = kind
    this.code = code
    this.stats = stats
    this.networkDetails = networkDetails
  }
}

interface ActiveFill {
  cancelled: boolean
  loader: SegmentLoader
  prefetch: boolean
  promise: Promise<VirtualSegmentResult>
}

export class StreamFiller {
  private readonly active = new Map<VirtualSegment, ActiveFill>()
  private hlsConfig: HlsConfig | undefined
  private readonly onEvent: HlsLoaderEventHandler
  private readonly options: SegmentLoaderOptions
  private readonly scheduler: RequestScheduler
  private readonly transport: HttpTransport

  constructor(
    scheduler: RequestScheduler,
    options: SegmentLoaderOptions,
    onEvent: HlsLoaderEventHandler,
    transport: HttpTransport,
  ) {
    this.scheduler = scheduler
    this.options = options
    this.onEvent = onEvent
    this.transport = transport
  }

  configure(hlsConfig: HlsConfig): void {
    this.hlsConfig ??= hlsConfig
  }

  ensure(
    segment: VirtualSegment,
    config: LoaderConfiguration,
    prefetch: boolean,
  ): Promise<VirtualSegmentResult> {
    const ready = segment.result
    if (segment.state === 'ready' && ready !== undefined) {
      return Promise.resolve(ready)
    }

    const current = this.active.get(segment)
    if (current !== undefined) {
      if (!prefetch) {
        current.prefetch = false
        this.scheduler.notify()
      }
      return current.promise
    }

    const hlsConfig = this.hlsConfig
    if (hlsConfig === undefined) {
      return Promise.reject(new Error('HLS 并行加载器尚未取得 hls.js 配置'))
    }

    segment.markFilling()
    let resolvePromise: (result: VirtualSegmentResult) => void = () => undefined
    let rejectPromise: (error: SegmentFillFailure) => void = () => undefined
    const promise = new Promise<VirtualSegmentResult>((resolve, reject) => {
      resolvePromise = resolve
      rejectPromise = reject
    })
    void promise.catch(() => undefined)

    const loader = new SegmentLoader(
      hlsConfig,
      this.scheduler,
      this.options,
      this.transport,
      stats => {
        segment.updateStats(stats)
      },
      () => segment.hardDemands > 0,
    )
    const fill: ActiveFill = { cancelled: false, loader, prefetch, promise }
    this.active.set(segment, fill)
    if (prefetch) {
      this.emitSegmentEvent(segment, 'prefetch-started')
    }

    loader.load(segment.context, config, {
      onAbort: (stats, _context, networkDetails) => {
        this.finishFailure(
          segment,
          fill,
          new SegmentFillFailure('aborted', 'Segment 填充已取消', 0, stats, networkDetails),
          rejectPromise,
        )
      },
      onError: (error, _context, networkDetails, stats) => {
        this.finishFailure(
          segment,
          fill,
          new SegmentFillFailure('error', error.text, error.code, stats, networkDetails),
          rejectPromise,
        )
      },
      onSuccess: (response, stats, _context, networkDetails) => {
        if (!(response.data instanceof ArrayBuffer)) {
          this.finishFailure(
            segment,
            fill,
            new SegmentFillFailure(
              'error',
              'Segment 下载没有返回 ArrayBuffer',
              response.code ?? 0,
              stats,
              networkDetails,
            ),
            rejectPromise,
          )
          return
        }

        const result: VirtualSegmentResult = { networkDetails, response, stats }
        this.active.delete(segment)
        segment.updateChunks(loader.getDiagnostics())
        segment.markReady(result)
        if (fill.prefetch) {
          this.emitSegmentEvent(segment, 'prefetch-ready')
        }
        resolvePromise(result)
        loader.destroy()
      },
      onTimeout: (stats, _context, networkDetails) => {
        this.finishFailure(
          segment,
          fill,
          new SegmentFillFailure(
            'timeout',
            `Segment 加载超过 ${config.loadPolicy.maxLoadTimeMs}ms`,
            0,
            stats,
            networkDetails,
          ),
          rejectPromise,
        )
      },
    })

    return promise
  }

  cancel(segment: VirtualSegment): void {
    const fill = this.active.get(segment)
    if (fill === undefined) {
      segment.clearData()
      return
    }
    fill.cancelled = true
    if (fill.prefetch) {
      this.emitSegmentEvent(segment, 'prefetch-cancelled')
    }
    fill.loader.abort()
  }

  destroy(): void {
    for (const segment of [...this.active.keys()]) {
      this.cancel(segment)
    }
    this.active.clear()
  }

  getSegmentDiagnostics(segment: VirtualSegment): HlsLoaderDiagnosticChunk[] {
    return this.active.get(segment)?.loader.getDiagnostics() ?? segment.chunks
  }

  private finishFailure(
    segment: VirtualSegment,
    fill: ActiveFill,
    failure: SegmentFillFailure,
    reject: (error: SegmentFillFailure) => void,
  ): void {
    if (this.active.get(segment) !== fill) {
      return
    }
    this.active.delete(segment)
    segment.updateChunks(fill.loader.getDiagnostics())
    if (fill.cancelled) {
      segment.markEmpty(failure.stats)
    } else {
      segment.markFailed(failure.stats)
    }
    reject(failure)
    fill.loader.destroy()
  }

  private emitSegmentEvent(
    segment: VirtualSegment,
    action: 'prefetch-cancelled' | 'prefetch-ready' | 'prefetch-started',
  ): void {
    try {
      this.onEvent({
        action,
        segmentSn: segment.fragment.sn,
        segmentStart: segment.start,
        streamId: segment.stream.id,
        timestamp: Date.now(),
        type: 'segment-state',
        url: segment.context.url,
      })
    } catch {
      // 观测回调不能影响填充流程
    }
  }
}
