import type {
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  Loader as HlsLoader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderStats,
} from 'hls.js'
import type { ParallelSegmentLoader } from './parallel-segment-loader'
import { createLoaderStats } from './stats'
import type {
  SegmentLoadFailure,
  VirtualStreamSegment,
  VirtualStreamSegmentOutcome,
} from './virtual-stream-segment'

export function createStoryaFragmentLoader(
  loader: ParallelSegmentLoader,
): FragmentLoaderConstructor {
  return class StoryaFragmentLoader implements HlsLoader<FragmentLoaderContext> {
    context: FragmentLoaderContext | null = null
    stats: LoaderStats = createLoaderStats()

    private callbacks: LoaderCallbacks<FragmentLoaderContext> | undefined
    private initialRetry = 0
    private loadStartedAt = 0
    private networkDetails: Response | null = null
    private progressive = false
    private reading = false
    private settled = false
    private timeoutTimer: ReturnType<typeof globalThis.setTimeout> | undefined
    private unsubscribe: (() => void) | undefined

    constructor(config: HlsConfig) {
      loader.configure(config)
    }

    load(
      context: FragmentLoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<FragmentLoaderContext>,
    ): void {
      if (this.context !== null) {
        throw new Error('fLoader 实例只能加载一个 Segment')
      }

      this.context = context
      this.callbacks = callbacks
      this.initialRetry = this.stats.retry
      this.loadStartedAt = performance.now()
      this.stats.loading.start = this.loadStartedAt
      this.progressive = callbacks.onProgress !== undefined && Number.isFinite(config.highWaterMark)

      loader.update(state => {
        const streamId = `${context.frag.type}:${context.frag.level}`
        state.ensureStream(streamId).ensureSegment(context, loader.chunkSize).startReading()
        return undefined
      })
      this.reading = true
      this.unsubscribe = loader.subscribe(() => this.checkSegment())

      const maxLoadTimeMs = config.loadPolicy.maxLoadTimeMs
      if (Number.isFinite(maxLoadTimeMs) && maxLoadTimeMs > 0) {
        this.timeoutTimer = globalThis.setTimeout(() => this.timeout(), maxLoadTimeMs)
      }
      this.checkSegment()
    }

    abort(): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      const callbacks = this.callbacks
      this.stats.aborted = true
      this.stats.loading.end = performance.now()
      this.settle()
      callbacks?.onAbort?.(this.stats, context, this.networkDetails)
    }

    destroy(): void {
      if (!this.settled) {
        this.settle()
      }
      this.callbacks = undefined
      this.context = null
    }

    getCacheAge(): number | null {
      const age = this.networkDetails?.headers.get('age')
      return age === null || age === undefined ? null : Number.parseFloat(age)
    }

    getResponseHeader(name: string): string | null {
      return this.networkDetails?.headers.get(name) ?? null
    }

    private checkSegment(): void {
      if (this.settled || this.context === null) {
        return
      }
      if (loader.state.destroyed) {
        this.fail({
          code: 0,
          message: 'ParallelSegmentLoader 已经销毁',
          response: null,
        })
        return
      }

      const segment = loader.state.locateSegment(this.context)
      if (segment === undefined) {
        this.fail({ code: 0, message: '读取的 Segment 不存在', response: null })
        return
      }

      this.updateStats(segment)
      if (segment.outcome.type === 'ready') {
        this.succeed(segment.outcome)
      } else if (segment.outcome.type === 'failed') {
        this.fail(segment.outcome.failure)
      }
    }

    private succeed(result: Extract<VirtualStreamSegmentOutcome, { type: 'ready' }>): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      const callbacks = this.callbacks
      const data = result.data.slice(0)
      this.networkDetails = result.response
      this.settle()

      if (this.progressive && callbacks?.onProgress !== undefined) {
        callbacks.onProgress(this.stats, context, data, result.response)
      }
      callbacks?.onSuccess(
        {
          code: result.code,
          data: this.progressive ? new ArrayBuffer(0) : data,
          url: result.url,
        },
        this.stats,
        context,
        result.response,
      )
    }

    private fail(failure: SegmentLoadFailure): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      const callbacks = this.callbacks
      this.networkDetails = failure.response
      this.settle()
      callbacks?.onError(
        { code: failure.code, text: failure.message },
        context,
        failure.response,
        this.stats,
      )
    }

    private timeout(): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      const callbacks = this.callbacks
      this.stats.loading.end = performance.now()
      this.settle()
      callbacks?.onTimeout(this.stats, context, this.networkDetails)
    }

    private settle(): void {
      this.settled = true
      this.unsubscribe?.()
      this.unsubscribe = undefined
      if (this.timeoutTimer !== undefined) {
        globalThis.clearTimeout(this.timeoutTimer)
        this.timeoutTimer = undefined
      }

      if (!this.reading || this.context === null) {
        return
      }
      this.reading = false
      if (loader.state.destroyed) {
        return
      }

      const context = this.context
      loader.update(state => {
        state.locateSegment(context)?.stopReading()
        return undefined
      })
    }

    private updateStats(segment: VirtualStreamSegment): void {
      const completedAt = segment.outcome.type === 'pending' ? 0 : segment.outcome.completedAt
      const loaded = segment.loadedBytes
      const segmentStartedAt = segment.startedAt ?? 0
      const end = completedAt || performance.now()

      this.stats.loaded = loaded
      this.stats.retry = this.initialRetry + segment.retryCount
      this.stats.total = segment.length ?? 0
      this.stats.chunkCount = segment.chunks.filter(chunk => chunk.state === 'ready').length
      const elapsed = end - segmentStartedAt
      const bandwidthEstimate = loader.bandwidthEstimate
      if (segment.outcome.type === 'ready' && segmentStartedAt > 0 && loaded > 0) {
        // hls.js 会用 loading.start 到 parsing.end 采样带宽, 因此必须排除预加载后的缓存驻留时间
        const deliveredAt = performance.now()
        const actualTransferDuration = Math.max(elapsed, 1)
        const transferDuration =
          bandwidthEstimate > 0 ? (loaded * 8 * 1000) / bandwidthEstimate : actualTransferDuration
        const timeToFirstByte =
          segment.firstByteAt === undefined
            ? 0
            : Math.max(0, segment.firstByteAt - segmentStartedAt)
        this.stats.loading.end = deliveredAt
        this.stats.loading.first = deliveredAt - transferDuration
        this.stats.loading.start = this.stats.loading.first - timeToFirstByte
        this.stats.bwEstimate =
          bandwidthEstimate > 0 ? bandwidthEstimate : (loaded * 8 * 1000) / actualTransferDuration
        return
      }

      this.stats.loading.start = this.loadStartedAt
      this.stats.loading.first =
        segment.firstByteAt === undefined ? 0 : Math.max(this.loadStartedAt, segment.firstByteAt)
      this.stats.loading.end = completedAt
      this.stats.bwEstimate =
        segmentStartedAt > 0 && elapsed > 0 ? (loaded * 8 * 1000) / elapsed : 0
    }
  }
}
