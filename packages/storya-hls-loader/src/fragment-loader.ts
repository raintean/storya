import type {
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderStats,
} from 'hls.js'
import type { HttpTransportResponse } from 'storya-transport'
import { VirtualStreamReadFailure, VirtualStreamSegmentReader } from './virtual-stream'
import type { VirtualStreamSegmentResult } from './virtual-stream'
import { copyVirtualStreamStatistics, createLoaderStats } from './stats'

export interface FragmentLoaderSession {
  configure(config: HlsConfig): void
  read(
    context: FragmentLoaderContext,
    config: LoaderConfiguration,
    stats: LoaderStats,
  ): VirtualStreamSegmentReader
}

export function createVirtualFragmentLoader(
  session: FragmentLoaderSession,
): FragmentLoaderConstructor {
  return class StoryaVirtualFragmentLoader implements Loader<FragmentLoaderContext> {
    context: FragmentLoaderContext | null = null
    stats: LoaderStats = createLoaderStats()

    private callbacks: LoaderCallbacks<FragmentLoaderContext> | undefined
    private loadTimer: number | undefined
    private networkDetails: HttpTransportResponse | null = null
    private progressive = false
    private reader: VirtualStreamSegmentReader | undefined
    private settled = false

    constructor(config: HlsConfig) {
      session.configure(config)
    }

    load(
      context: FragmentLoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<FragmentLoaderContext>,
    ): void {
      if (this.reader !== undefined) {
        throw new Error('fLoader 的实例只能加载一次')
      }
      this.context = context
      this.callbacks = callbacks
      this.progressive = callbacks.onProgress !== undefined && Number.isFinite(config.highWaterMark)
      const reader = session.read(context, config, this.stats)
      this.reader = reader
      const maxLoadTimeMs = config.loadPolicy.maxLoadTimeMs
      if (Number.isFinite(maxLoadTimeMs) && maxLoadTimeMs > 0) {
        this.loadTimer = globalThis.setTimeout(() => this.timeout(), maxLoadTimeMs)
      }
      void reader.result.then(
        result => this.succeed(result),
        cause => this.fail(cause),
      )
    }

    abort(): void {
      if (this.settled || this.reader === undefined || this.context === null) {
        return
      }
      this.settled = true
      this.clearLoadTimer()
      this.stats.aborted = true
      this.stats.loading.end = performance.now()
      this.reader.cancel()
      this.callbacks?.onAbort?.(this.stats, this.context, this.networkDetails)
    }

    destroy(): void {
      if (!this.settled) {
        this.reader?.cancel()
      }
      this.clearLoadTimer()
      this.settled = true
      this.callbacks = undefined
      this.reader = undefined
      this.context = null
    }

    getCacheAge(): number | null {
      const age = this.networkDetails?.headers.get('age')
      return age === null || age === undefined ? null : Number.parseFloat(age)
    }

    getResponseHeader(name: string): string | null {
      return this.networkDetails?.headers.get(name) ?? null
    }

    private succeed(result: VirtualStreamSegmentResult): void {
      if (this.settled || this.context === null) {
        return
      }
      this.settled = true
      this.clearLoadTimer()
      this.networkDetails = result.networkDetails
      copyVirtualStreamStatistics(this.stats, result.statistics)
      const callbacks = this.callbacks
      const data = result.data.slice(0)
      if (callbacks?.onProgress !== undefined) {
        callbacks.onProgress(this.stats, this.context, data, result.networkDetails)
      }
      callbacks?.onSuccess(
        this.progressive
          ? { code: result.code, data: new ArrayBuffer(0), url: result.url }
          : { code: result.code, data, url: result.url },
        this.stats,
        this.context,
        result.networkDetails,
      )
    }

    private fail(cause: unknown): void {
      if (this.settled || this.context === null) {
        return
      }
      this.settled = true
      this.clearLoadTimer()

      if (cause instanceof VirtualStreamReadFailure) {
        this.networkDetails = cause.networkDetails
        if (cause.kind === 'timeout') {
          this.callbacks?.onTimeout(this.stats, this.context, cause.networkDetails)
          return
        }
        if (cause.kind === 'aborted') {
          this.stats.aborted = true
          this.callbacks?.onAbort?.(this.stats, this.context, cause.networkDetails)
          return
        }
        this.callbacks?.onError(
          { code: cause.code, text: cause.message },
          this.context,
          cause.networkDetails,
          this.stats,
        )
        return
      }

      const error = cause instanceof Error ? cause : new Error('未知 Segment 加载错误')
      this.callbacks?.onError(
        { code: 0, text: error.message },
        this.context,
        this.networkDetails,
        this.stats,
      )
    }

    private timeout(): void {
      if (this.settled || this.context === null) {
        return
      }
      this.settled = true
      this.loadTimer = undefined
      this.stats.loading.end = performance.now()
      this.reader?.cancel()
      this.callbacks?.onTimeout(this.stats, this.context, this.networkDetails)
    }

    private clearLoadTimer(): void {
      if (this.loadTimer !== undefined) {
        globalThis.clearTimeout(this.loadTimer)
        this.loadTimer = undefined
      }
    }
  }
}
