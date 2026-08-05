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
import { SegmentFillFailure } from './stream-filler'
import type { VirtualSegmentResult } from './virtual-stream'
import { copyLoaderStats, createLoaderStats } from './stats'

export type SegmentReaderReleaseReason = 'aborted' | 'failed' | 'succeeded'

export interface SegmentReader {
  promise: Promise<VirtualSegmentResult>
  release(reason: SegmentReaderReleaseReason): void
}

export interface FragmentLoaderSession {
  configure(config: HlsConfig): void
  read(
    context: FragmentLoaderContext,
    config: LoaderConfiguration,
    stats: LoaderStats,
  ): SegmentReader
}

export function createVirtualFragmentLoader(
  session: FragmentLoaderSession,
): FragmentLoaderConstructor {
  return class StoryaVirtualFragmentLoader implements Loader<FragmentLoaderContext> {
    context: FragmentLoaderContext | null = null
    stats: LoaderStats = createLoaderStats()

    private callbacks: LoaderCallbacks<FragmentLoaderContext> | undefined
    private networkDetails: HttpTransportResponse | null = null
    private progressive = false
    private reader: SegmentReader | undefined
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
      void reader.promise.then(
        result => this.succeed(result),
        cause => this.fail(cause),
      )
    }

    abort(): void {
      if (this.settled || this.reader === undefined || this.context === null) {
        return
      }
      this.settled = true
      this.stats.aborted = true
      this.stats.loading.end = performance.now()
      this.reader.release('aborted')
      this.callbacks?.onAbort?.(this.stats, this.context, this.networkDetails)
    }

    destroy(): void {
      if (!this.settled) {
        this.reader?.release('aborted')
      }
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

    private succeed(result: VirtualSegmentResult): void {
      if (this.settled || this.context === null || this.reader === undefined) {
        return
      }
      this.settled = true
      this.networkDetails = result.networkDetails
      copyLoaderStats(this.stats, result.stats)
      this.reader.release('succeeded')
      const callbacks = this.callbacks
      const data = cloneResponseData(result.response.data)
      if (callbacks?.onProgress !== undefined && data instanceof ArrayBuffer) {
        callbacks.onProgress(this.stats, this.context, data, result.networkDetails)
      }
      callbacks?.onSuccess(
        this.progressive
          ? { ...result.response, data: new ArrayBuffer(0) }
          : { ...result.response, data },
        this.stats,
        this.context,
        result.networkDetails,
      )
    }

    private fail(cause: unknown): void {
      if (this.settled || this.context === null || this.reader === undefined) {
        return
      }
      this.settled = true
      this.reader.release('failed')

      if (cause instanceof SegmentFillFailure) {
        this.networkDetails = cause.networkDetails
        copyLoaderStats(this.stats, cause.stats)
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
  }
}

function cloneResponseData(data: VirtualSegmentResult['response']['data']): ArrayBuffer {
  if (!(data instanceof ArrayBuffer)) {
    throw new Error('虚拟 Segment 中不存在可交付的 ArrayBuffer')
  }
  return data.slice(0)
}
