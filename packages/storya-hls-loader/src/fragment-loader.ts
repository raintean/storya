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
import { copyLoaderStats, createLoaderStats } from './stats'

export interface SegmentLoadFailure {
  code: number
  message: string
  response: Response | null
}

export type SegmentObservation =
  | { state: 'pending'; stats: LoaderStats }
  | { failure: SegmentLoadFailure; state: 'failed'; stats: LoaderStats }
  | {
      code: number
      data: ArrayBuffer
      response: Response
      state: 'ready'
      stats: LoaderStats
      url: string
    }

export function createStoryaFragmentLoader(
  loader: ParallelSegmentLoader,
): FragmentLoaderConstructor {
  return class StoryaFragmentLoader implements HlsLoader<FragmentLoaderContext> {
    context: FragmentLoaderContext | null = null
    stats: LoaderStats = createLoaderStats()

    private callbacks: LoaderCallbacks<FragmentLoaderContext> | undefined
    private readonly changeController = new AbortController()
    private networkDetails: Response | null = null
    private reading = false
    private settled = false
    private timeoutTimer: ReturnType<typeof globalThis.setTimeout> | undefined

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
      this.reading = true
      loader.startReading(context)

      const maxLoadTimeMs = config.loadPolicy.maxLoadTimeMs
      if (Number.isFinite(maxLoadTimeMs) && maxLoadTimeMs > 0) {
        this.timeoutTimer = globalThis.setTimeout(() => this.timeout(), maxLoadTimeMs)
      }
      void this.observe(config)
    }

    abort(): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      this.settled = true
      this.clearTimeout()
      this.changeController.abort()
      this.stopReading()
      this.stats.aborted = true
      this.stats.loading.end = performance.now()
      this.callbacks?.onAbort?.(this.stats, context, this.networkDetails)
    }

    destroy(): void {
      this.settled = true
      this.clearTimeout()
      this.changeController.abort()
      this.stopReading()
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

    private async observe(config: LoaderConfiguration): Promise<void> {
      while (!this.settled && this.context !== null) {
        const revision = loader.revision
        const observation = loader.inspectSegment(this.context)
        copyLoaderStats(this.stats, observation.stats)

        if (observation.state === 'ready') {
          this.succeed(observation, config)
          return
        }
        if (observation.state === 'failed') {
          this.fail(observation.failure)
          return
        }

        try {
          await loader.waitForChange(revision, this.changeController.signal)
        } catch {
          return
        }
      }
    }

    private succeed(
      result: Extract<SegmentObservation, { state: 'ready' }>,
      config: LoaderConfiguration,
    ): void {
      if (this.settled || this.context === null) {
        return
      }

      const context = this.context
      const callbacks = this.callbacks
      const data = result.data.slice(0)
      const progressive =
        callbacks?.onProgress !== undefined && Number.isFinite(config.highWaterMark)

      this.settled = true
      this.clearTimeout()
      this.networkDetails = result.response
      this.stopReading()
      if (callbacks?.onProgress !== undefined) {
        callbacks.onProgress(this.stats, context, data, result.response)
      }
      callbacks?.onSuccess(
        {
          code: result.code,
          data: progressive ? new ArrayBuffer(0) : data,
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
      this.settled = true
      this.clearTimeout()
      this.networkDetails = failure.response
      this.stopReading()
      this.callbacks?.onError(
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
      this.settled = true
      this.timeoutTimer = undefined
      this.changeController.abort()
      this.stopReading()
      this.stats.loading.end = performance.now()
      this.callbacks?.onTimeout(this.stats, context, this.networkDetails)
    }

    private clearTimeout(): void {
      if (this.timeoutTimer === undefined) {
        return
      }
      globalThis.clearTimeout(this.timeoutTimer)
      this.timeoutTimer = undefined
    }

    private stopReading(): void {
      if (!this.reading || this.context === null) {
        return
      }
      this.reading = false
      loader.stopReading(this.context)
    }
  }
}
