import Hls from 'hls.js'
import type {
  AudioTrackLoadedData,
  FragBufferedData,
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  LevelLoadedData,
  LoaderConfiguration,
  LoaderStats,
} from 'hls.js'
import type { HlsLoaderEventHandler, HlsLoaderSegmentAction } from './events'
import type {
  HlsLoaderDiagnosticSegment,
  HlsLoaderDiagnosticsSnapshot,
  HlsLoaderDiagnosticStream,
} from './diagnostics'
import { createVirtualFragmentLoader } from './fragment-loader'
import type {
  FragmentLoaderSession,
  SegmentReader,
  SegmentReaderReleaseReason,
} from './fragment-loader'
import type { SegmentLoaderOptions } from './segment-loader'
import { RequestScheduler } from './scheduler'
import { StreamFiller } from './stream-filler'
import { VirtualSegment, VirtualStream, VirtualStreamRegistry } from './virtual-stream'

export interface HlsParallelLoaderOptions {
  getPlaybackRate?: () => number
  getPlaybackTime?: () => number
  onEvent?: HlsLoaderEventHandler
}

export interface HlsParallelLoader {
  readonly fragmentLoader: FragmentLoaderConstructor

  attach(hls: Hls): void
  destroy(): void
  getDiagnostics(): HlsLoaderDiagnosticsSnapshot
  setEnabled(enabled: boolean): void
}

const chunkSize = 2 * 1024 * 1024
const maxConcurrency = 6
const prefetchDepth = 6

class HlsParallelLoaderSession implements HlsParallelLoader, FragmentLoaderSession {
  readonly fragmentLoader: FragmentLoaderConstructor

  private readonly activeStreams = new Map<string, VirtualStream>()
  private destroyed = false
  private enabled = true
  private readonly filler: StreamFiller
  private hls: Hls | undefined
  private readonly onEvent: HlsLoaderEventHandler
  private reconcilePending = false
  private readonly registry = new VirtualStreamRegistry()
  private readonly scheduler: RequestScheduler

  constructor(options: HlsParallelLoaderOptions) {
    const getPlaybackRate = options.getPlaybackRate ?? (() => 1)
    const getPlaybackTime = options.getPlaybackTime ?? (() => 0)
    this.onEvent = options.onEvent ?? (() => undefined)
    this.scheduler = new RequestScheduler({
      getPlaybackRate,
      getPlaybackTime,
      maxConcurrency,
      scheduleIntervalMs: 200,
    })
    const loaderOptions: SegmentLoaderOptions = {
      chunkSize,
      finishingRatio: 0.8,
      finishingRemainingMs: 300,
      idleTimeoutMs: 5_000,
      maxLookAheadBytes: chunkSize * maxConcurrency,
      maxRescueAttempts: 2,
      minSlowThroughputSamples: 3,
      minRequestLifetimeMs: 300,
      onEvent: this.onEvent,
      slowThroughputRatio: 0.35,
      slowThroughputWindowMs: 1_000,
    }
    this.filler = new StreamFiller(this.scheduler, loaderOptions, this.onEvent)
    this.fragmentLoader = createVirtualFragmentLoader(this)
  }

  attach(hls: Hls): void {
    if (this.destroyed) {
      throw new Error('已经销毁的 HLS 并行加载会话不能重新绑定')
    }
    if (this.hls === hls) {
      return
    }
    this.detach()
    this.hls = hls
    this.configure(hls.config)
    hls.on(Hls.Events.LEVEL_LOADED, this.handleLevelLoaded)
    hls.on(Hls.Events.AUDIO_TRACK_LOADED, this.handleAudioTrackLoaded)
    hls.on(Hls.Events.FRAG_BUFFERED, this.handleFragBuffered)
    hls.on(Hls.Events.DESTROYING, this.handleHlsDestroying)
  }

  configure(config: HlsConfig): void {
    this.filler.configure(config)
  }

  read(
    context: FragmentLoaderContext,
    config: LoaderConfiguration,
    stats: LoaderStats,
  ): SegmentReader {
    if (this.destroyed) {
      return {
        promise: Promise.reject(new Error('HLS 并行加载会话已经销毁')),
        release: () => undefined,
      }
    }

    const segment = this.registry.resolve(context)
    const stream = segment.stream
    const previousState = segment.state
    segment.hardDemands += 1
    if (segment.isMediaSegment) {
      segment.playbackDemand = true
    }
    segment.addReaderStats(stats)
    stream.loaderConfig = config
    if (this.enabled && stream.kind !== 'subtitle' && segment.isMediaSegment) {
      this.activate(stream, segment)
    }

    this.emitDemandEvent(
      segment,
      previousState === 'ready'
        ? 'demand-ready'
        : previousState === 'filling'
          ? 'demand-loading'
          : 'demand-miss',
    )
    const promise = this.filler.ensure(segment, config, false)
    this.requestReconcile()

    let released = false
    return {
      promise,
      release: reason => {
        if (released) {
          return
        }
        released = true
        segment.removeReaderStats(stats)
        segment.hardDemands = Math.max(0, segment.hardDemands - 1)
        this.releaseDemand(stream, segment, reason)
        this.requestReconcile()
      },
    }
  }

  setEnabled(enabled: boolean): void {
    if (this.enabled === enabled) {
      return
    }
    this.enabled = enabled
    if (!enabled) {
      for (const stream of this.registry.values()) {
        stream.active = false
        stream.clearPlaybackDemands()
      }
      this.activeStreams.clear()
    }
    this.requestReconcile()
  }

  getDiagnostics(): HlsLoaderDiagnosticsSnapshot {
    const streams = [...this.registry.values()].map(stream => this.getStreamDiagnostics(stream))
    return {
      activeRequests: this.scheduler.getActiveRequestCount(),
      enabled: this.enabled,
      estimatedThroughputBytesPerSecond: this.scheduler.getEstimatedThroughput(),
      maxConcurrency,
      streams,
      timestamp: Date.now(),
    }
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.detach()
    this.filler.destroy()
    this.activeStreams.clear()
    this.registry.clear()
  }

  private readonly handleLevelLoaded = (_event: string, data: LevelLoadedData) => {
    this.registry.updateMain(data.level, data.details)
    this.requestReconcile()
  }

  private readonly handleAudioTrackLoaded = (_event: string, data: AudioTrackLoadedData) => {
    this.registry.updateAudio(data.groupId, data.id, data.details)
    this.requestReconcile()
  }

  private readonly handleFragBuffered = (_event: string, data: FragBufferedData) => {
    if (data.part !== null) {
      return
    }
    const segment = this.registry.find(data.frag)
    if (segment === undefined) {
      return
    }
    segment.playbackDemand = false
    this.requestReconcile()
  }

  private readonly handleHlsDestroying = () => {
    this.destroy()
  }

  private activate(stream: VirtualStream, segment: VirtualSegment): void {
    const previous = this.activeStreams.get(stream.kind)
    if (previous !== undefined && previous !== stream) {
      previous.active = false
      previous.clearPlaybackDemands()
    }
    this.activeStreams.set(stream.kind, stream)
    stream.active = true
    if (segment.isMediaSegment) {
      stream.anchor = segment
    }
  }

  private releaseDemand(
    stream: VirtualStream,
    segment: VirtualSegment,
    reason: SegmentReaderReleaseReason,
  ): void {
    if (reason === 'aborted') {
      segment.playbackDemand = false
    }
    if (reason === 'aborted' && stream.anchor === segment && segment.hardDemands === 0) {
      stream.active = false
      if (this.activeStreams.get(stream.kind) === stream) {
        this.activeStreams.delete(stream.kind)
      }
    }
  }

  private requestReconcile(): void {
    if (this.reconcilePending || this.destroyed) {
      return
    }
    this.reconcilePending = true
    queueMicrotask(() => {
      this.reconcilePending = false
      this.reconcile()
    })
  }

  private reconcile(): void {
    if (this.destroyed) {
      return
    }

    const wanted = new Map<VirtualSegment, { config: LoaderConfiguration; prefetch: boolean }>()
    for (const stream of this.registry.values()) {
      const config = stream.loaderConfig
      for (const segment of stream.allSegments()) {
        const waitingForHlsRetry = segment.playbackDemand && segment.state === 'failed'
        if (
          (segment.hardDemands > 0 || (segment.playbackDemand && !waitingForHlsRetry)) &&
          config !== undefined
        ) {
          wanted.set(segment, { config, prefetch: false })
        }
      }
      if (!this.enabled || !stream.active || config === undefined) {
        continue
      }
      if (stream.anchor?.playbackDemand && stream.anchor.state === 'failed') {
        continue
      }
      for (const segment of stream.getPrefetchSegments(prefetchDepth)) {
        if (!wanted.has(segment)) {
          wanted.set(segment, { config, prefetch: true })
        }
      }
    }

    for (const stream of this.registry.values()) {
      for (const segment of stream.allSegments()) {
        const plan = wanted.get(segment)
        if (plan !== undefined) {
          void this.filler.ensure(segment, plan.config, plan.prefetch).catch(() => undefined)
          continue
        }
        if (segment.hardDemands === 0 && !segment.playbackDemand) {
          this.filler.cancel(segment)
          stream.releaseRetired(segment)
        }
      }
    }
  }

  private emitDemandEvent(segment: VirtualSegment, action: HlsLoaderSegmentAction): void {
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
      // 观测回调不能影响加载流程
    }
  }

  private getStreamDiagnostics(stream: VirtualStream): HlsLoaderDiagnosticStream {
    const prefetchSegments = new Set(stream.getPrefetchSegments(prefetchDepth))
    const segments: HlsLoaderDiagnosticSegment[] = stream
      .getDiagnosticSegments(3, prefetchDepth + 3)
      .map(segment => {
        const chunks = this.filler.getSegmentDiagnostics(segment)
        const loadedBytes =
          segment.result?.stats.loaded ??
          chunks.reduce((total, chunk) => total + chunk.receivedBytes, 0)
        const totalBytes =
          segment.result?.stats.total ??
          chunks.reduce(
            (maximum, chunk) =>
              Math.max(maximum, chunk.endOffset ?? chunk.startOffset + chunk.receivedBytes),
            0,
          )
        return {
          anchor: stream.anchor === segment,
          chunks,
          duration: segment.duration,
          hardDemands: segment.hardDemands,
          key: segment.key,
          loadedBytes,
          playbackDemand: segment.playbackDemand,
          prefetch: prefetchSegments.has(segment),
          segmentSn: segment.fragment.sn,
          start: segment.start,
          state: segment.state,
          totalBytes,
          url: segment.context.url,
        }
      })
    return {
      active: stream.active,
      id: stream.id,
      kind: stream.kind,
      level: stream.level,
      segments,
    }
  }

  private detach(): void {
    const hls = this.hls
    if (hls === undefined) {
      return
    }
    hls.off(Hls.Events.LEVEL_LOADED, this.handleLevelLoaded)
    hls.off(Hls.Events.AUDIO_TRACK_LOADED, this.handleAudioTrackLoaded)
    hls.off(Hls.Events.FRAG_BUFFERED, this.handleFragBuffered)
    hls.off(Hls.Events.DESTROYING, this.handleHlsDestroying)
    this.hls = undefined
  }
}

export function createHlsParallelLoader(options: HlsParallelLoaderOptions = {}): HlsParallelLoader {
  return new HlsParallelLoaderSession(options)
}

export const DEFAULT_CHUNK_SIZE = chunkSize
export const DEFAULT_MAX_CONCURRENCY = maxConcurrency
export const DEFAULT_PREFETCH_DEPTH = prefetchDepth
