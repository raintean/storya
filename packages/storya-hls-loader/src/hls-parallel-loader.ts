import Hls from 'hls.js'
import type {
  AudioTrackLoadedData,
  Fragment,
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  LevelDetails,
  LevelLoadedData,
  LoaderConfiguration,
  LoaderStats,
  SubtitleTrackLoadedData,
} from 'hls.js'
import { FetchHttpTransport } from 'storya-transport'
import type { HttpTransport } from 'storya-transport'
import { createDiagnosticsSnapshot } from './diagnostics'
import type { HlsLoaderDiagnosticsSnapshot } from './diagnostics'
import type { HlsLoaderEventHandler, HlsLoaderSegmentAction } from './events'
import { createVirtualFragmentLoader } from './fragment-loader'
import type { FragmentLoaderSession } from './fragment-loader'
import { StreamFiller } from './stream-filler'
import type { StreamFillerOptions } from './stream-filler'
import { VirtualStreamReadFailure, VirtualStreamRegistry } from './virtual-stream'
import type {
  VirtualStreamFillPolicy,
  VirtualStreamRetryPolicy,
  VirtualStreamSegmentDescriptor,
  VirtualStreamSegmentReader,
} from './virtual-stream'
import { copyVirtualStreamStatistics } from './stats'

export interface HlsParallelLoaderOptions {
  getPlaybackRate?: () => number
  getPlaybackTime?: () => number
  onEvent?: HlsLoaderEventHandler
  prefetchAheadSegments?: number
  transport?: HttpTransport
}

export interface HlsParallelLoader {
  readonly fragmentLoader: FragmentLoaderConstructor

  attach(hls: Hls): void
  destroy(): void
  getDiagnostics(): HlsLoaderDiagnosticsSnapshot
}

const chunkSize = 2 * 1024 * 1024
const maxConcurrency = 6
const prefetchAheadSegments = 6

class HlsParallelLoaderSession implements HlsParallelLoader, FragmentLoaderSession {
  readonly fragmentLoader: FragmentLoaderConstructor

  private destroyed = false
  private readonly fillers: StreamFiller[]
  private readonly fragmentStreams = new WeakMap<Fragment, string>()
  private hls: Hls | undefined
  private hlsConfig: HlsConfig | undefined
  private readonly onEvent: HlsLoaderEventHandler
  private readonly registry: VirtualStreamRegistry
  private readonly transport: HttpTransport

  constructor(options: HlsParallelLoaderOptions) {
    const configuredPrefetch = options.prefetchAheadSegments ?? prefetchAheadSegments
    if (!Number.isSafeInteger(configuredPrefetch) || configuredPrefetch < 0) {
      throw new Error('prefetchAheadSegments 必须是非负整数')
    }

    const getPlaybackRate = options.getPlaybackRate ?? (() => 1)
    const getPlaybackTime = options.getPlaybackTime ?? (() => 0)
    this.onEvent = options.onEvent ?? (() => undefined)
    this.transport = options.transport ?? new FetchHttpTransport()
    this.registry = new VirtualStreamRegistry({
      chunkSize,
      prefetchAheadSegments: configuredPrefetch,
    })
    const fillerOptions: StreamFillerOptions = {
      finishingRatio: 0.8,
      finishingRemainingMs: 300,
      getPlaybackRate,
      getPlaybackTime,
      idleTimeoutMs: 5_000,
      maxRescueAttempts: 2,
      minRequestLifetimeMs: 300,
      minSlowThroughputSamples: 3,
      onEvent: this.onEvent,
      slowThroughputRatio: 0.35,
      slowThroughputWindowMs: 1_000,
    }
    this.fillers = Array.from(
      { length: maxConcurrency },
      (_, index) => new StreamFiller(index + 1, this.registry, this.transport, fillerOptions),
    )
    this.fragmentLoader = createVirtualFragmentLoader(this)
    for (const filler of this.fillers) {
      filler.start()
    }
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
    hls.on(Hls.Events.SUBTITLE_TRACK_LOADED, this.handleSubtitleTrackLoaded)
    hls.on(Hls.Events.DESTROYING, this.handleHlsDestroying)
  }

  configure(config: HlsConfig): void {
    this.hlsConfig ??= config
  }

  read(
    context: FragmentLoaderContext,
    config: LoaderConfiguration,
    stats: LoaderStats,
  ): VirtualStreamSegmentReader {
    if (this.destroyed) {
      throw new Error('HLS 并行加载会话已经销毁')
    }
    const hlsConfig = this.hlsConfig
    if (hlsConfig === undefined) {
      throw new Error('HLS 并行加载器尚未取得 hls.js 配置')
    }

    const streamId = this.fragmentStreams.get(context.frag) ?? createProvisionalStreamId(context)
    this.fragmentStreams.set(context.frag, streamId)
    const reader = this.registry.createSegmentReader({
      fillPolicy: createFillPolicy(config),
      onStatistics: statistics => copyVirtualStreamStatistics(stats, statistics),
      segment: createSegmentDescriptor(context, hlsConfig),
      streamId,
    })
    this.emitReaderEvent(reader, context, 'reader-created')
    void reader.result.then(
      () => this.emitReaderEvent(reader, context, 'reader-ready'),
      failure =>
        this.emitReaderEvent(
          reader,
          context,
          failure instanceof VirtualStreamReadFailure && failure.kind === 'aborted'
            ? 'reader-cancelled'
            : 'reader-failed',
        ),
    )
    return reader
  }

  getDiagnostics(): HlsLoaderDiagnosticsSnapshot {
    return createDiagnosticsSnapshot(
      this.registry.snapshot(),
      this.fillers.map(filler => filler.getState()),
      maxConcurrency,
    )
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.detach()
    for (const filler of this.fillers) {
      filler.destroy()
    }
    this.registry.destroy()
    this.transport.destroy()
  }

  private readonly handleLevelLoaded = (_event: string, data: LevelLoadedData) => {
    this.updateTopology(`main:${data.level}`, data.details)
  }

  private readonly handleAudioTrackLoaded = (_event: string, data: AudioTrackLoadedData) => {
    this.updateTopology(`audio:${data.groupId}:${data.id}`, data.details)
  }

  private readonly handleSubtitleTrackLoaded = (_event: string, data: SubtitleTrackLoadedData) => {
    this.updateTopology(`subtitle:${data.groupId}:${data.id}`, data.details)
  }

  private readonly handleHlsDestroying = () => {
    this.destroy()
  }

  private emitReaderEvent(
    reader: VirtualStreamSegmentReader,
    context: FragmentLoaderContext,
    action: HlsLoaderSegmentAction,
  ): void {
    const segment = reader.segment
    try {
      this.onEvent({
        action,
        segmentSn: context.frag.sn,
        segmentStart: segment.start,
        streamId: segment.stream.id,
        timestamp: Date.now(),
        type: 'segment-state',
        url: context.url,
      })
    } catch {
      // 观测回调不能影响加载流程
    }
  }

  private updateTopology(streamId: string, details: LevelDetails): void {
    const hlsConfig = this.hlsConfig
    if (hlsConfig === undefined) {
      return
    }
    const descriptors: VirtualStreamSegmentDescriptor[] = []
    for (const fragment of details.fragments) {
      const previousStreamId = this.fragmentStreams.get(fragment)
      if (previousStreamId !== undefined && previousStreamId !== streamId) {
        this.registry.mergeStream(previousStreamId, streamId)
      }
      this.fragmentStreams.set(fragment, streamId)
      descriptors.push(createSegmentDescriptor(createFragmentContext(fragment), hlsConfig))

      const initSegment = fragment.initSegment
      if (initSegment !== null) {
        const previousInitStreamId = this.fragmentStreams.get(initSegment)
        if (previousInitStreamId !== undefined && previousInitStreamId !== streamId) {
          this.registry.mergeStream(previousInitStreamId, streamId)
        }
        this.fragmentStreams.set(initSegment, streamId)
        descriptors.push(createSegmentDescriptor(createFragmentContext(initSegment), hlsConfig))
      }
    }
    this.registry.updateStream(streamId, descriptors)
  }

  private detach(): void {
    const hls = this.hls
    if (hls === undefined) {
      return
    }
    hls.off(Hls.Events.LEVEL_LOADED, this.handleLevelLoaded)
    hls.off(Hls.Events.AUDIO_TRACK_LOADED, this.handleAudioTrackLoaded)
    hls.off(Hls.Events.SUBTITLE_TRACK_LOADED, this.handleSubtitleTrackLoaded)
    hls.off(Hls.Events.DESTROYING, this.handleHlsDestroying)
    this.hls = undefined
  }
}

export function createHlsParallelLoader(options: HlsParallelLoaderOptions = {}): HlsParallelLoader {
  return new HlsParallelLoaderSession(options)
}

export const DEFAULT_CHUNK_SIZE = chunkSize
export const DEFAULT_MAX_CONCURRENCY = maxConcurrency
export const DEFAULT_PREFETCH_AHEAD_SEGMENTS = prefetchAheadSegments

function createProvisionalStreamId(context: FragmentLoaderContext): string {
  const fragment = context.frag
  return `provisional:${fragment.type}:${fragment.level}:${fragment.baseurl}`
}

function createSegmentDescriptor(
  context: FragmentLoaderContext,
  hlsConfig: HlsConfig,
): VirtualStreamSegmentDescriptor {
  const rangeStart = context.rangeStart ?? 0
  const configuredRangeEnd = context.rangeEnd ?? 0
  const rangeEnd = configuredRangeEnd > rangeStart ? configuredRangeEnd : undefined
  return {
    key: createSegmentKey(context),
    position: {
      duration: context.part?.duration ?? context.frag.duration,
      start: context.part?.start ?? context.frag.start,
    },
    prefetch: context.part === null && context.frag.sn !== 'initSegment' && !context.frag.gap,
    resource: {
      createRequest: async parameters => {
        const requestContext: FragmentLoaderContext = {
          ...context,
          headers: Object.fromEntries(parameters.headers.entries()),
          rangeEnd: parameters.rangeEnd ?? 0,
          rangeStart: parameters.rangeStart,
        }
        const init: RequestInit = {
          credentials: 'same-origin',
          headers: parameters.headers,
          method: parameters.method,
          mode: 'cors',
          signal: parameters.signal,
        }
        return (
          (await hlsConfig.fetchSetup?.(requestContext, init)) ?? new Request(context.url, init)
        )
      },
      headers: context.headers ?? {},
      rangeEnd,
      rangeStart,
      url: context.url,
    },
  }
}

function createFillPolicy(config: LoaderConfiguration): VirtualStreamFillPolicy {
  return {
    errorRetry: createRetryPolicy(config.loadPolicy.errorRetry),
    maxTimeToFirstByteMs: config.loadPolicy.maxTimeToFirstByteMs,
    timeoutRetry: createRetryPolicy(config.loadPolicy.timeoutRetry),
  }
}

function createRetryPolicy(
  policy: LoaderConfiguration['loadPolicy']['errorRetry'],
): VirtualStreamRetryPolicy | undefined {
  if (policy === null || policy === undefined) {
    return undefined
  }
  const normalized: VirtualStreamRetryPolicy = {
    backoff: policy.backoff ?? 'exponential',
    maxNumRetry: policy.maxNumRetry,
    maxRetryDelayMs: policy.maxRetryDelayMs,
    retryDelayMs: policy.retryDelayMs,
  }
  if (policy.shouldRetry === undefined) {
    return normalized
  }
  return {
    ...normalized,
    shouldRetry: (retryCount, timeout, error) =>
      policy.shouldRetry?.(
        policy,
        retryCount,
        timeout,
        { code: error.code, text: error.message, url: error.url },
        true,
      ) ?? true,
  }
}

function createFragmentContext(fragment: Fragment): FragmentLoaderContext {
  return {
    frag: fragment,
    headers: {},
    part: null,
    rangeEnd: fragment.byteRangeEndOffset ?? 0,
    rangeStart: fragment.byteRangeStartOffset ?? 0,
    responseType: 'arraybuffer',
    url: fragment.url,
  }
}

function createSegmentKey(context: FragmentLoaderContext): string {
  const part = context.part
  const fragment = context.frag
  if (fragment.sn === 'initSegment') {
    return ['init', fragment.url, context.rangeStart ?? 0, context.rangeEnd ?? 0].join('|')
  }
  return [
    `sn:${fragment.sn}`,
    `cc:${fragment.cc ?? 0}`,
    `part:${part?.index ?? ''}`,
    `start:${part?.start ?? fragment.start}`,
    `range:${context.rangeStart ?? 0}-${context.rangeEnd ?? 0}`,
  ].join('|')
}
