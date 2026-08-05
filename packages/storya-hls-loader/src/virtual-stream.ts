import type {
  Fragment,
  FragmentLoaderContext,
  LevelDetails,
  LoaderConfiguration,
  LoaderResponse,
  LoaderStats,
} from 'hls.js'
import type { HlsLoaderDiagnosticChunk } from './diagnostics'
import { copyLoaderStats, createLoaderStats } from './stats'

export type VirtualSegmentState = 'empty' | 'failed' | 'filling' | 'ready'
export type VirtualStreamKind = 'audio' | 'main' | 'subtitle'

export interface VirtualSegmentResult {
  networkDetails: Response | null
  response: LoaderResponse
  stats: LoaderStats
}

export class VirtualSegment {
  readonly key: string
  readonly stream: VirtualStream

  context: FragmentLoaderContext
  chunks: HlsLoaderDiagnosticChunk[] = []
  hardDemands = 0
  playbackDemand = false
  result: VirtualSegmentResult | undefined
  state: VirtualSegmentState = 'empty'

  private readonly readerStats = new Set<LoaderStats>()
  private readonly stats = createLoaderStats()

  constructor(stream: VirtualStream, context: FragmentLoaderContext) {
    this.stream = stream
    this.context = context
    this.key = createSegmentKey(context)
  }

  get duration(): number {
    return this.context.part?.duration ?? this.context.frag.duration
  }

  get fragment(): Fragment {
    return this.context.frag
  }

  get isMediaSegment(): boolean {
    return this.context.part === null && this.context.frag.sn !== 'initSegment'
  }

  get start(): number {
    return this.context.part?.start ?? this.context.frag.start
  }

  addReaderStats(stats: LoaderStats): void {
    this.readerStats.add(stats)
    copyLoaderStats(stats, this.stats)
  }

  removeReaderStats(stats: LoaderStats): void {
    this.readerStats.delete(stats)
  }

  updateContext(context: FragmentLoaderContext): void {
    this.context = context
  }

  updateStats(stats: LoaderStats): void {
    copyLoaderStats(this.stats, stats)
    for (const readerStats of this.readerStats) {
      copyLoaderStats(readerStats, stats)
    }
  }

  markFilling(): void {
    this.result = undefined
    this.state = 'filling'
  }

  markReady(result: VirtualSegmentResult): void {
    this.result = result
    this.state = 'ready'
    this.updateStats(result.stats)
  }

  markFailed(stats: LoaderStats): void {
    this.result = undefined
    this.state = 'failed'
    this.updateStats(stats)
  }

  markEmpty(stats?: LoaderStats): void {
    this.result = undefined
    this.chunks = []
    this.state = 'empty'
    if (stats !== undefined) {
      this.updateStats(stats)
    }
  }

  clearData(): void {
    if (this.state === 'ready' || this.state === 'failed') {
      this.result = undefined
      this.chunks = []
      this.state = 'empty'
    }
  }

  updateChunks(chunks: HlsLoaderDiagnosticChunk[]): void {
    this.chunks = chunks
  }
}

export class VirtualStream {
  readonly id: string
  readonly kind: VirtualStreamKind

  active = false
  anchor: VirtualSegment | undefined
  loaderConfig: LoaderConfiguration | undefined

  private readonly auxiliarySegments = new Map<string, VirtualSegment>()
  private readonly segmentsByKey = new Map<string, VirtualSegment>()
  private segments: VirtualSegment[] = []

  constructor(id: string, kind: VirtualStreamKind) {
    this.id = id
    this.kind = kind
  }

  get level(): number {
    return this.anchor?.fragment.level ?? this.segments[0]?.fragment.level ?? -1
  }

  updatePlaylist(
    details: LevelDetails,
    registerFragment: (fragment: Fragment, segment: VirtualSegment) => void,
  ): void {
    const nextSegments: VirtualSegment[] = []
    for (const fragment of details.fragments) {
      const context = createFragmentContext(fragment)
      const key = createSegmentKey(context)
      const segment = this.segmentsByKey.get(key) ?? new VirtualSegment(this, context)
      segment.updateContext(context)
      this.segmentsByKey.set(key, segment)
      nextSegments.push(segment)
      registerFragment(fragment, segment)

      const initSegment = fragment.initSegment
      if (initSegment !== null) {
        const initContext = createFragmentContext(initSegment)
        const initKey = createSegmentKey(initContext)
        const init = this.auxiliarySegments.get(initKey) ?? new VirtualSegment(this, initContext)
        init.updateContext(initContext)
        this.auxiliarySegments.set(initKey, init)
        registerFragment(initSegment, init)
      }
    }

    this.segments = nextSegments
  }

  addStandalone(context: FragmentLoaderContext): VirtualSegment {
    const key = createSegmentKey(context)
    const collection =
      context.part === null && context.frag.sn !== 'initSegment'
        ? this.segmentsByKey
        : this.auxiliarySegments
    const segment = collection.get(key) ?? new VirtualSegment(this, context)
    segment.updateContext(context)
    collection.set(key, segment)
    if (segment.isMediaSegment && !this.segments.includes(segment)) {
      this.segments.push(segment)
      this.segments.sort((left, right) => left.start - right.start)
    }
    return segment
  }

  getPrefetchSegments(depth: number): VirtualSegment[] {
    if (!this.active || this.anchor === undefined || !this.anchor.isMediaSegment) {
      return []
    }
    const anchorIndex = this.segments.indexOf(this.anchor)
    if (anchorIndex < 0) {
      return []
    }
    return this.segments
      .slice(anchorIndex + 1)
      .filter(segment => !segment.fragment.gap)
      .slice(0, depth)
  }

  allSegments(): Iterable<VirtualSegment> {
    return [...this.segmentsByKey.values(), ...this.auxiliarySegments.values()]
  }

  clearPlaybackDemands(): void {
    for (const segment of this.allSegments()) {
      segment.playbackDemand = false
    }
  }

  getDiagnosticSegments(before: number, after: number): VirtualSegment[] {
    const anchor = this.anchor
    if (anchor !== undefined) {
      const anchorIndex = this.segments.indexOf(anchor)
      if (anchorIndex >= 0) {
        const from = Math.max(0, anchorIndex - before)
        return this.segments.slice(from, anchorIndex + after + 1)
      }
    }

    return [...this.allSegments()]
      .filter(
        segment => segment.hardDemands > 0 || segment.playbackDemand || segment.state !== 'empty',
      )
      .sort((left, right) => left.start - right.start)
      .slice(-Math.max(1, before + after + 1))
  }

  releaseRetired(segment: VirtualSegment): void {
    if (segment.hardDemands > 0 || segment.playbackDemand || this.segments.includes(segment)) {
      return
    }
    this.segmentsByKey.delete(segment.key)
  }
}

export class VirtualStreamRegistry {
  private readonly fragmentSegments = new WeakMap<Fragment, VirtualSegment>()
  private readonly streams = new Map<string, VirtualStream>()

  updateMain(level: number, details: LevelDetails): VirtualStream {
    return this.updateStream(`main:${level}:${details.url}`, 'main', details)
  }

  updateAudio(groupId: string, id: number, details: LevelDetails): VirtualStream {
    return this.updateStream(`audio:${groupId}:${id}:${details.url}`, 'audio', details)
  }

  resolve(context: FragmentLoaderContext): VirtualSegment {
    const known = this.fragmentSegments.get(context.frag)
    if (known !== undefined) {
      if (context.part !== null) {
        return known.stream.addStandalone(context)
      }
      known.updateContext(context)
      return known
    }

    const kind = normalizeStreamKind(context.frag.type)
    const fallbackId = `${kind}:${context.frag.level}:${context.frag.baseurl}`
    const stream = this.streams.get(fallbackId) ?? new VirtualStream(fallbackId, kind)
    this.streams.set(fallbackId, stream)
    const segment = stream.addStandalone(context)
    this.fragmentSegments.set(context.frag, segment)
    return segment
  }

  values(): Iterable<VirtualStream> {
    return this.streams.values()
  }

  find(fragment: Fragment): VirtualSegment | undefined {
    return this.fragmentSegments.get(fragment)
  }

  clear(): void {
    this.streams.clear()
  }

  private updateStream(id: string, kind: VirtualStreamKind, details: LevelDetails): VirtualStream {
    const stream = this.streams.get(id) ?? new VirtualStream(id, kind)
    this.streams.set(id, stream)
    stream.updatePlaylist(details, (fragment, segment) => {
      this.fragmentSegments.set(fragment, segment)
    })
    return stream
  }
}

export function createFragmentContext(fragment: Fragment): FragmentLoaderContext {
  const rangeStart = fragment.byteRangeStartOffset
  const rangeEnd = fragment.byteRangeEndOffset
  return {
    frag: fragment,
    headers: {},
    part: null,
    rangeEnd: rangeEnd ?? 0,
    rangeStart: rangeStart ?? 0,
    responseType: 'arraybuffer',
    url: fragment.url,
  }
}

function createSegmentKey(context: FragmentLoaderContext): string {
  const part = context.part
  const segment = part ?? context.frag
  return [
    segment.url,
    context.rangeStart ?? 0,
    context.rangeEnd ?? 0,
    context.frag.sn,
    part?.index ?? '',
  ].join('|')
}

function normalizeStreamKind(type: string): VirtualStreamKind {
  if (type === 'audio' || type === 'subtitle') {
    return type
  }
  return 'main'
}
