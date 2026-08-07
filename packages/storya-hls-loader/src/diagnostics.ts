import type { StreamFillerStateSnapshot } from './stream-filler'
import { estimateThroughput } from './stream-filler'
import type {
  VirtualStreamChunkSnapshot,
  VirtualStreamRegistrySnapshot,
  VirtualStreamSegmentSnapshot,
} from './virtual-stream'

export type HlsLoaderDiagnosticChunkState =
  | 'complete'
  | 'failed'
  | 'loading'
  | 'partial'
  | 'queued'
  | 'retrying'

export interface HlsLoaderDiagnosticChunk {
  attempt: number
  endOffset: number | undefined
  fillerId: number | undefined
  key: string
  networkRetries: number
  preemptions: number
  receivedBytes: number
  rescueAttempts: number
  slowRetries: number
  startOffset: number
  state: HlsLoaderDiagnosticChunkState
  throughputBytesPerSecond: number
  writerId: number | undefined
}

export type HlsLoaderDiagnosticSegmentState = 'empty' | 'failed' | 'filling' | 'ready'

export interface HlsLoaderDiagnosticSegment {
  chunks: HlsLoaderDiagnosticChunk[]
  duration: number
  key: string
  loadedBytes: number
  prefetch: boolean
  readerCount: number
  start: number
  state: HlsLoaderDiagnosticSegmentState
  totalBytes: number
  url: string
}

export interface HlsLoaderDiagnosticFrontier {
  barrier: boolean
  confirmed: boolean
  generation: number
  segmentKey: string
}

export interface HlsLoaderDiagnosticStream {
  frontier: HlsLoaderDiagnosticFrontier | undefined
  id: string
  segments: HlsLoaderDiagnosticSegment[]
}

export interface HlsLoaderDiagnosticsSnapshot {
  activeRequests: number
  estimatedThroughputBytesPerSecond: number
  fillers: StreamFillerStateSnapshot[]
  maxConcurrency: number
  registryRevision: number
  streams: HlsLoaderDiagnosticStream[]
  timestamp: number
}

export function createDiagnosticsSnapshot(
  registry: VirtualStreamRegistrySnapshot,
  fillers: readonly StreamFillerStateSnapshot[],
  maxConcurrency: number,
): HlsLoaderDiagnosticsSnapshot {
  return {
    activeRequests: fillers.filter(filler => filler.startedAt !== undefined).length,
    estimatedThroughputBytesPerSecond: estimateThroughput(registry),
    fillers: fillers.map(filler => ({ ...filler })),
    maxConcurrency,
    registryRevision: registry.revision,
    streams: registry.streams
      .map(stream => ({
        frontier: stream.frontier === undefined ? undefined : { ...stream.frontier },
        id: stream.id,
        segments: stream.segments
          .filter(shouldExposeSegment)
          .map(segment => createSegmentDiagnostics(segment)),
      }))
      .filter(stream => stream.frontier !== undefined || stream.segments.length > 0),
    timestamp: Date.now(),
  }
}

function shouldExposeSegment(segment: VirtualStreamSegmentSnapshot): boolean {
  return segment.chunks.length > 0 || segment.readerCount > 0 || segment.state !== 'empty'
}

function createSegmentDiagnostics(
  segment: VirtualStreamSegmentSnapshot,
): HlsLoaderDiagnosticSegment {
  const chunks = segment.chunks.map(chunk => createChunkDiagnostics(chunk))
  return {
    chunks,
    duration: segment.duration,
    key: segment.key,
    loadedBytes: chunks.reduce((total, chunk) => total + chunk.receivedBytes, 0),
    prefetch: segment.prefetch,
    readerCount: segment.readerCount,
    start: segment.start,
    state: segment.state,
    totalBytes: chunks.reduce(
      (maximum, chunk) =>
        Math.max(maximum, chunk.endOffset ?? chunk.startOffset + chunk.receivedBytes),
      0,
    ),
    url: segment.url,
  }
}

function createChunkDiagnostics(chunk: VirtualStreamChunkSnapshot): HlsLoaderDiagnosticChunk {
  return {
    attempt: chunk.attemptCount,
    endOffset: chunk.endOffset,
    fillerId: chunk.writer?.fillerId,
    key: chunk.key,
    networkRetries: chunk.networkRetries,
    preemptions: chunk.preemptions,
    receivedBytes: chunk.receivedLength,
    rescueAttempts: chunk.rescueAttempts,
    slowRetries: chunk.slowRetries,
    startOffset: chunk.startOffset,
    state:
      chunk.contentState === 'complete'
        ? 'complete'
        : chunk.writer !== undefined
          ? 'loading'
          : chunk.failure !== undefined && chunk.retryAt > performance.now()
            ? 'retrying'
            : chunk.failure !== undefined
              ? 'failed'
              : chunk.contentState === 'partial'
                ? 'partial'
                : 'queued',
    throughputBytesPerSecond: chunk.throughputBytesPerSecond,
    writerId: chunk.writer?.id,
  }
}
