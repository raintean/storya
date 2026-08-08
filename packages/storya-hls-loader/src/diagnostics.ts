import { type ParallelSegmentLoaderState } from './parallel-segment-loader-state'
import type { VirtualStream } from './virtual-stream'
import type { VirtualStreamChunk } from './virtual-stream-chunk'
import type { VirtualStreamSegment } from './virtual-stream-segment'

export type DiagnosticWorkerState = 'idle' | 'loading' | 'stopped'
export type DiagnosticSegmentState = 'empty' | 'failed' | 'filling' | 'ready'
export type DiagnosticChunkState = 'empty' | 'failed' | 'filling' | 'ready'

export interface ParallelSegmentLoaderDiagnostics {
  activeRequests: number
  destroyed: boolean
  maxConcurrency: number
  revision: number
  streams: VirtualStreamDiagnostics[]
  timestamp: number
  workers: WorkerDiagnostics[]
}

export interface VirtualStreamDiagnostics {
  id: string
  segments: SegmentDiagnostics[]
  window: string[]
}

export interface SegmentDiagnostics {
  chunks: ChunkDiagnostics[]
  duration: number
  httpStatus: number
  key: string
  loadedBytes: number
  readerCount: number
  sequential: boolean
  start: number
  state: DiagnosticSegmentState
  totalBytes: number | undefined
  url: string
  windowIndex: number | null
}

export interface ChunkDiagnostics {
  attempt: number
  endExclusive: number | undefined
  failure: string | undefined
  generation: number | undefined
  key: string
  loadedBytes: number
  start: number
  state: DiagnosticChunkState
}

export interface WorkerDiagnostics {
  chunkKey: string | undefined
  id: number
  requestEnd: number | undefined
  requestStart: number | undefined
  segmentKey: string | undefined
  startedAt: number | undefined
  state: DiagnosticWorkerState
  streamId: string | undefined
}

export function createParallelSegmentLoaderDiagnostics(
  state: ParallelSegmentLoaderState,
  maxConcurrency: number,
  workers: WorkerDiagnostics[],
): ParallelSegmentLoaderDiagnostics {
  return {
    activeRequests: workers.filter(worker => worker.state === 'loading').length,
    destroyed: state.destroyed,
    maxConcurrency,
    revision: state.revision,
    streams: [...state.streams.values()]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map(stream => createStreamDiagnostics(stream)),
    timestamp: Date.now(),
    workers,
  }
}

function createStreamDiagnostics(stream: VirtualStream): VirtualStreamDiagnostics {
  return {
    id: stream.id,
    segments: [...stream.segments.values()]
      .sort((left, right) => left.start - right.start || left.key.localeCompare(right.key))
      .map(segment => createSegmentDiagnostics(stream, segment)),
    window: [...stream.window],
  }
}

function createSegmentDiagnostics(
  stream: VirtualStream,
  segment: VirtualStreamSegment,
): SegmentDiagnostics {
  let readyChunk: VirtualStreamChunk | undefined
  for (const chunk of segment.chunks) {
    if (chunk.phase.type === 'ready') {
      readyChunk = chunk
    }
  }
  const windowIndex = stream.window.indexOf(segment.key)
  return {
    chunks: segment.chunks.map(chunk => createChunkDiagnostics(chunk)),
    duration: segment.duration,
    httpStatus:
      segment.outcome.type === 'ready'
        ? segment.outcome.code
        : segment.outcome.type === 'failed'
          ? segment.outcome.failure.code
          : readyChunk?.phase.type === 'ready'
            ? readyChunk.phase.response.status
            : 0,
    key: segment.key,
    loadedBytes: segment.loadedBytes,
    readerCount: segment.readerCount,
    sequential: segment.sequential,
    start: segment.start,
    state: segment.state,
    totalBytes: segment.length,
    url:
      segment.outcome.type === 'ready'
        ? segment.outcome.url
        : readyChunk?.phase.type === 'ready'
          ? readyChunk.phase.url
          : segment.context.url,
    windowIndex: windowIndex === -1 ? null : windowIndex,
  }
}

function createChunkDiagnostics(chunk: VirtualStreamChunk): ChunkDiagnostics {
  return {
    attempt: chunk.attempt,
    endExclusive: chunk.endExclusive,
    failure:
      chunk.phase.type === 'failed'
        ? chunk.phase.failure.message
        : chunk.phase.type === 'empty'
          ? chunk.phase.lastFailure
          : undefined,
    generation: chunk.phase.type === 'filling' ? chunk.phase.generation : undefined,
    key: chunk.key,
    loadedBytes: chunk.loadedBytes,
    start: chunk.start,
    state: chunk.state,
  }
}
