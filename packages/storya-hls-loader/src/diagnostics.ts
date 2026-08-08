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
  fillId: number | undefined
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
