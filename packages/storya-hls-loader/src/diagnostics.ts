export type HlsLoaderDiagnosticChunkState =
  | 'cancelled'
  | 'complete'
  | 'loading'
  | 'network-retrying'
  | 'preempted'
  | 'queued'
  | 'slow-retrying'

export interface HlsLoaderDiagnosticChunk {
  attempt: number
  endOffset: number | undefined
  id: number
  networkRetries: number
  preemptions: number
  receivedBytes: number
  rescueAttempts: number
  running: boolean
  slowRetries: number
  startOffset: number
  state: HlsLoaderDiagnosticChunkState
  throughputBytesPerSecond: number
}

export type HlsLoaderDiagnosticSegmentState = 'empty' | 'failed' | 'filling' | 'ready'

export interface HlsLoaderDiagnosticSegment {
  anchor: boolean
  chunks: HlsLoaderDiagnosticChunk[]
  duration: number
  hardDemands: number
  key: string
  loadedBytes: number
  playbackDemand: boolean
  prefetch: boolean
  segmentSn: number | string
  start: number
  state: HlsLoaderDiagnosticSegmentState
  totalBytes: number
  url: string
}

export interface HlsLoaderDiagnosticStream {
  active: boolean
  id: string
  kind: 'audio' | 'main' | 'subtitle'
  level: number
  segments: HlsLoaderDiagnosticSegment[]
}

export interface HlsLoaderDiagnosticsSnapshot {
  activeRequests: number
  enabled: boolean
  estimatedThroughputBytesPerSecond: number
  maxConcurrency: number
  streams: HlsLoaderDiagnosticStream[]
  timestamp: number
}
