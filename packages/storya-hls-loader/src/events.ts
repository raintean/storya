export type HlsLoaderAbortReason = 'preempted' | 'slow-connection'

export interface HlsLoaderAbortEvent {
  attempt: number
  baselineThroughputBytesPerSecond: number | undefined
  chunkEnd: number | undefined
  chunkLoadedBytes: number
  chunkStart: number
  elapsedMs: number
  loadedBytes: number
  reason: HlsLoaderAbortReason
  remainingBytes: number | undefined
  requestEnd: number | undefined
  requestStart: number
  segmentDuration: number
  segmentKey: string
  segmentStart: number
  streamId: string
  throughputBytesPerSecond: number
  timestamp: number
  type: 'request-aborted'
  url: string
}

export type HlsLoaderSegmentAction =
  | 'reader-cancelled'
  | 'reader-created'
  | 'reader-failed'
  | 'reader-ready'

export interface HlsLoaderSegmentEvent {
  action: HlsLoaderSegmentAction
  segmentSn: number | string
  segmentStart: number
  streamId: string
  timestamp: number
  type: 'segment-state'
  url: string
}

export type HlsLoaderEvent = HlsLoaderAbortEvent | HlsLoaderSegmentEvent

export type HlsLoaderEventHandler = (event: HlsLoaderEvent) => void
