import type { MediaLane } from './scheduler'

export type ParallelFragmentLoaderAbortReason = 'preempted' | 'slow-connection'

export interface ParallelFragmentLoaderAbortEvent {
  attempt: number
  baselineThroughputBytesPerSecond: number | undefined
  chunkEnd: number | undefined
  chunkLoadedBytes: number
  chunkStart: number
  elapsedMs: number
  lane: MediaLane
  loadedBytes: number
  reason: ParallelFragmentLoaderAbortReason
  remainingBytes: number | undefined
  requestEnd: number | undefined
  requestStart: number
  segmentDuration: number
  segmentSn: number | string
  segmentStart: number
  throughputBytesPerSecond: number
  timestamp: number
  type: 'request-aborted'
  url: string
}

export type ParallelFragmentLoaderEvent = ParallelFragmentLoaderAbortEvent

export type ParallelFragmentLoaderEventHandler = (event: ParallelFragmentLoaderEvent) => void
