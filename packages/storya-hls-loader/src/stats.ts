import type { LoaderStats } from 'hls.js'
import type { VirtualStreamStatistics } from './virtual-stream'

export function createLoaderStats(): LoaderStats {
  return {
    aborted: false,
    loaded: 0,
    retry: 0,
    total: 0,
    chunkCount: 0,
    bwEstimate: 0,
    loading: { start: 0, first: 0, end: 0 },
    parsing: { start: 0, end: 0 },
    buffering: { start: 0, first: 0, end: 0 },
  }
}

export function copyLoaderStats(target: LoaderStats, source: LoaderStats): void {
  target.aborted = source.aborted
  target.loaded = source.loaded
  target.retry = source.retry
  target.total = source.total
  target.chunkCount = source.chunkCount
  target.bwEstimate = source.bwEstimate
  target.loading.start = source.loading.start
  target.loading.first = source.loading.first
  target.loading.end = source.loading.end
  target.parsing.start = source.parsing.start
  target.parsing.end = source.parsing.end
  target.buffering.start = source.buffering.start
  target.buffering.first = source.buffering.first
  target.buffering.end = source.buffering.end
}

export function copyVirtualStreamStatistics(
  target: LoaderStats,
  source: VirtualStreamStatistics,
): void {
  target.loaded = source.loaded
  target.retry = source.retry
  target.total = source.total
  target.chunkCount = source.chunkCount
  target.bwEstimate = source.bwEstimate
  target.loading.start = source.loading.start
  target.loading.first = source.loading.first
  target.loading.end = source.loading.end
  target.parsing.start = source.parsing.start
  target.parsing.end = source.parsing.end
  target.buffering.start = source.buffering.start
  target.buffering.first = source.buffering.first
  target.buffering.end = source.buffering.end
}
