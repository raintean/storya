import type { LoaderStats } from 'hls.js'

export function createLoaderStats(): LoaderStats {
  return {
    aborted: false,
    buffering: { end: 0, first: 0, start: 0 },
    bwEstimate: 0,
    chunkCount: 0,
    loaded: 0,
    loading: { end: 0, first: 0, start: 0 },
    parsing: { end: 0, start: 0 },
    retry: 0,
    total: 0,
  }
}

export function copyLoaderStats(target: LoaderStats, source: LoaderStats): void {
  target.aborted = source.aborted
  target.buffering.end = source.buffering.end
  target.buffering.first = source.buffering.first
  target.buffering.start = source.buffering.start
  target.bwEstimate = source.bwEstimate
  target.chunkCount = source.chunkCount
  target.loaded = source.loaded
  target.loading.end = source.loading.end
  target.loading.first = source.loading.first
  target.loading.start = source.loading.start
  target.parsing.end = source.parsing.end
  target.parsing.start = source.parsing.start
  target.retry = source.retry
  target.total = source.total
}

export function cloneLoaderStats(source: LoaderStats): LoaderStats {
  const target = createLoaderStats()
  copyLoaderStats(target, source)
  return target
}
