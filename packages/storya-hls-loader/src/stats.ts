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
