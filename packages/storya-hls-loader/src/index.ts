import type { FragmentLoaderConstructor, HlsConfig } from 'hls.js'
import {
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CONCURRENCY,
  ParallelFragmentLoader,
} from './parallel-fragment-loader'
import type { ParallelFragmentLoaderOptions as InternalLoaderOptions } from './parallel-fragment-loader'
import type { ParallelFragmentLoaderEventHandler } from './events'
import { ChunkScheduler } from './scheduler'

export interface ParallelFragmentLoaderOptions {
  getPlaybackRate?: () => number
  getPlaybackTime?: () => number
  onEvent?: ParallelFragmentLoaderEventHandler
}

const maxConcurrency = DEFAULT_MAX_CONCURRENCY
const chunkSize = DEFAULT_CHUNK_SIZE

export function createParallelFragmentLoader(
  options: ParallelFragmentLoaderOptions = {},
): FragmentLoaderConstructor {
  const getPlaybackRate = options.getPlaybackRate ?? (() => 1)
  const getPlaybackTime = options.getPlaybackTime ?? (() => 0)
  const scheduler = new ChunkScheduler({
    getPlaybackRate,
    getPlaybackTime,
    maxConcurrency,
    scheduleIntervalMs: 200,
  })
  const loaderOptions: InternalLoaderOptions = {
    chunkSize,
    finishingRatio: 0.8,
    finishingRemainingMs: 300,
    idleTimeoutMs: 5_000,
    maxLookAheadBytes: chunkSize * maxConcurrency,
    maxRescueAttempts: 2,
    minSlowThroughputSamples: 3,
    minRequestLifetimeMs: 300,
    onEvent: options.onEvent ?? (() => undefined),
    slowThroughputRatio: 0.35,
    slowThroughputWindowMs: 1_000,
  }

  return class StoryaParallelFragmentLoader extends ParallelFragmentLoader {
    constructor(config: HlsConfig) {
      super(config, scheduler, loaderOptions)
    }
  }
}

export { DEFAULT_CHUNK_SIZE, DEFAULT_MAX_CONCURRENCY } from './parallel-fragment-loader'
export type {
  ParallelFragmentLoaderAbortEvent,
  ParallelFragmentLoaderAbortReason,
  ParallelFragmentLoaderEvent,
  ParallelFragmentLoaderEventHandler,
} from './events'
