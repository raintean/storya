import type { FragmentLoaderConstructor, HlsConfig } from 'hls.js'
import { FetchHttpTransport, type HttpTransport } from 'storya-transport'
import { SegmentLoadWorker } from './segment-load-worker'
import {
  createParallelSegmentLoaderDiagnostics,
  type ParallelSegmentLoaderDiagnostics,
} from './diagnostics'
import { createStoryaFragmentLoader } from './fragment-loader'
import { ParallelSegmentLoaderState } from './parallel-segment-loader-state'

export const DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024
export const DEFAULT_MAX_CONCURRENCY = 6
export const DEFAULT_WINDOW_SIZE = 6

const defaultIdleTimeoutMs = 5_000
const defaultMaxRescueAttempts = 1
const maxTransferSamples = 64

interface TransferSample {
  bytes: number
  completedAt: number
  startedAt: number
}

export interface ParallelSegmentLoaderOptions {
  chunkSize?: number
  idleTimeoutMs?: number
  maxConcurrency?: number
  maxRescueAttempts?: number
  transport?: HttpTransport
  windowSize?: number
}

export class ParallelSegmentLoader {
  private static readonly owners = new WeakMap<FragmentLoaderConstructor, ParallelSegmentLoader>()

  readonly chunkSize: number
  readonly fLoader: FragmentLoaderConstructor
  readonly idleTimeoutMs: number
  readonly maxConcurrency: number
  readonly maxRescueAttempts: number
  readonly state: ParallelSegmentLoaderState
  readonly windowSize: number

  private bandwidthEstimateValue = 0
  private config: HlsConfig | undefined
  private readonly listeners = new Set<() => void>()
  private notificationScheduled = false
  private readonly transport: HttpTransport
  private readonly transferSamples: TransferSample[] = []
  private updating = false
  private readonly workers: SegmentLoadWorker[]

  constructor(options: ParallelSegmentLoaderOptions = {}) {
    this.chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE
    this.idleTimeoutMs = options.idleTimeoutMs ?? defaultIdleTimeoutMs
    this.maxConcurrency = options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY
    this.maxRescueAttempts = options.maxRescueAttempts ?? defaultMaxRescueAttempts
    this.windowSize = options.windowSize ?? DEFAULT_WINDOW_SIZE
    requirePositiveInteger(this.chunkSize, 'chunkSize')
    requirePositiveInteger(this.idleTimeoutMs, 'idleTimeoutMs')
    requirePositiveInteger(this.maxConcurrency, 'maxConcurrency')
    requireNonNegativeInteger(this.maxRescueAttempts, 'maxRescueAttempts')
    requirePositiveInteger(this.windowSize, 'windowSize')

    this.state = new ParallelSegmentLoaderState()
    this.transport = options.transport ?? new FetchHttpTransport()
    this.fLoader = createStoryaFragmentLoader(this)
    ParallelSegmentLoader.owners.set(this.fLoader, this)

    this.workers = Array.from(
      { length: this.maxConcurrency },
      (_, index) =>
        new SegmentLoadWorker({
          id: index + 1,
          loader: this,
          transport: this.transport,
        }),
    )
    for (const worker of this.workers) {
      worker.start()
    }
  }

  static fromFragmentLoader(
    constructor: FragmentLoaderConstructor | undefined,
  ): ParallelSegmentLoader | undefined {
    return constructor === undefined ? undefined : this.owners.get(constructor)
  }

  get hlsConfig(): HlsConfig | undefined {
    return this.config
  }

  get bandwidthEstimate(): number {
    return this.bandwidthEstimateValue
  }

  configure(config: HlsConfig): void {
    if (this.state.destroyed) {
      throw new Error('ParallelSegmentLoader 已经销毁')
    }
    if (this.config !== undefined && this.config !== config) {
      throw new Error('一个 ParallelSegmentLoader 只能绑定一个 Hls 实例')
    }
    this.config = config
  }

  update(mutation: (state: ParallelSegmentLoaderState) => undefined): void {
    if (this.state.destroyed) {
      throw new Error('ParallelSegmentLoader 已经销毁')
    }
    if (this.updating) {
      throw new Error('ParallelSegmentLoader.update() 不能嵌套调用')
    }

    // 所有共享状态修改必须同步完成, mutation 不能跨越 await
    this.updating = true
    try {
      const result: unknown = mutation(this.state)
      if (isPromiseLike(result)) {
        throw new Error('ParallelSegmentLoader.update() 不能跨越 await')
      }
      this.state.reconcile()
    } finally {
      this.updating = false
      this.state.revision += 1
      this.notifyListeners()
    }
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  getDiagnostics(): ParallelSegmentLoaderDiagnostics {
    return createParallelSegmentLoaderDiagnostics(
      this.state,
      this.maxConcurrency,
      this.bandwidthEstimateValue,
      this.workers.map(worker => worker.getDiagnostics()),
    )
  }

  recordTransfer(bytes: number, startedAt: number, completedAt: number): void {
    if (
      !Number.isSafeInteger(bytes) ||
      bytes <= 0 ||
      !Number.isFinite(startedAt) ||
      !Number.isFinite(completedAt) ||
      completedAt <= startedAt
    ) {
      return
    }

    this.transferSamples.push({ bytes, completedAt, startedAt })
    if (this.transferSamples.length > maxTransferSamples) {
      this.transferSamples.splice(0, this.transferSamples.length - maxTransferSamples)
    }
    this.bandwidthEstimateValue = estimateBandwidth(this.transferSamples)
  }

  destroy(): void {
    if (this.state.destroyed) {
      return
    }

    ParallelSegmentLoader.owners.delete(this.fLoader)
    this.update(state => {
      state.destroyed = true
      return undefined
    })
    for (const worker of this.workers) {
      worker.destroy()
    }
    this.transport.destroy()
  }

  private notifyListeners(): void {
    if (this.notificationScheduled) {
      return
    }
    this.notificationScheduled = true
    queueMicrotask(() => {
      this.notificationScheduled = false
      for (const listener of [...this.listeners]) {
        listener()
      }
      if (this.state.destroyed) {
        this.listeners.clear()
      }
    })
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    'then' in value &&
    typeof value.then === 'function'
  )
}

function requirePositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} 必须是正整数`)
  }
}

function requireNonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} 必须是非负整数`)
  }
}

function estimateBandwidth(samples: readonly TransferSample[]): number {
  // 并行 GET 的字节全部计入, 但重叠的活跃时间只计算一次
  const sorted = [...samples].sort(
    (left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt,
  )
  let activeStartedAt: number | undefined
  let activeCompletedAt = 0
  let activeDuration = 0
  let bytes = 0

  for (const sample of sorted) {
    bytes += sample.bytes
    if (activeStartedAt === undefined) {
      activeStartedAt = sample.startedAt
      activeCompletedAt = sample.completedAt
      continue
    }
    if (sample.startedAt > activeCompletedAt) {
      activeDuration += activeCompletedAt - activeStartedAt
      activeStartedAt = sample.startedAt
      activeCompletedAt = sample.completedAt
      continue
    }
    activeCompletedAt = Math.max(activeCompletedAt, sample.completedAt)
  }

  if (activeStartedAt !== undefined) {
    activeDuration += activeCompletedAt - activeStartedAt
  }
  return activeDuration > 0 ? (bytes * 8 * 1000) / activeDuration : 0
}
