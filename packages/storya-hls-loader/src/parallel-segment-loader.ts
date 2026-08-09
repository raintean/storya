import type { FragmentLoaderConstructor, HlsConfig } from 'hls.js'
import { FetchHttpTransport, type HttpTransport } from 'storya-transport'
import { SegmentLoadWorker } from './segment-load-worker'
import {
  createParallelSegmentLoaderDiagnostics,
  type ParallelSegmentLoaderDiagnostics,
} from './diagnostics'
import { createStoryaFragmentLoader } from './fragment-loader'
import { ParallelSegmentLoaderState } from './parallel-segment-loader-state'
import { RescueTracker, type RescueEventRecord } from './rescue-tracker'
import { TransferTracker, type TransferRateComparison } from './transfer-tracker'
import type { VirtualStreamChunk } from './virtual-stream-chunk'

export const DEFAULT_CHUNK_SIZE = 2 * 1024 * 1024
export const DEFAULT_MAX_CONCURRENCY = 6
export const DEFAULT_RESCUE_OPTIONS = Object.freeze({
  maxAttempts: 2,
  slowRateThresholdRatio: 0.25,
  stallTimeoutMs: 4_000,
})
export const DEFAULT_WINDOW_SIZE = 6

const NOTIFICATION_INTERVAL_MS = 8

export interface ParallelSegmentLoaderRescueOptions {
  maxAttempts?: number
  slowRateThresholdRatio?: number
  stallTimeoutMs?: number
}

export interface ParallelSegmentLoaderOptions {
  chunkSize?: number
  maxConcurrency?: number
  rescue?: false | ParallelSegmentLoaderRescueOptions
  transport?: HttpTransport
  windowSize?: number
}

export class ParallelSegmentLoader {
  private static readonly owners = new WeakMap<FragmentLoaderConstructor, ParallelSegmentLoader>()

  readonly chunkSize: number
  readonly fLoader: FragmentLoaderConstructor
  readonly maxConcurrency: number
  readonly rescue: Readonly<Required<ParallelSegmentLoaderRescueOptions>>
  readonly state: ParallelSegmentLoaderState
  readonly windowSize: number

  private config: HlsConfig | undefined
  private lastNotificationAt: number | undefined
  private readonly listeners = new Set<() => void>()
  private notificationDirty = false
  private notificationMicrotaskScheduled = false
  private notificationTimer: ReturnType<typeof globalThis.setTimeout> | undefined
  private readonly rescueTracker = new RescueTracker()
  private readonly transport: HttpTransport
  private readonly transferTracker: TransferTracker
  private updating = false
  private readonly workers: SegmentLoadWorker[]

  constructor(options: ParallelSegmentLoaderOptions = {}) {
    this.chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE
    this.maxConcurrency = options.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY
    this.rescue = resolveRescueOptions(options.rescue)
    this.windowSize = options.windowSize ?? DEFAULT_WINDOW_SIZE
    requirePositiveInteger(this.chunkSize, 'chunkSize')
    requirePositiveInteger(this.maxConcurrency, 'maxConcurrency')
    requireNonNegativeInteger(this.rescue.maxAttempts, 'rescue.maxAttempts')
    requireRatio(this.rescue.slowRateThresholdRatio, 'rescue.slowRateThresholdRatio')
    requirePositiveInteger(this.rescue.stallTimeoutMs, 'rescue.stallTimeoutMs')
    requirePositiveInteger(this.windowSize, 'windowSize')

    this.state = new ParallelSegmentLoaderState()
    this.transferTracker = new TransferTracker(this.rescue.stallTimeoutMs)
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
    return this.transferTracker.bandwidthEstimate
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
      this.transferTracker.bandwidthEstimate,
      this.rescueTracker.getDiagnostics(),
      this.workers.map(worker => worker.getDiagnostics()),
    )
  }

  recordTransfer(bytes: number, startedAt: number, completedAt: number): void {
    this.transferTracker.recordCompletedTransfer(bytes, startedAt, completedAt)
  }

  startTransfer(generation: number, startedAt: number): void {
    this.transferTracker.start(generation, startedAt)
  }

  startTransferBody(generation: number, startedAt: number): void {
    this.transferTracker.startBody(generation, startedAt)
  }

  recordTransferProgress(generation: number, bytes: number, recordedAt: number): void {
    this.transferTracker.recordProgress(generation, bytes, recordedAt)
  }

  finishTransfer(generation: number, completedAt: number, bodyCompleted: boolean): void {
    this.transferTracker.finish(generation, completedAt, bodyCompleted)
  }

  compareTransferRate(generation: number, now: number): TransferRateComparison | undefined {
    return this.transferTracker.compareWithPeers(generation, now)
  }

  recordRescue(chunk: VirtualStreamChunk, event: RescueEventRecord): void {
    this.rescueTracker.record(chunk, event)
  }

  recordExhaustedStall(): void {
    this.rescueTracker.recordExhaustedStall()
  }

  markRescueRecovered(chunk: VirtualStreamChunk): void {
    this.rescueTracker.markRecovered(chunk)
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
    this.notificationDirty = true
    this.scheduleNotification()
  }

  private scheduleNotification(): void {
    if (this.notificationMicrotaskScheduled) {
      return
    }

    const now = performance.now()
    const elapsed = this.lastNotificationAt === undefined ? Infinity : now - this.lastNotificationAt
    const remaining = NOTIFICATION_INTERVAL_MS - elapsed
    if (remaining > 0) {
      if (this.notificationTimer === undefined) {
        this.notificationTimer = globalThis.setTimeout(() => {
          this.notificationTimer = undefined
          this.scheduleNotification()
        }, remaining)
      }
      return
    }

    if (this.notificationTimer !== undefined) {
      globalThis.clearTimeout(this.notificationTimer)
      this.notificationTimer = undefined
    }
    this.notificationMicrotaskScheduled = true
    queueMicrotask(() => {
      this.notificationMicrotaskScheduled = false
      if (!this.notificationDirty) {
        return
      }

      this.notificationDirty = false
      this.lastNotificationAt = performance.now()
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

function requireRatio(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0 || value >= 1) {
    throw new Error(`${name} 必须是大于等于 0 且小于 1 的有限数值`)
  }
}

function resolveRescueOptions(
  options: false | ParallelSegmentLoaderRescueOptions | undefined,
): Readonly<Required<ParallelSegmentLoaderRescueOptions>> {
  return Object.freeze({
    ...DEFAULT_RESCUE_OPTIONS,
    ...(options === false ? { maxAttempts: 0 } : options),
  })
}
