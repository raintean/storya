export interface SchedulerOptions {
  getPlaybackRate: () => number
  getPlaybackTime: () => number
  maxConcurrency: number
  scheduleIntervalMs: number
}

export interface ScheduledChunk {
  readonly createdAt: number
  readonly id: number

  canRun(now: number): boolean
  getPriority(playbackTime: number, playbackRate: number): number
  isComplete(): boolean
  isProtected(now: number): boolean
  isRunning(): boolean
  start(): void
  suspend(): void
}

interface ThroughputEstimate {
  bytesPerSecond: number
  samples: number
}

const defaultThroughputBytesPerSecond = 2_000_000
const throughputEwmaAlpha = 0.3

export class ChunkScheduler {
  private estimate: ThroughputEstimate | undefined
  private readonly options: SchedulerOptions
  private readonly tasks = new Set<ScheduledChunk>()
  private schedulePending = false
  private timer: number | undefined

  constructor(options: SchedulerOptions) {
    this.options = options
  }

  add(task: ScheduledChunk): void {
    this.tasks.add(task)
    this.ensureTimer()
    this.requestSchedule()
  }

  remove(task: ScheduledChunk): void {
    this.tasks.delete(task)
    if (this.tasks.size === 0) {
      this.stopTimer()
    }
    this.requestSchedule()
  }

  notify(): void {
    this.requestSchedule()
  }

  getEstimatedThroughput(): number {
    return this.estimate?.bytesPerSecond ?? defaultThroughputBytesPerSecond
  }

  reportThroughput(bytes: number, durationMs: number): void {
    if (bytes <= 0 || durationMs < 100) {
      return
    }

    const sample = (bytes * 1_000) / durationMs
    const current = this.estimate
    if (current === undefined) {
      this.estimate = { bytesPerSecond: sample, samples: 1 }
      return
    }

    current.bytesPerSecond =
      current.bytesPerSecond * (1 - throughputEwmaAlpha) + sample * throughputEwmaAlpha
    current.samples += 1
  }

  hasThroughputSamples(minimumSamples: number): boolean {
    return (this.estimate?.samples ?? 0) >= minimumSamples
  }

  private ensureTimer(): void {
    if (this.timer !== undefined) {
      return
    }

    this.timer = globalThis.setInterval(() => {
      this.schedule()
    }, this.options.scheduleIntervalMs)
  }

  private stopTimer(): void {
    if (this.timer === undefined) {
      return
    }

    globalThis.clearInterval(this.timer)
    this.timer = undefined
  }

  private requestSchedule(): void {
    if (this.schedulePending) {
      return
    }

    this.schedulePending = true
    queueMicrotask(() => {
      this.schedulePending = false
      this.schedule()
    })
  }

  private schedule(): void {
    const now = performance.now()
    const playbackTime = this.readFinite(this.options.getPlaybackTime(), 0)
    const playbackRate = Math.max(this.readFinite(this.options.getPlaybackRate(), 1), 0.1)
    const available = [...this.tasks].filter(task => !task.isComplete() && task.canRun(now))
    const protectedTasks = available.filter(task => task.isRunning() && task.isProtected(now))
    const selected = new Set<ScheduledChunk>(protectedTasks)

    const ranked = available
      .filter(task => !selected.has(task))
      .sort((left, right) => {
        const difference =
          left.getPriority(playbackTime, playbackRate) -
          right.getPriority(playbackTime, playbackRate)
        return difference || left.createdAt - right.createdAt || left.id - right.id
      })

    for (const task of ranked) {
      if (selected.size >= this.options.maxConcurrency) {
        break
      }
      selected.add(task)
    }

    for (const task of available) {
      if (task.isRunning() && !selected.has(task)) {
        task.suspend()
      }
    }

    for (const task of selected) {
      if (!task.isRunning() && !task.isComplete()) {
        task.start()
      }
    }
  }

  private readFinite(value: number, fallback: number): number {
    return Number.isFinite(value) ? value : fallback
  }
}
