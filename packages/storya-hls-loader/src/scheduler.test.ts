import { ChunkScheduler } from './scheduler.ts'
import type { ScheduledChunk } from './scheduler.ts'

class TestChunk implements ScheduledChunk {
  readonly createdAt = performance.now()
  readonly id: number
  private complete = false
  private readonly mediaTime: number
  private running = false
  starts = 0
  suspends = 0

  constructor(id: number, mediaTime: number) {
    this.id = id
    this.mediaTime = mediaTime
  }

  canRun(): boolean {
    return !this.complete
  }

  getPriority(playbackTime: number, playbackRate: number): number {
    return ((this.mediaTime - playbackTime) * 1_000) / playbackRate
  }

  isComplete(): boolean {
    return this.complete
  }

  isProtected(): boolean {
    return false
  }

  isRunning(): boolean {
    return this.running
  }

  start(): void {
    this.running = true
    this.starts += 1
  }

  suspend(): void {
    this.running = false
    this.suspends += 1
  }

  finish(): void {
    this.complete = true
    this.running = false
  }
}

let playbackTime = 0
const scheduler = new ChunkScheduler({
  getPlaybackRate: () => 1,
  getPlaybackTime: () => playbackTime,
  maxConcurrency: 2,
  scheduleIntervalMs: 60_000,
})
const first = new TestChunk(1, 10)
const second = new TestChunk(2, 11)
const third = new TestChunk(3, 12)

scheduler.add(first)
scheduler.add(second)
scheduler.add(third)
await flushSchedule()
assertRunning([first, second], [third], '初始调度')

playbackTime = 9
scheduler.notify()
await flushSchedule()
assertRunning([first, second], [third], '播放时间推进后')
if (!areEqual(first.suspends, 0) || !areEqual(second.suspends, 0)) {
  throw new Error('播放时间推进不应改变 Chunk 的相对顺序')
}

const urgent = new TestChunk(4, 9.5)
scheduler.add(urgent)
await flushSchedule()
assertRunning([urgent, first], [second, third], '更早的任务加入后')
if (!areEqual(second.suspends, 1)) {
  throw new Error(`更早的任务应抢占最低优先级任务, 实际抢占 ${second.suspends} 次`)
}

for (const task of [first, second, third, urgent]) {
  task.finish()
  scheduler.remove(task)
}

function assertRunning(running: TestChunk[], waiting: TestChunk[], scene: string): void {
  for (const task of running) {
    if (!task.isRunning()) {
      throw new Error(`${scene}失败, Chunk ${task.id} 应当运行`)
    }
  }
  for (const task of waiting) {
    if (task.isRunning()) {
      throw new Error(`${scene}失败, Chunk ${task.id} 不应运行`)
    }
  }
}

async function flushSchedule(): Promise<void> {
  await new Promise<void>(resolve => queueMicrotask(resolve))
}

function areEqual(left: number, right: number): boolean {
  return left === right
}
