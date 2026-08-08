import type { FragmentLoaderContext } from 'hls.js'
import { splitByteRanges } from './byte-ranges'
import { VirtualStreamChunk } from './virtual-stream-chunk'

export type VirtualStreamSegmentState = 'empty' | 'failed' | 'filling' | 'ready'

export interface SegmentLoadFailure {
  code: number
  message: string
  response: Response | null
}

export type VirtualStreamSegmentOutcome =
  | { type: 'pending' }
  | {
      code: number
      completedAt: number
      data: ArrayBuffer
      response: Response
      type: 'ready'
      url: string
    }
  | {
      completedAt: number
      failure: SegmentLoadFailure
      type: 'failed'
    }

export class VirtualStreamSegment {
  readonly chunks: VirtualStreamChunk[] = []
  context: FragmentLoaderContext
  readonly declaredRange: boolean
  duration: number
  fallbackAttempted = false
  readonly key: string
  length: number | undefined
  outcome: VirtualStreamSegmentOutcome = { type: 'pending' }
  readerCount = 0
  readonly resourceStart: number
  sequential = false
  start: number
  readonly streamId: string
  firstByteAt: number | undefined
  retryCount = 0
  startedAt: number | undefined
  validator: string | null = null

  constructor(streamId: string, context: FragmentLoaderContext, chunkSize: number) {
    const rangeStart = context.rangeStart ?? 0
    const rangeEnd = context.rangeEnd ?? 0
    this.streamId = streamId
    this.context = cloneFragmentLoaderContext(context)
    this.declaredRange = rangeEnd > rangeStart
    this.duration = context.part?.duration ?? context.frag.duration
    this.key = VirtualStreamSegment.createKey(context)
    this.length = this.declaredRange ? rangeEnd - rangeStart : undefined
    this.resourceStart = this.declaredRange ? rangeStart : 0
    this.start = context.part?.start ?? context.frag.start
    this.planChunks(chunkSize)
  }

  static createKey(context: FragmentLoaderContext): string {
    return [
      context.frag.type,
      context.frag.level,
      context.frag.sn,
      context.frag.cc,
      context.part?.index ?? 'segment',
      context.url,
      context.rangeStart ?? 0,
      context.rangeEnd ?? 0,
    ].join('\n')
  }

  get state(): VirtualStreamSegmentState {
    if (this.outcome.type === 'ready') {
      return 'ready'
    }
    if (this.outcome.type === 'failed') {
      return 'failed'
    }
    return this.chunks.some(chunk => chunk.state === 'filling') ? 'filling' : 'empty'
  }

  get loadedBytes(): number {
    return this.chunks.reduce((total, chunk) => total + chunk.loadedBytes, 0)
  }

  updateContext(context: FragmentLoaderContext): void {
    this.context = cloneFragmentLoaderContext(context)
    this.duration = context.part?.duration ?? context.frag.duration
    this.start = context.part?.start ?? context.frag.start
  }

  startReading(): void {
    this.readerCount += 1
    this.resetFailure()
  }

  stopReading(): void {
    this.readerCount = Math.max(0, this.readerCount - 1)
  }

  planChunks(chunkSize: number): void {
    // 长度未知时先规划一个 discovery Chunk
    const ranges =
      this.length === undefined
        ? [{ endExclusive: chunkSize, start: 0 }]
        : splitByteRanges(0, this.length, chunkSize)

    for (const [index, range] of ranges.entries()) {
      const existing = this.chunks.find(chunk => chunk.start === range.start)
      if (existing === undefined) {
        this.chunks.push(
          new VirtualStreamChunk(
            `${this.key}\nchunk:${range.start}`,
            index,
            range.start,
            range.endExclusive,
            true,
          ),
        )
      } else {
        existing.endExclusive = range.endExclusive
        existing.index = index
      }
    }
    this.chunks.sort((left, right) => left.start - right.start)
  }

  fallbackToSequential(): void {
    this.fallbackAttempted = true
    this.sequential = true
    this.length = undefined
    this.outcome = { type: 'pending' }
    this.chunks.splice(
      0,
      this.chunks.length,
      new VirtualStreamChunk(`${this.key}\nchunk:0`, 0, 0, undefined, false),
    )
  }

  fail(failure: SegmentLoadFailure, completedAt: number): void {
    this.outcome = { completedAt, failure, type: 'failed' }
    // 保留已经 ready 的 Chunk, 让后续正式读取只重试缺失部分
    for (const chunk of this.chunks) {
      if (chunk.phase.type === 'filling') {
        chunk.fail(failure)
      }
    }
  }

  assemble(completedAt: number): boolean {
    if (this.outcome.type === 'ready') {
      return true
    }
    if (this.outcome.type === 'failed') {
      return false
    }
    if (
      this.length === undefined ||
      this.chunks.length === 0 ||
      this.chunks.some(
        chunk =>
          chunk.phase.type !== 'ready' ||
          chunk.phase.data === undefined ||
          chunk.endExclusive === undefined,
      )
    ) {
      return false
    }

    const result = new Uint8Array(this.length)
    let cursor = 0
    for (const chunk of this.chunks) {
      if (
        chunk.start !== cursor ||
        chunk.endExclusive === undefined ||
        chunk.phase.type !== 'ready' ||
        chunk.phase.data === undefined
      ) {
        return false
      }
      result.set(chunk.phase.data, chunk.start)
      cursor = chunk.endExclusive
    }
    let lastChunk: VirtualStreamChunk | undefined
    for (const chunk of this.chunks) {
      if (chunk.phase.type === 'ready') {
        lastChunk = chunk
      }
    }
    if (cursor !== this.length || lastChunk?.phase.type !== 'ready') {
      return false
    }

    this.outcome = {
      code: lastChunk.phase.response.status,
      completedAt,
      data: result.buffer,
      response: lastChunk.phase.response,
      type: 'ready',
      url: lastChunk.phase.url,
    }
    // 组装 canonical Segment 后释放各 Chunk 持有的重复数据
    for (const chunk of this.chunks) {
      chunk.clearReadyData()
    }
    return true
  }

  private resetFailure(): void {
    if (this.outcome.type !== 'failed') {
      return
    }
    this.outcome = { type: 'pending' }
    for (const chunk of this.chunks) {
      chunk.resetFailure()
    }
  }
}

function cloneFragmentLoaderContext(context: FragmentLoaderContext): FragmentLoaderContext {
  return {
    ...context,
    headers: { ...(context.headers ?? {}) },
  }
}
