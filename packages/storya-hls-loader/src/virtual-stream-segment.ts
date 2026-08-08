import type { FragmentLoaderContext } from 'hls.js'
import { splitByteRanges } from './byte-ranges'
import { VirtualStreamChunk } from './virtual-stream-chunk'

export type SegmentPlanningMethod = 'head' | 'range' | 'sequential'
export type SegmentPlanningSource = 'content-range' | 'head' | 'playlist' | 'response'
export type SegmentRangeMode = 'parallel' | 'sequential' | 'unverified'
export type VirtualStreamSegmentState =
  | 'failed'
  | 'filling'
  | 'planning'
  | 'queued'
  | 'ready'
  | 'verifying'

export type SegmentPlanningPhase =
  | {
      lastFailure: string | undefined
      method: SegmentPlanningMethod
      type: 'pending'
    }
  | {
      generation: number
      method: SegmentPlanningMethod
      startedAt: number
      type: 'probing'
      workerId: number
    }
  | {
      source: SegmentPlanningSource
      type: 'planned'
    }

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
  planning: SegmentPlanningPhase
  rangeMode: SegmentRangeMode = 'unverified'
  readerCount = 0
  readonly resourceStart: number
  start: number
  readonly streamId: string
  firstByteAt: number | undefined
  retryCount = 0
  startedAt: number | undefined
  validator: string | null = null

  constructor(
    streamId: string,
    context: FragmentLoaderContext,
    chunkSize: number,
    planningMethod: SegmentPlanningMethod,
  ) {
    const rangeStart = context.rangeStart ?? 0
    const rangeEnd = context.rangeEnd ?? 0
    this.streamId = streamId
    this.context = cloneFragmentLoaderContext(context)
    this.declaredRange = rangeEnd > rangeStart
    this.duration = context.part?.duration ?? context.frag.duration
    this.key = VirtualStreamSegment.createKey(context)
    this.length = this.declaredRange ? rangeEnd - rangeStart : undefined
    this.planning = this.declaredRange
      ? { source: 'playlist', type: 'planned' }
      : { lastFailure: undefined, method: planningMethod, type: 'pending' }
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
    if (this.planning.type !== 'planned') {
      return 'planning'
    }
    const filling = this.chunks.some(chunk => chunk.state === 'filling')
    if (filling && this.rangeMode === 'unverified') {
      return 'verifying'
    }
    return filling ? 'filling' : 'queued'
  }

  get loadedBytes(): number {
    return this.chunks.reduce((total, chunk) => total + chunk.loadedBytes, 0)
  }

  get sequential(): boolean {
    return this.rangeMode === 'sequential'
  }

  updateContext(context: FragmentLoaderContext): void {
    this.context = cloneFragmentLoaderContext(context)
    this.duration = context.part?.duration ?? context.frag.duration
    this.start = context.part?.start ?? context.frag.start
  }

  startReading(): void {
    this.readerCount += 1
    this.preferRangePlanning()
    this.resetFailure()
  }

  stopReading(): void {
    this.readerCount = Math.max(0, this.readerCount - 1)
  }

  preferRangePlanning(): void {
    if (this.planning.type === 'pending' && this.planning.method === 'head') {
      this.planning = {
        lastFailure: this.planning.lastFailure,
        method: 'range',
        type: 'pending',
      }
    }
  }

  claimPlanning(workerId: number, generation: number, startedAt: number): boolean {
    if (this.planning.type !== 'pending') {
      return false
    }
    this.planning = {
      generation,
      method: this.planning.method,
      startedAt,
      type: 'probing',
      workerId,
    }
    return true
  }

  isPlanningCurrent(generation: number): boolean {
    return this.planning.type === 'probing' && this.planning.generation === generation
  }

  releasePlanning(generation: number, lastFailure?: string): boolean {
    if (!this.isPlanningCurrent(generation) || this.planning.type !== 'probing') {
      return false
    }
    this.planning = {
      lastFailure,
      method: this.planning.method,
      type: 'pending',
    }
    return true
  }

  fallbackPlanning(generation: number, lastFailure: string): boolean {
    if (!this.isPlanningCurrent(generation)) {
      return false
    }
    this.planning = { lastFailure, method: 'range', type: 'pending' }
    return true
  }

  completePlanning(
    length: number,
    source: SegmentPlanningSource,
    chunkSize: number,
    generation?: number,
  ): boolean {
    if (
      !Number.isSafeInteger(length) ||
      length <= 0 ||
      (generation !== undefined && !this.isPlanningCurrent(generation))
    ) {
      return false
    }
    this.length = length
    this.planning = { source, type: 'planned' }
    this.planChunks(chunkSize)
    return true
  }

  ensureLeadingChunk(chunkSize: number, rangeEnabled: boolean): VirtualStreamChunk {
    let chunk = this.chunks.find(item => item.start === 0)
    if (chunk === undefined) {
      chunk = new VirtualStreamChunk(
        `${this.key}\nchunk:0`,
        0,
        0,
        rangeEnabled ? chunkSize : undefined,
        rangeEnabled,
      )
      this.chunks.push(chunk)
    }
    return chunk
  }

  verifyRange(): void {
    this.rangeMode = 'parallel'
  }

  useSequentialRange(): void {
    this.rangeMode = 'sequential'
  }

  planChunks(chunkSize: number): void {
    if (this.length === undefined) {
      return
    }

    this.chunks.sort((left, right) => left.start - right.start)
    let plannedEnd = 0
    for (const chunk of this.chunks) {
      if (chunk.start !== plannedEnd || chunk.endExclusive === undefined) {
        break
      }
      plannedEnd = chunk.endExclusive
    }
    const ranges = splitByteRanges(plannedEnd, this.length, chunkSize)

    for (const range of ranges) {
      this.chunks.push(
        new VirtualStreamChunk(
          `${this.key}\nchunk:${range.start}`,
          this.chunks.length,
          range.start,
          range.endExclusive,
          true,
        ),
      )
    }
    this.chunks.sort((left, right) => left.start - right.start)
    for (const [index, chunk] of this.chunks.entries()) {
      chunk.index = index
    }
  }

  fallbackToSequential(lastFailure: string): void {
    this.fallbackAttempted = true
    this.rangeMode = 'sequential'
    this.length = undefined
    this.outcome = { type: 'pending' }
    this.planning = { lastFailure, method: 'sequential', type: 'pending' }
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
    if (this.planning.type === 'probing') {
      this.planning = {
        lastFailure: 'Segment 失败后重新规划',
        method: this.planning.method,
        type: 'pending',
      }
    }
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
