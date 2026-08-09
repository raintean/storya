import type { SegmentLoadFailure } from './virtual-stream-segment'

export type VirtualStreamChunkState = 'empty' | 'failed' | 'filling' | 'ready'

export interface VirtualStreamChunkCompletion {
  completedAt: number
  data: Uint8Array
  firstByteAt: number
  response: Response
  url: string
}

export type VirtualStreamChunkPhase =
  | { lastFailure: string | undefined; type: 'empty' }
  | {
      data: Uint8Array | undefined
      generation: number
      loadedBytes: number
      startedAt: number
      type: 'filling'
      workerId: number
    }
  | {
      byteLength: number
      completedAt: number
      data: Uint8Array | undefined
      firstByteAt: number
      response: Response
      type: 'ready'
      url: string
    }
  | {
      failure: SegmentLoadFailure
      type: 'failed'
    }

export class VirtualStreamChunk {
  attempt = 0
  endExclusive: number | undefined
  index: number
  readonly key: string
  phase: VirtualStreamChunkPhase = { lastFailure: undefined, type: 'empty' }
  rangeEnabled: boolean
  rescueAttempts = 0
  readonly start: number

  constructor(
    key: string,
    index: number,
    start: number,
    endExclusive: number | undefined,
    rangeEnabled: boolean,
  ) {
    this.key = key
    this.index = index
    this.start = start
    this.endExclusive = endExclusive
    this.rangeEnabled = rangeEnabled
  }

  get state(): VirtualStreamChunkState {
    return this.phase.type
  }

  get loadedBytes(): number {
    if (this.phase.type === 'filling') {
      return this.phase.loadedBytes
    }
    if (this.phase.type === 'ready') {
      return this.phase.byteLength
    }
    return 0
  }

  claim(workerId: number, generation: number, startedAt: number): boolean {
    if (this.phase.type !== 'empty') {
      return false
    }
    this.attempt += 1
    this.phase = {
      data: undefined,
      generation,
      loadedBytes: 0,
      startedAt,
      type: 'filling',
      workerId,
    }
    return true
  }

  // generation 阻止取消或重新领取后的迟到结果写回
  isCurrent(generation: number): boolean {
    return this.phase.type === 'filling' && this.phase.generation === generation
  }

  updateProgress(generation: number, loadedBytes: number, data?: Uint8Array): boolean {
    if (!this.isCurrent(generation) || this.phase.type !== 'filling') {
      return false
    }
    if (data !== undefined) {
      if (loadedBytes !== this.phase.loadedBytes + data.byteLength) {
        return false
      }
      if (this.phase.data === undefined || this.phase.data.byteLength < loadedBytes) {
        const capacity = Math.max(loadedBytes, (this.phase.data?.byteLength ?? 0) * 2)
        const buffer = new Uint8Array(capacity)
        if (this.phase.data !== undefined) {
          buffer.set(this.phase.data.subarray(0, this.phase.loadedBytes))
        }
        this.phase.data = buffer
      }
      this.phase.data.set(data, this.phase.loadedBytes)
    }
    this.phase.loadedBytes = loadedBytes
    return true
  }

  copyProgressData(generation: number): Uint8Array | undefined {
    if (
      !this.isCurrent(generation) ||
      this.phase.type !== 'filling' ||
      this.phase.data === undefined
    ) {
      return undefined
    }
    return this.phase.data.slice(0, this.phase.loadedBytes)
  }

  complete(generation: number, completion: VirtualStreamChunkCompletion): boolean {
    if (!this.isCurrent(generation)) {
      return false
    }
    this.phase = {
      byteLength: completion.data.byteLength,
      completedAt: completion.completedAt,
      data: completion.data,
      firstByteAt: completion.firstByteAt,
      response: completion.response,
      type: 'ready',
      url: completion.url,
    }
    return true
  }

  release(generation: number, lastFailure?: string): boolean {
    if (!this.isCurrent(generation)) {
      return false
    }
    this.phase = { lastFailure, type: 'empty' }
    return true
  }

  rescue(generation: number, reason: string): boolean {
    if (!this.isCurrent(generation)) {
      return false
    }
    this.rescueAttempts += 1
    this.phase = { lastFailure: reason, type: 'empty' }
    return true
  }

  fail(failure: SegmentLoadFailure, generation?: number): boolean {
    if (generation !== undefined && !this.isCurrent(generation)) {
      return false
    }
    this.phase = { failure, type: 'failed' }
    return true
  }

  resetFailure(): void {
    if (this.phase.type === 'failed') {
      this.rescueAttempts = 0
      this.phase = { lastFailure: undefined, type: 'empty' }
    }
  }

  clearReadyData(): void {
    if (this.phase.type === 'ready') {
      this.phase.data = undefined
    }
  }
}
