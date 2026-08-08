import type { HttpTransport } from 'storya-transport'
import type { WorkerDiagnostics } from './diagnostics'
import type { ParallelSegmentLoader } from './parallel-segment-loader'
import { ChunkFillWork, type ChunkFillWorkOptions } from './chunk-fill-work'
import type { VirtualStreamChunk } from './virtual-stream-chunk'
import type { VirtualStreamSegment } from './virtual-stream-segment'

interface ChunkCandidate {
  chunk: VirtualStreamChunk
  segment: VirtualStreamSegment
  windowIndex: number
}

interface ChunkFillWorkerOptions {
  id: number
  loader: ParallelSegmentLoader
  transport: HttpTransport
}

export class ChunkFillWorker {
  readonly id: number

  private activeWork: ChunkFillWork | undefined
  private destroyed = false
  private readonly loader: ParallelSegmentLoader
  private scheduled = false
  private state: 'idle' | 'loading' | 'stopped' = 'idle'
  private readonly transport: HttpTransport
  private unsubscribe: (() => void) | undefined

  constructor(options: ChunkFillWorkerOptions) {
    this.id = options.id
    this.loader = options.loader
    this.transport = options.transport
  }

  start(): void {
    if (this.unsubscribe !== undefined || this.destroyed) {
      return
    }
    this.unsubscribe = this.loader.subscribe(() => this.handleChange())
    this.schedule()
  }

  destroy(): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.unsubscribe?.()
    this.unsubscribe = undefined
    this.activeWork?.cancel(new DOMException('ChunkFillWorker 已销毁', 'AbortError'))
    this.state = 'stopped'
  }

  getDiagnostics(): WorkerDiagnostics {
    return {
      chunkKey: this.activeWork?.chunkKey,
      id: this.id,
      requestEnd: this.activeWork?.requestEnd,
      requestStart: this.activeWork?.requestStart,
      segmentKey: this.activeWork?.segmentKey,
      startedAt: this.activeWork?.startedAt,
      state: this.state,
      streamId: this.activeWork?.streamId,
    }
  }

  private schedule(): void {
    if (this.destroyed || this.scheduled || this.activeWork !== undefined) {
      return
    }
    this.scheduled = true
    queueMicrotask(() => {
      this.scheduled = false
      if (this.destroyed || this.activeWork !== undefined) {
        return
      }

      const work = this.takeNextWork()
      if (work === undefined) {
        this.state = 'idle'
        return
      }

      this.activeWork = work
      this.state = 'loading'
      void work
        .run()
        .catch(() => undefined)
        .finally(() => {
          this.activeWork = undefined
          if (!this.destroyed) {
            this.state = 'idle'
            this.schedule()
          }
        })
    })
  }

  private takeNextWork(): ChunkFillWork | undefined {
    const candidate = this.selectBestChunk()
    if (candidate === undefined) {
      return undefined
    }

    const startedAt = performance.now()
    let options: ChunkFillWorkOptions | undefined
    this.loader.update(state => {
      const segment = state.streams
        .get(candidate.segment.streamId)
        ?.segments.get(candidate.segment.key)
      const chunk = segment?.chunks.find(item => item.key === candidate.chunk.key)
      if (segment === undefined || chunk === undefined || segment.outcome.type !== 'pending') {
        return undefined
      }

      const generation = state.allocateGeneration()
      if (!chunk.claim(this.id, generation, startedAt)) {
        return undefined
      }
      segment.startedAt ??= startedAt
      const requestStart = chunk.rangeEnabled
        ? segment.resourceStart + chunk.start
        : segment.resourceStart
      const requestEnd = chunk.rangeEnabled
        ? chunk.endExclusive === undefined
          ? undefined
          : segment.resourceStart + chunk.endExclusive
        : undefined
      options = {
        chunkKey: chunk.key,
        context: segment.context,
        generation,
        loader: this.loader,
        rangeEnabled: chunk.rangeEnabled,
        requestEnd,
        requestStart,
        resourceLength:
          segment.length === undefined ? undefined : segment.resourceStart + segment.length,
        segmentKey: segment.key,
        startedAt,
        streamId: segment.streamId,
        transport: this.transport,
      }
      return undefined
    })
    return options === undefined ? undefined : new ChunkFillWork(options)
  }

  private selectBestChunk(): ChunkCandidate | undefined {
    let best: ChunkCandidate | undefined
    for (const stream of this.loader.state.streams.values()) {
      for (const segment of stream.segments.values()) {
        if (segment.outcome.type !== 'pending') {
          continue
        }
        const index = stream.window.indexOf(segment.key)
        if (index === -1 && segment.readerCount === 0) {
          continue
        }
        const windowIndex = index === -1 ? Number.MAX_SAFE_INTEGER : index
        for (const chunk of segment.chunks) {
          if (chunk.phase.type !== 'empty') {
            continue
          }
          const candidate = { chunk, segment, windowIndex }
          if (best === undefined || compareChunkCandidates(candidate, best) < 0) {
            best = candidate
          }
        }
      }
    }
    return best
  }

  private handleChange(): void {
    if (this.destroyed) {
      return
    }
    if (this.activeWork === undefined) {
      this.schedule()
      return
    }
    if (!this.activeWork.isCurrent() || this.shouldPreempt(this.activeWork)) {
      this.activeWork.cancel(new DOMException('Chunk Fill 已失效', 'AbortError'))
    }
  }

  private shouldPreempt(work: ChunkFillWork): boolean {
    const urgent = this.selectBestChunk()
    if (urgent === undefined || urgent.segment.readerCount === 0) {
      return false
    }

    const active = this.locateActiveWork(work)
    if (active === undefined || active.segment.readerCount > 0) {
      return false
    }
    const activeStream = this.loader.state.streams.get(active.segment.streamId)
    const activeIndex = activeStream?.window.indexOf(active.segment.key) ?? -1
    const activeCandidate: ChunkCandidate = {
      chunk: active.chunk,
      segment: active.segment,
      windowIndex: activeIndex === -1 ? Number.MAX_SAFE_INTEGER : activeIndex,
    }

    let victim: ChunkCandidate | undefined
    for (const stream of this.loader.state.streams.values()) {
      for (const segment of stream.segments.values()) {
        if (segment.readerCount > 0) {
          continue
        }
        const index = stream.window.indexOf(segment.key)
        const windowIndex = index === -1 ? Number.MAX_SAFE_INTEGER : index
        for (const chunk of segment.chunks) {
          if (chunk.phase.type !== 'filling') {
            continue
          }
          const candidate = { chunk, segment, windowIndex }
          if (victim === undefined || compareChunkCandidates(candidate, victim) > 0) {
            victim = candidate
          }
        }
      }
    }

    return (
      victim?.chunk.phase.type === 'filling' &&
      victim.chunk.phase.workerId === this.id &&
      compareChunkCandidates(urgent, activeCandidate) < 0
    )
  }

  private locateActiveWork(
    work: ChunkFillWork,
  ): { chunk: VirtualStreamChunk; segment: VirtualStreamSegment } | undefined {
    const segment = this.loader.state.streams.get(work.streamId)?.segments.get(work.segmentKey)
    const chunk = segment?.chunks.find(item => item.key === work.chunkKey)
    return segment !== undefined && chunk?.isCurrent(work.generation)
      ? { chunk, segment }
      : undefined
  }
}

function compareChunkCandidates(left: ChunkCandidate, right: ChunkCandidate): number {
  const leftDirect = left.segment.readerCount > 0
  const rightDirect = right.segment.readerCount > 0
  if (leftDirect !== rightDirect) {
    return leftDirect ? -1 : 1
  }
  return (
    left.windowIndex - right.windowIndex ||
    left.chunk.index - right.chunk.index ||
    left.chunk.key.localeCompare(right.chunk.key)
  )
}
