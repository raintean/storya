import type { HttpTransport } from 'storya-transport'
import type { WorkerDiagnostics } from './diagnostics'
import type { ParallelSegmentLoader } from './parallel-segment-loader'
import { SegmentFetchWork, type SegmentFetchWorkOptions } from './segment-fetch-work'
import { SegmentPlanningWork, type SegmentPlanningWorkOptions } from './segment-planning-work'
import type { VirtualStreamChunk } from './virtual-stream-chunk'
import type { VirtualStreamSegment } from './virtual-stream-segment'

interface WorkCandidate {
  chunk: VirtualStreamChunk | undefined
  segment: VirtualStreamSegment
  type: 'chunk' | 'head' | 'range' | 'sequential'
  windowIndex: number
}

interface SegmentLoadWorkerOptions {
  id: number
  loader: ParallelSegmentLoader
  transport: HttpTransport
}

export class SegmentLoadWorker {
  readonly id: number

  private activeWork: SegmentFetchWork | SegmentPlanningWork | undefined
  private destroyed = false
  private readonly loader: ParallelSegmentLoader
  private scheduled = false
  private state: 'idle' | 'loading' | 'stopped' = 'idle'
  private readonly transport: HttpTransport
  private unsubscribe: (() => void) | undefined

  constructor(options: SegmentLoadWorkerOptions) {
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
    this.activeWork?.cancel(new DOMException('SegmentLoadWorker 已销毁', 'AbortError'))
    this.state = 'stopped'
  }

  getDiagnostics(): WorkerDiagnostics {
    return {
      chunkKey: this.activeWork?.chunkKey,
      id: this.id,
      method: this.activeWork?.method,
      requestEnd: this.activeWork?.requestEnd,
      requestStart: this.activeWork?.requestStart,
      segmentKey: this.activeWork?.segmentKey,
      startedAt: this.activeWork?.startedAt,
      state: this.state,
      streamId: this.activeWork?.streamId,
      task: this.activeWork?.task,
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

  private takeNextWork(): SegmentFetchWork | SegmentPlanningWork | undefined {
    const candidate = this.selectBestWork()
    if (candidate === undefined) {
      return undefined
    }

    const startedAt = performance.now()
    let chunkOptions: SegmentFetchWorkOptions | undefined
    let planningOptions: SegmentPlanningWorkOptions | undefined
    this.loader.update(state => {
      const segment = state.streams
        .get(candidate.segment.streamId)
        ?.segments.get(candidate.segment.key)
      if (segment === undefined || segment.outcome.type !== 'pending') {
        return undefined
      }

      const generation = state.allocateGeneration()
      if (candidate.type === 'head') {
        if (!segment.claimPlanning(this.id, generation, startedAt)) {
          return undefined
        }
        planningOptions = {
          context: segment.context,
          generation,
          loader: this.loader,
          segmentKey: segment.key,
          startedAt,
          streamId: segment.streamId,
          transport: this.transport,
        }
        return undefined
      }

      const planning = candidate.type === 'range' || candidate.type === 'sequential'
      if (planning && !segment.claimPlanning(this.id, generation, startedAt)) {
        return undefined
      }
      const chunk = planning
        ? segment.ensureLeadingChunk(this.loader.chunkSize, candidate.type === 'range')
        : candidate.chunk === undefined
          ? undefined
          : segment.chunks.find(item => item.key === candidate.chunk?.key)
      if (chunk === undefined || !chunk.claim(this.id, generation, startedAt)) {
        if (planning) {
          segment.releasePlanning(generation)
        }
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
      chunkOptions = {
        chunkKey: chunk.key,
        context: segment.context,
        generation,
        loader: this.loader,
        planning,
        rangeEnabled: chunk.rangeEnabled,
        requestEnd,
        requestStart,
        resourceLength:
          segment.length === undefined ? undefined : segment.resourceStart + segment.length,
        rescueAvailable: chunk.rescueAttempts < this.loader.rescue.maxAttempts,
        segmentKey: segment.key,
        stallDetectionEnabled: this.loader.rescue.maxAttempts > 0,
        startedAt,
        streamId: segment.streamId,
        transport: this.transport,
      }
      return undefined
    })
    if (planningOptions !== undefined) {
      return new SegmentPlanningWork(planningOptions)
    }
    return chunkOptions === undefined ? undefined : new SegmentFetchWork(chunkOptions)
  }

  private selectBestWork(): WorkCandidate | undefined {
    let best: WorkCandidate | undefined
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
        if (segment.planning.type === 'pending') {
          const candidate: WorkCandidate = {
            chunk: undefined,
            segment,
            type: segment.planning.method,
            windowIndex,
          }
          if (
            (candidate.type === 'head' || stream.canScheduleData(segment.key)) &&
            (best === undefined || compareWorkCandidates(candidate, best) < 0)
          ) {
            best = candidate
          }
          continue
        }
        if (segment.planning.type === 'probing' || !stream.canScheduleData(segment.key)) {
          continue
        }
        for (const chunk of segment.chunks) {
          if (
            chunk.phase.type !== 'empty' ||
            (segment.rangeMode === 'unverified' && chunk.index !== 0)
          ) {
            continue
          }
          const candidate: WorkCandidate = { chunk, segment, type: 'chunk', windowIndex }
          if (best === undefined || compareWorkCandidates(candidate, best) < 0) {
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
    if (!this.activeWork.isCurrent()) {
      this.activeWork.cancel(new DOMException('Chunk Fill 已失效', 'AbortError'))
    }
  }
}

function compareWorkCandidates(left: WorkCandidate, right: WorkCandidate): number {
  const leftDirect = left.segment.readerCount > 0
  const rightDirect = right.segment.readerCount > 0
  if (leftDirect !== rightDirect) {
    return leftDirect ? -1 : 1
  }
  return (
    left.windowIndex - right.windowIndex ||
    (left.chunk?.index ?? 0) - (right.chunk?.index ?? 0) ||
    (left.chunk?.key ?? left.segment.key).localeCompare(right.chunk?.key ?? right.segment.key)
  )
}
