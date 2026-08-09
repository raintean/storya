const maxRecentRescueEvents = 64

export type RescueReason = 'slow' | 'stall'
export type RescueOutcome = 'pending' | 'recovered'

export interface RescueEventDiagnostics {
  attempt: number
  chunkKey: string
  continueEtaMs: number | undefined
  currentRate: number | undefined
  discardedBytes: number
  elapsedMs: number
  generation: number
  id: number
  outcome: RescueOutcome
  peerCount: number | undefined
  peerMedianRate: number | undefined
  reason: RescueReason
  retryEtaMs: number | undefined
  segmentKey: string
  streamId: string
  timestamp: number
}

export interface RescueStatisticsDiagnostics {
  discardedBytes: number
  exhaustedStallCount: number
  pendingEvents: number
  recentEvents: RescueEventDiagnostics[]
  recoveredEvents: number
  slowEvents: number
  stallEvents: number
  totalEvents: number
}

export interface RescueEventRecord {
  attempt: number
  chunkKey: string
  continueEtaMs?: number
  currentRate?: number
  discardedBytes: number
  elapsedMs: number
  generation: number
  peerCount?: number
  peerMedianRate?: number
  reason: RescueReason
  retryEtaMs?: number
  segmentKey: string
  streamId: string
  timestamp: number
}

interface PendingChunkRescues {
  count: number
  eventIds: number[]
}

export class RescueTracker {
  private discardedBytes = 0
  private exhaustedStallCount = 0
  private nextEventId = 1
  private readonly pendingByChunk = new WeakMap<object, PendingChunkRescues>()
  private readonly recentEvents: RescueEventDiagnostics[] = []
  private readonly recentEventsById = new Map<number, RescueEventDiagnostics>()
  private recoveredEvents = 0
  private slowEvents = 0
  private stallEvents = 0
  private totalEvents = 0

  record(chunk: object, record: RescueEventRecord): void {
    const event: RescueEventDiagnostics = {
      ...record,
      continueEtaMs: record.continueEtaMs,
      currentRate: record.currentRate,
      id: this.nextEventId,
      outcome: 'pending',
      peerCount: record.peerCount,
      peerMedianRate: record.peerMedianRate,
      retryEtaMs: record.retryEtaMs,
    }
    this.nextEventId += 1
    this.totalEvents += 1
    this.discardedBytes += record.discardedBytes
    if (record.reason === 'stall') {
      this.stallEvents += 1
    } else {
      this.slowEvents += 1
    }

    const pending = this.pendingByChunk.get(chunk) ?? { count: 0, eventIds: [] }
    pending.count += 1
    pending.eventIds.push(event.id)
    if (pending.eventIds.length > maxRecentRescueEvents) {
      pending.eventIds.splice(0, pending.eventIds.length - maxRecentRescueEvents)
    }
    this.pendingByChunk.set(chunk, pending)

    this.recentEvents.push(event)
    this.recentEventsById.set(event.id, event)
    if (this.recentEvents.length > maxRecentRescueEvents) {
      const evicted = this.recentEvents.shift()
      if (evicted !== undefined) {
        this.recentEventsById.delete(evicted.id)
      }
    }
  }

  recordExhaustedStall(): void {
    this.exhaustedStallCount += 1
  }

  markRecovered(chunk: object): void {
    const pending = this.pendingByChunk.get(chunk)
    if (pending === undefined) {
      return
    }
    this.pendingByChunk.delete(chunk)
    this.recoveredEvents += pending.count
    for (const eventId of pending.eventIds) {
      const event = this.recentEventsById.get(eventId)
      if (event !== undefined) {
        event.outcome = 'recovered'
      }
    }
  }

  getDiagnostics(): RescueStatisticsDiagnostics {
    return {
      discardedBytes: this.discardedBytes,
      exhaustedStallCount: this.exhaustedStallCount,
      pendingEvents: this.totalEvents - this.recoveredEvents,
      recentEvents: this.recentEvents.map(event => ({ ...event })),
      recoveredEvents: this.recoveredEvents,
      slowEvents: this.slowEvents,
      stallEvents: this.stallEvents,
      totalEvents: this.totalEvents,
    }
  }
}
