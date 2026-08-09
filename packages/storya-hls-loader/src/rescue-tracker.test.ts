import { RescueTracker, type RescueEventRecord } from './rescue-tracker.ts'

const tracker = new RescueTracker()
const chunk = {}

tracker.record(chunk, createRecord('stall', 10))
tracker.record(chunk, {
  ...createRecord('slow', 20),
  continueEtaMs: 4_000,
  currentRate: 100,
  peerCount: 2,
  peerMedianRate: 500,
  retryEtaMs: 2_100,
})
tracker.recordExhaustedStall()

const pending = tracker.getDiagnostics()
if (
  pending.totalEvents !== 2 ||
  pending.exhaustedStallCount !== 1 ||
  pending.stallEvents !== 1 ||
  pending.slowEvents !== 1 ||
  pending.pendingEvents !== 2 ||
  pending.recoveredEvents !== 0 ||
  pending.discardedBytes !== 30 ||
  pending.recentEvents.some(event => event.outcome !== 'pending')
) {
  throw new Error('RescueTracker 应当累计救援原因、丢弃字节和 pending 事件')
}

tracker.markRecovered(chunk)
const recovered = tracker.getDiagnostics()
if (
  recovered.pendingEvents !== 0 ||
  recovered.recoveredEvents !== 2 ||
  recovered.recentEvents.some(event => event.outcome !== 'recovered')
) {
  throw new Error('同一个 Chunk 完成后应当恢复它的全部救援事件')
}

const firstSnapshotEvent = recovered.recentEvents[0]
if (firstSnapshotEvent === undefined) {
  throw new Error('RescueTracker 应当返回最近事件')
}
firstSnapshotEvent.outcome = 'pending'
if (tracker.getDiagnostics().recentEvents[0]?.outcome !== 'recovered') {
  throw new Error('诊断快照不能暴露内部可变事件')
}

for (let index = 0; index < 65; index += 1) {
  tracker.record({}, createRecord('stall', 1))
}
const bounded = tracker.getDiagnostics()
if (bounded.totalEvents !== 67 || bounded.recentEvents.length !== 64) {
  throw new Error('RescueTracker 应当只保留最近 64 个事件, 同时维持累计计数')
}

function createRecord(
  reason: RescueEventRecord['reason'],
  discardedBytes: number,
): RescueEventRecord {
  return {
    attempt: 1,
    chunkKey: 'chunk-1',
    discardedBytes,
    elapsedMs: 2_000,
    generation: 1,
    reason,
    segmentKey: 'segment-1',
    streamId: 'stream-1',
    timestamp: 1_000,
  }
}
