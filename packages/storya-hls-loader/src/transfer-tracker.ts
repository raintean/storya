const maxTransferSamples = 64

interface ActiveTransfer {
  bodyStartedAt: number | undefined
  bytes: number
  generation: number
  progress: TransferProgressSample[]
  startedAt: number
}

interface CompletedPeerTransfer {
  bodyStartedAt: number
  bytes: number
  completedAt: number
  startedAt: number
}

interface TransferProgressSample {
  at: number
  bytes: number
}

interface TransferSample {
  bytes: number
  completedAt: number
  startedAt: number
}

export interface TransferRateComparison {
  currentRate: number
  peerCount: number
  peerMedianRate: number
  peerMedianTtfbMs: number
}

export class TransferTracker {
  private readonly active = new Map<number, ActiveTransfer>()
  private bandwidthEstimateValue = 0
  private readonly completedPeers: CompletedPeerTransfer[] = []
  private readonly samples: TransferSample[] = []
  private readonly windowMs: number

  constructor(windowMs: number) {
    this.windowMs = windowMs
  }

  get bandwidthEstimate(): number {
    return this.bandwidthEstimateValue
  }

  start(generation: number, startedAt: number): void {
    if (!Number.isSafeInteger(generation) || !Number.isFinite(startedAt)) {
      return
    }
    this.active.set(generation, {
      bodyStartedAt: undefined,
      bytes: 0,
      generation,
      progress: [],
      startedAt,
    })
  }

  startBody(generation: number, startedAt: number): void {
    const transfer = this.active.get(generation)
    if (
      transfer === undefined ||
      transfer.bodyStartedAt !== undefined ||
      !Number.isFinite(startedAt)
    ) {
      return
    }
    transfer.bodyStartedAt = startedAt
    transfer.progress.push({ at: startedAt, bytes: 0 })
  }

  recordProgress(generation: number, bytes: number, recordedAt: number): void {
    const transfer = this.active.get(generation)
    if (
      transfer === undefined ||
      !Number.isSafeInteger(bytes) ||
      bytes <= 0 ||
      !Number.isFinite(recordedAt)
    ) {
      return
    }
    if (transfer.bodyStartedAt === undefined) {
      this.startBody(generation, recordedAt)
    }
    transfer.bytes += bytes
    const last = transfer.progress.at(-1)
    if (last?.at === recordedAt) {
      last.bytes = transfer.bytes
    } else {
      transfer.progress.push({ at: recordedAt, bytes: transfer.bytes })
    }
    pruneProgress(transfer.progress, recordedAt - this.windowMs)
  }

  finish(generation: number, completedAt: number, bodyCompleted: boolean): void {
    const transfer = this.active.get(generation)
    if (transfer === undefined) {
      return
    }
    this.active.delete(generation)
    if (!Number.isFinite(completedAt) || completedAt <= transfer.startedAt) {
      return
    }
    this.recordCompletedTransfer(transfer.bytes, transfer.startedAt, completedAt)
    if (
      bodyCompleted &&
      transfer.bytes > 0 &&
      transfer.bodyStartedAt !== undefined &&
      completedAt > transfer.bodyStartedAt
    ) {
      this.completedPeers.push({
        bodyStartedAt: transfer.bodyStartedAt,
        bytes: transfer.bytes,
        completedAt,
        startedAt: transfer.startedAt,
      })
      if (this.completedPeers.length > maxTransferSamples) {
        this.completedPeers.splice(0, this.completedPeers.length - maxTransferSamples)
      }
    }
  }

  recordCompletedTransfer(bytes: number, startedAt: number, completedAt: number): void {
    if (
      !Number.isSafeInteger(bytes) ||
      bytes <= 0 ||
      !Number.isFinite(startedAt) ||
      !Number.isFinite(completedAt) ||
      completedAt <= startedAt
    ) {
      return
    }
    this.samples.push({ bytes, completedAt, startedAt })
    if (this.samples.length > maxTransferSamples) {
      this.samples.splice(0, this.samples.length - maxTransferSamples)
    }
    this.bandwidthEstimateValue = estimateBandwidth(this.samples)
  }

  compareWithPeers(generation: number, now: number): TransferRateComparison | undefined {
    const current = this.active.get(generation)
    if (
      current?.bodyStartedAt === undefined ||
      !Number.isFinite(now) ||
      now - current.bodyStartedAt < this.windowMs
    ) {
      return undefined
    }

    const currentRate = calculateActiveRate(current, now, this.windowMs)
    if (currentRate <= 0) {
      return undefined
    }

    const peerRates: number[] = []
    const peerTtfbValues: number[] = []
    for (const peer of this.active.values()) {
      if (peer.generation === generation || peer.bodyStartedAt === undefined) {
        continue
      }
      const rate = calculateActiveRate(peer, now, this.windowMs)
      if (rate <= 0) {
        continue
      }
      peerRates.push(rate)
      peerTtfbValues.push(Math.max(0, peer.bodyStartedAt - peer.startedAt))
    }

    const peerCutoff = Math.max(current.bodyStartedAt, now - this.windowMs)
    for (const peer of this.completedPeers) {
      if (peer.completedAt < peerCutoff) {
        continue
      }
      const duration = peer.completedAt - peer.bodyStartedAt
      if (duration <= 0) {
        continue
      }
      peerRates.push((peer.bytes * 1000) / duration)
      peerTtfbValues.push(Math.max(0, peer.bodyStartedAt - peer.startedAt))
    }

    if (peerRates.length < 2) {
      return undefined
    }
    return {
      currentRate,
      peerCount: peerRates.length,
      peerMedianRate: median(peerRates),
      peerMedianTtfbMs: median(peerTtfbValues),
    }
  }
}

function calculateActiveRate(transfer: ActiveTransfer, now: number, windowMs: number): number {
  if (transfer.bodyStartedAt === undefined || now <= transfer.bodyStartedAt) {
    return 0
  }
  const startedAt = Math.max(transfer.bodyStartedAt, now - windowMs)
  let startedBytes = 0
  for (const sample of transfer.progress) {
    if (sample.at > startedAt) {
      break
    }
    startedBytes = sample.bytes
  }
  const duration = now - startedAt
  return duration > 0 ? ((transfer.bytes - startedBytes) * 1000) / duration : 0
}

function pruneProgress(samples: TransferProgressSample[], cutoff: number): void {
  while (samples.length > 2 && (samples[1]?.at ?? Number.POSITIVE_INFINITY) <= cutoff) {
    samples.shift()
  }
}

function median(values: readonly number[]): number {
  const sorted = [...values].sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  const upper = sorted[middle] ?? 0
  return sorted.length % 2 === 0 ? ((sorted[middle - 1] ?? upper) + upper) / 2 : upper
}

function estimateBandwidth(samples: readonly TransferSample[]): number {
  // 并行 GET 的字节全部计入, 但重叠的活跃时间只计算一次
  const sorted = [...samples].sort(
    (left, right) => left.startedAt - right.startedAt || left.completedAt - right.completedAt,
  )
  let activeStartedAt: number | undefined
  let activeCompletedAt = 0
  let activeDuration = 0
  let bytes = 0

  for (const sample of sorted) {
    bytes += sample.bytes
    if (activeStartedAt === undefined) {
      activeStartedAt = sample.startedAt
      activeCompletedAt = sample.completedAt
      continue
    }
    if (sample.startedAt > activeCompletedAt) {
      activeDuration += activeCompletedAt - activeStartedAt
      activeStartedAt = sample.startedAt
      activeCompletedAt = sample.completedAt
      continue
    }
    activeCompletedAt = Math.max(activeCompletedAt, sample.completedAt)
  }

  if (activeStartedAt !== undefined) {
    activeDuration += activeCompletedAt - activeStartedAt
  }
  return activeDuration > 0 ? (bytes * 8 * 1000) / activeDuration : 0
}
