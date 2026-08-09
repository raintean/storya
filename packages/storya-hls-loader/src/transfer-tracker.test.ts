import { TransferTracker } from './transfer-tracker.ts'

const estimator = new TransferTracker(2_000)
estimator.recordCompletedTransfer(100, 0, 100)
estimator.recordCompletedTransfer(100, 0, 100)
if (estimator.bandwidthEstimate !== 16_000) {
  throw new Error('并行 GET 应按重叠时间区间计算聚合带宽')
}
estimator.recordCompletedTransfer(100, 200, 300)
if (Math.abs(estimator.bandwidthEstimate - 12_000) > 0.001) {
  throw new Error('不连续 GET 应排除请求之间的空闲时间')
}

const peers = new TransferTracker(2_000)
for (const generation of [1, 2, 3]) {
  peers.start(generation, 0)
  peers.startBody(generation, 100)
}
peers.recordProgress(1, 100, 1_100)
peers.recordProgress(1, 100, 2_100)
for (const generation of [2, 3]) {
  peers.recordProgress(generation, 1_000, 2_100)
  peers.finish(generation, 2_100, true)
}

const comparison = peers.compareWithPeers(1, 2_100)
if (
  comparison === undefined ||
  comparison.currentRate !== 100 ||
  comparison.peerCount !== 2 ||
  comparison.peerMedianRate !== 500 ||
  comparison.peerMedianTtfbMs !== 100
) {
  throw new Error('同期已完成 GET 应参与活动请求的相对速率比较')
}

const activePeers = new TransferTracker(2_000)
for (const generation of [1, 2, 3]) {
  activePeers.start(generation, 0)
  activePeers.startBody(generation, 0)
}
activePeers.recordProgress(1, 100, 2_000)
activePeers.recordProgress(2, 1_000, 2_000)
activePeers.recordProgress(3, 1_000, 2_000)

const activeComparison = activePeers.compareWithPeers(1, 2_000)
if (
  activeComparison === undefined ||
  activeComparison.currentRate !== 50 ||
  activeComparison.peerCount !== 2 ||
  activeComparison.peerMedianRate !== 500
) {
  throw new Error('同期活动 GET 应当参与当前请求的相对速率比较')
}
