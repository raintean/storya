import Hls from 'hls.js'
import type { Fragment, Level, MediaFragment } from 'hls.js'
import { ParallelAudioStreamController } from './parallel-audio-stream-controller.ts'
import { ParallelSegmentLoader } from './parallel-segment-loader.ts'

class TestParallelAudioStreamController extends ParallelAudioStreamController {
  private resetTransmuxerCount = 0

  getResetTransmuxerCount(): number {
    return this.resetTransmuxerCount
  }

  resetAfterSeekForTest(): void {
    this.resetTransmuxerAfterSeekAbort()
  }

  setCurrentFragmentForTest(fragment: MediaFragment | null): void {
    this.fragCurrent = fragment
  }

  updateWindowForTest(fragment: MediaFragment, level: Level): void {
    this.updateSegmentWindow(fragment, level)
  }

  protected override resetTransmuxer(): void {
    this.resetTransmuxerCount += 1
  }
}

const originalFetch = globalThis.fetch
globalThis.fetch = async () => new Response(new Uint8Array([1]).buffer, { status: 200 })

const loader = new ParallelSegmentLoader({ maxConcurrency: 2, windowSize: 3 })
const hls = new Hls({
  audioStreamController: TestParallelAudioStreamController,
  autoStartLoad: false,
  fLoader: loader.fLoader,
  progressive: false,
})
const controller = (hls as unknown as { audioStreamController: TestParallelAudioStreamController })
  .audioStreamController
const firstTrackFragments = Array.from({ length: 6 }, (_, index) => createFragment(index, 0))
const firstTrack = {
  details: { fragments: firstTrackFragments, partList: null },
} as unknown as Level

try {
  controller.updateWindowForTest(firstTrackFragments[0] as MediaFragment, firstTrack)
  assertWindow(loader, 'audio:0', [0, 1, 2])

  loader.update(state => {
    const stream = state.streams.get('audio:0')
    const segmentKey = stream?.window[0]
    const segment = segmentKey === undefined ? undefined : stream?.segments.get(segmentKey)
    if (segment === undefined) {
      throw new Error('音频窗口缺少首个 Segment')
    }
    segment.startReading()
    segment.stopReading()
    return undefined
  })
  assertWindow(loader, 'audio:0', [0, 1, 2])

  controller.updateWindowForTest(firstTrackFragments[1] as MediaFragment, firstTrack)
  assertWindow(loader, 'audio:0', [1, 2, 3])

  const secondTrackFragments = Array.from({ length: 6 }, (_, index) => createFragment(index, 1))
  const secondTrack = {
    details: { fragments: secondTrackFragments, partList: null },
  } as unknown as Level
  controller.updateWindowForTest(secondTrackFragments[0] as MediaFragment, secondTrack)
  assertWindow(loader, 'audio:1', [0, 1, 2])
  if (loader.getDiagnostics().streams.some(stream => stream.id === 'audio:0')) {
    throw new Error('切换音轨时应清除旧音频窗口')
  }

  controller.setCurrentFragmentForTest(secondTrackFragments[0] as MediaFragment)
  controller.resetAfterSeekForTest()
  if (controller.getResetTransmuxerCount() !== 0) {
    throw new Error('seek 保留当前音频渐进请求时不应重置 transmuxer')
  }

  controller.setCurrentFragmentForTest(null)
  controller.resetAfterSeekForTest()
  if (controller.getResetTransmuxerCount() !== 1) {
    throw new Error('seek 取消当前音频渐进请求后应重置 transmuxer')
  }

  controller.stopLoad()
  if (loader.getDiagnostics().streams.length !== 0) {
    throw new Error('AudioStreamController.stopLoad 应清除音频窗口')
  }
} finally {
  hls.destroy()
  loader.destroy()
  globalThis.fetch = originalFetch
}

function createFragment(index: number, level: number): Fragment {
  return {
    cc: 0,
    duration: 2,
    gap: false,
    level,
    sn: index,
    start: index * 2,
    type: 'audio',
    url: `https://example.com/audio-${String(level)}-${String(index)}.m4s`,
  } as unknown as Fragment
}

function assertWindow(loader: ParallelSegmentLoader, streamId: string, expectedSn: number[]): void {
  const stream = loader.getDiagnostics().streams.find(candidate => candidate.id === streamId)
  if (stream === undefined) {
    throw new Error(`Controller 没有建立 ${streamId} VirtualStream`)
  }
  const actualSn = stream.segments
    .sort((left, right) => (left.windowIndex ?? 0) - (right.windowIndex ?? 0))
    .map(segment => Number.parseInt(segment.key.split('\n')[2] ?? '', 10))
  if (JSON.stringify(actualSn) !== JSON.stringify(expectedSn)) {
    throw new Error(
      `窗口错误, 期望 ${JSON.stringify(expectedSn)}, 实际 ${JSON.stringify(actualSn)}`,
    )
  }
  if (stream.window.length !== expectedSn.length) {
    throw new Error('VirtualStream.window 与 Segment 数量不一致')
  }
}
