import Hls from 'hls.js'
import type { Fragment, Level, MediaFragment } from 'hls.js'
import { ParallelSegmentLoader } from './parallel-segment-loader.ts'
import { ParallelStreamController } from './parallel-stream-controller.ts'

class TestParallelStreamController extends ParallelStreamController {
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

  setBitrateTestForTest(value: boolean): void {
    this.bitrateTest = value
  }

  protected override resetTransmuxer(): void {
    this.resetTransmuxerCount += 1
  }
}

const originalFetch = globalThis.fetch
globalThis.fetch = async () => new Response(new Uint8Array([1]).buffer, { status: 200 })

const loader = new ParallelSegmentLoader({ maxConcurrency: 2, windowSize: 3 })
const hls = new Hls({
  autoStartLoad: false,
  fLoader: loader.fLoader,
  progressive: false,
  streamController: TestParallelStreamController,
})
const controller = (hls as unknown as { streamController: TestParallelStreamController })
  .streamController
const fragments = Array.from({ length: 8 }, (_, index) => createFragment(index))
const level = { details: { fragments, partList: null } } as unknown as Level

try {
  controller.updateWindowForTest(fragments[0] as MediaFragment, level)
  assertWindow(loader, [0, 1, 2])

  controller.updateWindowForTest(fragments[1] as MediaFragment, level)
  assertWindow(loader, [1, 2, 3])

  controller.setBitrateTestForTest(true)
  controller.updateWindowForTest(fragments[2] as MediaFragment, level)
  if (loader.getDiagnostics().streams.length !== 0) {
    throw new Error('bandwidth test 期间应清除预加载窗口')
  }

  controller.setBitrateTestForTest(false)
  controller.updateWindowForTest(fragments[2] as MediaFragment, level)

  controller.setCurrentFragmentForTest(fragments[2] as MediaFragment)
  controller.resetAfterSeekForTest()
  if (controller.getResetTransmuxerCount() !== 0) {
    throw new Error('seek 保留当前渐进请求时不应重置 transmuxer')
  }

  controller.setCurrentFragmentForTest(null)
  controller.resetAfterSeekForTest()
  if (controller.getResetTransmuxerCount() !== 1) {
    throw new Error('seek 取消当前渐进请求后应重置 transmuxer')
  }

  controller.stopLoad()
  if (loader.getDiagnostics().streams.length !== 0) {
    throw new Error('stopLoad 应清除窗口和没有 reader 的 Segment')
  }
} finally {
  hls.destroy()
  loader.destroy()
  globalThis.fetch = originalFetch
}

function createFragment(index: number): Fragment {
  return {
    cc: 0,
    duration: 2,
    gap: false,
    level: 0,
    sn: index,
    start: index * 2,
    type: 'main',
    url: `https://example.com/${index}.ts`,
  } as unknown as Fragment
}

function assertWindow(loader: ParallelSegmentLoader, expectedSn: number[]): void {
  const stream = loader.getDiagnostics().streams[0]
  if (stream === undefined) {
    throw new Error('Controller 没有建立 VirtualStream')
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
