import type { Fragment } from 'hls.js'
import { createFragmentContext, VirtualStream } from './virtual-stream.ts'

const video = new VirtualStream('main:0:test', 'main')
const audio = new VirtualStream('audio:0:test', 'audio')
const videoSegments = Array.from({ length: 9 }, (_, index) =>
  video.addStandalone(createFragmentContext(createFragment('main', index))),
)
const audioSegments = Array.from({ length: 9 }, (_, index) =>
  audio.addStandalone(createFragmentContext(createFragment('audio', index))),
)

video.active = true
video.anchor = videoSegments[0]
assertSequence(video.getPrefetchSegments(6), [1, 2, 3, 4, 5, 6], '初始视频窗口')

videoSegments[1]?.markFilling()
assertSequence(video.getPrefetchSegments(6), [1, 2, 3, 4, 5, 6], 'loading 仍占据窗口')

video.anchor = videoSegments[1]
assertSequence(video.getPrefetchSegments(6), [2, 3, 4, 5, 6, 7], '需求前沿推进')

audio.active = true
audio.anchor = audioSegments[3]
assertSequence(audio.getPrefetchSegments(6), [4, 5, 6, 7, 8], '音频独立窗口')
assertSequence(video.getPrefetchSegments(6), [2, 3, 4, 5, 6, 7], '音频不影响视频')

video.active = false
assertSequence(video.getPrefetchSegments(6), [], '失活流不再预填充')

function createFragment(type: 'audio' | 'main', sn: number): Fragment {
  return {
    baseurl: `https://example.com/${type}/index.m3u8`,
    byteRangeEndOffset: undefined,
    byteRangeStartOffset: undefined,
    duration: 2,
    gap: false,
    initSegment: null,
    level: 0,
    sn,
    start: sn * 2,
    type,
    url: `https://example.com/${type}/${sn}.ts`,
  } as unknown as Fragment
}

function assertSequence(
  segments: ReturnType<VirtualStream['getPrefetchSegments']>,
  expected: number[],
  scene: string,
): void {
  const actual = segments.map(segment => segment.fragment.sn)
  if (
    actual.length !== expected.length ||
    actual.some((value, index) => value !== expected[index])
  ) {
    throw new Error(`${scene}失败, 期望 ${expected.join(',')}, 实际 ${actual.join(',')}`)
  }
}
