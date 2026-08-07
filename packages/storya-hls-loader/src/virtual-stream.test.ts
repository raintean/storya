import { VirtualStreamRegistry } from './virtual-stream.ts'
import type { VirtualStreamFillPolicy, VirtualStreamSegmentDescriptor } from './virtual-stream.ts'

const registry = new VirtualStreamRegistry({ chunkSize: 4, prefetchAheadSegments: 2 })
const videoSegments = Array.from({ length: 5 }, (_, index) => createSegment('video', index))
const audioSegments = Array.from({ length: 5 }, (_, index) => createSegment('audio', index))
registry.updateStream('track:video', videoSegments)
registry.updateStream('track:audio', audioSegments)

const videoReader = registry.createSegmentReader({
  fillPolicy: createFillPolicy(),
  segment: videoSegments[0] as VirtualStreamSegmentDescriptor,
  streamId: 'track:video',
})
const audioReader = registry.createSegmentReader({
  fillPolicy: createFillPolicy(),
  segment: audioSegments[1] as VirtualStreamSegmentDescriptor,
  streamId: 'track:audio',
})

let snapshot = registry.snapshot()
assertWindow(snapshot, 'track:video', [0, 2, 4])
assertWindow(snapshot, 'track:audio', [2, 4, 6])

const videoChunk = snapshot.streams
  .find(stream => stream.id === 'track:video')
  ?.segments.flatMap(segment => segment.chunks)[0]
if (videoChunk === undefined) {
  throw new Error('视频虚拟流没有物化 Chunk')
}
const firstWriter = registry.tryAcquireChunkWriter({ chunkKey: videoChunk.key, fillerId: 1 })
const duplicateWriter = registry.tryAcquireChunkWriter({ chunkKey: videoChunk.key, fillerId: 2 })
if (firstWriter === undefined || duplicateWriter !== undefined) {
  throw new Error('同一个 Chunk 必须只能原子取得一个 Writer')
}

const response = new Response(new Uint8Array([1, 2, 3, 4]), {
  headers: { 'content-length': '4', 'content-range': 'bytes 0-3/4' },
  status: 206,
})
firstWriter.acceptResponse({
  contentLength: 4,
  networkDetails: response,
  responseEnd: 4,
  responseStart: 0,
  resourceLength: 4,
  status: 206,
})
firstWriter.append(new Uint8Array([1, 2]))
firstWriter.release('preempted')

const resumedWriter = registry.tryAcquireChunkWriter({ chunkKey: videoChunk.key, fillerId: 2 })
if (resumedWriter === undefined || resumedWriter.receivedLength !== 2) {
  throw new Error('Writer 释放后 Chunk 必须保留已经接受的数据')
}
resumedWriter.append(new Uint8Array([3, 4]))
resumedWriter.complete()
const videoResult = await videoReader.result
if (new Uint8Array(videoResult.data).join(',') !== '1,2,3,4') {
  throw new Error('SegmentReader 没有从 Chunk 取得完整数据')
}

audioReader.cancel()
snapshot = registry.snapshot()
const audioStream = snapshot.streams.find(stream => stream.id === 'track:audio')
if (
  audioStream?.frontier !== undefined ||
  audioStream?.segments.some(segment => segment.chunks.length > 0)
) {
  throw new Error('未确认 Reader 取消后应撤销对应 provisional 窗口')
}

registry.destroy()

function assertWindow(
  current: ReturnType<VirtualStreamRegistry['snapshot']>,
  streamId: string,
  expectedStarts: number[],
): void {
  const stream = current.streams.find(candidate => candidate.id === streamId)
  const actual =
    stream?.segments.filter(segment => segment.chunks.length > 0).map(segment => segment.start) ??
    []
  if (
    actual.length !== expectedStarts.length ||
    actual.some((value, index) => value !== expectedStarts[index])
  ) {
    throw new Error(
      `${streamId} 窗口错误, 期望 ${expectedStarts.join(',')}, 实际 ${actual.join(',')}`,
    )
  }
}

function createSegment(track: string, index: number): VirtualStreamSegmentDescriptor {
  return {
    key: `${track}:${index}`,
    position: { duration: 2, start: index * 2 },
    prefetch: true,
    resource: {
      createRequest: parameters =>
        Promise.resolve(
          new Request(`https://example.com/${track}/${index}.ts`, {
            headers: parameters.headers,
            method: parameters.method,
            signal: parameters.signal,
          }),
        ),
      headers: {},
      rangeEnd: undefined,
      rangeStart: 0,
      url: `https://example.com/${track}/${index}.ts`,
    },
  }
}

function createFillPolicy(): VirtualStreamFillPolicy {
  return {
    errorRetry: undefined,
    maxTimeToFirstByteMs: 5_000,
    timeoutRetry: undefined,
  }
}
