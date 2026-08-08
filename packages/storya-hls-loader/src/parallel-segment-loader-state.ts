import type { FragmentLoaderContext } from 'hls.js'
import { VirtualStream } from './virtual-stream'
import { VirtualStreamSegment } from './virtual-stream-segment'

export class ParallelSegmentLoaderState {
  destroyed = false
  nextGeneration = 1
  revision = 0
  readonly streams = new Map<string, VirtualStream>()

  ensureStream(streamId: string): VirtualStream {
    let stream = this.streams.get(streamId)
    if (stream === undefined) {
      stream = new VirtualStream(streamId)
      this.streams.set(streamId, stream)
    }
    return stream
  }

  locateSegment(context: FragmentLoaderContext): VirtualStreamSegment | undefined {
    const streamId = `${context.frag.type}:${context.frag.level}`
    return this.streams.get(streamId)?.segments.get(VirtualStreamSegment.createKey(context))
  }

  allocateGeneration(): number {
    const generation = this.nextGeneration
    this.nextGeneration += 1
    return generation
  }

  reconcile(): void {
    if (this.destroyed) {
      this.streams.clear()
      return
    }

    // Segment 仅在窗口内或存在正式 reader 时存活
    for (const [streamId, stream] of this.streams) {
      stream.prune()
      if (stream.window.length === 0 && stream.segments.size === 0) {
        this.streams.delete(streamId)
      }
    }
  }
}
