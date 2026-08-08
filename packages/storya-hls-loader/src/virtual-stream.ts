import type { Fragment, FragmentLoaderContext } from 'hls.js'
import { VirtualStreamSegment } from './virtual-stream-segment'

export class VirtualStream {
  readonly id: string
  readonly segments = new Map<string, VirtualStreamSegment>()
  window: string[] = []

  constructor(id: string) {
    this.id = id
  }

  ensureSegment(context: FragmentLoaderContext, chunkSize: number): VirtualStreamSegment {
    const key = VirtualStreamSegment.createKey(context)
    let segment = this.segments.get(key)
    if (segment === undefined) {
      segment = new VirtualStreamSegment(this.id, context, chunkSize)
      this.segments.set(key, segment)
    } else {
      segment.updateContext(context)
    }
    return segment
  }

  replaceWindow(fragments: readonly Fragment[], chunkSize: number): void {
    this.window = fragments.map(fragment => {
      const context: FragmentLoaderContext = {
        frag: fragment,
        headers: {},
        part: null,
        rangeEnd: fragment.byteRangeEndOffset ?? 0,
        rangeStart: fragment.byteRangeStartOffset ?? 0,
        responseType: 'arraybuffer',
        type: 'media-fragment' as FragmentLoaderContext['type'],
        url: fragment.url,
      }
      const segment = this.ensureSegment(context, chunkSize)
      return segment.key
    })
  }

  prune(): void {
    const window = new Set(this.window)
    for (const [segmentKey, segment] of this.segments) {
      if (!window.has(segmentKey) && segment.readerCount === 0) {
        this.segments.delete(segmentKey)
      }
    }
  }
}
