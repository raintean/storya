import type { Fragment, FragmentLoaderContext } from 'hls.js'
import { type SegmentPlanningMethod, VirtualStreamSegment } from './virtual-stream-segment'

export class VirtualStream {
  readonly id: string
  readonly segments = new Map<string, VirtualStreamSegment>()
  window: string[] = []

  constructor(id: string) {
    this.id = id
  }

  ensureSegment(
    context: FragmentLoaderContext,
    chunkSize: number,
    planningMethod: SegmentPlanningMethod = 'range',
  ): VirtualStreamSegment {
    const key = VirtualStreamSegment.createKey(context)
    let segment = this.segments.get(key)
    if (segment === undefined) {
      segment = new VirtualStreamSegment(this.id, context, chunkSize, planningMethod)
      this.segments.set(key, segment)
    } else {
      segment.updateContext(context)
      if (planningMethod === 'range') {
        segment.preferRangePlanning()
      }
    }
    return segment
  }

  replaceWindow(fragments: readonly Fragment[], chunkSize: number): void {
    this.window = fragments.map((fragment, index) => {
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
      const segment = this.ensureSegment(context, chunkSize, index === 0 ? 'range' : 'head')
      return segment.key
    })
  }

  canScheduleData(segmentKey: string): boolean {
    const index = this.window.indexOf(segmentKey)
    if (index <= 0) {
      return true
    }
    for (const precedingKey of this.window.slice(0, index)) {
      const preceding = this.segments.get(precedingKey)
      if (
        preceding !== undefined &&
        preceding.outcome.type === 'pending' &&
        preceding.rangeMode === 'unverified'
      ) {
        return false
      }
    }
    return true
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
