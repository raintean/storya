import Hls from 'hls.js'
import type { Level, MediaFragment } from 'hls.js'
import {
  createWindowDescriptor,
  DEFAULT_WINDOW_SIZE,
  ParallelSegmentLoader,
} from './parallel-segment-loader'

const NativeStreamController = Hls.DefaultConfig.streamController

export class ParallelStreamController extends NativeStreamController {
  private activeStreamId: string | undefined

  public override stopLoad(): void {
    this.getSegmentLoader()?.clearWindow(this.activeStreamId)
    this.activeStreamId = undefined
    super.stopLoad()
  }

  protected override loadFragment(
    fragment: MediaFragment,
    level: Level,
    targetBufferTime: number,
  ): void {
    this.updateSegmentWindow(fragment, level)
    super.loadFragment(fragment, level, targetBufferTime)
  }

  protected updateSegmentWindow(fragment: MediaFragment, level: Level): void {
    const loader = this.getSegmentLoader()
    if (loader === undefined) {
      throw new Error(
        'ParallelStreamController 必须与同一个 ParallelSegmentLoader.fLoader 配合使用',
      )
    }

    const details = level.details
    if (
      this.bitrateTest ||
      details === undefined ||
      (this.config.lowLatencyMode && (details.partList?.length ?? 0) > 0)
    ) {
      loader.clearWindow(this.activeStreamId)
      this.activeStreamId = undefined
      return
    }

    const fragments = details.fragments
    const currentIndex = fragments.findIndex(
      candidate =>
        candidate === fragment ||
        (candidate.sn === fragment.sn &&
          candidate.level === fragment.level &&
          candidate.cc === fragment.cc),
    )
    const selected =
      currentIndex === -1
        ? [fragment]
        : fragments.slice(currentIndex, currentIndex + DEFAULT_WINDOW_SIZE)
    const descriptors = selected
      .filter(candidate => !candidate.gap && Boolean(candidate.url))
      .map(candidate => createWindowDescriptor(candidate))
    const streamId = `${fragment.type}:${fragment.level}`

    loader.replaceWindow(streamId, descriptors, this.config, this.activeStreamId)
    this.activeStreamId = streamId
  }

  private getSegmentLoader(): ParallelSegmentLoader | undefined {
    return ParallelSegmentLoader.fromFragmentLoader(this.config.fLoader)
  }
}
