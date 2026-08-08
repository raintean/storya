import Hls from 'hls.js'
import type { Level, MediaFragment } from 'hls.js'
import { ParallelSegmentLoader } from './parallel-segment-loader'

const NativeStreamController = Hls.DefaultConfig.streamController

export class ParallelStreamController extends NativeStreamController {
  private activeStreamId: string | undefined

  public override stopLoad(): void {
    const loader = this.requireSegmentLoader()
    if (this.activeStreamId !== undefined) {
      const activeStreamId = this.activeStreamId
      loader.update(state => {
        const stream = state.streams.get(activeStreamId)
        if (stream !== undefined) {
          stream.window = []
        }
        return undefined
      })
    }
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
    const loader = this.requireSegmentLoader()
    loader.configure(this.config)

    const details = level.details
    if (
      this.bitrateTest ||
      details === undefined ||
      (this.config.lowLatencyMode && (details.partList?.length ?? 0) > 0)
    ) {
      if (this.activeStreamId !== undefined) {
        const activeStreamId = this.activeStreamId
        loader.update(state => {
          const stream = state.streams.get(activeStreamId)
          if (stream !== undefined) {
            stream.window = []
          }
          return undefined
        })
      }
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
        : fragments.slice(currentIndex, currentIndex + loader.windowSize)
    const window = selected.filter(candidate => !candidate.gap && Boolean(candidate.url))
    const streamId = `${fragment.type}:${fragment.level}`

    loader.update(state => {
      if (this.activeStreamId !== undefined && this.activeStreamId !== streamId) {
        const previous = state.streams.get(this.activeStreamId)
        if (previous !== undefined) {
          previous.window = []
        }
      }
      state.ensureStream(streamId).replaceWindow(window, loader.chunkSize)
      return undefined
    })
    this.activeStreamId = streamId
  }

  private requireSegmentLoader(): ParallelSegmentLoader {
    const loader = ParallelSegmentLoader.fromFragmentLoader(this.config.fLoader)
    if (loader === undefined) {
      throw new Error(
        'ParallelStreamController 必须与同一个 ParallelSegmentLoader.fLoader 配合使用',
      )
    }
    return loader
  }
}
