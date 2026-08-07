import { useRef } from 'react'
import type {
  HlsLoaderDiagnosticChunk,
  HlsLoaderDiagnosticFrontier,
  HlsLoaderDiagnosticSegment,
  HlsLoaderDiagnosticsSnapshot,
  HlsLoaderDiagnosticStream,
} from 'storya-hls-loader'

interface VirtualStreamMapProps {
  levelLabels: Map<number, string>
  playbackTime: number
  snapshot: HlsLoaderDiagnosticsSnapshot
}

const minimumTimelineSpan = 12
const playbackInitialPosition = 0.15
const playbackPanThreshold = 0.35
const timelinePanStep = 0.2

interface TimelineViewport {
  bounds: TimelineBounds
  segmentBounds: TimelineBounds
  streamKey: string
}

export function VirtualStreamMap({ levelLabels, playbackTime, snapshot }: VirtualStreamMapProps) {
  const timelineViewportRef = useRef<TimelineViewport | null>(null)
  const streams = snapshot.streams.filter(stream => stream.segments.length > 0).sort(compareStreams)
  if (streams.length === 0) {
    timelineViewportRef.current = null
    return (
      <div className="stream-empty">
        <span>NO VIRTUAL STREAM DATA</span>
        <p>加载 HLS 后，这里会显示每条虚拟流的需求、缓存和 Chunk 调度状态。</p>
      </div>
    )
  }

  const streamKey = streams
    .map(stream => stream.id)
    .sort()
    .join('\0')
  const viewport = updateTimelineViewport(
    timelineViewportRef.current,
    streams,
    streamKey,
    playbackTime,
  )
  timelineViewportRef.current = viewport
  const bounds = viewport.bounds
  const markers = [{ className: 'playback', label: '播放', value: playbackTime }]

  return (
    <div className="stream-map">
      <div className="stream-ruler">
        <span>VIRTUAL STREAM</span>
        <div>
          {createTicks(bounds.start, bounds.end).map(tick => (
            <b key={tick} style={{ left: `${toPercent(tick, bounds)}%` }}>
              {formatTime(tick)}
            </b>
          ))}
        </div>
      </div>

      <div className="stream-rows">
        <div className="stream-markers" aria-hidden="true">
          {markers.map(marker => {
            const edge = findMarkerEdge(marker.value, bounds)
            const position = toClampedPercent(marker.value, bounds)
            const boundary = position <= 0 ? 'is-at-start' : position >= 100 ? 'is-at-end' : ''
            return (
              <i
                className={`marker marker-${marker.className} ${boundary} ${edge === undefined ? '' : `is-${edge}`}`}
                key={marker.className}
                style={{ left: `${position}%` }}
              >
                <span>
                  {edge === 'before' ? '← ' : ''}
                  {marker.label}
                  {edge === 'after' ? ' →' : ''}
                </span>
              </i>
            )
          })}
        </div>

        {streams.map(stream => (
          <StreamRow
            bounds={bounds}
            key={stream.id}
            label={getStreamLabel(stream, levelLabels)}
            stream={stream}
          />
        ))}
      </div>
    </div>
  )
}

function compareStreams(left: HlsLoaderDiagnosticStream, right: HlsLoaderDiagnosticStream): number {
  return left.id.localeCompare(right.id)
}

function StreamRow({
  bounds,
  label,
  stream,
}: {
  bounds: TimelineBounds
  label: string
  stream: HlsLoaderDiagnosticStream
}) {
  const cached = stream.segments.filter(segment => segment.state === 'ready').length
  const pending = stream.segments.filter(
    segment => segment.prefetch && segment.state !== 'ready',
  ).length
  const readerCount = stream.segments.reduce((total, segment) => total + segment.readerCount, 0)
  const frontier = stream.frontier
  return (
    <div className={`stream-row ${readerCount > 0 ? 'has-readers' : ''}`}>
      <div className="stream-label">
        <strong>{label}</strong>
        <small>{getStreamTypeLabel(stream.id)}</small>
        <span>
          reader {readerCount} · cache {cached} · need {pending}
        </span>
        {frontier === undefined ? null : (
          <span>
            frontier g{frontier.generation} · {getFrontierLabel(frontier)}
          </span>
        )}
      </div>
      <div className="segment-track">
        {stream.segments.map(segment => (
          <SegmentCell
            bounds={bounds}
            frontier={stream.frontier?.segmentKey === segment.key}
            key={segment.key}
            segment={segment}
          />
        ))}
      </div>
    </div>
  )
}

function SegmentCell({
  bounds,
  frontier,
  segment,
}: {
  bounds: TimelineBounds
  frontier: boolean
  segment: HlsLoaderDiagnosticSegment
}) {
  const end = segment.start + Math.max(segment.duration, 0.01)
  const left = toPercent(segment.start, bounds)
  const width = Math.max(0.8, toPercent(end, bounds) - left)
  const classes = [
    'segment-cell',
    `is-${segment.state}`,
    segment.prefetch ? 'is-prefetch' : '',
    segment.readerCount > 0 ? 'has-readers' : '',
    frontier ? 'is-frontier' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div
      className={classes}
      style={{ left: `${left}%`, width: `${width}%` }}
      title={createSegmentTitle(segment)}
    >
      <span className="segment-name">{formatSegmentKey(segment.key)}</span>
      <div className="chunk-track">
        {segment.chunks.length > 0 ? (
          segment.chunks.map(chunk => <ChunkCell chunk={chunk} key={chunk.key} segment={segment} />)
        ) : (
          <i className="chunk-placeholder" />
        )}
      </div>
      <span className="segment-state">{getSegmentStateLabel(segment)}</span>
    </div>
  )
}

function ChunkCell({
  chunk,
  segment,
}: {
  chunk: HlsLoaderDiagnosticChunk
  segment: HlsLoaderDiagnosticSegment
}) {
  const total = Math.max(
    segment.totalBytes,
    ...segment.chunks.map(item => item.endOffset ?? item.startOffset + item.receivedBytes),
    1,
  )
  const end = chunk.endOffset ?? total
  const left = (chunk.startOffset / total) * 100
  const width = Math.max(3, ((end - chunk.startOffset) / total) * 100)
  return (
    <i
      className={`chunk is-${chunk.state} ${chunk.preemptions > 0 ? 'was-preempted' : ''}`}
      style={{ left: `${left}%`, width: `${width}%` }}
      title={createChunkTitle(chunk)}
    />
  )
}

interface TimelineBounds {
  end: number
  start: number
}

function findSegmentBounds(streams: HlsLoaderDiagnosticStream[]): TimelineBounds {
  const segments = streams.flatMap(stream => stream.segments)
  const first = Math.min(...segments.map(segment => segment.start))
  const last = Math.max(...segments.map(segment => segment.start + segment.duration))
  return { end: last, start: first }
}

function updateTimelineViewport(
  current: TimelineViewport | null,
  streams: HlsLoaderDiagnosticStream[],
  streamKey: string,
  playbackTime: number,
): TimelineViewport {
  const segmentBounds = findSegmentBounds(streams)
  if (
    current === null ||
    current.streamKey !== streamKey ||
    current.segmentBounds.start !== segmentBounds.start ||
    current.segmentBounds.end !== segmentBounds.end
  ) {
    return {
      bounds: createTimelineViewport(segmentBounds, playbackTime),
      segmentBounds,
      streamKey,
    }
  }

  const span = current.bounds.end - current.bounds.start
  if (playbackTime < current.bounds.start || playbackTime > current.bounds.end) {
    return {
      ...current,
      bounds: positionTimelineAtPlayback(span, playbackTime),
    }
  }

  const playbackPosition = toPercent(playbackTime, current.bounds) / 100
  if (playbackPosition < playbackPanThreshold) {
    return current
  }

  const shift = span * timelinePanStep
  return {
    ...current,
    bounds: {
      end: current.bounds.end + shift,
      start: current.bounds.start + shift,
    },
  }
}

function createTimelineViewport(
  segmentBounds: TimelineBounds,
  playbackTime: number,
): TimelineBounds {
  const segmentSpan = segmentBounds.end - segmentBounds.start
  const contentAhead = Math.max(0, segmentBounds.end - playbackTime)
  const span = Math.max(
    minimumTimelineSpan,
    segmentSpan,
    contentAhead / (1 - playbackInitialPosition),
  )
  return positionTimelineAtPlayback(span, playbackTime)
}

function positionTimelineAtPlayback(span: number, playbackTime: number): TimelineBounds {
  const start = Math.max(0, playbackTime - span * playbackInitialPosition)
  return { end: start + span, start }
}

function createTicks(start: number, end: number): number[] {
  const step = (end - start) / 4
  return Array.from({ length: 5 }, (_, index) => start + step * index)
}

function toPercent(value: number, bounds: TimelineBounds): number {
  return ((value - bounds.start) / Math.max(bounds.end - bounds.start, 1)) * 100
}

function toClampedPercent(value: number, bounds: TimelineBounds): number {
  return Math.min(100, Math.max(0, toPercent(value, bounds)))
}

function findMarkerEdge(value: number, bounds: TimelineBounds): 'after' | 'before' | undefined {
  if (value < bounds.start) {
    return 'before'
  }
  if (value > bounds.end) {
    return 'after'
  }
  return undefined
}

function getStreamLabel(
  stream: HlsLoaderDiagnosticStream,
  levelLabels: Map<number, string>,
): string {
  const [kind, identity] = stream.id.split(':')
  if (kind === 'audio') {
    return `Audio · ${stream.id.split(':').slice(1, 3).join('/')}`
  }
  if (kind === 'subtitle') {
    return `Subtitle · ${stream.id.split(':').slice(1, 3).join('/')}`
  }
  if (kind === 'main') {
    const level = Number(identity)
    return levelLabels.get(level) ?? `Level ${identity ?? '?'}`
  }
  return stream.id
}

function getStreamTypeLabel(streamId: string): string {
  const kind = streamId.split(':', 1)[0]
  if (kind === 'main') {
    return 'MAIN TRACK'
  }
  if (kind === 'audio') {
    return 'AUDIO TRACK'
  }
  if (kind === 'subtitle') {
    return 'SUBTITLE TRACK'
  }
  return 'VIRTUAL STREAM'
}

function getFrontierLabel(frontier: HlsLoaderDiagnosticFrontier): string {
  if (frontier.barrier) {
    return 'BARRIER'
  }
  return frontier.confirmed ? 'CONFIRMED' : 'PROVISIONAL'
}

function getSegmentStateLabel(segment: HlsLoaderDiagnosticSegment): string {
  if (segment.readerCount > 0) {
    return segment.state === 'failed' ? 'READER · FAILED' : 'HLS READER'
  }
  if (segment.state === 'ready') {
    return 'CACHED'
  }
  if (segment.state === 'filling') {
    return segment.prefetch ? 'PREFETCH' : 'LOADING'
  }
  if (segment.state === 'failed') {
    return 'FAILED'
  }
  return segment.prefetch ? 'NEEDED' : 'EMPTY'
}

function createSegmentTitle(segment: HlsLoaderDiagnosticSegment): string {
  return [
    `Segment ${segment.key}`,
    `${segment.start.toFixed(2)}s – ${(segment.start + segment.duration).toFixed(2)}s`,
    getSegmentStateLabel(segment),
    `readers ${segment.readerCount} · prefetch ${segment.prefetch ? 'yes' : 'no'}`,
    `${formatBytes(segment.loadedBytes)} / ${formatBytes(segment.totalBytes)}`,
  ].join('\n')
}

function createChunkTitle(chunk: HlsLoaderDiagnosticChunk): string {
  return [
    `Chunk ${chunk.key} · ${chunk.state}`,
    `${formatBytes(chunk.startOffset)} – ${formatBytes(chunk.endOffset)}`,
    `received ${formatBytes(chunk.receivedBytes)}`,
    `filler ${chunk.fillerId ?? '—'} · writer ${chunk.writerId ?? '—'}`,
    `slow ${chunk.slowRetries} · preempted ${chunk.preemptions} · retry ${chunk.networkRetries}`,
  ].join('\n')
}

function formatSegmentKey(key: string): string {
  const sequence = /(?:^|\|)sn:([^|]+)/u.exec(key)?.[1]
  return sequence === undefined ? key : `S${sequence}`
}

function formatTime(value: number): string {
  const minutes = Math.floor(Math.max(0, value) / 60)
  const seconds = Math.floor(Math.max(0, value) % 60)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function formatBytes(value: number | undefined): string {
  if (value === undefined || value <= 0) {
    return '—'
  }
  if (value < 1024) {
    return `${value} B`
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KiB`
  }
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`
}
