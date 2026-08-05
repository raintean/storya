import { useRef } from 'react'
import type {
  HlsLoaderDiagnosticChunk,
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
const inactiveStreamRetentionMs = 10_000
const playbackInitialPosition = 0.15
const playbackPanThreshold = 0.35
const timelinePanStep = 0.2

interface StreamVisibility {
  active: boolean
  inactiveAt: number
}

interface TimelineViewport {
  bounds: TimelineBounds
  streamKey: string
}

export function VirtualStreamMap({ levelLabels, playbackTime, snapshot }: VirtualStreamMapProps) {
  const visibilityRef = useRef(new Map<string, StreamVisibility>())
  const timelineViewportRef = useRef<TimelineViewport | null>(null)
  const now = snapshot.timestamp || Date.now()
  const streamIds = new Set(snapshot.streams.map(stream => stream.id))

  for (const stream of snapshot.streams) {
    const visibility = visibilityRef.current.get(stream.id)
    if (stream.active) {
      visibilityRef.current.set(stream.id, { active: true, inactiveAt: 0 })
    } else if (visibility === undefined || visibility.active) {
      visibilityRef.current.set(stream.id, { active: false, inactiveAt: now })
    }
  }
  for (const streamId of visibilityRef.current.keys()) {
    if (!streamIds.has(streamId)) {
      visibilityRef.current.delete(streamId)
    }
  }

  const streams = snapshot.streams
    .filter(stream => {
      if (stream.segments.length === 0) {
        return false
      }
      if (stream.active) {
        return true
      }
      const visibility = visibilityRef.current.get(stream.id)
      return visibility !== undefined && now - visibility.inactiveAt < inactiveStreamRetentionMs
    })
    .sort(compareStreams)
  if (streams.length === 0) {
    timelineViewportRef.current = null
    return (
      <div className="stream-empty">
        <span>NO VIRTUAL STREAM DATA</span>
        <p>加载 HLS 后，这里会显示每条虚拟流的需求、缓存和 Chunk 调度状态。</p>
      </div>
    )
  }

  const activeStreams = streams.filter(stream => stream.active)
  const timelineStreams = activeStreams.length > 0 ? activeStreams : streams
  const streamKey = timelineStreams
    .map(stream => stream.id)
    .sort()
    .join('\0')
  const bounds = updateTimelineViewport(
    timelineViewportRef.current,
    timelineStreams,
    streamKey,
    playbackTime,
  )
  timelineViewportRef.current = { bounds, streamKey }
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
  if (left.active !== right.active) {
    return left.active ? -1 : 1
  }

  const kindOrder = { audio: 1, main: 0, subtitle: 2 }
  return (
    kindOrder[left.kind] - kindOrder[right.kind] ||
    left.level - right.level ||
    left.id.localeCompare(right.id)
  )
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
  return (
    <div className={`stream-row ${stream.active ? 'is-active' : ''}`}>
      <div className="stream-label">
        <strong>{label}</strong>
        <small>
          {stream.kind.toUpperCase()} · {stream.active ? 'ACTIVE' : 'IDLE'}
        </small>
        <span>
          cache {cached} · need {pending}
        </span>
      </div>
      <div className="segment-track">
        {stream.segments.map(segment => (
          <SegmentCell bounds={bounds} key={segment.key} segment={segment} />
        ))}
      </div>
    </div>
  )
}

function SegmentCell({
  bounds,
  segment,
}: {
  bounds: TimelineBounds
  segment: HlsLoaderDiagnosticSegment
}) {
  const end = segment.start + Math.max(segment.duration, 0.01)
  const left = toPercent(segment.start, bounds)
  const width = Math.max(0.8, toPercent(end, bounds) - left)
  const classes = [
    'segment-cell',
    `is-${segment.state}`,
    segment.prefetch ? 'is-prefetch' : '',
    segment.hardDemands > 0 || segment.playbackDemand ? 'is-demanded' : '',
    segment.anchor ? 'is-anchor' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div
      className={classes}
      style={{ left: `${left}%`, width: `${width}%` }}
      title={createSegmentTitle(segment)}
    >
      <span className="segment-name">S{String(segment.segmentSn)}</span>
      <div className="chunk-track">
        {segment.chunks.length > 0 ? (
          segment.chunks.map(chunk => <ChunkCell chunk={chunk} key={chunk.id} segment={segment} />)
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
): TimelineBounds {
  const segmentBounds = findSegmentBounds(streams)
  if (current === null || current.streamKey !== streamKey) {
    return createTimelineViewport(segmentBounds, playbackTime)
  }

  const span = current.bounds.end - current.bounds.start
  if (playbackTime < current.bounds.start || playbackTime > current.bounds.end) {
    return positionTimelineAtPlayback(span, playbackTime)
  }

  const playbackPosition = toPercent(playbackTime, current.bounds) / 100
  if (playbackPosition < playbackPanThreshold) {
    return current.bounds
  }

  const shift = span * timelinePanStep
  return {
    end: current.bounds.end + shift,
    start: current.bounds.start + shift,
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
  if (stream.kind === 'audio') {
    return `Audio · ${stream.id.split(':').slice(1, 3).join('/')}`
  }
  if (stream.kind === 'subtitle') {
    return 'Subtitle'
  }
  return levelLabels.get(stream.level) ?? `Level ${stream.level}`
}

function getSegmentStateLabel(segment: HlsLoaderDiagnosticSegment): string {
  if (segment.playbackDemand && segment.state === 'failed') {
    return 'NEED · FAILED'
  }
  if (segment.hardDemands > 0) {
    return 'HLS READER'
  }
  if (segment.playbackDemand) {
    return 'PLAYBACK NEED'
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
    `Segment ${String(segment.segmentSn)}`,
    `${segment.start.toFixed(2)}s – ${(segment.start + segment.duration).toFixed(2)}s`,
    getSegmentStateLabel(segment),
    `${formatBytes(segment.loadedBytes)} / ${formatBytes(segment.totalBytes)}`,
  ].join('\n')
}

function createChunkTitle(chunk: HlsLoaderDiagnosticChunk): string {
  return [
    `Chunk ${chunk.id} · ${chunk.state}`,
    `${formatBytes(chunk.startOffset)} – ${formatBytes(chunk.endOffset)}`,
    `received ${formatBytes(chunk.receivedBytes)}`,
    `slow ${chunk.slowRetries} · preempted ${chunk.preemptions} · retry ${chunk.networkRetries}`,
  ].join('\n')
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
