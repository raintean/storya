import { useRef } from 'react'
import type {
  ChunkDiagnostics,
  ParallelSegmentLoaderDiagnostics,
  SegmentDiagnostics,
  VirtualStreamDiagnostics,
} from 'storya-hls-loader'

interface VirtualStreamMapProps {
  levelLabels: Map<number, string>
  playbackTime: number
  snapshot: ParallelSegmentLoaderDiagnostics
}

const visibleSegmentCount = 7

interface TimelineViewport {
  bounds: TimelineBounds
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
          <i className="marker marker-playback is-at-start">
            <span>播放</span>
          </i>
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

function compareStreams(left: VirtualStreamDiagnostics, right: VirtualStreamDiagnostics): number {
  return left.id.localeCompare(right.id)
}

function StreamRow({
  bounds,
  label,
  stream,
}: {
  bounds: TimelineBounds
  label: string
  stream: VirtualStreamDiagnostics
}) {
  const cached = stream.segments.filter(segment => segment.state === 'ready').length
  const pending = stream.segments.filter(
    segment => segment.windowIndex !== null && segment.state !== 'ready',
  ).length
  const readerCount = stream.segments.reduce((total, segment) => total + segment.readerCount, 0)
  return (
    <div className={`stream-row ${readerCount > 0 ? 'has-readers' : ''}`}>
      <div className="stream-label">
        <strong>{label}</strong>
        <small>{getStreamTypeLabel(stream.id)}</small>
        <span>
          reader {readerCount} · cache {cached} · need {pending}
        </span>
        <span>window {stream.window.length} · revision driven</span>
      </div>
      <div className="segment-track">
        {stream.segments.map(segment => (
          <SegmentCell bounds={bounds} key={segment.key} segment={segment} />
        ))}
      </div>
    </div>
  )
}

function SegmentCell({ bounds, segment }: { bounds: TimelineBounds; segment: SegmentDiagnostics }) {
  const end = segment.start + Math.max(segment.duration, 0.01)
  const left = toPercent(segment.start, bounds)
  const width = Math.max(0.8, toPercent(end, bounds) - left)
  const state = getSegmentStatePresentation(segment)
  const classes = [
    'segment-cell',
    `is-${segment.state}`,
    segment.windowIndex !== null ? 'is-prefetch' : '',
    segment.readerCount > 0 ? 'has-readers' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <div
      className={classes}
      style={{ left: `${left}%`, width: `${width}%` }}
      title={createSegmentTitle(segment)}
    >
      <div className="segment-heading">
        <span className="segment-name">{formatSegmentKey(segment.key)}</span>
        <b>{getSegmentRoleLabel(segment)}</b>
      </div>
      <div className="segment-state">
        <strong>{state.primary}</strong>
        <span>{state.detail}</span>
      </div>
      <div className="chunk-track">
        {segment.chunks.length > 0 ? (
          segment.chunks.map(chunk => <ChunkCell chunk={chunk} key={chunk.key} segment={segment} />)
        ) : (
          <i className={`chunk-placeholder is-${segment.state}`} />
        )}
      </div>
    </div>
  )
}

function ChunkCell({ chunk, segment }: { chunk: ChunkDiagnostics; segment: SegmentDiagnostics }) {
  const total = Math.max(
    segment.totalBytes ?? 0,
    ...segment.chunks.map(item => item.endExclusive ?? item.start + item.loadedBytes),
    1,
  )
  const end = chunk.endExclusive ?? total
  const left = (chunk.start / total) * 100
  const width = Math.max(3, ((end - chunk.start) / total) * 100)
  return (
    <i
      className={`chunk is-${getChunkStyleState(chunk)}`}
      style={{ left: `${left}%`, width: `${width}%` }}
      title={createChunkTitle(chunk)}
    />
  )
}

interface TimelineBounds {
  end: number
  start: number
}

function updateTimelineViewport(
  current: TimelineViewport | null,
  streams: VirtualStreamDiagnostics[],
  streamKey: string,
  playbackTime: number,
): TimelineViewport {
  if (current === null || current.streamKey !== streamKey) {
    const durations = streams
      .flatMap(stream => stream.segments)
      .filter(segment => segment.windowIndex !== null && segment.duration > 0)
      .map(segment => segment.duration)
      .sort((left, right) => left - right)
    const segmentDuration = durations[Math.floor(durations.length / 2)] ?? 2
    return {
      bounds: positionTimelineAtPlayback(segmentDuration * visibleSegmentCount, playbackTime),
      streamKey,
    }
  }

  const span = current.bounds.end - current.bounds.start
  return {
    ...current,
    bounds: positionTimelineAtPlayback(span, playbackTime),
  }
}

function positionTimelineAtPlayback(span: number, playbackTime: number): TimelineBounds {
  const start = Number.isFinite(playbackTime) ? Math.max(0, playbackTime) : 0
  return { end: start + span, start }
}

function createTicks(start: number, end: number): number[] {
  const step = (end - start) / 4
  const first = Math.floor(start / step) * step
  const ticks: number[] = []
  for (let tick = first; tick <= end; tick += step) {
    ticks.push(tick)
  }
  return ticks
}

function toPercent(value: number, bounds: TimelineBounds): number {
  return ((value - bounds.start) / Math.max(bounds.end - bounds.start, 1)) * 100
}

function getStreamLabel(
  stream: VirtualStreamDiagnostics,
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

interface SegmentStatePresentation {
  detail: string
  primary: string
}

function getSegmentStatePresentation(segment: SegmentDiagnostics): SegmentStatePresentation {
  if (segment.state === 'planning') {
    const method = segment.planningMethod?.toUpperCase() ?? 'PLAN'
    return {
      detail: segment.planningState === 'probing' ? 'LOADING' : 'QUEUED',
      primary: method,
    }
  }
  if (segment.state === 'verifying') {
    return { detail: 'VERIFYING', primary: 'RANGE' }
  }
  if (segment.state === 'ready') {
    return {
      detail: segment.sequential ? 'SEQUENTIAL' : `${String(segment.chunks.length)} CHUNKS`,
      primary: 'CACHED',
    }
  }
  if (segment.state === 'failed') {
    return { detail: 'LOAD ERROR', primary: 'FAILED' }
  }
  const ready = segment.chunks.filter(chunk => chunk.state === 'ready').length
  const filling = segment.chunks.filter(chunk => chunk.state === 'filling').length
  return segment.state === 'filling'
    ? {
        detail: `${String(ready + filling)}/${String(segment.chunks.length)}`,
        primary: 'FILLING',
      }
    : { detail: `${String(segment.chunks.length)} CHUNKS`, primary: 'PLANNED' }
}

function getSegmentRoleLabel(segment: SegmentDiagnostics): string {
  if (segment.readerCount > 0) {
    return 'READER'
  }
  return segment.windowIndex === null ? 'INACTIVE' : 'PREFETCH'
}

function createSegmentTitle(segment: SegmentDiagnostics): string {
  const state = getSegmentStatePresentation(segment)
  return [
    `Segment ${segment.key}`,
    `${segment.start.toFixed(2)}s – ${(segment.start + segment.duration).toFixed(2)}s`,
    `${state.primary} · ${state.detail}`,
    `role ${getSegmentRoleLabel(segment)} · plan ${segment.planningState}/${segment.planningSource ?? segment.planningMethod ?? '—'}`,
    `readers ${segment.readerCount} · window ${segment.windowIndex ?? 'no'}`,
    `HTTP ${segment.httpStatus || '—'} · range ${segment.rangeMode}`,
    `${formatBytes(segment.loadedBytes)} / ${formatBytes(segment.totalBytes)}`,
  ].join('\n')
}

function createChunkTitle(chunk: ChunkDiagnostics): string {
  return [
    `Chunk ${chunk.key} · ${chunk.state}`,
    `${formatBytes(chunk.start)} – ${formatBytes(chunk.endExclusive)}`,
    `received ${formatBytes(chunk.loadedBytes)}`,
    `generation ${chunk.generation ?? '—'} · attempt ${chunk.attempt} · rescue ${chunk.rescueAttempts}`,
    chunk.failure ?? 'no failure',
  ].join('\n')
}

function formatSegmentKey(key: string): string {
  const sequence = key.split('\n')[2]
  return sequence === undefined ? key : `S${sequence}`
}

function getChunkStyleState(chunk: ChunkDiagnostics): string {
  if (chunk.state === 'ready') {
    return 'complete'
  }
  if (chunk.state === 'filling') {
    return chunk.rescueAttempts > 0 ? 'rescuing' : 'loading'
  }
  return chunk.state === 'empty' ? 'queued' : 'failed'
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
