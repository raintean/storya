import Hls from 'hls.js'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import {
  createHlsParallelLoader,
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CONCURRENCY,
  DEFAULT_PREFETCH_DEPTH,
} from 'storya-hls-loader'
import type { HlsLoaderDiagnosticsSnapshot, HlsLoaderSegmentEvent } from 'storya-hls-loader'
import { WebSocketHttpTransport } from 'storya-transport'

import { VirtualStreamMap } from './virtual-stream-map'

const defaultSource = 'https://v.gsuus.com/play/bkRrAora/index.m3u8'

interface PlaybackMetrics {
  bandwidth: number
  bufferAhead: number
  bufferedEnd: number
  currentTime: number
  duration: number
}

interface HlsFrontier {
  loadedEnd: number
  loadingTime: number
}

interface LogEntry {
  details: LogEntryDetail[]
  id: number
  message: string
  tag: string
  time: string
  tone: LogTone
}

interface LogEntryDetail {
  label: string
  value: string
}

interface LogEntryOptions {
  details?: LogEntryDetail[]
  tag?: string
}

interface LoaderEventCounts {
  cacheHits: number
  preempted: number
  prefetched: number
  rescued: number
}

interface QualityLevel {
  bitrate: number
  height: number
  index: number
  label: string
}

interface PlaybackLevel {
  bitrate: number
  height: number
  index: number
  width: number
}

type LoaderMode = 'native' | 'parallel'
type LogTone = 'default' | 'error' | 'preempted' | 'rescued' | 'success'
type TransportMode = 'fetch' | 'websocket'

const initialMetrics: PlaybackMetrics = {
  bandwidth: 0,
  bufferAhead: 0,
  bufferedEnd: 0,
  currentTime: 0,
  duration: 0,
}

const initialFrontier: HlsFrontier = {
  loadedEnd: 0,
  loadingTime: 0,
}

const initialLoaderEventCounts: LoaderEventCounts = {
  cacheHits: 0,
  preempted: 0,
  prefetched: 0,
  rescued: 0,
}

const emptyDiagnostics: HlsLoaderDiagnosticsSnapshot = {
  activeRequests: 0,
  enabled: true,
  estimatedThroughputBytesPerSecond: 0,
  maxConcurrency: DEFAULT_MAX_CONCURRENCY,
  streams: [],
  timestamp: 0,
}

export function App() {
  const videoRef = useRef<HTMLVideoElement>(null)
  const hlsRef = useRef<Hls | null>(null)
  const parallelLoaderRef = useRef<ReturnType<typeof createHlsParallelLoader> | null>(null)
  const logIdRef = useRef(0)
  const playbackLevelIndexRef = useRef(-1)
  const [loaderMode, setLoaderMode] = useState<LoaderMode>('parallel')
  const [transportMode, setTransportMode] = useState<TransportMode>('fetch')
  const [workerUrl, setWorkerUrl] = useState('')
  const [activeTransportMode, setActiveTransportMode] = useState<TransportMode | null>(null)
  const [source, setSource] = useState(defaultSource)
  const [qualityLevels, setQualityLevels] = useState<QualityLevel[]>([])
  const [selectedLevel, setSelectedLevel] = useState(-2)
  const [playbackLevel, setPlaybackLevel] = useState<PlaybackLevel | null>(null)
  const [status, setStatus] = useState('等待加载')
  const [metrics, setMetrics] = useState(initialMetrics)
  const [frontier, setFrontier] = useState(initialFrontier)
  const [diagnostics, setDiagnostics] = useState(emptyDiagnostics)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [loaderEventCounts, setLoaderEventCounts] = useState(initialLoaderEventCounts)
  const [error, setError] = useState<string | null>(null)

  const appendLog = useCallback(
    (message: string, tone: LogTone = 'default', options: LogEntryOptions = {}) => {
      logIdRef.current += 1
      const entry: LogEntry = {
        details: options.details ?? [],
        id: logIdRef.current,
        message,
        tag: options.tag ?? '系统',
        time: new Date().toLocaleTimeString('zh-CN', { hour12: false }),
        tone,
      }
      setLogs(current => [entry, ...current].slice(0, 50))
    },
    [],
  )

  useEffect(() => {
    const timer = window.setInterval(() => {
      const video = videoRef.current
      if (video !== null) {
        const bufferedEnd = findBufferedEnd(video)
        setMetrics({
          bandwidth: hlsRef.current?.bandwidthEstimate ?? 0,
          bufferAhead: Math.max(0, bufferedEnd - video.currentTime),
          bufferedEnd,
          currentTime: video.currentTime,
          duration: video.duration,
        })
      }

      const parallelLoader = parallelLoaderRef.current
      if (parallelLoader !== null) {
        setDiagnostics(parallelLoader.getDiagnostics())
      }
    }, 180)

    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    return () => hlsRef.current?.destroy()
  }, [])

  const startPlayback = useCallback(
    (initialLevel?: number) => {
      const video = videoRef.current
      const normalizedSource = source.trim()
      if (video === null || normalizedSource.length === 0) {
        return
      }

      hlsRef.current?.destroy()
      hlsRef.current = null
      parallelLoaderRef.current = null
      video.pause()
      video.removeAttribute('src')
      video.load()
      setLogs([])
      setLoaderEventCounts(initialLoaderEventCounts)
      setQualityLevels([])
      setSelectedLevel(-2)
      setPlaybackLevel(null)
      setMetrics(initialMetrics)
      setFrontier(initialFrontier)
      setDiagnostics(emptyDiagnostics)
      setError(null)
      playbackLevelIndexRef.current = -1
      setStatus('正在加载播放列表')

      if (Hls.isSupported()) {
        let relayEndpoint: string | undefined
        if (transportMode === 'websocket') {
          try {
            relayEndpoint = resolveRelayEndpoint(workerUrl)
          } catch (cause) {
            const message = cause instanceof Error ? cause.message : 'Worker URL 无效'
            setStatus('等待有效的 Worker URL')
            setError(message)
            appendLog(message, 'error', { tag: 'Transport' })
            return
          }
        }

        const parallelLoader = createHlsParallelLoader({
          getPlaybackRate: () => video.playbackRate,
          getPlaybackTime: () => video.currentTime,
          onEvent: event => {
            if (event.type === 'segment-state') {
              countSegmentEvent(event, setLoaderEventCounts)
              appendSegmentStateLog(event, appendLog)
              return
            }

            const rescued = event.reason === 'slow-connection'
            setLoaderEventCounts(current => ({
              ...current,
              preempted: current.preempted + (rescued ? 0 : 1),
              rescued: current.rescued + (rescued ? 1 : 0),
            }))
            appendLog(
              rescued ? '慢速请求已中止并重新调度' : '低优先级请求已暂停并让出通道',
              rescued ? 'rescued' : 'preempted',
              {
                details: [
                  {
                    label: 'Segment',
                    value: `${String(event.segmentSn)} · ${event.segmentStart.toFixed(2)}s`,
                  },
                  {
                    label: '请求范围',
                    value: formatRange(event.requestStart, event.requestEnd),
                  },
                  {
                    label: '本次加载',
                    value: `${formatBytes(event.loadedBytes)} · Chunk ${formatBytes(event.chunkLoadedBytes)}`,
                  },
                  { label: '剩余数据', value: formatBytes(event.remainingBytes) },
                  {
                    label: '当前速率',
                    value: formatThroughput(event.throughputBytesPerSecond),
                  },
                  ...(event.baselineThroughputBytesPerSecond === undefined
                    ? []
                    : [
                        {
                          label: '参考速率',
                          value: formatThroughput(event.baselineThroughputBytesPerSecond),
                        },
                      ]),
                  {
                    label: '请求状态',
                    value: `${event.elapsedMs.toFixed(0)} ms · Attempt ${event.attempt}`,
                  },
                ],
                tag: rescued ? '慢速补救' : '请求抢占',
              },
            )
          },
          ...(relayEndpoint === undefined
            ? {}
            : {
                transport: new WebSocketHttpTransport(relayEndpoint, {
                  debug: true,
                  maxConnections: DEFAULT_MAX_CONCURRENCY * 2,
                }),
              }),
        })
        parallelLoader.setEnabled(loaderMode === 'parallel')
        parallelLoaderRef.current = parallelLoader

        const hls = new Hls({
          autoStartLoad: false,
          preferManagedMediaSource: true,
          preserveManualLevelOnError: true,
          progressive: false,
          ...(loaderMode === 'parallel' ? { fLoader: parallelLoader.fragmentLoader } : {}),
        })
        parallelLoader.attach(hls)
        hlsRef.current = hls

        hls.on(Hls.Events.MANIFEST_PARSED, (_, data) => {
          const levels = data.levels
            .map((level, index) => ({
              bitrate: level.averageBitrate,
              height: level.height,
              index,
              label: formatLevelLabel(level, index),
            }))
            .sort(
              (left, right) =>
                left.bitrate - right.bitrate ||
                left.height - right.height ||
                left.index - right.index,
            )
          const highestLevel = levels.at(-1)?.index ?? 0
          const targetLevel =
            initialLevel !== undefined &&
            (initialLevel === -1 || levels.some(level => level.index === initialLevel))
              ? initialLevel
              : highestLevel
          const targetLabel =
            targetLevel === -1
              ? 'Auto'
              : (levels.find(level => level.index === targetLevel)?.label ?? `Level ${targetLevel}`)

          setQualityLevels(levels)
          setSelectedLevel(targetLevel)
          hls.loadLevel = targetLevel
          hls.startLoad()
          setStatus(targetLevel === -1 ? '自动清晰度' : `固定清晰度: ${targetLabel}`)
          appendLog(
            targetLevel === -1
              ? 'Manifest 解析完成, 启用 ABR'
              : `Manifest 解析完成, 固定 ${targetLabel}`,
            'success',
            { tag: 'Manifest' },
          )
          void video.play().catch(() => {
            setStatus('已就绪, 请点击播放器开始')
          })
        })
        hls.on(Hls.Events.FRAG_LOADING, (_, data) => {
          setFrontier(current => ({ ...current, loadingTime: data.frag.start }))
          const part =
            data.part === null || data.part === undefined ? '' : `, Part ${data.part.index}`
          appendLog(`开始加载 Segment ${String(data.frag.sn)}${part}`, 'default', {
            details: [
              { label: 'Level', value: `L${data.frag.level}` },
              { label: '时间位置', value: `${data.frag.start.toFixed(2)}s` },
            ],
            tag: 'Segment',
          })
        })
        hls.on(Hls.Events.FRAG_LOADED, (_, data) => {
          setFrontier(current => ({
            ...current,
            loadedEnd: data.frag.start + data.frag.duration,
          }))
          const elapsed = data.frag.stats.loading.end - data.frag.stats.loading.start
          appendLog(`Segment ${String(data.frag.sn)} 加载完成`, 'success', {
            details: [
              { label: '数据量', value: formatBytes(data.frag.stats.loaded) },
              { label: '耗时', value: `${elapsed.toFixed(0)} ms` },
            ],
            tag: '加载完成',
          })
        })
        hls.on(Hls.Events.FRAG_CHANGED, (_, data) => {
          const levelIndex = data.frag.level
          const level = hls.levels[levelIndex]
          if (level === undefined) {
            return
          }

          setPlaybackLevel({
            bitrate: level.averageBitrate,
            height: level.height,
            index: levelIndex,
            width: level.width,
          })
          if (playbackLevelIndexRef.current !== levelIndex) {
            playbackLevelIndexRef.current = levelIndex
            appendLog(`实际播放切换到 ${level.width}×${level.height}`, 'success', {
              tag: '播放',
            })
          }
        })
        hls.on(Hls.Events.ERROR, (_, data) => {
          const message = `${data.details}: ${data.error.message}`
          appendLog(message, data.fatal ? 'error' : 'default', {
            tag: data.fatal ? '错误' : '自动恢复',
          })
          if (data.fatal) {
            setStatus(`播放失败: ${data.details}`)
            setError(message)
          }
        })

        hls.attachMedia(video)
        hls.loadSource(normalizedSource)
        setActiveTransportMode(transportMode)
        appendLog(
          loaderMode === 'parallel'
            ? `启用 ${DEFAULT_MAX_CONCURRENCY} 路并行 Range Loader`
            : '启用 hls.js 原生 FetchLoader',
          'default',
          { tag: '加载器' },
        )
        appendLog(
          relayEndpoint === undefined
            ? '并行加载器配置为 Browser Fetch Transport'
            : `并行加载器配置为 WebSocket Relay: ${relayEndpoint}`,
          'default',
          { tag: 'Transport' },
        )
        return
      }

      if (video.canPlayType('application/vnd.apple.mpegurl')) {
        video.src = normalizedSource
        setStatus('使用浏览器原生 HLS')
        appendLog('当前环境回退到浏览器原生 HLS', 'default', { tag: '加载器' })
        void video.play().catch(() => undefined)
        return
      }

      setStatus('当前浏览器不支持 HLS')
      setError('没有可用的 HLS 播放路径')
      appendLog('没有可用的 HLS 播放路径', 'error', { tag: '错误' })
    },
    [appendLog, loaderMode, source, transportMode, workerUrl],
  )

  const handleLoaderModeChange = (value: string) => {
    const mode: LoaderMode = value === 'native' ? 'native' : 'parallel'
    setLoaderMode(mode)

    const hls = hlsRef.current
    const parallelLoader = parallelLoaderRef.current
    if (hls === null || parallelLoader === null) {
      return
    }

    parallelLoader.setEnabled(mode === 'parallel')
    if (mode === 'parallel') {
      hls.config.fLoader = parallelLoader.fragmentLoader
      appendLog('切换到并行 Range, 从后续新 Fragment 生效', 'default', { tag: '加载器' })
      return
    }

    delete hls.config.fLoader
    appendLog('切换到 hls.js 原生加载, 从后续新 Fragment 生效', 'default', {
      tag: '加载器',
    })
  }

  const handleTransportModeChange = (value: string) => {
    const mode: TransportMode = value === 'websocket' ? 'websocket' : 'fetch'
    setTransportMode(mode)
    if (hlsRef.current !== null && mode !== activeTransportMode) {
      appendLog('Transport 设置已改变, 点击 LOAD STREAM 重建加载会话', 'default', {
        tag: 'Transport',
      })
    }
  }

  const handleLevelChange = (value: string) => {
    const level = Number(value)
    setSelectedLevel(level)

    const hls = hlsRef.current
    if (hls === null) {
      return
    }

    if (!hls.hasEnoughToStart) {
      startPlayback(level)
      return
    }
    hls.nextLevel = level
    setFrontier(current => ({ ...current, loadedEnd: metrics.currentTime }))
    if (level === -1) {
      setStatus('自动清晰度')
      appendLog('重新启用 ABR', 'default', { tag: '清晰度' })
      return
    }

    const label = qualityLevels.find(option => option.index === level)?.label ?? `Level ${level}`
    setStatus(`固定清晰度: ${label}`)
    appendLog(`切换固定清晰度到 ${label}`, 'default', { tag: '清晰度' })
  }

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    startPlayback()
  }

  const levelLabels = useMemo(
    () =>
      new Map(
        qualityLevels.map(level => [level.index, level.label.split(' · ')[0] ?? level.label]),
      ),
    [qualityLevels],
  )
  const segments = diagnostics.streams.flatMap(stream => stream.segments)
  const cachedSegments = segments.filter(segment => segment.state === 'ready')
  const cachedBytes = cachedSegments.reduce(
    (total, segment) => total + Math.max(segment.loadedBytes, segment.totalBytes),
    0,
  )
  const neededSegments = segments.filter(
    segment => segment.prefetch && segment.state !== 'ready',
  ).length

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
          </span>
          <div>
            <p>STORYA STREAMING LAB</p>
            <h1>
              STORYA<span>LAB</span>
            </h1>
          </div>
        </div>
        <div className="runtime-state">
          <span className="live-dot" />
          <div>
            <strong>{status}</strong>
            <small>hls.js {Hls.version} · virtual stream loader</small>
          </div>
        </div>
      </header>

      <form className="source-bar panel" onSubmit={handleSubmit}>
        <label htmlFor="source">HLS SOURCE</label>
        <input
          id="source"
          type="url"
          value={source}
          onChange={event => setSource(event.target.value)}
          placeholder="https://example.com/master.m3u8"
          required
        />
        <button type="submit">
          LOAD STREAM
          <span>↗</span>
        </button>
      </form>

      {error === null ? null : <div className="error-banner">SYSTEM ERROR · {error}</div>}

      <section className="metric-grid">
        <MetricCard
          accent="green"
          label="BUFFER AHEAD"
          value={`${metrics.bufferAhead.toFixed(1)} s`}
          note={`until ${formatDuration(metrics.bufferedEnd)}`}
        />
        <MetricCard
          accent="blue"
          label="PLAYHEAD"
          value={formatDuration(metrics.currentTime)}
          note={`duration ${formatDuration(metrics.duration)}`}
        />
        <MetricCard
          accent="violet"
          label="HLS FRONTIER"
          value={formatDuration(frontier.loadedEnd)}
          note={`loading ${formatDuration(frontier.loadingTime)}`}
        />
        <MetricCard
          accent="amber"
          label="VIRTUAL CACHE"
          value={`${cachedSegments.length} SEG`}
          note={`${formatBytes(cachedBytes)} · need ${neededSegments}`}
        />
        <MetricCard
          accent="green"
          label="CONCURRENCY"
          value={`${diagnostics.activeRequests} / ${diagnostics.maxConcurrency}`}
          note={`${diagnostics.streams.filter(stream => stream.active).length} active streams`}
        />
        <MetricCard
          accent="blue"
          label="THROUGHPUT"
          value={formatThroughput(diagnostics.estimatedThroughputBytesPerSecond)}
          note={`hls estimate ${formatBandwidth(metrics.bandwidth)}`}
        />
      </section>

      <section className="workspace-grid">
        <div className="main-column">
          <section className="panel stream-panel">
            <PanelHeading index="01" eyebrow="SCHEDULER" title="虚拟流状态">
              <div className="stream-legend">
                <span data-state="cached">缓存</span>
                <span data-state="loading">加载</span>
                <span data-state="needed">待加载</span>
                <span data-state="slow">慢速补救</span>
                <span data-state="preempted">被抢占</span>
              </div>
            </PanelHeading>
            <VirtualStreamMap
              levelLabels={levelLabels}
              playbackTime={metrics.currentTime}
              snapshot={diagnostics}
            />
          </section>

          <section className="panel player-panel">
            <PanelHeading index="02" eyebrow="PLAYBACK" title="视频输出">
              <div className="inline-controls">
                <label htmlFor="quality">QUALITY</label>
                <select
                  id="quality"
                  value={selectedLevel}
                  disabled={qualityLevels.length === 0}
                  onChange={event => handleLevelChange(event.target.value)}
                >
                  {selectedLevel === -2 ? <option value={-2}>未加载</option> : null}
                  <option value={-1}>Auto</option>
                  {qualityLevels.map(level => (
                    <option key={level.index} value={level.index}>
                      {level.label}
                    </option>
                  ))}
                </select>
              </div>
            </PanelHeading>
            <div className="video-frame">
              <video
                ref={videoRef}
                className="video"
                controls
                crossOrigin="anonymous"
                playsInline
                onPlaying={() => setStatus('正在播放')}
                onWaiting={() => setStatus('等待媒体数据')}
              />
              <span className="video-badge">LIVE SIGNAL</span>
              <span className="video-level">
                {playbackLevel === null
                  ? 'WAITING FOR MEDIA'
                  : `L${playbackLevel.index} · ${playbackLevel.width}×${playbackLevel.height}`}
              </span>
            </div>
            <BufferTimeline metrics={metrics} />
          </section>
        </div>

        <aside className="side-column">
          <section className="panel control-panel">
            <PanelHeading index="CTRL" eyebrow="RUNTIME" title="加载参数" />
            <div className="control-body">
              <label htmlFor="loader-mode">LOADER MODE</label>
              <select
                id="loader-mode"
                value={loaderMode}
                onChange={event => handleLoaderModeChange(event.target.value)}
              >
                <option value="parallel">并行 Range</option>
                <option value="native">hls.js 原生</option>
              </select>
              <label htmlFor="transport-mode">TRANSPORT</label>
              <select
                id="transport-mode"
                value={transportMode}
                onChange={event => handleTransportModeChange(event.target.value)}
              >
                <option value="fetch">Browser Fetch</option>
                <option value="websocket">WebSocket Relay</option>
              </select>
              {transportMode === 'websocket' ? (
                <div className="transport-endpoint">
                  <label htmlFor="worker-url">WORKER URL</label>
                  <input
                    id="worker-url"
                    type="url"
                    value={workerUrl}
                    onChange={event => setWorkerUrl(event.target.value)}
                    placeholder="https://storya-edge-worker.example.workers.dev"
                    required
                  />
                  <small>可填写 Worker 根 URL 或完整的 wss://.../transport 地址</small>
                </div>
              ) : null}
              <dl className="runtime-facts">
                <div>
                  <dt>REQUEST LIMIT</dt>
                  <dd>{DEFAULT_MAX_CONCURRENCY}</dd>
                </div>
                <div>
                  <dt>SESSION TRANSPORT</dt>
                  <dd>{formatTransportMode(activeTransportMode)}</dd>
                </div>
                <div>
                  <dt>WS POOL LIMIT</dt>
                  <dd>{activeTransportMode === 'websocket' ? DEFAULT_MAX_CONCURRENCY * 2 : '—'}</dd>
                </div>
                <div>
                  <dt>CHUNK SIZE</dt>
                  <dd>{formatBytes(DEFAULT_CHUNK_SIZE)}</dd>
                </div>
                <div>
                  <dt>PREFETCH DEPTH</dt>
                  <dd>{DEFAULT_PREFETCH_DEPTH} segments</dd>
                </div>
                <div>
                  <dt>PLAYBACK LEVEL</dt>
                  <dd>{playbackLevel === null ? '—' : `L${playbackLevel.index}`}</dd>
                </div>
              </dl>
            </div>
          </section>

          <section className="panel event-panel">
            <div className="event-heading">
              <PanelHeading index="LOG" eyebrow="TELEMETRY" title="加载事件" />
              <div className="event-summary">
                <span data-tone="success">命中 {loaderEventCounts.cacheHits}</span>
                <span data-tone="success">预取 {loaderEventCounts.prefetched}</span>
                <span data-tone="preempted">抢占 {loaderEventCounts.preempted}</span>
                <span data-tone="rescued">补救 {loaderEventCounts.rescued}</span>
                <button
                  type="button"
                  disabled={logs.length === 0}
                  onClick={() => {
                    setLogs([])
                    setLoaderEventCounts(initialLoaderEventCounts)
                  }}
                >
                  CLEAR
                </button>
              </div>
            </div>
            <EventLog logs={logs} />
          </section>
        </aside>
      </section>

      <footer className="footer-note">
        <span>STORYA HLS LOADER · DIAGNOSTIC BUILD</span>
        <span>Range requests are visible in browser Network tools</span>
      </footer>
    </main>
  )
}

function PanelHeading({
  children,
  eyebrow,
  index,
  title,
}: {
  children?: React.ReactNode
  eyebrow: string
  index: string
  title: string
}) {
  return (
    <header className="panel-heading">
      <span className="panel-index">{index}</span>
      <div>
        <small>{eyebrow}</small>
        <h2>{title}</h2>
      </div>
      {children === undefined ? null : <div className="panel-actions">{children}</div>}
    </header>
  )
}

function MetricCard({
  accent,
  label,
  note,
  value,
}: {
  accent: 'amber' | 'blue' | 'green' | 'violet'
  label: string
  note: string
  value: string
}) {
  return (
    <article className="metric-card panel" data-accent={accent}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{note}</small>
    </article>
  )
}

function BufferTimeline({ metrics }: { metrics: PlaybackMetrics }) {
  const duration = Number.isFinite(metrics.duration) && metrics.duration > 0 ? metrics.duration : 1
  const playhead = Math.min(100, (metrics.currentTime / duration) * 100)
  const buffered = Math.min(100, (metrics.bufferedEnd / duration) * 100)
  return (
    <div className="buffer-timeline">
      <div className="buffer-labels">
        <span>MEDIA BUFFER</span>
        <b>
          {formatDuration(metrics.currentTime)} / {formatDuration(metrics.duration)}
        </b>
      </div>
      <div className="buffer-track">
        <i className="buffered-range" style={{ width: `${buffered}%` }} />
        <i className="playhead-position" style={{ left: `${playhead}%` }} />
      </div>
    </div>
  )
}

function EventLog({ logs }: { logs: LogEntry[] }) {
  return (
    <ol className="event-list" aria-live="polite">
      {logs.length === 0 ? (
        <li className="event-empty">
          <span>AWAITING TELEMETRY</span>
          加载视频后显示 Segment 与调度事件
        </li>
      ) : (
        logs.map(log => (
          <li key={log.id} data-tone={log.tone}>
            <div className="event-meta">
              <time>{log.time}</time>
              <span>{log.tag}</span>
            </div>
            <div className="event-content">
              <strong>{log.message}</strong>
              {log.details.length === 0 ? null : (
                <dl>
                  {log.details.map(detail => (
                    <div key={detail.label}>
                      <dt>{detail.label}</dt>
                      <dd>{detail.value}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          </li>
        ))
      )}
    </ol>
  )
}

function countSegmentEvent(
  event: HlsLoaderSegmentEvent,
  setCounts: React.Dispatch<React.SetStateAction<LoaderEventCounts>>,
) {
  if (event.action !== 'demand-ready' && event.action !== 'prefetch-ready') {
    return
  }
  setCounts(current => ({
    ...current,
    cacheHits: current.cacheHits + (event.action === 'demand-ready' ? 1 : 0),
    prefetched: current.prefetched + (event.action === 'prefetch-ready' ? 1 : 0),
  }))
}

function appendSegmentStateLog(
  event: HlsLoaderSegmentEvent,
  appendLog: (message: string, tone?: LogTone, options?: LogEntryOptions) => void,
) {
  const definitions: Partial<
    Record<HlsLoaderSegmentEvent['action'], [message: string, tag: string, tone: LogTone]>
  > = {
    'demand-miss': ['hls.js 请求尚未填充的 Segment', '即时需求', 'default'],
    'demand-ready': ['hls.js 命中虚拟流缓存', '缓存命中', 'success'],
    'prefetch-cancelled': ['预填充 Segment 已退出窗口', '预取取消', 'preempted'],
    'prefetch-ready': ['预填充 Segment 已就绪', '预取完成', 'success'],
    'prefetch-started': ['开始填充后续 Segment', '预填充', 'default'],
  }
  const definition = definitions[event.action]
  if (definition === undefined) {
    return
  }
  appendLog(`${definition[0]} ${String(event.segmentSn)}`, definition[2], {
    details: [
      { label: '虚拟流', value: event.streamId },
      { label: '时间位置', value: `${event.segmentStart.toFixed(2)}s` },
    ],
    tag: definition[1],
  })
}

function formatLevelLabel(
  level: { averageBitrate: number; height?: number },
  index: number,
): string {
  const resolution = level.height === undefined ? `Level ${index}` : `${level.height}p`
  return `${resolution} · ${(level.averageBitrate / 1_000_000).toFixed(2)} Mbps`
}

function findBufferedEnd(video: HTMLVideoElement): number {
  for (let index = 0; index < video.buffered.length; index += 1) {
    const start = video.buffered.start(index)
    const end = video.buffered.end(index)
    if (video.currentTime >= start && video.currentTime <= end) {
      return end
    }
  }
  return video.currentTime
}

function formatDuration(value: number): string {
  if (!Number.isFinite(value)) {
    return 'LIVE'
  }
  const minutes = Math.floor(Math.max(0, value) / 60)
  const seconds = Math.floor(Math.max(0, value) % 60)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function formatBandwidth(value: number): string {
  return value > 0 ? `${(value / 1_000_000).toFixed(2)} Mbps` : 'WAITING'
}

function formatThroughput(bytesPerSecond: number): string {
  return formatBandwidth(bytesPerSecond * 8)
}

function formatBytes(value: number | undefined): string {
  if (value === undefined) {
    return 'UNKNOWN'
  }
  if (value < 1024) {
    return `${value} B`
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KiB`
  }
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`
}

function formatRange(start: number, endExclusive: number | undefined): string {
  return `${formatBytes(start)} – ${endExclusive === undefined ? 'EOF' : formatBytes(endExclusive)}`
}

function formatTransportMode(mode: TransportMode | null): string {
  if (mode === null) {
    return '—'
  }
  return mode === 'websocket' ? 'WebSocket Relay' : 'Browser Fetch'
}

function resolveRelayEndpoint(value: string): string {
  const normalized = value.trim()
  if (normalized.length === 0) {
    throw new Error('使用 WebSocket Relay 前需要填写 Worker URL')
  }

  let url: URL
  try {
    url = new URL(normalized)
  } catch {
    throw new Error('Worker URL 格式无效')
  }
  if (url.protocol === 'http:') {
    url.protocol = 'ws:'
  } else if (url.protocol === 'https:') {
    url.protocol = 'wss:'
  } else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('Worker URL 必须使用 http、https、ws 或 wss')
  }
  if (url.pathname === '' || url.pathname === '/') {
    url.pathname = '/transport'
  }
  url.hash = ''
  return url.toString()
}
