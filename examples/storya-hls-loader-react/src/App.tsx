import Hls from 'hls.js'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import {
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CONCURRENCY,
  DEFAULT_RESCUE_OPTIONS,
  DEFAULT_WINDOW_SIZE,
  ParallelAudioStreamController,
  ParallelSegmentLoader,
  ParallelStreamController,
} from 'storya-hls-loader'
import type { ParallelSegmentLoaderDiagnostics } from 'storya-hls-loader'
import { FetchHttpTransport, ProxyHttpTransport, WebSocketHttpTransport } from 'storya-transport'

import { VirtualStreamMap } from './virtual-stream-map'

const defaultSource =
  'https://cdn.radiantmediatechs.com/rmp/media/samples-for-rmp-site/04052024-lac-de-bimont/hls/playlist.m3u8'

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

interface LoaderParameterInputs {
  chunkSizeMiB: string
  maxConcurrency: string
  rescueEnabled: boolean
  rescueMaxAttempts: string
  slowRateThresholdPercent: string
  stallTimeoutMs: string
  windowSize: string
}

interface LoaderParameters {
  chunkSize: number
  maxConcurrency: number
  rescue:
    | false
    | {
        maxAttempts: number
        slowRateThresholdRatio: number
        stallTimeoutMs: number
      }
  windowSize: number
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
type TransportMode = 'fetch' | 'proxy' | 'websocket'

const websocketCancelTimeoutMs = 10_000
const websocketConnectTimeoutMs = 10_000
const websocketDefaultMaxResponseBytes = 32 * 1024 * 1024
const websocketIdleConnectionTimeoutMs = 30_000
const websocketMaxConnections = DEFAULT_MAX_CONCURRENCY * 2
const websocketMaxRequestsPerConnection = 50
const websocketMinIdleConnections = 6

const defaultLoaderParameterInputs: LoaderParameterInputs = {
  chunkSizeMiB: String(DEFAULT_CHUNK_SIZE / (1024 * 1024)),
  maxConcurrency: String(DEFAULT_MAX_CONCURRENCY),
  rescueEnabled: true,
  rescueMaxAttempts: String(DEFAULT_RESCUE_OPTIONS.maxAttempts),
  slowRateThresholdPercent: String(DEFAULT_RESCUE_OPTIONS.slowRateThresholdRatio * 100),
  stallTimeoutMs: String(DEFAULT_RESCUE_OPTIONS.stallTimeoutMs),
  windowSize: String(DEFAULT_WINDOW_SIZE),
}

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

const emptyDiagnostics: ParallelSegmentLoaderDiagnostics = {
  activeRequests: 0,
  bandwidthEstimate: 0,
  destroyed: false,
  maxConcurrency: DEFAULT_MAX_CONCURRENCY,
  revision: 0,
  rescue: {
    discardedBytes: 0,
    exhaustedStallCount: 0,
    pendingEvents: 0,
    recentEvents: [],
    recoveredEvents: 0,
    slowEvents: 0,
    stallEvents: 0,
    totalEvents: 0,
  },
  streams: [],
  timestamp: 0,
  workers: [],
}

export function App() {
  const videoRef = useRef<HTMLVideoElement>(null)
  const hlsRef = useRef<Hls | null>(null)
  const parallelLoaderRef = useRef<ParallelSegmentLoader | null>(null)
  const logIdRef = useRef(0)
  const playbackLevelIndexRef = useRef(-1)
  const rescueEventOutcomesRef = useRef(new Map<number, 'pending' | 'recovered'>())
  const [loaderMode, setLoaderMode] = useState<LoaderMode>('parallel')
  const [transportMode, setTransportMode] = useState<TransportMode>('fetch')
  const [workerUrl, setWorkerUrl] = useState('')
  const [proxyOriginsText, setProxyOriginsText] = useState('')
  const [loaderParameterInputs, setLoaderParameterInputs] = useState(defaultLoaderParameterInputs)
  const [activeLoaderMode, setActiveLoaderMode] = useState<LoaderMode | null>(null)
  const [activeTransportMode, setActiveTransportMode] = useState<TransportMode | null>(null)
  const [activeLoaderParameters, setActiveLoaderParameters] = useState<LoaderParameters | null>(
    null,
  )
  const [source, setSource] = useState(defaultSource)
  const [qualityLevels, setQualityLevels] = useState<QualityLevel[]>([])
  const [selectedLevel, setSelectedLevel] = useState(-2)
  const [playbackLevel, setPlaybackLevel] = useState<PlaybackLevel | null>(null)
  const [status, setStatus] = useState('等待加载')
  const [metrics, setMetrics] = useState(initialMetrics)
  const [frontier, setFrontier] = useState(initialFrontier)
  const [diagnostics, setDiagnostics] = useState(emptyDiagnostics)
  const [logs, setLogs] = useState<LogEntry[]>([])
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
        const snapshot = parallelLoader.getDiagnostics()
        setDiagnostics(snapshot)
        const nextOutcomes = new Map<number, 'pending' | 'recovered'>()
        for (const event of snapshot.rescue.recentEvents) {
          const previousOutcome = rescueEventOutcomesRef.current.get(event.id)
          nextOutcomes.set(event.id, event.outcome)
          if (previousOutcome === undefined) {
            const details: LogEntryDetail[] = [
              { label: 'Chunk', value: event.chunkKey },
              { label: 'Attempt', value: String(event.attempt) },
              { label: '已丢弃', value: formatBytes(event.discardedBytes) },
              { label: '触发耗时', value: `${event.elapsedMs.toFixed(0)} ms` },
              { label: '结果', value: event.outcome === 'recovered' ? '已恢复' : '重试中' },
            ]
            if (
              event.reason === 'slow' &&
              event.currentRate !== undefined &&
              event.peerMedianRate !== undefined
            ) {
              details.push(
                { label: '当前速率', value: formatBandwidth(event.currentRate * 8) },
                { label: 'Peer 中位数', value: formatBandwidth(event.peerMedianRate * 8) },
                { label: 'Peer 数量', value: String(event.peerCount ?? 0) },
              )
              if (event.continueEtaMs !== undefined && event.retryEtaMs !== undefined) {
                details.push(
                  { label: '继续 ETA', value: `${event.continueEtaMs.toFixed(0)} ms` },
                  { label: '重试 ETA', value: `${event.retryEtaMs.toFixed(0)} ms` },
                )
              }
            }
            appendLog(
              event.reason === 'stall'
                ? 'Chunk 停滞, 已取消并重试'
                : 'Chunk 相对慢速, 已取消并重试',
              'rescued',
              { details, tag: '救援' },
            )
          } else if (previousOutcome === 'pending' && event.outcome === 'recovered') {
            appendLog(`Chunk ${event.chunkKey} 救援后恢复`, 'success', { tag: '救援完成' })
          }
        }
        rescueEventOutcomesRef.current = nextOutcomes
      }
    }, 180)

    return () => window.clearInterval(timer)
  }, [appendLog])

  const destroyPlaybackSession = useCallback(() => {
    const hls = hlsRef.current
    const parallelLoader = parallelLoaderRef.current
    hlsRef.current = null
    parallelLoaderRef.current = null
    hls?.destroy()
    parallelLoader?.destroy()
  }, [])

  useEffect(() => destroyPlaybackSession, [destroyPlaybackSession])

  const startPlayback = useCallback(
    (initialLevel?: number) => {
      const video = videoRef.current
      const normalizedSource = source.trim()
      if (video === null || normalizedSource.length === 0) {
        return
      }

      let loaderParameters: LoaderParameters | null = null
      if (loaderMode === 'parallel') {
        try {
          loaderParameters = parseLoaderParameters(loaderParameterInputs)
        } catch (cause) {
          const message = cause instanceof Error ? cause.message : 'Loader 参数无效'
          setStatus('等待有效的 Loader 参数')
          setError(message)
          appendLog(message, 'error', { tag: '加载器' })
          return
        }
      }

      destroyPlaybackSession()
      video.pause()
      video.removeAttribute('src')
      video.load()
      setLogs([])
      setQualityLevels([])
      setSelectedLevel(-2)
      setPlaybackLevel(null)
      setMetrics(initialMetrics)
      setFrontier(initialFrontier)
      setDiagnostics(emptyDiagnostics)
      rescueEventOutcomesRef.current = new Map()
      setActiveLoaderMode(null)
      setActiveTransportMode(null)
      setActiveLoaderParameters(null)
      setError(null)
      playbackLevelIndexRef.current = -1
      setStatus('正在加载播放列表')

      if (Hls.isSupported()) {
        let relayEndpoint: string | undefined
        let proxyOrigins: string[] | undefined
        if (loaderMode === 'parallel' && transportMode === 'websocket') {
          try {
            relayEndpoint = resolveRelayEndpoint(workerUrl)
          } catch (cause) {
            const message = cause instanceof Error ? cause.message : 'Worker URL 无效'
            setStatus('等待有效的 Worker URL')
            setError(message)
            appendLog(message, 'error', { tag: 'Transport' })
            return
          }
        } else if (loaderMode === 'parallel' && transportMode === 'proxy') {
          try {
            proxyOrigins = parseProxyOrigins(proxyOriginsText)
          } catch (cause) {
            const message = cause instanceof Error ? cause.message : 'Proxy Origins 无效'
            setStatus('等待有效的 Proxy Origins')
            setError(message)
            appendLog(message, 'error', { tag: 'Transport' })
            return
          }
        }

        const transport =
          loaderMode !== 'parallel'
            ? undefined
            : relayEndpoint === undefined
              ? proxyOrigins === undefined
                ? new FetchHttpTransport()
                : new ProxyHttpTransport(proxyOrigins)
              : new WebSocketHttpTransport(relayEndpoint, {
                  cancelTimeoutMs: websocketCancelTimeoutMs,
                  connectTimeoutMs: websocketConnectTimeoutMs,
                  defaultMaxResponseBytes: websocketDefaultMaxResponseBytes,
                  debug: true,
                  idleConnectionTimeoutMs: websocketIdleConnectionTimeoutMs,
                  maxConnections: websocketMaxConnections,
                  maxRequestsPerConnection: websocketMaxRequestsPerConnection,
                  minIdleConnections: websocketMinIdleConnections,
                })

        const parallelLoader =
          loaderMode === 'parallel'
            ? new ParallelSegmentLoader({
                ...(loaderParameters ?? {}),
                ...(transport === undefined ? {} : { transport }),
              })
            : null
        parallelLoaderRef.current = parallelLoader

        const hls = new Hls({
          autoStartLoad: false,
          preferManagedMediaSource: true,
          preserveManualLevelOnError: true,
          progressive: false,
          ...(parallelLoader === null
            ? {}
            : {
                audioStreamController: ParallelAudioStreamController,
                fLoader: parallelLoader.fLoader,
                streamController: ParallelStreamController,
              }),
        })
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
        setActiveLoaderMode(loaderMode)
        setActiveTransportMode(loaderMode === 'parallel' ? transportMode : null)
        setActiveLoaderParameters(loaderParameters)
        appendLog(
          loaderParameters === null
            ? '启用 hls.js 原生 FetchLoader'
            : `启用 ${String(loaderParameters.maxConcurrency)} 路并行 Range Loader`,
          'default',
          { tag: '加载器' },
        )
        if (loaderParameters !== null) {
          const rescueSummary =
            loaderParameters.rescue === false
              ? 'rescue 关闭'
              : `rescue ${String(loaderParameters.rescue.maxAttempts)} 次, 停滞 ${String(loaderParameters.rescue.stallTimeoutMs)} ms, 慢速阈值 ${formatPercent(loaderParameters.rescue.slowRateThresholdRatio)}`
          appendLog(
            `窗口 ${String(loaderParameters.windowSize)} 个 Segment, Chunk ${formatBytes(loaderParameters.chunkSize)}, ${rescueSummary}`,
            'default',
            { tag: '调度策略' },
          )
          appendLog(
            relayEndpoint !== undefined
              ? `并行加载器配置为 WebSocket Relay: ${relayEndpoint}`
              : proxyOrigins !== undefined
                ? `并行加载器配置为 HTTP Proxy Transport: ${proxyOrigins.length} 个 Origin`
                : '并行加载器配置为 Browser Fetch Transport',
            'default',
            { tag: 'Transport' },
          )
        }
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
    [
      appendLog,
      destroyPlaybackSession,
      loaderParameterInputs,
      loaderMode,
      proxyOriginsText,
      source,
      transportMode,
      workerUrl,
    ],
  )

  const handleLoaderModeChange = (value: string) => {
    const mode: LoaderMode = value === 'native' ? 'native' : 'parallel'
    setLoaderMode(mode)

    if (hlsRef.current !== null && mode !== activeLoaderMode) {
      appendLog('加载器设置已改变, 点击 LOAD STREAM 重建加载会话', 'default', {
        tag: '加载器',
      })
    }
  }

  const handleTransportModeChange = (value: string) => {
    const mode: TransportMode =
      value === 'websocket' ? 'websocket' : value === 'proxy' ? 'proxy' : 'fetch'
    setTransportMode(mode)
    if (
      hlsRef.current !== null &&
      activeLoaderMode === 'parallel' &&
      mode !== activeTransportMode
    ) {
      appendLog('Transport 设置已改变, 点击 LOAD STREAM 重建加载会话', 'default', {
        tag: 'Transport',
      })
    }
  }

  const handleLoaderParameterChange = <Name extends keyof LoaderParameterInputs>(
    name: Name,
    value: LoaderParameterInputs[Name],
  ) => {
    setLoaderParameterInputs(current => ({ ...current, [name]: value }))
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
    (total, segment) => total + Math.max(segment.loadedBytes, segment.totalBytes ?? 0),
    0,
  )
  const neededSegments = segments.filter(
    segment => segment.windowIndex !== null && segment.state !== 'ready',
  ).length
  const readyChunkBytes = segments
    .flatMap(segment => segment.chunks)
    .filter(chunk => chunk.state === 'ready')
    .reduce((total, chunk) => total + chunk.loadedBytes, 0)
  const chunks = segments.flatMap(segment => segment.chunks)
  const readyChunks = chunks.filter(chunk => chunk.state === 'ready').length
  const fillingChunks = chunks.filter(chunk => chunk.state === 'filling').length
  const failedChunks = chunks.filter(chunk => chunk.state === 'failed').length

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

      <form id="stream-source-form" className="source-bar panel" onSubmit={handleSubmit}>
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
          note={`${diagnostics.streams.filter(hasActiveReader).length} reader streams`}
        />
        <MetricCard
          accent="blue"
          label="READY CHUNKS"
          value={formatBytes(readyChunkBytes)}
          note={`loader ${formatBandwidth(diagnostics.bandwidthEstimate)} · hls ${formatBandwidth(metrics.bandwidth)}`}
        />
      </section>

      <section className="workspace-grid">
        <div className="main-column">
          <section className="panel stream-panel">
            <PanelHeading index="01" eyebrow="LOADER STATE" title="VirtualStream 状态">
              <div className="stream-legend">
                <span data-state="cached">缓存</span>
                <span data-state="planning">探测</span>
                <span data-state="loading">填充</span>
                <span data-state="needed">待领取</span>
                <span data-state="rescuing">补救</span>
                <span data-state="failed">失败</span>
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
                disabled={loaderMode !== 'parallel'}
                onChange={event => handleTransportModeChange(event.target.value)}
              >
                <option value="fetch">Browser Fetch</option>
                <option value="proxy">HTTP Proxy</option>
                <option value="websocket">WebSocket Relay</option>
              </select>
              <p className="transport-note">
                {transportMode === 'proxy'
                  ? 'Proxy 的物理请求为可缓存 200, Loader 会从响应头恢复逻辑 206.'
                  : transportMode === 'websocket'
                    ? '媒体请求通过 WebSocket relay, Segment 中显示上游逻辑状态.'
                    : 'Fetch 遇到 CORS 隐藏 Content-Range 时会用一个 HEAD 200 读取长度, 不重复下载完整 Segment.'}
              </p>
              {loaderMode === 'parallel' && transportMode === 'websocket' ? (
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
              {loaderMode === 'parallel' && transportMode === 'proxy' ? (
                <div className="transport-endpoint">
                  <label htmlFor="proxy-origins">PROXY ORIGINS</label>
                  <textarea
                    id="proxy-origins"
                    rows={3}
                    value={proxyOriginsText}
                    onChange={event => setProxyOriginsText(event.target.value)}
                    placeholder={'https://proxy-1.example.com\nhttps://proxy-2.example.com'}
                    required
                  />
                  <small>填写一个或多个 HTTP(S) Origin, 使用换行、空格或逗号分隔</small>
                </div>
              ) : null}
              {loaderMode === 'parallel' ? (
                <div className="loader-parameters">
                  <div className="loader-parameters-heading">
                    <span>PARALLEL POLICY</span>
                    <small>NEXT SESSION</small>
                  </div>
                  <div className="loader-parameter-grid">
                    <label className="loader-parameter" htmlFor="window-size">
                      <span>WINDOW</span>
                      <div>
                        <input
                          id="window-size"
                          form="stream-source-form"
                          type="number"
                          min="1"
                          max="24"
                          step="1"
                          value={loaderParameterInputs.windowSize}
                          onChange={event =>
                            handleLoaderParameterChange('windowSize', event.target.value)
                          }
                          required
                        />
                        <small>SEG</small>
                      </div>
                    </label>
                    <label className="loader-parameter" htmlFor="max-concurrency">
                      <span>WORKERS</span>
                      <div>
                        <input
                          id="max-concurrency"
                          form="stream-source-form"
                          type="number"
                          min="1"
                          max="12"
                          step="1"
                          value={loaderParameterInputs.maxConcurrency}
                          onChange={event =>
                            handleLoaderParameterChange('maxConcurrency', event.target.value)
                          }
                          required
                        />
                        <small>REQ</small>
                      </div>
                    </label>
                    <label className="loader-parameter" htmlFor="chunk-size">
                      <span>CHUNK SIZE</span>
                      <div>
                        <input
                          id="chunk-size"
                          form="stream-source-form"
                          type="number"
                          min="0.25"
                          max="16"
                          step="0.25"
                          value={loaderParameterInputs.chunkSizeMiB}
                          onChange={event =>
                            handleLoaderParameterChange('chunkSizeMiB', event.target.value)
                          }
                          required
                        />
                        <small>MiB</small>
                      </div>
                    </label>
                  </div>
                  <div
                    className="rescue-parameters"
                    data-disabled={!loaderParameterInputs.rescueEnabled}
                  >
                    <div className="rescue-parameters-heading">
                      <span>REQUEST RESCUE</span>
                      <label className="rescue-toggle">
                        <input
                          form="stream-source-form"
                          type="checkbox"
                          checked={loaderParameterInputs.rescueEnabled}
                          onChange={event =>
                            handleLoaderParameterChange('rescueEnabled', event.target.checked)
                          }
                        />
                        <span>{loaderParameterInputs.rescueEnabled ? 'ENABLED' : 'DISABLED'}</span>
                      </label>
                    </div>
                    <div className="loader-parameter-grid">
                      <label className="loader-parameter" htmlFor="stall-timeout">
                        <span>STALL TIMEOUT</span>
                        <div>
                          <input
                            id="stall-timeout"
                            form="stream-source-form"
                            type="number"
                            min="100"
                            max="60000"
                            step="100"
                            value={loaderParameterInputs.stallTimeoutMs}
                            onChange={event =>
                              handleLoaderParameterChange('stallTimeoutMs', event.target.value)
                            }
                            disabled={!loaderParameterInputs.rescueEnabled}
                            required={loaderParameterInputs.rescueEnabled}
                          />
                          <small>MS</small>
                        </div>
                      </label>
                      <label className="loader-parameter" htmlFor="slow-rate-threshold">
                        <span>SLOW THRESHOLD</span>
                        <div>
                          <input
                            id="slow-rate-threshold"
                            form="stream-source-form"
                            type="number"
                            min="0"
                            max="99"
                            step="1"
                            value={loaderParameterInputs.slowRateThresholdPercent}
                            onChange={event =>
                              handleLoaderParameterChange(
                                'slowRateThresholdPercent',
                                event.target.value,
                              )
                            }
                            disabled={!loaderParameterInputs.rescueEnabled}
                            required={loaderParameterInputs.rescueEnabled}
                          />
                          <small>% PEER</small>
                        </div>
                      </label>
                      <label className="loader-parameter is-wide" htmlFor="max-rescue-attempts">
                        <span>RESCUE LIMIT</span>
                        <div>
                          <input
                            id="max-rescue-attempts"
                            form="stream-source-form"
                            type="number"
                            min="1"
                            max="10"
                            step="1"
                            value={loaderParameterInputs.rescueMaxAttempts}
                            onChange={event =>
                              handleLoaderParameterChange('rescueMaxAttempts', event.target.value)
                            }
                            disabled={!loaderParameterInputs.rescueEnabled}
                            required={loaderParameterInputs.rescueEnabled}
                          />
                          <small>TRY</small>
                        </div>
                      </label>
                    </div>
                    <p>停滞表示连续无数据. 慢速阈值相对于同期 GET 中位速率, 设为 0 只检测停滞.</p>
                  </div>
                  <p>参数在下一次加载会话生效.</p>
                </div>
              ) : null}
              <dl className="runtime-facts">
                <div>
                  <dt>REQUEST LIMIT</dt>
                  <dd>{activeLoaderParameters?.maxConcurrency ?? '—'}</dd>
                </div>
                <div>
                  <dt>SESSION LOADER</dt>
                  <dd>{formatLoaderMode(activeLoaderMode)}</dd>
                </div>
                <div>
                  <dt>SESSION TRANSPORT</dt>
                  <dd>{formatTransportMode(activeTransportMode)}</dd>
                </div>
                <div>
                  <dt>WS POOL LIMIT</dt>
                  <dd>{activeTransportMode === 'websocket' ? websocketMaxConnections : '—'}</dd>
                </div>
                <div>
                  <dt>CHUNK SIZE</dt>
                  <dd>
                    {activeLoaderParameters === null
                      ? '—'
                      : formatBytes(activeLoaderParameters.chunkSize)}
                  </dd>
                </div>
                <div>
                  <dt>PREFETCH AHEAD</dt>
                  <dd>
                    {activeLoaderParameters === null
                      ? '—'
                      : `${String(activeLoaderParameters.windowSize - 1)} segments`}
                  </dd>
                </div>
                <div>
                  <dt>STALL TIMEOUT</dt>
                  <dd>
                    {activeLoaderParameters === null
                      ? '—'
                      : activeLoaderParameters.rescue === false
                        ? 'OFF'
                        : `${String(activeLoaderParameters.rescue.stallTimeoutMs)} ms`}
                  </dd>
                </div>
                <div>
                  <dt>SLOW THRESHOLD</dt>
                  <dd>
                    {activeLoaderParameters === null
                      ? '—'
                      : activeLoaderParameters.rescue === false
                        ? 'OFF'
                        : formatPercent(activeLoaderParameters.rescue.slowRateThresholdRatio)}
                  </dd>
                </div>
                <div>
                  <dt>RESCUE LIMIT</dt>
                  <dd>
                    {activeLoaderParameters === null
                      ? '—'
                      : activeLoaderParameters.rescue === false
                        ? 'OFF'
                        : `${String(activeLoaderParameters.rescue.maxAttempts)} rescues · ${String(activeLoaderParameters.rescue.maxAttempts + 1)} attempts`}
                  </dd>
                </div>
                <div>
                  <dt>RESCUE EVENTS</dt>
                  <dd>
                    {diagnostics.rescue.totalEvents} total · {diagnostics.rescue.recoveredEvents}{' '}
                    recovered
                  </dd>
                </div>
                <div>
                  <dt>RESCUE REASONS</dt>
                  <dd>
                    {diagnostics.rescue.stallEvents} stall · {diagnostics.rescue.slowEvents} slow ·{' '}
                    {diagnostics.rescue.exhaustedStallCount} exhausted
                  </dd>
                </div>
                <div>
                  <dt>DISCARDED BODY</dt>
                  <dd>{formatBytes(diagnostics.rescue.discardedBytes)}</dd>
                </div>
                <div>
                  <dt>STATE UPDATES</dt>
                  <dd>{diagnostics.revision}</dd>
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
                <span data-tone="success">Segment {cachedSegments.length}</span>
                <span data-tone="success">Chunk {readyChunks}</span>
                <span data-tone="preempted">填充 {fillingChunks}</span>
                <span data-tone="error">失败 {failedChunks}</span>
                <span data-tone="rescued">救援 {diagnostics.rescue.totalEvents}</span>
                <span data-tone="preempted">请求 {diagnostics.activeRequests}</span>
                <button
                  type="button"
                  disabled={logs.length === 0}
                  onClick={() => {
                    setLogs([])
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

function formatTransportMode(mode: TransportMode | null): string {
  if (mode === null) {
    return '—'
  }
  if (mode === 'websocket') {
    return 'WebSocket Relay'
  }
  return mode === 'proxy' ? 'HTTP Proxy' : 'Browser Fetch'
}

function formatLoaderMode(mode: LoaderMode | null): string {
  if (mode === null) {
    return '—'
  }
  return mode === 'parallel' ? 'Parallel Range' : 'hls.js Native'
}

function formatPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(0)}% peer median`
}

function hasActiveReader(stream: ParallelSegmentLoaderDiagnostics['streams'][number]): boolean {
  return stream.segments.some(segment => segment.readerCount > 0)
}

function parseLoaderParameters(inputs: LoaderParameterInputs): LoaderParameters {
  const windowSize = Number(inputs.windowSize)
  const maxConcurrency = Number(inputs.maxConcurrency)
  const chunkSizeMiB = Number(inputs.chunkSizeMiB)
  const rescueMaxAttempts = Number(inputs.rescueMaxAttempts)
  const slowRateThresholdPercent = Number(inputs.slowRateThresholdPercent)
  const stallTimeoutMs = Number(inputs.stallTimeoutMs)

  if (!Number.isSafeInteger(windowSize) || windowSize < 1 || windowSize > 24) {
    throw new Error('预加载窗口必须是 1 到 24 之间的整数')
  }
  if (!Number.isSafeInteger(maxConcurrency) || maxConcurrency < 1 || maxConcurrency > 12) {
    throw new Error('并发数必须是 1 到 12 之间的整数')
  }
  if (
    !Number.isFinite(chunkSizeMiB) ||
    chunkSizeMiB < 0.25 ||
    chunkSizeMiB > 16 ||
    !Number.isSafeInteger(chunkSizeMiB * 4)
  ) {
    throw new Error('Chunk 大小必须是 0.25 到 16 MiB, 步长为 0.25 MiB')
  }
  if (
    inputs.rescueEnabled &&
    (!Number.isSafeInteger(stallTimeoutMs) || stallTimeoutMs < 100 || stallTimeoutMs > 60_000)
  ) {
    throw new Error('停滞检测时间必须是 100 到 60000 ms 之间的整数')
  }
  if (
    inputs.rescueEnabled &&
    (!Number.isSafeInteger(slowRateThresholdPercent) ||
      slowRateThresholdPercent < 0 ||
      slowRateThresholdPercent >= 100)
  ) {
    throw new Error('慢速阈值必须是 0 到 99 之间的整数百分比')
  }
  if (
    inputs.rescueEnabled &&
    (!Number.isSafeInteger(rescueMaxAttempts) || rescueMaxAttempts < 1 || rescueMaxAttempts > 10)
  ) {
    throw new Error('Rescue 次数必须是 1 到 10 之间的整数')
  }

  return {
    chunkSize: Math.round(chunkSizeMiB * 1024 * 1024),
    maxConcurrency,
    rescue: inputs.rescueEnabled
      ? {
          maxAttempts: rescueMaxAttempts,
          slowRateThresholdRatio: slowRateThresholdPercent / 100,
          stallTimeoutMs,
        }
      : false,
    windowSize,
  }
}

function parseProxyOrigins(value: string): string[] {
  const values = value
    .split(/[\s,]+/u)
    .map(item => item.trim())
    .filter(item => item.length > 0)
  if (values.length === 0) {
    throw new Error('使用 HTTP Proxy 前需要填写至少一个 Proxy Origin')
  }

  const origins = values.map(value => {
    let url: URL
    try {
      url = new URL(value)
    } catch {
      throw new Error(`Proxy Origin 格式无效: ${value}`)
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error(`Proxy Origin 必须使用 http 或 https: ${value}`)
    }
    return url.origin
  })
  return [...new Set(origins)]
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
