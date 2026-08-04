import Hls from 'hls.js'
import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import {
  createParallelFragmentLoader,
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CONCURRENCY,
} from 'storya-hls-loader'

const defaultSource = 'https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8'

interface PlaybackMetrics {
  bufferAhead: number
  currentTime: number
  duration: number
  bandwidth: number
}

interface LogEntry {
  id: number
  message: string
  time: string
  tone: 'default' | 'error' | 'success'
}

type LoaderMode = 'native' | 'parallel'

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

const initialMetrics: PlaybackMetrics = {
  bufferAhead: 0,
  currentTime: 0,
  duration: 0,
  bandwidth: 0,
}

function formatDuration(value: number): string {
  if (!Number.isFinite(value)) {
    return '直播'
  }

  const minutes = Math.floor(value / 60)
  const seconds = Math.floor(value % 60)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function formatBandwidth(value: number): string {
  return value > 0 ? `${(value / 1_000_000).toFixed(2)} Mbps` : '等待采样'
}

function formatLevelLabel(
  level: { height?: number; averageBitrate: number },
  index: number,
): string {
  const resolution = level.height === undefined ? `Level ${index}` : `${level.height}p`
  return `${resolution} · ${(level.averageBitrate / 1_000_000).toFixed(2)} Mbps`
}

function findBufferAhead(video: HTMLVideoElement): number {
  for (let index = 0; index < video.buffered.length; index += 1) {
    const start = video.buffered.start(index)
    const end = video.buffered.end(index)
    if (video.currentTime >= start && video.currentTime <= end) {
      return Math.max(0, end - video.currentTime)
    }
  }

  return 0
}

export function App() {
  const videoRef = useRef<HTMLVideoElement>(null)
  const hlsRef = useRef<Hls | null>(null)
  const logIdRef = useRef(0)
  const parallelLoaderRef = useRef<ReturnType<typeof createParallelFragmentLoader> | null>(null)
  const playbackLevelIndexRef = useRef(-1)
  const [loaderMode, setLoaderMode] = useState<LoaderMode>('parallel')
  const [source, setSource] = useState(defaultSource)
  const [qualityLevels, setQualityLevels] = useState<QualityLevel[]>([])
  const [selectedLevel, setSelectedLevel] = useState(-2)
  const [playbackLevel, setPlaybackLevel] = useState<PlaybackLevel | null>(null)
  const [status, setStatus] = useState('等待加载')
  const [metrics, setMetrics] = useState(initialMetrics)
  const [logs, setLogs] = useState<LogEntry[]>([])

  const appendLog = useCallback((message: string, tone: LogEntry['tone'] = 'default') => {
    logIdRef.current += 1
    const entry: LogEntry = {
      id: logIdRef.current,
      message,
      time: new Date().toLocaleTimeString('zh-CN', { hour12: false }),
      tone,
    }
    setLogs(current => [entry, ...current].slice(0, 18))
  }, [])

  useEffect(() => {
    const timer = window.setInterval(() => {
      const video = videoRef.current
      if (video === null) {
        return
      }

      setMetrics({
        currentTime: video.currentTime,
        duration: video.duration,
        bufferAhead: findBufferAhead(video),
        bandwidth: hlsRef.current?.bandwidthEstimate ?? 0,
      })
    }, 500)

    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    return () => hlsRef.current?.destroy()
  }, [])

  const startPlayback = useCallback(() => {
    const video = videoRef.current
    const normalizedSource = source.trim()
    if (video === null || normalizedSource.length === 0) {
      return
    }

    hlsRef.current?.destroy()
    hlsRef.current = null
    video.pause()
    video.removeAttribute('src')
    video.load()
    setLogs([])
    setQualityLevels([])
    setSelectedLevel(-2)
    setPlaybackLevel(null)
    playbackLevelIndexRef.current = -1
    setStatus('正在加载播放列表')

    if (Hls.isSupported()) {
      const FragmentLoader = createParallelFragmentLoader({
        getPlaybackRate: () => video.playbackRate,
        getPlaybackTime: () => video.currentTime,
        onEvent: event => console.info('[storya-hls-loader]', event),
      })
      parallelLoaderRef.current = FragmentLoader
      const hls = new Hls({
        autoStartLoad: false,
        preferManagedMediaSource: true,
        preserveManualLevelOnError: true,
        progressive: true,
        ...(loaderMode === 'parallel' ? { fLoader: FragmentLoader } : {}),
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
        const highestLabel = levels.at(-1)?.label ?? `Level ${highestLevel}`

        setQualityLevels(levels)
        setSelectedLevel(highestLevel)
        hls.loadLevel = highestLevel
        hls.startLoad()
        setStatus(`固定清晰度: ${highestLabel}`)
        appendLog(`Manifest 解析完成, 固定 ${highestLabel}`, 'success')
        void video.play().catch(() => {
          setStatus('已就绪, 请点击播放器开始')
        })
      })
      hls.on(Hls.Events.FRAG_LOADING, (_, data) => {
        const part =
          data.part === null || data.part === undefined ? '' : `, Part ${data.part.index}`
        appendLog(`开始加载 Segment ${String(data.frag.sn)}${part}`)
      })
      hls.on(Hls.Events.FRAG_LOADED, (_, data) => {
        const size = (data.frag.stats.loaded / 1024).toFixed(0)
        appendLog(`Segment ${String(data.frag.sn)} 完成, ${size} KiB`, 'success')
      })
      hls.on(Hls.Events.FRAG_CHANGED, (_, data) => {
        const levelIndex = data.frag.level
        const level = hls.levels[levelIndex]
        if (level === undefined) {
          return
        }

        setPlaybackLevel({
          index: levelIndex,
          width: level.width,
          height: level.height,
          bitrate: level.averageBitrate,
        })

        if (playbackLevelIndexRef.current !== levelIndex) {
          playbackLevelIndexRef.current = levelIndex
          appendLog(`实际播放切换到 ${level.width}×${level.height}`, 'success')
        }
      })
      hls.on(Hls.Events.ERROR, (_, data) => {
        appendLog(`${data.details}: ${data.error.message}`, 'error')
        if (data.fatal) {
          setStatus(`播放失败: ${data.details}`)
        }
      })

      hls.attachMedia(video)
      hls.loadSource(normalizedSource)
      appendLog(
        loaderMode === 'parallel'
          ? `启用 ${DEFAULT_MAX_CONCURRENCY} 路并行 Range Loader`
          : '启用 hls.js 原生 FetchLoader',
      )
      return
    }

    if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = normalizedSource
      setStatus('使用原生 HLS, 不启用并行 Loader')
      appendLog('当前环境回退到原生 HLS')
      void video.play().catch(() => {})
      return
    }

    setStatus('当前浏览器不支持 HLS')
    appendLog('没有可用的 HLS 播放路径', 'error')
  }, [appendLog, loaderMode, source])

  const handleLoaderModeChange = (value: string) => {
    const mode: LoaderMode = value === 'native' ? 'native' : 'parallel'
    setLoaderMode(mode)

    const hls = hlsRef.current
    const parallelLoader = parallelLoaderRef.current
    if (hls === null || parallelLoader === null) {
      return
    }

    if (mode === 'parallel') {
      hls.config.fLoader = parallelLoader
      appendLog('切换到并行 Range, 从后续新 Fragment 生效')
      return
    }

    delete hls.config.fLoader
    appendLog('切换到 hls.js 原生加载, 从后续新 Fragment 生效')
  }

  const handleLevelChange = (value: string) => {
    const level = Number(value)
    setSelectedLevel(level)

    const hls = hlsRef.current
    if (hls === null) {
      return
    }

    hls.nextLevel = level
    if (level === -1) {
      setStatus('自动清晰度')
      appendLog('重新启用 ABR')
      return
    }

    const label = qualityLevels.find(option => option.index === level)?.label ?? `Level ${level}`
    setStatus(`固定清晰度: ${label}`)
    appendLog(`切换固定清晰度到 ${label}`)
  }

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    startPlayback()
  }

  return (
    <main className="shell">
      <header className="hero">
        <div>
          <p className="eyebrow">STORYA NETWORK LAB</p>
          <h1>HLS 并行加载实验台</h1>
          <p className="lede">
            使用自定义 hls.js Fragment Loader 探索单个 Segment 的并行 Range 下载行为。
          </p>
        </div>
        <span className="status" data-active={status.includes('失败') ? 'false' : 'true'}>
          {status}
        </span>
      </header>

      <section className="workspace">
        <div className="player-panel">
          <video
            ref={videoRef}
            className="video"
            controls
            crossOrigin="anonymous"
            playsInline
            onPlaying={() => setStatus('正在播放')}
            onWaiting={() => setStatus('等待媒体数据')}
          />

          <form className="source-form" onSubmit={handleSubmit}>
            <label htmlFor="source">HLS 地址</label>
            <div className="source-row">
              <input
                id="source"
                type="url"
                value={source}
                onChange={event => setSource(event.target.value)}
                placeholder="https://example.com/master.m3u8"
                required
              />
              <button type="submit">加载并播放</button>
            </div>
            <div className="form-options">
              <label htmlFor="loader-mode">加载模式</label>
              <select
                id="loader-mode"
                value={loaderMode}
                onChange={event => handleLoaderModeChange(event.target.value)}
              >
                <option value="parallel">并行 Range</option>
                <option value="native">hls.js 原生</option>
              </select>
              <label htmlFor="quality">清晰度</label>
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
              <span>
                并发 {DEFAULT_MAX_CONCURRENCY} · Chunk {DEFAULT_CHUNK_SIZE / 1024} KiB
              </span>
            </div>
          </form>
        </div>

        <aside className="telemetry">
          <div className="metrics">
            <article>
              <span>播放位置</span>
              <strong>{formatDuration(metrics.currentTime)}</strong>
            </article>
            <article>
              <span>媒体时长</span>
              <strong>{formatDuration(metrics.duration)}</strong>
            </article>
            <article>
              <span>前向缓冲</span>
              <strong>{metrics.bufferAhead.toFixed(1)} s</strong>
            </article>
            <article>
              <span>带宽估算</span>
              <strong>{formatBandwidth(metrics.bandwidth)}</strong>
            </article>
            <article>
              <span>当前播放分辨率</span>
              <strong>
                {playbackLevel === null
                  ? '等待播放'
                  : `${playbackLevel.width}×${playbackLevel.height}`}
              </strong>
            </article>
            <article>
              <span>实际播放 Level</span>
              <strong>
                {playbackLevel === null
                  ? '等待播放'
                  : `L${playbackLevel.index} · ${formatBandwidth(playbackLevel.bitrate)}`}
              </strong>
            </article>
          </div>

          <div className="log-panel">
            <div className="log-heading">
              <h2>加载事件</h2>
              <span>{logs.length} 条</span>
            </div>
            <ol aria-live="polite">
              {logs.length === 0 ? (
                <li className="empty">加载视频后显示 Segment 事件</li>
              ) : (
                logs.map(log => (
                  <li key={log.id} data-tone={log.tone}>
                    <time>{log.time}</time>
                    <span>{log.message}</span>
                  </li>
                ))
              )}
            </ol>
          </div>
        </aside>
      </section>

      <footer>
        打开浏览器开发者工具的 Network 面板, 可检查 Segment 请求的 <code>Range</code> 响应。
      </footer>
    </main>
  )
}
