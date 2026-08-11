import {
  DEFAULT_CHUNK_SIZE,
  DEFAULT_MAX_CONCURRENCY,
  DEFAULT_RESCUE_OPTIONS,
  DEFAULT_WINDOW_SIZE,
} from 'storya-hls-loader'

export interface ExampleSettings {
  loaderMode: LoaderMode
  loaderParameterInputs: LoaderParameterInputs
  progressiveEnabled: boolean
  source: string
  transportMode: TransportMode
  workerUrl: string
}

export interface ExampleSettingsStorage {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

export interface LoaderParameterInputs {
  chunkSizeMiB: string
  maxConcurrency: string
  rescueEnabled: boolean
  rescueMaxAttempts: string
  slowRateThresholdPercent: string
  stallTimeoutMs: string
  windowSize: string
}

export type LoaderMode = 'native' | 'parallel'
export type TransportMode = 'fetch' | 'websocket'

interface StoredExampleSettings extends ExampleSettings {
  version: number
}

const settingsStorageKey = 'storya-hls-loader-react.settings'
const settingsVersion = 1

export function createDefaultExampleSettings(): ExampleSettings {
  return {
    loaderMode: 'parallel',
    loaderParameterInputs: {
      chunkSizeMiB: String(DEFAULT_CHUNK_SIZE / (1024 * 1024)),
      maxConcurrency: String(DEFAULT_MAX_CONCURRENCY),
      rescueEnabled: true,
      rescueMaxAttempts: String(DEFAULT_RESCUE_OPTIONS.maxAttempts),
      slowRateThresholdPercent: String(DEFAULT_RESCUE_OPTIONS.slowRateThresholdRatio * 100),
      stallTimeoutMs: String(DEFAULT_RESCUE_OPTIONS.stallTimeoutMs),
      windowSize: String(DEFAULT_WINDOW_SIZE),
    },
    progressiveEnabled: true,
    source: '',
    transportMode: 'fetch',
    workerUrl: '',
  }
}

export function loadExampleSettings(
  storage: ExampleSettingsStorage | undefined = resolveLocalStorage(),
): ExampleSettings {
  const defaults = createDefaultExampleSettings()
  if (storage === undefined) {
    return defaults
  }

  try {
    const serialized = storage.getItem(settingsStorageKey)
    if (serialized === null) {
      return defaults
    }
    const value: unknown = JSON.parse(serialized)
    return parseStoredSettings(value, defaults)
  } catch {
    return defaults
  }
}

export function saveExampleSettings(
  settings: ExampleSettings,
  storage: ExampleSettingsStorage | undefined = resolveLocalStorage(),
): void {
  if (storage === undefined) {
    return
  }

  const stored: StoredExampleSettings = { ...settings, version: settingsVersion }
  try {
    storage.setItem(settingsStorageKey, JSON.stringify(stored))
  } catch {
    // 浏览器禁用存储或容量不足时不影响示例运行
  }
}

function parseStoredSettings(value: unknown, defaults: ExampleSettings): ExampleSettings {
  if (!isRecord(value) || value.version !== settingsVersion) {
    return defaults
  }

  const inputs = isRecord(value.loaderParameterInputs) ? value.loaderParameterInputs : {}
  return {
    loaderMode:
      value.loaderMode === 'native' || value.loaderMode === 'parallel'
        ? value.loaderMode
        : defaults.loaderMode,
    loaderParameterInputs: {
      chunkSizeMiB: stringOrDefault(
        inputs.chunkSizeMiB,
        defaults.loaderParameterInputs.chunkSizeMiB,
      ),
      maxConcurrency: stringOrDefault(
        inputs.maxConcurrency,
        defaults.loaderParameterInputs.maxConcurrency,
      ),
      rescueEnabled:
        typeof inputs.rescueEnabled === 'boolean'
          ? inputs.rescueEnabled
          : defaults.loaderParameterInputs.rescueEnabled,
      rescueMaxAttempts: stringOrDefault(
        inputs.rescueMaxAttempts,
        defaults.loaderParameterInputs.rescueMaxAttempts,
      ),
      slowRateThresholdPercent: stringOrDefault(
        inputs.slowRateThresholdPercent,
        defaults.loaderParameterInputs.slowRateThresholdPercent,
      ),
      stallTimeoutMs: stringOrDefault(
        inputs.stallTimeoutMs,
        defaults.loaderParameterInputs.stallTimeoutMs,
      ),
      windowSize: stringOrDefault(inputs.windowSize, defaults.loaderParameterInputs.windowSize),
    },
    progressiveEnabled:
      typeof value.progressiveEnabled === 'boolean'
        ? value.progressiveEnabled
        : defaults.progressiveEnabled,
    source: stringOrDefault(value.source, defaults.source),
    transportMode:
      value.transportMode === 'fetch' || value.transportMode === 'websocket'
        ? value.transportMode
        : defaults.transportMode,
    workerUrl: stringOrDefault(value.workerUrl, defaults.workerUrl),
  }
}

function resolveLocalStorage(): ExampleSettingsStorage | undefined {
  try {
    return globalThis.localStorage
  } catch {
    return undefined
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function stringOrDefault(value: unknown, fallback: string): string {
  return typeof value === 'string' ? value : fallback
}
