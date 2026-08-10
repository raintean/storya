import {
  createDefaultExampleSettings,
  loadExampleSettings,
  saveExampleSettings,
} from './example-settings'
import type { ExampleSettingsStorage } from './example-settings'

class MemoryStorage implements ExampleSettingsStorage {
  value: string | null = null

  getItem(): string | null {
    return this.value
  }

  setItem(_key: string, value: string): void {
    this.value = value
  }
}

function testDefaultsDoNotContainSource(): void {
  const settings = createDefaultExampleSettings()
  assert(settings.source === '', '首次访问不应内置 HLS 播放地址')
  assert(settings.loaderMode === 'parallel', '默认 Loader 模式错误')
  assert(settings.transportMode === 'fetch', '默认 Transport 模式错误')
}

function testSettingsRoundTrip(): void {
  const storage = new MemoryStorage()
  const settings = createDefaultExampleSettings()
  settings.source = 'https://media.example.com/master.m3u8'
  settings.workerUrl = 'https://ws-tx.example.com'
  settings.transportMode = 'websocket'
  settings.loaderParameterInputs.windowSize = '9'
  settings.loaderParameterInputs.rescueEnabled = false

  saveExampleSettings(settings, storage)
  const restored = loadExampleSettings(storage)
  assert(JSON.stringify(restored) === JSON.stringify(settings), '保存后的配置没有完整恢复')
}

function testWorkerUrlSurvivesTransportSwitch(): void {
  const storage = new MemoryStorage()
  const settings = createDefaultExampleSettings()
  settings.workerUrl = 'https://ws-tx.example.com'
  settings.transportMode = 'fetch'

  saveExampleSettings(settings, storage)
  const restored = loadExampleSettings(storage)
  assert(restored.transportMode === 'fetch', 'Transport 模式没有保存')
  assert(restored.workerUrl === settings.workerUrl, '切换到 Fetch 后丢失了 Worker URL')
}

function testInvalidSettingsFallBackByField(): void {
  const storage = new MemoryStorage()
  storage.value = JSON.stringify({
    loaderMode: 'invalid',
    loaderParameterInputs: {
      rescueEnabled: false,
      windowSize: '7',
    },
    source: 'https://media.example.com/playlist.m3u8',
    transportMode: 'websocket',
    version: 1,
    workerUrl: 12,
  })

  const defaults = createDefaultExampleSettings()
  const restored = loadExampleSettings(storage)
  assert(restored.loaderMode === defaults.loaderMode, '无效 Loader 模式没有回退默认值')
  assert(restored.transportMode === 'websocket', '有效 Transport 模式没有恢复')
  assert(restored.loaderParameterInputs.windowSize === '7', '有效窗口参数没有恢复')
  assert(!restored.loaderParameterInputs.rescueEnabled, '有效 Rescue 开关没有恢复')
  assert(
    restored.loaderParameterInputs.chunkSizeMiB === defaults.loaderParameterInputs.chunkSizeMiB,
    '缺失参数没有回退默认值',
  )
  assert(restored.workerUrl === '', '无效 Worker URL 类型没有回退默认值')
}

function testStorageFailuresDoNotBreakSettings(): void {
  const brokenStorage: ExampleSettingsStorage = {
    getItem() {
      throw new Error('读取失败')
    },
    setItem() {
      throw new Error('写入失败')
    },
  }

  const settings = loadExampleSettings(brokenStorage)
  assert(settings.source === '', '存储读取失败时没有回退默认配置')
  saveExampleSettings(settings, brokenStorage)
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

testDefaultsDoNotContainSource()
testSettingsRoundTrip()
testWorkerUrlSurvivesTransportSwitch()
testInvalidSettingsFallBackByField()
testStorageFailuresDoNotBreakSettings()
