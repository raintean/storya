import type { FragmentLoaderContext, HlsConfig, LoaderConfiguration, LoaderResponse } from 'hls.js'
import { createHlsParallelLoader } from './index.ts'

const payload = new Uint8Array([1, 2, 3, 4])
const originalFetch = globalThis.fetch
globalThis.fetch = async () =>
  new Response(payload.slice().buffer, {
    headers: { 'content-length': String(payload.byteLength) },
    status: 200,
  })

try {
  const parallel = createHlsParallelLoader()
  const LoaderConstructor = parallel.fragmentLoader
  const loader = new LoaderConstructor({} as HlsConfig)
  const progressPayloads: Uint8Array[] = []
  const response = await new Promise<LoaderResponse>((resolve, reject) => {
    loader.load(createInitSegmentContext(), createLoaderConfig(), {
      onError: error => reject(new Error(error.text)),
      onProgress: (_stats, _context, data) => progressPayloads.push(toUint8Array(data)),
      onSuccess: resolve,
      onTimeout: () => reject(new Error('Init Segment 加载超时')),
    })
  })

  assertBytes(progressPayloads, 'onProgress')
  if (!(response.data instanceof ArrayBuffer)) {
    throw new Error('onSuccess 没有收到 ArrayBuffer')
  }
  assertBytes([new Uint8Array(response.data)], 'onSuccess')
  parallel.destroy()
} finally {
  globalThis.fetch = originalFetch
}

function createInitSegmentContext(): FragmentLoaderContext {
  return {
    frag: {
      duration: 0,
      sn: 'initSegment',
      start: 0,
      type: 'main',
    },
    headers: {},
    part: null,
    rangeEnd: 0,
    rangeStart: 0,
    responseType: 'arraybuffer',
    url: 'https://example.com/init.mp4',
  } as FragmentLoaderContext
}

function createLoaderConfig(): LoaderConfiguration {
  return {
    highWaterMark: Number.POSITIVE_INFINITY,
    loadPolicy: {
      errorRetry: null,
      maxLoadTimeMs: 10_000,
      maxTimeToFirstByteMs: 5_000,
      timeoutRetry: null,
    },
    maxRetry: 0,
    maxRetryDelay: 0,
    retryDelay: 0,
    timeout: 10_000,
  } as LoaderConfiguration
}

function toUint8Array(data: string | ArrayBuffer | ArrayBufferView): Uint8Array {
  if (typeof data === 'string') {
    throw new Error('onProgress 返回了字符串数据')
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data)
  }
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}

function assertBytes(actualPayloads: Uint8Array[], callbackName: string): void {
  if (actualPayloads.length !== 1) {
    throw new Error(`${callbackName} 应当调用一次, 实际 ${actualPayloads.length} 次`)
  }
  const actual = actualPayloads[0]
  if (
    actual === undefined ||
    actual.byteLength !== payload.byteLength ||
    actual.some((value, index) => value !== payload[index])
  ) {
    throw new Error(`${callbackName} 没有收到完整 Init Segment`)
  }
}
