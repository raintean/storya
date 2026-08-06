import {
  createProxyUrl,
  decodeProxyTargetUrl,
  encodeProxyTargetUrl,
  HttpTransportFailure,
  ProxyHttpTransport,
} from './index.ts'

const targetUrl = 'https://media.example.com/视频/segment.ts?token=a/b+c=='
const encodedTarget = encodeProxyTargetUrl(targetUrl)
assert(!encodedTarget.includes('='), 'Base64URL 不应包含 padding')
assert(!encodedTarget.includes('/'), 'Base64URL 不应包含路径分隔符')
assert(decodeProxyTargetUrl(encodedTarget) === targetUrl, 'Base64URL 编解码没有保持 URL')
const headProxyUrl = createProxyUrl('https://proxy.example.com', targetUrl, undefined, 'HEAD')
const fullProxyUrl = createProxyUrl('https://proxy.example.com', targetUrl)
assert(headProxyUrl !== fullProxyUrl, 'HEAD 和完整 GET 不应共享同一个 Proxy URL')
assert(
  decodeProxyTargetUrl(
    new URL(headProxyUrl).pathname.replace(/^\/proxy\//u, '').replace(/\.jpg$/u, ''),
  ) === targetUrl,
  'HEAD descriptor 没有保持 target URL',
)

const origins = Array.from({ length: 6 }, (_, index) => `https://proxy-${index}.example.com`)
const requestedMethods: string[] = []
const requestedRanges: string[] = []
const requestedUrls: string[] = []
const transport = new ProxyHttpTransport(origins, async request => {
  requestedMethods.push(request.method)
  requestedRanges.push(request.headers.get('range') ?? '')
  requestedUrls.push(request.url)
  const responseTarget = 'https://cdn.example.com/final.bin'
  const requestTarget = decodeProxyTargetUrl(
    new URL(request.url).pathname.replace(/^\/proxy\//u, '').replace(/\.jpg$/u, ''),
  )
  const isHeadRequest =
    request.url ===
    createProxyUrl(new URL(request.url).origin, new Request(targetUrl).url, undefined, 'HEAD')
  const isRangeRequest =
    requestTarget === new Request(targetUrl).url && request.method === 'GET' && !isHeadRequest
  const response = new Response(isRangeRequest ? 'proxy' : null, {
    headers: isHeadRequest
      ? {
          'x-storya-proxy-content-length': '5',
          'x-storya-proxy-status': '200',
        }
      : isRangeRequest
        ? {
            'content-length': '5',
            'content-type': 'image/jpeg',
            'x-storya-proxy-content-range': 'bytes 0-4/5',
            'x-storya-proxy-content-type': 'video/mp2t',
            'x-storya-proxy-status': '206',
          }
        : { 'content-length': '5' },
    status: 200,
  })
  Object.defineProperty(response, 'url', {
    value: createProxyUrl(
      new URL(request.url).origin,
      responseTarget,
      isRangeRequest ? { endInclusive: 4, start: 0 } : undefined,
    ),
  })
  return response
})

const first = await transport.request(new Request(targetUrl, { headers: { Range: 'bytes=0-4' } }))
assert(requestedRanges[0] === '', 'Proxy transport 没有从 CDN 请求中移除 Range header')
assert(first.status === 206, 'Proxy transport 没有还原原始 206 状态')
assert(first.statusText === 'Partial Content', 'Proxy transport 没有还原 206 status text')
assert(first.headers.get('content-range') === 'bytes 0-4/5', 'Proxy transport 丢失 Content-Range')
assert(
  first.headers.get('content-type') === 'video/mp2t',
  'Proxy transport 没有还原原始 Content-Type',
)
assert(
  first.headers.get('x-storya-proxy-content-type') === null,
  'Proxy 内部 Content-Type header 泄漏给上层',
)
assert(first.headers.get('x-storya-proxy-status') === null, 'Proxy 内部状态 header 泄漏给上层')
assert(first.url === 'https://cdn.example.com/final.bin', '没有还原最终上游 URL')
assert(new TextDecoder().decode(await first.arrayBuffer()) === 'proxy', 'Proxy 响应 body 错误')

await transport.request(new Request(targetUrl, { headers: { Range: 'bytes=0-4' } }))
assert(
  new URL(requestedUrls[0] ?? '').origin === new URL(requestedUrls[1] ?? '').origin,
  '相同 Chunk 没有稳定映射到同一 Proxy Origin',
)

for (let index = 1; index < origins.length; index += 1) {
  const start = index * 2 * 1024 * 1024
  await transport.request(
    new Request(targetUrl, {
      headers: { Range: `bytes=${start}-${start + 2 * 1024 * 1024 - 1}` },
    }),
  )
}
const chunkOrigins = new Set(
  [requestedUrls[0], ...requestedUrls.slice(2)].map(url => new URL(url ?? '').origin),
)
assert(chunkOrigins.size === origins.length, '连续 Chunk 没有轮转使用所有 Proxy Origin')

const headResponse = await transport.request(new Request(targetUrl, { method: 'HEAD' }))
assert(requestedMethods.at(-1) === 'GET', 'Proxy transport 没有把逻辑 HEAD 转换为物理 GET')
assert(requestedRanges.at(-1) === '', 'HEAD 请求不应携带 Range')
assert(headResponse.status === 200, 'Proxy transport 没有还原 HEAD 状态码')
assert(headResponse.headers.get('content-length') === '5', 'Proxy transport 没有还原 HEAD 长度')

const ignoredRangeTransport = new ProxyHttpTransport([origins[0] ?? ''], async request => {
  assert(request.headers.get('range') === null, '忽略 Range 测试仍向 CDN 发送了 Range header')
  return new Response('full response', {
    headers: {
      'content-length': '13',
      'x-storya-proxy-status': '200',
    },
    status: 200,
  })
})
const ignoredRangeResponse = await ignoredRangeTransport.request(
  new Request(targetUrl, { headers: { Range: 'bytes=0-4' } }),
)
assert(ignoredRangeResponse.status === 200, 'Proxy transport 没有还原源站忽略 Range 的 200')
ignoredRangeTransport.destroy()

const missingMetadataTransport = new ProxyHttpTransport([origins[0] ?? ''], async () =>
  Promise.resolve(new Response('chunk', { status: 200 })),
)
let missingMetadataRejected = false
try {
  await missingMetadataTransport.request(
    new Request(targetUrl, { headers: { Range: 'bytes=0-4' } }),
  )
} catch (error) {
  missingMetadataRejected =
    error instanceof HttpTransportFailure && error.code === 'invalid-response'
}
assert(missingMetadataRejected, 'Proxy transport 没有拒绝缺少内部状态的 Range 响应')
missingMetadataTransport.destroy()

let invalidRangeRejected = false
try {
  await transport.request(new Request(targetUrl, { headers: { Range: 'bytes=0-' } }))
} catch (error) {
  invalidRangeRejected = error instanceof HttpTransportFailure && error.code === 'protocol-error'
}
assert(invalidRangeRejected, 'Proxy transport 没有拒绝非闭区间 Range')

let invalidMethodRejected = false
try {
  await transport.request(new Request(targetUrl, { method: 'POST' }))
} catch (error) {
  invalidMethodRejected = error instanceof HttpTransportFailure && error.code === 'protocol-error'
}
assert(invalidMethodRejected, 'Proxy transport 没有拒绝 POST')

transport.destroy()
let destroyedRejected = false
try {
  await transport.request(new Request(targetUrl))
} catch (error) {
  destroyedRejected = error instanceof HttpTransportFailure && error.code === 'destroyed'
}
assert(destroyedRejected, '销毁后的 Proxy transport 仍然接受请求')

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}
