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

const requestedUrls: string[] = []
const requestedRanges: string[] = []
const transport = new ProxyHttpTransport(
  ['https://proxy-a.example.com/path', 'https://proxy-b.example.com'],
  async request => {
    requestedUrls.push(request.url)
    requestedRanges.push(request.headers.get('range') ?? '')
    const responseTarget = request.url.includes('proxy-a')
      ? 'https://cdn.example.com/final-a.bin'
      : 'https://cdn.example.com/final-b.bin'
    const response = new Response('proxy', {
      headers: { 'content-range': 'bytes 0-4/5' },
      status: 206,
    })
    Object.defineProperty(response, 'url', {
      value: createProxyUrl(new URL(request.url).origin, responseTarget),
    })
    return response
  },
)

const first = await transport.request(new Request(targetUrl, { headers: { Range: 'bytes=0-4' } }))
const second = await transport.request(new Request('https://media.example.com/second.bin'))
assert(
  new URL(requestedUrls[0] ?? '').origin === 'https://proxy-a.example.com',
  '没有使用首个 Proxy',
)
assert(new URL(requestedUrls[1] ?? '').origin === 'https://proxy-b.example.com', '没有轮换 Proxy')
assert(requestedRanges[0] === 'bytes=0-4', 'Proxy transport 丢失 Range header')
assert(first.url === 'https://cdn.example.com/final-a.bin', '没有还原最终上游 URL')
assert(second.url === 'https://cdn.example.com/final-b.bin', '轮换请求的最终 URL 错误')
assert(new TextDecoder().decode(await first.arrayBuffer()) === 'proxy', 'Proxy 响应 body 错误')

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
