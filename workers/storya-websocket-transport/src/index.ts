import { createWebSocketRelayResponse } from './websocket-relay-session'

function json(body: unknown, init?: ResponseInit): Response {
  const headers = new Headers(init?.headers)
  headers.set('content-type', 'application/json; charset=utf-8')
  return new Response(JSON.stringify(body), { ...init, headers })
}

export default {
  fetch(request, _env, ctx): Response {
    const url = new URL(request.url)
    if (url.pathname === '/health') {
      return json({ service: 'storya-websocket-transport', status: 'ok' })
    }
    if (url.pathname === '/transport') {
      if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
        return json(
          { error: 'upgrade_required', message: 'Transport endpoint requires WebSocket.' },
          { status: 426 },
        )
      }
      return createWebSocketRelayResponse(request, ctx)
    }
    return json({ error: 'not_found', message: 'Edge capability was not found.' }, { status: 404 })
  },
} satisfies ExportedHandler<Env>
