function json(body: unknown, init?: ResponseInit): Response {
  const headers = new Headers(init?.headers)
  headers.set('content-type', 'application/json; charset=utf-8')

  return new Response(JSON.stringify(body), { ...init, headers })
}

export default {
  fetch(request): Response {
    const url = new URL(request.url)

    if (url.pathname === '/health') {
      return json({ service: 'storya-playback-relay', status: 'ok' })
    }

    return json(
      {
        error: 'not_implemented',
        message: 'Playback relay routing has not been configured yet.',
      },
      { status: 501 },
    )
  },
} satisfies ExportedHandler<Env>
