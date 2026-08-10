import type { HttpTransportResponse } from '../http-transport'

interface WebSocketEventMapLike {
  close: CloseEvent
  error: Event
  message: MessageEvent<unknown>
  open: Event
}

export interface WebSocketLike {
  binaryType: BinaryType
  readonly readyState: number

  addEventListener<K extends keyof WebSocketEventMapLike>(
    type: K,
    listener: (event: WebSocketEventMapLike[K]) => void,
  ): void
  close(code?: number, reason?: string): void
  removeEventListener<K extends keyof WebSocketEventMapLike>(
    type: K,
    listener: (event: WebSocketEventMapLike[K]) => void,
  ): void
  send(data: string | Blob | ArrayBuffer | ArrayBufferView<ArrayBuffer>): void
}

export type WebSocketFactory = (url: string) => WebSocketLike

export type WebSocketHttpTransportDebugEventType =
  | 'connection-created'
  | 'connection-opened'
  | 'connection-closed'

export interface WebSocketHttpTransportDebugEvent {
  ageMs: number
  code?: number
  connectionId: number
  error?: string
  initiator?: 'local' | 'remote'
  pendingRequestCount: number
  poolSize: number
  reason?: string
  requestCount: number
  state: WebSocketChannelState
  timestamp: number
  type: WebSocketHttpTransportDebugEventType
  wasClean?: boolean
}

export type WebSocketHttpTransportDebugLogger = (event: WebSocketHttpTransportDebugEvent) => void

export interface WebSocketHttpTransportOptions {
  cancelTimeoutMs: number
  connectTimeoutMs: number
  defaultMaxResponseBytes: number
  debug?: boolean | WebSocketHttpTransportDebugLogger
  idleConnectionTimeoutMs: number
  maxConnections: number
  maxRequestsPerConnection: number
  retainedIdleConnections: number
  webSocketFactory?: WebSocketFactory
}

export interface ResolvedWebSocketHttpTransportOptions extends Omit<
  WebSocketHttpTransportOptions,
  'debug' | 'webSocketFactory'
> {
  debugLogger: WebSocketHttpTransportDebugLogger | undefined
  webSocketFactory: WebSocketFactory
}

export interface PendingWebSocketRequest {
  abortListener: () => void
  maxResponseBytes: number
  reject: (reason: unknown) => void
  request: Request
  resolve: (response: HttpTransportResponse) => void
}

export type WebSocketChannelState = 'busy' | 'canceling' | 'closed' | 'connecting' | 'idle'
export type WebSocketChannelRetirementReason = 'idle' | 'max-requests'

export interface WebSocketChannelCloseDetails {
  code: number
  initiator: 'local' | 'remote'
  reason: string
  wasClean?: boolean
}
