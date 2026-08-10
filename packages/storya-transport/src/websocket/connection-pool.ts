import {
  createAbortError,
  HttpTransportFailure,
  type HttpTransportResponse,
} from '../http-transport'
import { WebSocketChannel } from './channel'
import type {
  PendingWebSocketRequest,
  ResolvedWebSocketHttpTransportOptions,
  WebSocketHttpTransportDebugEvent,
  WebSocketHttpTransportDebugEventType,
} from './types'
export class WebSocketConnectionPool {
  private readonly channels = new Set<WebSocketChannel>()
  private connectingCount = 0
  private destroyed = false
  private draining = false
  private readonly idleChannels = new IdleChannelList()
  private idleTimer: number | undefined
  private nextChannelId = 0
  private readonly pending = new Set<PendingWebSocketRequest>()

  constructor(
    private readonly url: string,
    private readonly options: ResolvedWebSocketHttpTransportOptions,
  ) {}

  request(request: Request, maxResponseBytes: number): Promise<HttpTransportResponse> {
    if (this.destroyed) {
      return Promise.reject(new HttpTransportFailure('destroyed', 'WebSocket transport 已经销毁'))
    }
    return new Promise<HttpTransportResponse>((resolve, reject) => {
      const pending: PendingWebSocketRequest = {
        abortListener: () => {
          if (this.pending.delete(pending)) {
            reject(createAbortError())
          }
        },
        maxResponseBytes,
        reject,
        request,
        resolve,
      }
      request.signal.addEventListener('abort', pending.abortListener, { once: true })
      this.pending.add(pending)
      this.drain()
    })
  }

  destroy(error: HttpTransportFailure): void {
    if (this.destroyed) {
      return
    }
    this.destroyed = true
    this.clearIdleTimer()
    for (const pending of this.pending.values()) {
      pending.request.signal.removeEventListener('abort', pending.abortListener)
      pending.reject(error)
    }
    this.pending.clear()
    for (const channel of [...this.channels]) {
      channel.destroy(error)
    }
    this.channels.clear()
    this.idleChannels.clear()
  }

  private createChannel(): void {
    const connectionId = ++this.nextChannelId
    this.connectingCount += 1
    let channel: WebSocketChannel
    try {
      channel = new WebSocketChannel(connectionId, this.url, this.options, {
        onClosed: (closed, error, opened, details) => {
          this.channels.delete(closed)
          this.idleChannels.delete(closed)
          if (!opened) {
            this.connectingCount = Math.max(0, this.connectingCount - 1)
          }
          this.emitDebug(closed, 'connection-closed', {
            ...details,
            ...(details.code === 1000 ? {} : { error: error.message }),
          })
          if (!opened) {
            this.rejectNextPending(error)
          }
          this.drain()
        },
        onIdle: idle => {
          this.idleChannels.addNewest(idle)
          this.drain()
        },
        onOpen: opened => {
          this.connectingCount = Math.max(0, this.connectingCount - 1)
          this.idleChannels.addNewest(opened)
          this.emitDebug(opened, 'connection-opened')
          this.drain()
        },
      })
    } catch (cause) {
      this.connectingCount = Math.max(0, this.connectingCount - 1)
      this.rejectNextPending(
        new HttpTransportFailure('connection-failed', 'WebSocket 连接创建失败', { cause }),
      )
      return
    }
    this.channels.add(channel)
    this.emitDebug(channel, 'connection-created', { reason: 'pending-request' })
  }

  private drain(): void {
    if (this.destroyed || this.draining) {
      return
    }
    this.draining = true

    try {
      let channel = this.idleChannels.takeNewest()
      while (channel !== undefined) {
        const pending = this.takePending()
        if (pending === undefined) {
          this.idleChannels.addNewest(channel)
          break
        }
        channel.execute(pending)
        channel = this.idleChannels.takeNewest()
      }

      while (
        this.pending.size > this.connectingCount &&
        this.channels.size < this.options.maxConnections
      ) {
        this.createChannel()
      }
    } finally {
      this.draining = false
      this.armIdleTimer()
    }
  }

  private takePending(): PendingWebSocketRequest | undefined {
    const entry = this.pending.values().next()
    if (entry.done) {
      return undefined
    }
    const pending = entry.value
    this.pending.delete(pending)
    pending.request.signal.removeEventListener('abort', pending.abortListener)
    return pending
  }

  private rejectNextPending(error: HttpTransportFailure): void {
    const pending = this.takePending()
    pending?.reject(error)
  }

  private reclaimIdleConnections(): void {
    this.idleTimer = undefined
    if (this.destroyed) {
      return
    }
    const now = performance.now()
    while (this.idleChannels.size > this.options.retainedIdleConnections) {
      const oldest = this.idleChannels.peekOldest()
      if (
        oldest === undefined ||
        now - oldest.getIdleSince() < this.options.idleConnectionTimeoutMs
      ) {
        break
      }
      this.idleChannels.delete(oldest)
      oldest.retire('idle')
    }
    this.armIdleTimer()
  }

  private armIdleTimer(): void {
    this.clearIdleTimer()
    if (this.idleChannels.size <= this.options.retainedIdleConnections) {
      return
    }
    const oldest = this.idleChannels.peekOldest()
    if (oldest === undefined) {
      return
    }
    const remainingMs = Math.max(
      0,
      oldest.getIdleSince() + this.options.idleConnectionTimeoutMs - performance.now(),
    )
    this.idleTimer = globalThis.setTimeout(() => this.reclaimIdleConnections(), remainingMs)
  }

  private clearIdleTimer(): void {
    if (this.idleTimer !== undefined) {
      globalThis.clearTimeout(this.idleTimer)
      this.idleTimer = undefined
    }
  }

  private emitDebug(
    channel: WebSocketChannel,
    type: WebSocketHttpTransportDebugEventType,
    details: Partial<
      Pick<WebSocketHttpTransportDebugEvent, 'code' | 'error' | 'initiator' | 'reason' | 'wasClean'>
    > = {},
  ): void {
    const logger = this.options.debugLogger
    if (logger === undefined) {
      return
    }
    try {
      logger({
        ageMs: channel.getAgeMs(),
        connectionId: channel.getId(),
        pendingRequestCount: this.pending.size,
        poolSize: this.channels.size,
        requestCount: channel.getRequestCount(),
        state: channel.getState(),
        timestamp: Date.now(),
        type,
        ...details,
      })
    } catch {
      // 调试日志不能影响 Transport 请求
    }
  }
}

interface IdleChannelNode {
  readonly channel: WebSocketChannel
  next: IdleChannelNode | undefined
  previous: IdleChannelNode | undefined
}

class IdleChannelList {
  private readonly nodes = new Map<WebSocketChannel, IdleChannelNode>()
  private newest: IdleChannelNode | undefined
  private oldest: IdleChannelNode | undefined

  get size(): number {
    return this.nodes.size
  }

  addNewest(channel: WebSocketChannel): void {
    this.delete(channel)
    const node: IdleChannelNode = {
      channel,
      next: undefined,
      previous: this.newest,
    }
    if (this.newest === undefined) {
      this.oldest = node
    } else {
      this.newest.next = node
    }
    this.newest = node
    this.nodes.set(channel, node)
  }

  clear(): void {
    this.nodes.clear()
    this.newest = undefined
    this.oldest = undefined
  }

  delete(channel: WebSocketChannel): boolean {
    const node = this.nodes.get(channel)
    if (node === undefined) {
      return false
    }
    this.nodes.delete(channel)
    if (node.previous === undefined) {
      this.oldest = node.next
    } else {
      node.previous.next = node.next
    }
    if (node.next === undefined) {
      this.newest = node.previous
    } else {
      node.next.previous = node.previous
    }
    return true
  }

  peekOldest(): WebSocketChannel | undefined {
    return this.oldest?.channel
  }

  takeNewest(): WebSocketChannel | undefined {
    const channel = this.newest?.channel
    if (channel !== undefined) {
      this.delete(channel)
    }
    return channel
  }
}
