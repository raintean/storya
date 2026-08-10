import { TransportFrameKind } from './generated/transport/http_pb.js'

export interface TransportFrame {
  kind: TransportFrameKind
  payload: Uint8Array
}

export const HTTP_RELAY_MAX_RESPONSE_BODY_BYTES = 32 * 1024 * 1024
export const TRANSPORT_FRAME_HEADER_SIZE = 1

export function encodeTransportFrame(
  kind: TransportFrameKind,
  payload?: Uint8Array,
): Uint8Array<ArrayBuffer> {
  assertFrameKind(kind)

  const frame = new Uint8Array(TRANSPORT_FRAME_HEADER_SIZE + (payload?.byteLength ?? 0))
  frame[0] = kind
  if (payload !== undefined) {
    frame.set(payload, TRANSPORT_FRAME_HEADER_SIZE)
  }
  return frame
}

export function decodeTransportFrame(data: ArrayBuffer | ArrayBufferView): TransportFrame {
  const bytes = toUint8Array(data)
  if (bytes.byteLength < TRANSPORT_FRAME_HEADER_SIZE) {
    throw new Error('Transport frame 长度不足')
  }

  const kind = bytes[0] as TransportFrameKind
  assertFrameKind(kind)
  return {
    kind,
    payload: bytes.subarray(TRANSPORT_FRAME_HEADER_SIZE),
  }
}

function assertFrameKind(kind: TransportFrameKind): void {
  if (
    kind !== TransportFrameKind.REQUEST_HEAD &&
    kind !== TransportFrameKind.RESPONSE_HEAD &&
    kind !== TransportFrameKind.RESPONSE_BODY &&
    kind !== TransportFrameKind.RESPONSE_END &&
    kind !== TransportFrameKind.CANCEL &&
    kind !== TransportFrameKind.CANCELED &&
    kind !== TransportFrameKind.ERROR
  ) {
    throw new Error(`未知的 Transport frame kind: ${kind}`)
  }
}

function toUint8Array(data: ArrayBuffer | ArrayBufferView): Uint8Array {
  return data instanceof ArrayBuffer
    ? new Uint8Array(data)
    : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}
