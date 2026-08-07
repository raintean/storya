import { TransportFrameKind } from './generated/transport/http_pb.js'

export interface TransportFrame {
  kind: TransportFrameKind
  payload: Uint8Array
  sequence: number
}

export const HTTP_RELAY_MAX_RESPONSE_BODY_BYTES = 32 * 1024 * 1024
export const TRANSPORT_FRAME_HEADER_SIZE = 5

export function encodeTransportFrame(
  kind: TransportFrameKind,
  sequence: number,
  payload: Uint8Array = new Uint8Array(),
): Uint8Array<ArrayBuffer> {
  assertFrameKind(kind)
  assertSequence(sequence)

  const frame = new Uint8Array(TRANSPORT_FRAME_HEADER_SIZE + payload.byteLength)
  frame[0] = kind
  new DataView(frame.buffer).setUint32(1, sequence)
  frame.set(payload, TRANSPORT_FRAME_HEADER_SIZE)
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
    sequence: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(1),
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

function assertSequence(sequence: number): void {
  if (!Number.isSafeInteger(sequence) || sequence < 0 || sequence > 0xffff_ffff) {
    throw new Error(`无效的 Transport frame sequence: ${sequence}`)
  }
}

function toUint8Array(data: ArrayBuffer | ArrayBufferView): Uint8Array {
  return data instanceof ArrayBuffer
    ? new Uint8Array(data)
    : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}
