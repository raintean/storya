import { TransportFrameKind } from './generated/transport/http_pb.js'

export interface TransportFrame {
  kind: TransportFrameKind
  payload: Uint8Array
  sequence: number
}

export const TRANSPORT_FRAME_HEADER_SIZE = 5

export function encodeTransportFrame(
  kind: TransportFrameKind,
  sequence: number,
  payload: Uint8Array = new Uint8Array(),
): Uint8Array<ArrayBuffer> {
  if (kind === TransportFrameKind.UNSPECIFIED) {
    throw new Error('Transport frame kind 不能为 UNSPECIFIED')
  }
  if (!Number.isSafeInteger(sequence) || sequence < 0 || sequence > 0xffff_ffff) {
    throw new Error(`无效的 Transport frame sequence: ${sequence}`)
  }

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
  if (kind === TransportFrameKind.UNSPECIFIED || kind > TransportFrameKind.ERROR) {
    throw new Error(`未知的 Transport frame kind: ${kind}`)
  }

  return {
    kind,
    payload: bytes.slice(TRANSPORT_FRAME_HEADER_SIZE),
    sequence: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(1),
  }
}

function toUint8Array(data: ArrayBuffer | ArrayBufferView): Uint8Array {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data)
  }
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}
