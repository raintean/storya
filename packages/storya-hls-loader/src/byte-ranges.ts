export interface ByteRange {
  endExclusive: number
  start: number
}

export function splitByteRanges(
  start: number,
  endExclusive: number,
  chunkSize: number,
): ByteRange[] {
  const length = endExclusive - start
  if (length <= 0) {
    return []
  }
  if (length <= chunkSize) {
    return [{ start, endExclusive }]
  }

  const fullChunks = Math.floor(length / chunkSize)
  const tail = length % chunkSize
  const ranges: ByteRange[] = []
  let cursor = start

  for (let index = 0; index < fullChunks; index += 1) {
    const isLastFullChunk = index === fullChunks - 1
    const mergeTail = isLastFullChunk && tail > 0 && tail < chunkSize / 2
    const rangeEnd = mergeTail ? endExclusive : cursor + chunkSize
    ranges.push({ start: cursor, endExclusive: rangeEnd })
    cursor = rangeEnd
  }

  if (cursor < endExclusive) {
    ranges.push({ start: cursor, endExclusive })
  }
  return ranges
}
