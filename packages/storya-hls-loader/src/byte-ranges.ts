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
    return [{ endExclusive, start }]
  }

  const fullChunks = Math.floor(length / chunkSize)
  const tail = length % chunkSize
  const ranges: ByteRange[] = []
  let cursor = start

  for (let index = 0; index < fullChunks; index += 1) {
    const lastFullChunk = index === fullChunks - 1
    const mergeTail = lastFullChunk && tail > 0 && tail < chunkSize / 2
    const rangeEnd = mergeTail ? endExclusive : cursor + chunkSize
    ranges.push({ endExclusive: rangeEnd, start: cursor })
    cursor = rangeEnd
  }

  if (cursor < endExclusive) {
    ranges.push({ endExclusive, start: cursor })
  }
  return ranges
}
