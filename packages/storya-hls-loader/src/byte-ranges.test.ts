import { splitByteRanges } from './byte-ranges.ts'

interface TestCase {
  expected: Array<[number, number]>
  length: number
  name: string
  start?: number
}

const chunkSize = 1024
const cases: TestCase[] = [
  {
    name: '空范围',
    length: 0,
    expected: [],
  },
  {
    name: '单个完整 Chunk',
    length: chunkSize,
    expected: [[0, chunkSize]],
  },
  {
    name: '小尾巴合并到前一个 Chunk',
    length: chunkSize * 2 + 256,
    expected: [
      [0, chunkSize],
      [chunkSize, chunkSize * 2 + 256],
    ],
  },
  {
    name: '半个 Chunk 的尾巴独立请求',
    length: chunkSize * 2 + chunkSize / 2,
    expected: [
      [0, chunkSize],
      [chunkSize, chunkSize * 2],
      [chunkSize * 2, chunkSize * 2 + chunkSize / 2],
    ],
  },
  {
    name: '大尾巴独立请求',
    length: chunkSize * 2 + 768,
    expected: [
      [0, chunkSize],
      [chunkSize, chunkSize * 2],
      [chunkSize * 2, chunkSize * 2 + 768],
    ],
  },
  {
    name: '保留 Segment 局部起点',
    start: chunkSize,
    length: chunkSize * 2 + 256,
    expected: [
      [chunkSize, chunkSize * 2],
      [chunkSize * 2, chunkSize * 3 + 256],
    ],
  },
]

for (const testCase of cases) {
  const start = testCase.start ?? 0
  const actual = splitByteRanges(start, start + testCase.length, chunkSize).map(
    range => [range.start, range.endExclusive] as [number, number],
  )
  if (JSON.stringify(actual) !== JSON.stringify(testCase.expected)) {
    throw new Error(
      `${testCase.name}失败, 期望 ${JSON.stringify(testCase.expected)}, 实际 ${JSON.stringify(actual)}`,
    )
  }
}
