/**
 * @module @ddes/gcp-store
 */

/**
 * @hidden
 */
export default function* chronologicalPartitionIterator(params: {
  group?: string
  start: Date
  end?: Date
  descending?: boolean
}): IterableIterator<{key: string; startsAt: Date; endsAt: Date}> {
  const {start, end = new Date(), group = 'default', descending} = params
  let partitionCursor = new Date(descending ? end : start)
  partitionCursor.setUTCHours(0, 0, 0, 0)

  while (descending ? partitionCursor >= start : partitionCursor <= end) {
    const endsAt = new Date(partitionCursor)
    endsAt.setUTCHours(23, 59, 59, 999)

    yield {
      key: `${partitionCursor
        .toISOString()
        .split('T')[0]
        .replace(/\-/g, '')}${group}`,
      startsAt: partitionCursor,
      endsAt,
    }
    partitionCursor = new Date(
      partitionCursor.valueOf() + 24 * 60 * 60 * 1000 * (descending ? -1 : 1)
    )
  }
}
