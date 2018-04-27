import timeout from '@async-generators/timeout'

export default async function<T>(
  source: AsyncIterable<T>,
  options: {maxWaitTime?: number} = {}
) {
  const items: T[] = []

  try {
    for await (const item of options.maxWaitTime
      ? timeout(source, options.maxWaitTime)
      : source) {
      items.push(item)
    }
  } catch (error) {
    if (error.toString() !== 'Error: timed out') {
      throw error
    }
  }

  return items
}
