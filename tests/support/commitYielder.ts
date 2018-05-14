import {AggregateKey, AggregateType, Commit} from '@ddes/core'
import {inspect} from 'util'

async function* commitYielder(
  commitPartials: any[],
  defaults: {
    aggregateType?: AggregateType
    aggregateKey?: AggregateKey
  } = {},
  options: {
    delay?: number
    neverEnding?: boolean
  } = {}
): AsyncIterableIterator<Commit> {
  if (options.delay) {
    await new Promise(resolve => setTimeout(resolve, options.delay!))
  }
  for (const commit of commitPartials) {
    const {
      aggregateVersion = 1,
      aggregateType = defaults.aggregateType,
      aggregateKey = defaults.aggregateKey,
      timestamp,
      events,
    } = commit

    if (!aggregateType || !aggregateKey || !events) {
      throw new Error(`Missing commit data ${inspect(commit)}`)
    }

    yield new Commit({
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
      events: events.map((event: object) => ({version: 1, ...event})),
    })
  }

  if (options.neverEnding) {
    await new Promise(resolve => {
      //
    })
  }
}
export default commitYielder
