import {AggregateCommit, EventStore} from './EventStore'
import {PollParams, pollWithBackoff} from './pollWithBackoff'
import {ExtractEventTypes} from './utilityTypes'

type ConstructAggregateCommitType<TAggregates extends Record<string, any>> = {
  [P in keyof TAggregates]: AggregateCommit<
    ExtractEventTypes<TAggregates[P]['events']>,
    TAggregates[P]['config']['type']
  >
}

export async function* pollEventStore<
  TAggregates,
  TAggregateCommitMap extends Record<string, any> = ConstructAggregateCommitType<TAggregates>,
  TAggregateCommit extends AggregateCommit = TAggregateCommitMap[keyof TAggregateCommitMap]
>(
  params: {
    store: EventStore
    aggregateRoots: TAggregates
    chronologicalPartition?: string
    initialCursor?: string | Date
  } & Partial<PollParams>
) {
  const {
    store,
    aggregateRoots,
    minDelay = 10,
    maxDelay = 1000,
    delayBackoffExponent = 2,
    chronologicalPartition,
    aborted,
  } = params

  let {initialCursor: cursor = new Date()} = params

  const poller = pollWithBackoff(
    {
      minDelay,
      maxDelay,
      delayBackoffExponent,
      aborted,
    },
    () =>
      store.chronologicalQuery<TAggregateCommit>({
        min: cursor,
        exclusiveMin: true,
        ...(aggregateRoots && {
          aggregateTypes: Object.values(aggregateRoots).map(v => v.config.type),
        }),
        chronologicalPartition: chronologicalPartition,
      })
  )

  for await (const commits of poller) {
    if (!commits) {
      yield // allow consumer to deal with empty poll
      continue
    }

    for await (const commit of commits) {
      cursor = commit.chronologicalKey
      yield commit
    }
  }
}
