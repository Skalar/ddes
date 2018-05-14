/**
 * @module @ddes/core
 */

import BatchMutator from './BatchMutator'
import Commit from './Commit'
import {AggregateEventUpcasters} from './types'

export default async function* upcastCommits(
  commits: AsyncIterableIterator<Commit> | Commit[],
  upcasters: AggregateEventUpcasters,
  options?: {lazyTransformation: boolean; batchMutator: BatchMutator}
): AsyncIterableIterator<Commit> {
  for await (const commit of commits) {
    let upcasted = false
    const aggregateUpcasters = upcasters[commit.aggregateType] || {}

    const upcastedEvents = commit.events.map(event => {
      let processedEvent = event
      let upcaster
      while (true) {
        const version = processedEvent.version || 1
        upcaster =
          aggregateUpcasters[processedEvent.type] &&
          aggregateUpcasters[processedEvent.type][version]

        if (upcaster) {
          upcasted = true
          processedEvent = {
            ...processedEvent,
            properties: upcaster(processedEvent.properties),
            version: version + 1,
          }
        } else {
          break
        }
      }

      return processedEvent
    })

    if (upcasted) {
      const {
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
        expiresAt,
      } = commit
      const upcastedCommit = new Commit({
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
        expiresAt,
        events: upcastedEvents,
      })

      if (options && options.lazyTransformation) {
        await options.batchMutator.put(upcastedCommit)
      }

      yield upcastedCommit
    } else {
      yield commit
    }
  }
}
