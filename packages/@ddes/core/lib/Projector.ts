/**
 * @module @ddes/core
 */

import {Commit} from './Commit'
import {Projection} from './Projection'
import {ProjectionWorker} from './ProjectionWorker'
import {Store} from './Store'
import {StorePoller, StorePollerParams} from './StorePoller'
import {EventWithMetadata} from './types'

interface ProjectorConstructorParams {
  projections: Projection[]
}

/**
 * The Projector polls a store and passes events to the given projections.
 */
export class Projector extends StorePoller {
  protected sortKeyCursor = '0'

  private projections: Projection[]
  private queues: Set<ProjectionWorker>

  constructor(projections: Projection[], storePollerParams: StorePollerParams) {
    super(storePollerParams)

    this.projections = projections

    this.filterAggregateTypes = [
      ...projections.reduce((aggregateTypes, projection) => {
        Object.keys(projection.aggregateClasses).forEach(aggregateType =>
          aggregateTypes.add(aggregateType)
        )

        return aggregateTypes
      }, new Set()),
    ]

    this.queues = this.projections.reduce(
      (queues, projection) =>
        queues.add(new ProjectionWorker({projection, maxSize: 100})),
      new Set()
    )
  }

  public async processCommit(commit: Commit) {
    for (const event of commit.events) {
      const {
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
        sortKey,
      } = commit

      const eventWithMetadata = {
        ...event,
        aggregateType,
        aggregateKey,
        aggregateVersion,
        timestamp,
        sortKey,
      }

      for (const queue of this.queues) {
        if (
          Object.keys(queue.projection.aggregateClasses).includes(
            eventWithMetadata.aggregateType
          )
        ) {
          await queue.addToQueue(eventWithMetadata)
        }
      }
    }
  }

  public async start() {
    const headSortKeys = await Promise.all(
      this.projections.map(projection => projection.getHeadSortKey())
    )
    this.sortKeyCursor = headSortKeys.reduce(
      (minSortKey, sortKey) => (sortKey <= minSortKey ? sortKey : minSortKey)
    )

    super.start()
  }
}
