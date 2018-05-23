/**
 * @module @ddes/core
 */

import Commit from './Commit'
import Projection from './Projection'
import ProjectionWorker from './ProjectionWorker'
import StorePoller from './StorePoller'
import {StorePollerParams} from './types'

/**
 * The Projector polls a store and passes events to the given projections.
 */
export default class Projector extends StorePoller {
  private projections: Projection[]
  private queues: Set<ProjectionWorker>

  constructor(
    projections: Projection[],
    params: StorePollerParams & {maxQueueSize?: number}
  ) {
    super(params)

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
        queues.add(
          new ProjectionWorker({
            projection,
            maxQueueSize: params.maxQueueSize || 100,
          })
        ),
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
