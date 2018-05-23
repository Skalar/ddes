/**
 * @module @ddes/core
 */

import Projection from './Projection'
import {EventWithMetadata} from './types'

export default class ProjectionWorker {
  public projection: Projection
  private queue: Set<EventWithMetadata>
  private maxQueueSize: number
  private addToQueueWaitResolvers: Array<() => void> = []
  private workerLoopRunning = false

  constructor(params: {projection: Projection; maxQueueSize: number}) {
    this.projection = params.projection
    this.maxQueueSize = params.maxQueueSize
    this.queue = new Set()
  }

  public async addToQueue(eventWithMetadata: EventWithMetadata) {
    if (this.queue.size >= this.maxQueueSize) {
      await new Promise(resolve => this.addToQueueWaitResolvers.push(resolve))
    }

    this.queue.add(eventWithMetadata)

    if (!this.workerLoopRunning) {
      this.workerLoop()
    }
  }

  private async workerLoop() {
    if (this.workerLoopRunning) {
      return
    }

    this.workerLoopRunning = true

    try {
      let pessimisticHeadSortKey
      let highestSortKeyProcessed

      while (this.queue.size > 0) {
        const eventsToProcess: Set<EventWithMetadata> = new Set()
        const streamIds: Set<string> = new Set()

        let eventSkipped = false
        const queueSnapshot = new Set(this.queue)

        queue: for (const event of queueSnapshot) {
          const streamId = `${event.aggregateType}|${event.aggregateKey}`

          if (streamIds.has(streamId)) {
            eventSkipped = true
            continue queue // we can only do one event per streamId at a time
          }

          const dependencies =
            this.projection.dependencies &&
            this.projection.dependencies[event.aggregateType]

          if (dependencies) {
            for (const eventToProcess of eventsToProcess) {
              if (dependencies[eventToProcess.aggregateType]) {
                const dependerAggregateClass = this.projection.aggregateClasses[
                  event.aggregateType
                ]
                const dependeeAggregateClass = this.projection.aggregateClasses[
                  eventToProcess.aggregateType
                ]

                const dependerData = {...event}
                const dependeeData = {...eventToProcess}

                if (dependerAggregateClass.keySchema) {
                  dependerData.keyProps = dependerAggregateClass.keySchema.keyPropsFromString(
                    event.aggregateKey
                  )
                }

                if (dependeeAggregateClass.keySchema) {
                  dependeeData.keyProps = dependeeAggregateClass.keySchema.keyPropsFromString(
                    eventToProcess.aggregateKey
                  )
                }

                if (
                  dependencies[eventToProcess.aggregateType](
                    dependerData,
                    dependeeData
                  )
                ) {
                  eventSkipped = true
                  continue queue // this event depends on one or more events in eventsToProcess
                }
              }
            }
          }

          if (!eventSkipped) {
            pessimisticHeadSortKey = event.sortKey
          }

          if (
            !highestSortKeyProcessed ||
            event.sortKey > highestSortKeyProcessed
          ) {
            highestSortKeyProcessed = event.sortKey
          }

          eventsToProcess.add(event)
          streamIds.add(streamId)
          this.queue.delete(event)

          if (eventsToProcess.size >= this.projection.maxBatchSize) {
            break queue
          }
        }

        await this.projection.processEvents(eventsToProcess)
        await this.projection.setHeadSortKey(pessimisticHeadSortKey)

        while (
          this.queue.size <= this.maxQueueSize &&
          this.addToQueueWaitResolvers.length
        ) {
          this.addToQueueWaitResolvers.shift()!()
        }
      }

      if (
        highestSortKeyProcessed &&
        highestSortKeyProcessed > pessimisticHeadSortKey
      ) {
        await this.projection.setHeadSortKey(highestSortKeyProcessed)
      }
    } finally {
      this.workerLoopRunning = false
    }
  }
}
