/**
 * @module @ddes/core
 */

import {Commit, MetaStore, Store} from './'
import {Aggregate} from './Aggregate'
import {AggregateType, EventWithMetadata} from './types'
import {jitteredBackoff} from './utils'

type ProjectionGroup = AggregateType[]

export interface ProjectionParams {
  name: string
  metaStore: MetaStore

  dependencies?: {
    [dependerType: string]: {
      [dependeeType: string]: (
        dependerEvent: EventWithMetadata,
        dependeeEvent: EventWithMetadata
      ) => boolean
    }
  }

  aggregateClasses: {[aggregateType: string]: typeof Aggregate}

  processEvents(events: Set<EventWithMetadata>): Promise<void>
}
export class Projection {
  public metaStore!: MetaStore
  public name!: string
  public maxBatchSize = 50
  public maxConcurrency = 1
  public processEvents!: (events: Set<EventWithMetadata>) => Promise<void>

  /**
   * Used to guarantee order of event processing while optimizing for parallel processing
   */
  public dependencies?: {
    [dependerType: string]: {
      [dependeeType: string]: (
        dependerEvent: EventWithMetadata,
        dependeeEvent: EventWithMetadata
      ) => boolean
    }
  } = {}

  public aggregateClasses!: {[aggregateType: string]: typeof Aggregate}

  constructor(params: ProjectionParams) {
    Object.assign(this, params)
  }

  public async whenSortKeyReached(
    sortKey: string,
    options: {
      initialDelay?: number
      maxDelay?: number
      backoffExponent?: number
      timeout?: number
    } = {}
  ): Promise<boolean> {
    const {
      initialDelay = 10,
      maxDelay = 500,
      backoffExponent = 2,
      timeout = 10000,
    } = options

    const startedAt = Date.now()

    let attempt = 0

    while (true) {
      attempt++

      const headSortKey = await this.getHeadSortKey()

      if (headSortKey >= sortKey) {
        return true
      }

      const delay = jitteredBackoff({
        initialValue: initialDelay,
        maxValue: maxDelay,
        backoffExponent,
        attempt,
      })
      if (Date.now() + delay > startedAt + timeout) {
        break
      } else {
        await new Promise(resolve => setTimeout(resolve, delay))
      }
    }

    return false
  }

  public async commitIsProcessed(
    commit: Commit,
    options?: {
      initialDelay?: number
      maxDelay?: number
      backoffExponent?: number
      timeout?: number
    }
  ) {
    return await this.whenSortKeyReached(commit.sortKey, options)
  }

  public async setHeadSortKey(sortKey: string) {
    await this.metaStore.put(
      [`Projection:${this.name}`, 'headSortKey'],
      sortKey
    )
  }

  public async getHeadSortKey() {
    return (
      (await this.metaStore.get([`Projection:${this.name}`, 'headSortKey'])) ||
      '0'
    )
  }

  /**
   * Make projection target ready (e.g. create elasticsearch index)
   */

  public async setup() {
    // noop
  }

  public async teardown() {
    await this.metaStore.delete([`Projection:${this.name}`, 'headSortKey'])
  }
}
