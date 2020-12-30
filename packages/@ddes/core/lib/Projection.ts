/**
 * @module @ddes/core
 */

import Aggregate from './Aggregate'
import Commit from './Commit'
import MetaStore from './MetaStore'
import {EventWithMetadata, ProjectionParams} from './types'
import {jitteredBackoff} from './utils'

export default class Projection {
  public metaStore!: MetaStore
  public name!: string
  public maxBatchSize = Infinity
  public processEvents!: (events: Set<EventWithMetadata>) => Promise<void>

  /**
   * Used to guarantee order of event processing while optimizing for parallel processing
   */
  public dependencies?: {
    [dependerType: string]: {
      [dependeeType: string]: (dependerEvent: EventWithMetadata, dependeeEvent: EventWithMetadata) => boolean
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
    const {initialDelay = 10, maxDelay = 500, backoffExponent = 2, timeout = 10000} = options

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
    await this.metaStore.put([`Projection:${this.name}`, 'headSortKey'], sortKey)
  }

  public async getHeadSortKey(fail = true) {
    const sortKey = await this.metaStore.get([`Projection:${this.name}`, 'headSortKey'])

    if (!sortKey && fail) {
      throw new Error('Projection has not been setup')
    }

    return sortKey
  }

  /**
   * Ready projection target and set initial headSortKey in MetaStore
   */
  public async setup(params: {startsAt: Date | string}) {
    const {startsAt} = params

    if (!(await this.getHeadSortKey(false))) {
      await this.setHeadSortKey(startsAt instanceof Date ? startsAt.toISOString().replace(/[^0-9]/g, '') : startsAt)
    }
  }

  public async teardown() {
    try {
      await this.metaStore.delete([`Projection:${this.name}`, 'headSortKey'])
    } catch (error) {
      if (error.code !== 'ResourceNotFoundException') {
        throw error
      }
    }
  }
}
