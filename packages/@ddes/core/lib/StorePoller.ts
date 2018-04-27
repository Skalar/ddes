/**
 * @module @ddes/core
 */

import * as debug from 'debug'
import {Commit} from './Commit'
import {Store} from './Store'
import * as utils from './utils'

import {AggregateType} from './types'

export interface StorePollerParams {
  store: Store
  sortKeyCursor?: string
  initalSleepPeriod?: number
  maxSleepPeriod?: number
  sleepPeriodBackoffExponent?: number
}

export abstract class StorePoller {
  public store: Store

  protected debug: debug.IDebugger

  protected stopRequested = false

  protected get shouldPoll() {
    return !this.stopRequested
  }

  /**
   * Initial number of milliseconds to wait before polling again,
   * after store has no more commits to yield.
   */
  protected initalSleepPeriod: number = 10

  /**
   * Maximum number of milliseconds to wait before polling again,
   * after store has no more commits to yield.
   */
  protected maxSleepPeriod: number = 1000

  /**
   * Exponent to use when increasing sleep period from initialSleepPeriod to maxSleepPeriod
   */
  protected sleepPeriodBackoffExponent: number = 2

  protected isPolling: boolean = false

  protected sortKeyCursor?: string

  protected filterAggregateTypes?: AggregateType[]

  protected initialPoll: boolean = true

  constructor(params: StorePollerParams) {
    const {store, ...rest} = params

    this.store = store
    Object.assign(this, rest)
    this.debug = debug(`DDES.${this.constructor.name}`)
  }

  public start() {
    this.stopRequested = false
    this.pollingLoop()
  }

  public stop() {
    this.stopRequested = true
  }

  public async pollingLoop() {
    if (this.isPolling) {
      return
    }

    this.debug('pollingLoop() started')

    try {
      this.isPolling = true
      let consecutiveEmptyPolls = 0

      while (this.shouldPoll) {
        let commitsCount = 0
        const params: {
          from?: string
          after?: string
          filterAggregateTypes?: AggregateType[]
        } = {
          ...(this.filterAggregateTypes && {
            filterAggregateTypes: this.filterAggregateTypes,
          }),
        }

        if (this.initialPoll) {
          params.from = this.sortKeyCursor
          this.initialPoll = false
        } else {
          params.after = this.sortKeyCursor
        }

        for await (const commit of this.store.chronologicalCommits(params)) {
          commitsCount++
          await this.processCommit(commit)
          this.sortKeyCursor = commit.sortKey
        }

        if (commitsCount === 0) {
          consecutiveEmptyPolls++

          const delay = utils.jitteredBackoff({
            initialValue: this.initalSleepPeriod,
            maxValue: this.maxSleepPeriod,
            backoffExponent: this.sleepPeriodBackoffExponent,
            attempt: consecutiveEmptyPolls,
          })

          this.debug(`empty poll, waiting ${delay}ms before next attempt`)
          await new Promise(resolve => setTimeout(resolve, delay))
        } else {
          this.debug(`processed ${commitsCount} commits yielded from poll`)
          consecutiveEmptyPolls = 0
        }
      }
    } catch (error) {
      if (!this.stopRequested) {
        throw error
      }
    } finally {
      this.isPolling = false
    }

    this.debug('shouldPoll is false, pollingLoop halted')
  }

  public abstract processCommit(commit: Commit): Promise<void>
}
