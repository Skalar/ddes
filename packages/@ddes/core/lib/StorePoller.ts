/**
 * @module @ddes/core
 */

import debug from 'debug'
import Commit from './Commit'
import EventStore from './EventStore'
import {
  AggregateEventUpcasters,
  AggregateType,
  StorePollerParams,
} from './types'
import upcastCommits from './upcastCommits'
import * as utils from './utils'

export default class StorePoller {
  public eventStore: EventStore
  public sortKeyCursor!: string | Date

  protected debug: debug.IDebugger

  protected stopRequested = false

  protected get shouldPoll() {
    return !this.stopRequested
  }

  /**
   * Initial number of milliseconds to wait before polling again,
   * after store has no more commits to yield.
   */
  protected initalSleepPeriod = 10

  /**
   * Maximum number of milliseconds to wait before polling again,
   * after store has no more commits to yield.
   */
  protected maxSleepPeriod = 1000

  /**
   * Exponent to use when increasing sleep period from initialSleepPeriod to maxSleepPeriod
   */
  protected sleepPeriodBackoffExponent = 2

  protected isPolling = false

  protected chronologicalGroup = 'default'
  protected filterAggregateTypes?: AggregateType[]
  protected upcasters?: AggregateEventUpcasters
  protected initialPoll = true

  constructor(params: StorePollerParams) {
    const {eventStore, ...rest} = params

    this.eventStore = eventStore
    Object.assign(this, rest)

    if (params.sortKeyCursor) {
      this.sortKeyCursor = params.sortKeyCursor
    }
    if (params.chronologicalGroup) {
      this.chronologicalGroup = params.chronologicalGroup
    }

    this.filterAggregateTypes = params.filterAggregateTypes

    if (params.processCommit) {
      this.processCommit = params.processCommit
    }

    this.debug = debug(`DDES.${this.constructor.name}`)
  }

  public start() {
    if (!this.sortKeyCursor) {
      throw new Error('sortKeyCursor not set')
    }

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
        for await (const resultSet of this.eventStore.chronologicalQuery({
          min: this.sortKeyCursor,
          exclusiveMin: true,
          group: this.chronologicalGroup,
          filterAggregateTypes: this.filterAggregateTypes,
        })) {
          for await (const commit of this.upcasters
            ? upcastCommits(resultSet.commits, this.upcasters)
            : resultSet.commits) {
            commitsCount++
            await this.processCommit(commit)
            this.sortKeyCursor = commit.sortKey
          }

          if (resultSet.cursor && resultSet.cursor > this.sortKeyCursor) {
            this.sortKeyCursor = resultSet.cursor
          }
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  public async processCommit(commit: Commit) {
    // void
  }
}
