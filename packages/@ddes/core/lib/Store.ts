/**
 * @module @ddes/core
 */

import Aggregate from './Aggregate'
import {BatchMutator} from './BatchMutator'
import {Commit} from './Commit'
import {
  AggregateEventUpcasters,
  AggregateKeyString,
  AggregateSnapshot,
  Iso8601Timestamp,
} from './types'

export abstract class Store {
  public upcasters?: AggregateEventUpcasters
  public lazyTransformation = false

  public lazyTransformationMutator?: BatchMutator

  /**
   * Performs necessary orchestration to ready the store
   */
  public abstract setup(): Promise<void>

  /**
   * Tears down all resources within namespace created by store
   */
  public abstract teardown(): Promise<void>

  /**
   * Get the most recent commit from the store
   */
  public abstract getHeadCommit(): Promise<Commit | null>

  /**
   * DANGER: Delete ALL commits in the store
   */
  // public abstract deleteAllCommits?(): Promise<void>

  // /**
  //  * Write commits efficiently to store, without consistency guarantees and automatic versioning
  //  */
  // batchWriteCommits?(): Promise<void>

  /**
   * Commit to the store
   */
  public abstract commit(commit: Commit): Promise<void>

  /**
   * Converts a Commit into an object suited for communicating with store
   */
  public abstract marshallCommit(commit: Commit): Promise<any>

  /**
   * Converts a marshalled commit into Commit
   */
  public abstract unmarshallCommit(marshalledCommit: any): Promise<Commit>

  public abstract chronologicalCommits(options: {
    from?: string
    after?: string
    before?: string
    descending?: boolean
    filterAggregateTypes?: string[]
    limit?: number
  }): AsyncIterableIterator<Commit>

  public abstract queryAggregateCommits(params: {
    type: string
    key?: AggregateKeyString
    consistentRead?: boolean
    minVersion?: number
    maxVersion?: number
    maxTime?: Iso8601Timestamp
    limit?: number
    descending?: boolean
  }): AsyncIterableIterator<Commit>

  public abstract getAggregateHeadCommit(params: {
    type: string
    key: AggregateKeyString
  }): Promise<Commit | null>

  public abstract writeSnapshot(params: {
    type: string
    key: string
    version: number
    state: object
    timestamp: Iso8601Timestamp
    compatibilityChecksum: string
  }): Promise<void>

  public abstract readSnapshot(params: {
    type: string
    key: string
  }): Promise<AggregateSnapshot | null>

  public abstract deleteSnapshots(aggregateNames?: string[]): Promise<void>

  public abstract createBatchMutator(): any

  public async *upcastCommits(
    commits: AsyncIterableIterator<Commit>
  ): AsyncIterableIterator<Commit> {
    const {upcasters = {}} = this
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
          active,
        } = commit
        const upcastedCommit = new Commit({
          aggregateType,
          aggregateKey,
          aggregateVersion,
          timestamp,
          active,
          events: upcastedEvents,
        })

        if (this.lazyTransformation) {
          if (!this.lazyTransformationMutator) {
            this.lazyTransformationMutator = this.createBatchMutator()
          }

          await this.lazyTransformationMutator!.put(upcastedCommit)
        }

        yield upcastedCommit
      } else {
        yield commit
      }
    }
  }
}
