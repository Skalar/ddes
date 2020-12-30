/**
 * @module @ddes/core
 */
import BatchMutator from './BatchMutator'
import Commit from './Commit'
import {AggregateKey, AggregateType, MarshalledCommit, StoreQueryResponse} from './types'

/**
 * Abstract interface for an Event Store
 */
export default abstract class EventStore {
  /**
   * Performs necessary orchestration to ready the store
   */
  public abstract setup(): Promise<void>

  /**
   * Tears down all resources created by the store
   */
  public abstract teardown(): Promise<void>

  /**
   * Unreliable, probably outdated, best effort commit count
   */
  public abstract bestEffortCount(): Promise<number>

  /**
   * Write a commit to the Store
   */
  public abstract commit(commit: Commit): Promise<void>

  /**
   * Query the commits of an [[Aggregate]] instance
   */
  public abstract queryAggregateCommits(
    type: AggregateType,
    key: AggregateKey,
    options?: {
      consistentRead?: boolean
      minVersion?: number
      maxVersion?: number
      maxTime?: Date | number
      limit?: number
      descending?: boolean
    }
  ): StoreQueryResponse

  /**
   * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
   */
  public abstract scanAggregateInstances(
    type: AggregateType,
    options?: {
      instanceLimit?: number
    }
  ): StoreQueryResponse

  /**
   * Get most recent commit for an [[Aggregate]] instance
   */
  public abstract getAggregateHeadCommit(type: AggregateType, key: AggregateKey): Promise<Commit | null>

  /**
   * Get the most recent commit in the given chronological group
   */
  public abstract getHeadCommit(chronologicalGroup?: string): Promise<Commit | null>

  /**
   * Scan store commits
   */
  public abstract scan(options?: {
    totalSegments?: number
    segment?: number
    filterAggregateTypes?: string[]
    startKey?: any
    limit?: number
    capacityLimit?: number
  }): StoreQueryResponse

  /**
   * Get a [[BatchMutator]] for the store
   */
  public abstract createBatchMutator(params?: {capacityLimit?: number}): BatchMutator<MarshalledCommit>

  /**
   * Retrieve commits from the store chronologically
   */
  public abstract chronologicalQuery(params: {
    group?: string
    min: string | Date
    max?: string | Date
    descending?: boolean
    limit?: number
    exclusiveMin?: boolean
    exclusiveMax?: boolean
    filterAggregateTypes?: AggregateType[]
  }): StoreQueryResponse

  /**
   * Human readable representation of the store instance
   */
  public abstract toString(): string
}
