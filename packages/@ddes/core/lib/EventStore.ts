/**
 * Abstract interface for an Event Store
 */
export abstract class EventStore {
  /**
   * Performs necessary orchestration to ready the store
   */
  public abstract setup(): Promise<any>

  /**
   * Tears down all resources created by the store
   */
  public abstract teardown(): Promise<any>

  /**
   * Commit aggregate events to the store
   */
  public abstract commit<TAggregateCommit extends AggregateCommit>(
    commitData: TAggregateCommit
  ): Promise<TAggregateCommit>

  /**
   * Query the commits of an [[Aggregate]] instance
   */
  public abstract queryAggregateCommits<TAggregateCommit extends AggregateCommit>(
    type: string,
    key: string,
    options?: {
      minVersion?: number
      maxVersion?: number
      maxTime?: Date | number
      limit?: number
      descending?: boolean
    }
  ): AsyncIterable<TAggregateCommit[]>

  /**
   * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
   */
  public abstract scanAggregateCommitsGroupedByKey<TAggregateCommit extends AggregateCommit>(
    type: string
  ): AsyncIterable<TAggregateCommit[]>

  /**
   * Get most recent commit for an [[Aggregate]] instance
   */
  public abstract getAggregateHeadCommit<TAggregateCommit extends AggregateCommit>(
    type: string,
    key: string
  ): Promise<TAggregateCommit | undefined>

  /**
   * Scan store commits
   */
  public abstract scan<TAggregateCommit extends AggregateCommit>(options?: {
    totalSegments?: number
    segment?: number
    aggregateTypes?: string[]
    cursor?: any
  }): AsyncIterable<TAggregateCommit[]>

  /**
   * Retrieve commits from the store chronologically
   */
  public abstract chronologicalQuery<TAggregateCommit extends AggregateCommit>(params: {
    min: string | Date
    max?: string | Date
    descending?: boolean
    limit?: number
    exclusiveMin?: boolean
    exclusiveMax?: boolean
    aggregateTypes?: string[]
    chronologicalPartition?: string
  }): AsyncIterable<TAggregateCommit[]>

  public abstract streamCommits<TAggregateCommit extends AggregateCommit>(params?: {
    aggregateTypes?: string[]
    chronologicalKey?: string
  }): AsyncIterable<TAggregateCommit[]>
  public abstract streamCommits<TAggregateCommit extends AggregateCommit>(
    params: {
      aggregateTypes?: string[]
      chronologicalKey?: string
    },
    yieldEmpty: true
  ): AsyncIterable<TAggregateCommit[] | undefined>
  public abstract streamCommits<TAggregateCommit extends AggregateCommit>(
    params: {
      aggregateTypes?: string[]
      chronologicalKey?: string
    },
    yieldEmpty: boolean
  ): AsyncIterable<TAggregateCommit[]>

  public abstract streamAggregateInstanceCommits<TAggregateCommit extends AggregateCommit>(
    aggregateType: string,
    key: string,
    minVersion?: number
  ): AsyncIterable<TAggregateCommit[]>
  public abstract streamAggregateInstanceCommits<TAggregateCommit extends AggregateCommit>(
    aggregateType: string,
    key: string,
    minVersion: number,
    yieldEmpty: true
  ): AsyncIterable<TAggregateCommit[] | undefined>
  public abstract streamAggregateInstanceCommits<TAggregateCommit extends AggregateCommit>(
    aggregateType: string,
    key: string,
    minVersion: number,
    yieldEmpty: boolean
  ): AsyncIterable<TAggregateCommit[] | undefined>

  public abstract chronologicalKey(data: {
    aggregateType: string
    aggregateKey: string
    aggregateVersion: number
    timestamp: number
  }): string
}

export interface AggregateEvent extends Record<string, any> {
  type: string
  version?: number
}

export interface AggregateCommit<
  TEvent extends AggregateEvent = AggregateEvent,
  TAggregateType extends string = string
> {
  aggregateType: TAggregateType
  aggregateKey: string
  aggregateVersion: number
  timestamp: number
  events: TEvent[]
  expiresAt?: number
  chronologicalPartition?: string
  chronologicalKey: string
}

export class VersionConflictError extends Error {
  public readonly commit: AggregateCommit

  constructor(commit: AggregateCommit) {
    super(
      `${commit.aggregateType}<${commit.aggregateKey}> already has commit version ${commit.aggregateVersion}`
    )
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'VersionConflictError'
    this.commit = commit
  }
}
