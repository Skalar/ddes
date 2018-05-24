/**
 * @module @ddes/aws-store
 */

import {DynamoDB, S3} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config'

export interface AutoscalingConfig {
  tableReadMin: number
  tableReadMax: number
  tableWriteMin: number
  tableWriteMax: number
  tableScaleInCooldown: number
  tableScaleOutCooldown: number
  chronologicalReadMin: number
  chronologicalReadMax: number
  chronologicalWriteMin: number
  chronologicalWriteMax: number
  chronologicalScaleInCooldown: number
  chronologicalScaleOutCooldown: number
  instancesReadMin: number
  instancesReadMax: number
  instancesWriteMin: number
  instancesWriteMax: number
  instancesScaleInCooldown: number
  instancesScaleOutCooldown: number
  utilizationTargetInPercent: number
}

export interface AwsEventStoreConfig {
  tableName: string
  initialCapacity?: StoreCapacityConfig
  autoscaling?: AutoscalingConfig
  awsConfig?: ConfigurationOptions
  s3ClientConfiguration?: S3.ClientConfiguration
  dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  createdAt?: Date
}

export interface StoreCapacityConfig {
  tableRead: number
  tableWrite: number
  chronologicalRead: number
  chronologicalWrite: number
  instancesRead: number
  instancesWrite: number
}

export interface MarshalledCommit extends DynamoDB.AttributeMap {
  /**
   * Aggregate stream id type e.g. 'Order:123' (table partition key)
   */
  s: {
    S: string
  }

  /**
   * Aggregate version (table sort key)
   */
  v: {
    N: string
  }

  /**
   * Chronological sort key (chronological index sort key)
   */
  g: {
    S: string
  }

  /**
   * Aggregate type
   */
  a: {
    S: string
  }

  /**
   * Aggregate root commit key (only set for version = 1 commits)
   */
  r: {
    S: string
  }

  /**
   * Commit timestamp
   */
  t: {
    N: string
  }

  /**
   * Events in gzipped JSON form
   */
  e: {
    B: string
  }

  /**
   * TTL timestamp (commit will be deleted at the set time)
   */
  x: {
    N: string
  }

  /**
   * Chronological index partition key
   *
   */
  p: {
    S: string
  }
}

export interface StoreQueryParams {
  startKey?: DynamoDB.Key
  keyExpressions?: string[]
  filterExpressions?: string[]
  filterAggregateTypes?: string[]
  queryVariables?: object
  limit?: number
  capacityLimit?: number
}

/**
 * @hidden
 */
export interface AwsEventStoreBatchMutatorQueueItem {
  startedPromise: Promise<any>
  startedResolver: () => void
  processedPromise: Promise<any>
  processedResolver: () => void
  capacityUnits: number
  processing: boolean
  item: any
  throttleCount: number
}
