/**
 * @module @ddes/aws-store
 */

import {DynamoDB, S3} from 'aws-sdk'

// TYPE ALIASES

export type CommitKey = string

// INTERFACES

export interface AutoscalingConfig {
  readMin: number
  readMax: number
  writeMin: number
  writeMax: number
  indexReadMin: number
  indexReadMax: number
  indexWriteMin: number
  indexWriteMax: number
  tableScaleInCooldown: number
  tableScaleOutCooldown: number
  indexScaleInCooldown: number
  indexScaleOutCooldown: number
  utilizationTargetInPercent: number
}

export interface AwsStoreConfig {
  tableName: string
  initialCapacity?: CapacityConfig
  autoscaling?: AutoscalingConfig
  snapshots?: SnapshotsConfig
  s3ClientConfiguration?: S3.ClientConfiguration
  dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  maxVersionDigits?: number
}

export interface CapacityConfig {
  read: number
  write: number
  indexRead: number
  indexWrite: number
}

export interface MarshalledCommit extends DynamoDB.AttributeMap {
  /**
   * Aggregate type e.g. 'Order' (primary partition key)
   */
  a: {
    S: string
  }

  /**
   * Aggregate key and version (primary range key)
   */
  k: {
    S: string
  }

  /**
   * Whether or not commit should be considered active 't' | 'f'
   *
   * allAggregates secondary index parition key
   */
  z: {
    S: string
  }

  /**
   * Commit sort key (allAggregates secondary index range key)
   */
  c: {
    S: string
  }

  /**
   * Commit timestamp (ISO 8601)
   */
  t: {
    S: string
  }

  /**
   * Events in gzipped JSON form
   */
  e: {
    B: string
  }
}

export interface SnapshotsConfig {
  s3BucketName: string
  keyPrefix?: string
  snapshotFrequency?: number
  manageBucket?: boolean
}
