import {
  OrderOptions,
  QueryFilterOperator,
  QueryInfo,
} from '@google-cloud/datastore/query'

/**
 * @module @ddes/gcp-store
 */

interface StoreQueryFilterParam {
  property: string
  operator: QueryFilterOperator
  value: any
}

interface StoreQueryOrderParam {
  property: string
  options?: OrderOptions
}

export interface StoreQueryParams {
  keyOnly?: boolean
  filters?: StoreQueryFilterParam[]
  orders?: StoreQueryOrderParam[]
  limit?: number
}

export interface QueryResult {
  items: object[]
  info: QueryInfo
}

export interface MarshalledCommitProperty {
  name: string
  value: string
  excludeFromIndexes?: boolean
}

export interface MarshalledCommit {
  /**
   * Aggregate stream id type e.g. 'Order:123' (table partition key)
   */
  s: string

  /**
   * Aggregate version (table sort key)
   */
  v: string

  /**
   * Chronological sort key (chronological index sort key)
   */
  g: string

  /**
   * Aggregate type
   */
  a: string

  /**
   * Aggregate root commit key (only set for version = 1 commits)
   */
  r: string

  /**
   * Commit timestamp
   */
  t: string

  /**
   * Events in gzipped JSON form
   */
  e: string

  /**
   * TTL timestamp (commit will be deleted at the set time)
   */
  x: string

  /**
   * Chronological index partition key
   *
   */
  p: string
}

/**
 * @hidden
 */
export interface GcpEventStoreBatchMutatorQueueItem {
  startedPromise: Promise<any>
  startedResolver: () => void
  processedPromise: Promise<any>
  processedResolver: () => void
  capacityUnits: number
  processing: boolean
  item: any
  throttleCount: number
}
