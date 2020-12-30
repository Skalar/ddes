/**
 * @module @ddes/core
 */

import Aggregate from './Aggregate'
import Commit from './Commit'
import EventStore from './EventStore'
import MetaStore from './MetaStore'

export interface Event {
  type: string
  version?: number
  properties: {
    [name: string]: any
  }
  [key: string]: any
}

export type EventInput = Partial<Event> & Pick<Event, 'type'>

export interface RetryConfig {
  timeout: number
  initialDelay: number
  maxDelay: number
  backoffExponent: number
  errorIsRetryable: (error: Error) => boolean
  beforeRetry?: () => Promise<any>
}

export type EventWithMetadata = Event &
  Pick<Commit, 'aggregateType' | 'aggregateKey' | 'aggregateVersion' | 'timestamp'> & {
    commitEventIndex: number
  }

export type Timestamp = number

/**
 * Object with properties that can be used by a [[KeySchema]] to produce an [[AggregateKey]]
 */
export type AggregateKeyProps = any

/**
 * A key that, within the scope of an aggregateType, uniquely identifies an aggregate.
 */
export type AggregateKey = string

export interface KeySchemaProperty {
  name: string
  optional?: boolean
  value?(props: object): any
}

/**
 * Event upcasting specification
 *
 * @example
 * ```typescript
 *
 * {
 *   Product: {
 *     Created: {
 *       1: props => ({...props, title: `${props.title}_fixed`})
 *     }
 *   }
 * }
 * ```
 * @param {string} resourceGroupName The name of the resource group within the
 * user's subscription. The name is case insensitive.
 */
export interface AggregateEventUpcasters {
  [aggregateType: string]: {
    [eventType: string]: {
      [eventVersion: number]: (eventProperties: object) => object
    }
  }
}

export interface HydrateOptions {
  version?: number
  time?: string | Timestamp
  consistentRead?: boolean
  useSnapshots?: boolean
  rewriteInvalidSnapshots?: boolean
}

/**
 * Internal state representation that the stateReducer understands
 * e.g. immutablejs objects
 */
export type InternalState = any

/**
 * String identifying an aggregate type/class
 */
export type AggregateType = string

export interface AggregateSnapshot {
  version: number
  state: object
  compatibilityChecksum: string
  timestamp: Timestamp
}

export type StoreCursor = string

export interface StoreQueryResultSet {
  items: any[]
  commits: AsyncIterableIterator<Commit>
  scannedCount: number
  consumedCapacity: any
  throttleCount: number
  cursor?: StoreCursor
}

export interface StoreQueryResponse {
  commits: AsyncIterableIterator<Commit>
  events: AsyncIterableIterator<Event>

  /**
   * @hidden
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<StoreQueryResultSet>
}

export type MarshalledCommit = any

export interface StorePollerParams {
  eventStore: EventStore
  chronologicalGroup?: string
  sortKeyCursor?: string | Date
  initalSleepPeriod?: number
  maxSleepPeriod?: number
  sleepPeriodBackoffExponent?: number
  filterAggregateTypes?: AggregateType[]
  processCommit?: (commit: Commit) => Promise<void>
  upcasters?: AggregateEventUpcasters
}

export interface ProjectionParams {
  name: string
  metaStore: MetaStore

  dependencies?: {
    [dependerType: string]: {
      [dependeeType: string]: (dependerEvent: EventWithMetadata, dependeeEvent: EventWithMetadata) => boolean
    }
  }

  aggregateClasses: {[aggregateType: string]: typeof Aggregate}

  processEvents(events: Set<EventWithMetadata>): Promise<void>
}

export type MetaStoreKey = [string, string]

/**
 * @hidden
 */
export type AggregateStatic<T> = {
  new (): T
} & typeof Aggregate
