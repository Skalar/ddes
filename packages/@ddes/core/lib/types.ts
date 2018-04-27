/**
 * @module @ddes/core
 */

import {Commit} from './Commit'

export type CommitOrCommits =
  | Commit[]
  | AsyncIterable<Commit>
  | Iterable<Commit>
  | Commit

export interface Event {
  type: string
  version?: number
  properties: {
    [name: string]: any
  }
  [key: string]: any
}

export type EventInput = Partial<Event> & Pick<Event, 'type'>

export interface JitteredRetryOptions {
  timeout: number
  initialDelay: number
  maxDelay: number
  backoffExponent: number
  errorIsRetryable: (error: Error) => boolean
  beforeRetry?: () => Promise<any>
}

export type EventWithMetadata = Event &
  Pick<
    Commit,
    'aggregateType' | 'aggregateKey' | 'aggregateVersion' | 'timestamp'
  >

export type Iso8601Timestamp = string

/**
 * Object with properties that can be used by a [[KeySchema]] to produce an [[AggregateKeyString]]
 */
export type AggregateKeyProps = any

/**
 * A key that, within the scope of an aggregateType, uniquely identifies an aggregate.
 */
export type AggregateKeyString = string

export type AggregateKey = AggregateKeyString | AggregateKeyProps

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
  time?: string | Iso8601Timestamp
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
  timestamp: Iso8601Timestamp
}
