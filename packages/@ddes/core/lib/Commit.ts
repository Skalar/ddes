/**
 * @module @ddes/core
 */

import {AggregateKey, AggregateType, Event, Timestamp} from './types'
import {toTimestamp} from './utils'

/**
 * Represents an aggregate changeset consisting of one or more events
 *
 */
export default class Commit {
  public static hasSameKey(a: Commit, b: Commit) {
    return (
      a.aggregateType === b.aggregateType &&
      a.aggregateKey === b.aggregateKey &&
      a.aggregateVersion === b.aggregateVersion
    )
  }

  public aggregateType: AggregateType
  public aggregateKey: AggregateKey
  public aggregateVersion: number
  public timestamp: Timestamp
  public chronologicalGroup: string
  public events: Event[]
  public expiresAt?: number

  /**
   *
   * @param {Object} attributes Commit attributes
   * @param {string} attributes.aggregateType Hey
   */
  constructor(attributes: {
    aggregateType: AggregateType
    aggregateKey: AggregateKey
    aggregateVersion: number
    timestamp?: Timestamp | string | Date
    sortKey?: string
    events: Event[]
    expiresAt?: number
    chronologicalGroup?: string
    storeKey?: {}
  }) {
    this.timestamp = toTimestamp(attributes.timestamp)
    this.events = attributes.events.map(event => ({version: 1, ...event}))
    this.aggregateType = attributes.aggregateType
    this.aggregateKey = attributes.aggregateKey
    this.aggregateVersion = attributes.aggregateVersion
    this.chronologicalGroup = attributes.chronologicalGroup || 'default'

    Object.defineProperty(this, 'storeKey', {
      enumerable: false,
      value: attributes.storeKey,
    })

    if (typeof attributes.expiresAt !== 'undefined') {
      this.expiresAt = attributes.expiresAt
    }
  }

  get storeKey(): {} | undefined {
    return
  }

  /**
   * String used to order commits in the store
   */
  get sortKey() {
    return [
      new Date(this.timestamp).toISOString().replace(/[^0-9]/g, ''),
      this.aggregateType,
      this.aggregateKey,
      this.aggregateVersion,
    ].join(':')
  }

  public toJSON() {
    const {aggregateType, aggregateKey, aggregateVersion, timestamp, expiresAt, events, chronologicalGroup} = this
    return {
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
      expiresAt,
      events,
      chronologicalGroup,
    }
  }
}
