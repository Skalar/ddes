/**
 * @module @ddes/core
 */

import {
  AggregateKeyString,
  AggregateType,
  Event,
  Iso8601Timestamp,
} from './types'
import {toIso8601Timestamp} from './utils'

/**
 * Represents an aggregate changeset consisting of one or more events
 *
 */
export class Commit {
  public aggregateType: AggregateType
  public aggregateKey: AggregateKeyString
  public aggregateVersion: number
  public timestamp: Iso8601Timestamp

  public events: Event[]
  public active: boolean = true

  /**
   *
   * @param {Object} attributes Commit attributes
   * @param {string} attributes.aggregateType Hey
   */
  constructor(attributes: {
    aggregateType: AggregateType
    aggregateKey: AggregateKeyString
    aggregateVersion: number
    timestamp?: Iso8601Timestamp | Date
    active?: boolean
    events: Event[]
  }) {
    this.timestamp = toIso8601Timestamp(attributes.timestamp)
    this.events = attributes.events.map(event => ({version: 1, ...event}))
    this.aggregateType = attributes.aggregateType
    this.aggregateKey = attributes.aggregateKey
    this.aggregateVersion = attributes.aggregateVersion

    if (typeof attributes.active !== 'undefined') {
      this.active = attributes.active
    }
  }

  /**
   * String used to order commits in the store
   */
  get sortKey() {
    return [
      this.timestamp.replace(/[^0-9]/g, ''),
      this.aggregateType,
      this.aggregateKey,
    ].join(':')
  }

  public toJSON() {
    const {
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
      active,
      events,
    } = this
    return {
      aggregateType,
      aggregateKey,
      aggregateVersion,
      timestamp,
      active,
      events,
    }
  }
}
