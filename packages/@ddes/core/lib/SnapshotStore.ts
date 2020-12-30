/**
 * @module @ddes/core
 */

import {AggregateKey, AggregateSnapshot, AggregateType, Timestamp} from './types'

/**
 * Abstract interface for a store that holds aggregate snapshots
 */
export default abstract class SnapshotStore {
  /**
   * Performs necessary orchestration to ready the store
   */
  public abstract setup(): Promise<void>

  /**
   * Tears down all resources created by the store
   */
  public abstract teardown(): Promise<void>

  /**
   * Write an aggregate instance snapshot to the store
   *
   * @param type e.g. 'Account'
   * @param key  e.g. '1234'
   */
  public abstract writeSnapshot(
    type: string,
    key: string,
    payload: {
      version: number
      state: object
      timestamp: Timestamp
      compatibilityChecksum: string
    }
  ): Promise<void>

  /**
   * Read an aggregate instance snapshot from store
   *
   * @param type e.g. 'Account'
   * @param key  e.g. '1234'
   */
  public abstract readSnapshot(type: AggregateType, key: AggregateKey): Promise<AggregateSnapshot | null>

  /**
   * Delete store snapshots
   */
  public abstract deleteSnapshots(): Promise<void>
}
