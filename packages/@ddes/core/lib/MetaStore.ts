/**
 * @module @ddes/core
 */
import {MetaStoreKey} from './types'

export default abstract class MetaStore {
  public abstract get(key: MetaStoreKey): Promise<any>
  public abstract put(key: MetaStoreKey, value: any, options?: {expiresAt?: Date | number}): Promise<void>

  public abstract delete(key: MetaStoreKey): Promise<void>

  public abstract list(partitionKey: string): AsyncIterableIterator<[string, any]>

  public abstract setup(): Promise<void>
  public abstract teardown(): Promise<void>
}
