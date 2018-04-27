/**
 * @module @ddes/core
 */

export type MetaStoreKey = [string, string]

export abstract class MetaStore {
  public abstract async get(key: MetaStoreKey): Promise<any>
  public abstract async put(
    key: MetaStoreKey,
    value: any,
    options?: {expiresAt?: Date}
  ): Promise<void>

  public abstract async delete(key: MetaStoreKey): Promise<void>

  public abstract list(
    partitionKey: string
  ): AsyncIterableIterator<[string, any]>

  public abstract async setup(): Promise<void>
  public abstract async teardown(): Promise<void>
}
