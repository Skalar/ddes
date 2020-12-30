import {EventStore, MetaStore, SnapshotStore} from '@ddes/core'

import {generateTestId} from '../testId'

export type AnyStore = EventStore | MetaStore | SnapshotStore

export default abstract class Store<TStore extends AnyStore = AnyStore> {
  protected stores: TStore[]

  constructor(protected testId: string = generateTestId()) {
    this.stores = []
  }

  public abstract eventStore(config?: any): EventStore
  public abstract metaStore(config?: any): MetaStore
  public abstract snapshotStore(config?: any): SnapshotStore

  public async setup(): Promise<void> {
    await Promise.all(this.stores.map(store => store.setup()))
  }

  public async teardown(): Promise<void> {
    await Promise.all(this.stores.map(store => store.teardown()))
  }

  protected addStore<T extends TStore>(store: T) {
    this.stores.push(store)
    return store
  }
}
