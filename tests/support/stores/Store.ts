import {EventStore, MetaStore, SnapshotStore} from '@ddes/core'

export default abstract class Store {
  public abstract setup(): Promise<void>
  public abstract teardown(): Promise<void>
  public abstract eventStore(config?: any): EventStore
  public abstract metaStore(config?: any): MetaStore
  public abstract snapshotStore(config?: any): SnapshotStore
}
