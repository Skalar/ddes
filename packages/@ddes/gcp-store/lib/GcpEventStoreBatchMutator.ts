import {AggregateKey, AggregateType, BatchMutator, Commit} from '@ddes/core'
import GcpEventStore from './GcpEventStore'
import {GcpEventStoreBatchMutatorQueueItem, MarshalledCommit} from './types'
import {marshallCommit, unmarshallCommit} from './utils'

export default class GcpEventStoreBatchMutator extends BatchMutator<
  MarshalledCommit
> {
  protected store: GcpEventStore
  protected queue: Set<GcpEventStoreBatchMutatorQueueItem> = new Set()
  protected maxItemsPerRequest: number = 25
  protected processQueueRunning: boolean = false
  protected capacityLimit?: number
  protected bufferSize: number = 100
  protected remainingCapacity?: {
    second: number
    units: number
  }

  constructor(params: {store: GcpEventStore; capacityLimit?: number}) {
    super()
    const {store, capacityLimit} = params
    this.store = store

    if (capacityLimit) {
      this.capacityLimit = capacityLimit
      this.bufferSize = capacityLimit
    }
  }

  public get saturated() {
    let pendingCount = 0

    for (const item of this.queue) {
      if (!item.processing) {
        pendingCount++
      }

      if (pendingCount >= this.bufferSize) {
        return true
      }
    }

    return false
  }

  public async delete(
    commits: Array<Commit | MarshalledCommit | Commit | MarshalledCommit>
  ): Promise<void> {
    for await (const commit of this.asIterable(commits)) {
      const {aggregateKey, aggregateType, aggregateVersion} =
        commit instanceof Commit ? commit : await unmarshallCommit(commit)

      const key = this.store.key(aggregateType, aggregateKey, aggregateVersion)

      await this.addToQueue({delete: {key}})
    }
  }

  public async put(): Promise<void> {
    return Promise.resolve()
  }

  private addToQueue(item: any) {
    console.log(item)
  }
}
