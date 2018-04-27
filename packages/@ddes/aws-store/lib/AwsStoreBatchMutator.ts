/**
 * @module @ddes/aws-store
 */

import {BatchMutator, Commit, CommitOrCommits, Store} from '@ddes/core'
import {AwsStore} from './AwsStore'

export class AwsStoreBatchMutator extends BatchMutator {
  public drained: Promise<void> = Promise.resolve()

  protected store: AwsStore
  protected queue: object[] = []
  protected maxItemsPerRequest: number
  protected queueSize: number
  protected pendingAddToQueuePromises: Array<() => void> = []
  protected workerLoopRunning: boolean = false

  constructor(params: {
    store: AwsStore
    maxItemsPerRequest?: number
    queueSize?: number
  }) {
    super()
    const {store, maxItemsPerRequest = 25, queueSize = 50} = params
    this.store = store
    this.maxItemsPerRequest = maxItemsPerRequest
    this.queueSize = queueSize
  }

  public async delete(commits: CommitOrCommits) {
    for await (const commit of this.asIterable(commits)) {
      const {a, k} = await this.store.marshallCommit(commit)
      await this.addToQueue({DeleteRequest: {Key: {a, k}}})
    }
  }

  public async put(commits: CommitOrCommits): Promise<void> {
    for await (const commit of this.asIterable(commits)) {
      await this.addToQueue({
        PutRequest: {Item: await this.store.marshallCommit(commit)},
      })
    }
  }

  public async addToQueue(item: object) {
    if (this.queue.length >= this.queueSize) {
      await new Promise(resolve => this.pendingAddToQueuePromises.push(resolve))
    }

    this.queue.push(item)

    if (!this.workerLoopRunning) {
      this.workerLoop()
    }
  }

  public async workerLoop() {
    if (this.workerLoopRunning) {
      return
    }
    this.workerLoopRunning = true

    let resolveFn: () => void

    this.drained = new Promise(resolve => (resolveFn = resolve))

    while (this.queue.length) {
      const items = this.queue.splice(0, this.maxItemsPerRequest)

      while (
        this.queue.length <= this.queueSize &&
        this.pendingAddToQueuePromises.length
      ) {
        this.pendingAddToQueuePromises.shift()!()
      }

      const {UnprocessedItems} = await this.store.dynamodb
        .batchWriteItem({
          RequestItems: {
            [this.store.tableName]: items,
          },
        })
        .promise()

      const itemsToRequeue =
        UnprocessedItems && UnprocessedItems[this.store.tableName]

      if (itemsToRequeue) {
        this.queue.splice(0, 0, ...itemsToRequeue)
      }
    }
    this.workerLoopRunning = false

    if (resolveFn!) {
      resolveFn!()
    }
  }
}
