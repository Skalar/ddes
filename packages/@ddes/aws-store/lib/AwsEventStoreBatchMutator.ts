/**
 * @module @ddes/aws-store
 */

import {BatchMutator, Commit} from '@ddes/core'
import * as debug from 'debug'
import * as equal from 'fast-deep-equal'
import AwsEventStore from './AwsEventStore'
import {AwsEventStoreBatchMutatorQueueItem, MarshalledCommit} from './types'
import {marshallCommit} from './utils'

/**
 * @hidden
 */
const log = debug('@ddes/aws-store:AwsEventStoreBatchMutator')

export default class AwsEventStoreBatchMutator extends BatchMutator<
  MarshalledCommit
> {
  protected store: AwsEventStore
  protected queue: Set<AwsEventStoreBatchMutatorQueueItem> = new Set()
  protected maxItemsPerRequest: number = 25
  protected processQueueRunning: boolean = false
  protected capacityLimit?: number
  protected bufferSize: number = 100
  protected remainingCapacity?: {
    second: number
    units: number
  }

  constructor(params: {store: AwsEventStore; capacityLimit?: number}) {
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
    commits: Array<Commit | MarshalledCommit> | Commit | MarshalledCommit
  ) {
    for await (const commit of this.asIterable(commits)) {
      const marshalledCommit =
        commit instanceof Commit ? await marshallCommit(commit) : commit

      const {s, v} = marshalledCommit

      await this.addToQueue(
        {DeleteRequest: {Key: {s, v}}},
        this.capacityUnitsForItem(marshalledCommit)
      )
    }
  }

  public async put(
    commits: Array<Commit | MarshalledCommit> | Commit | MarshalledCommit
  ): Promise<void> {
    for await (const commit of this.asIterable(commits)) {
      const marshalledCommit =
        commit instanceof Commit ? await marshallCommit(commit) : commit

      await this.addToQueue(
        {
          PutRequest: {
            Item: marshalledCommit,
          },
        },
        this.capacityUnitsForItem(marshalledCommit)
      )
    }
  }

  public get drained() {
    return Promise.all(
      [...this.queue.values()].map(queueItem => queueItem.processedPromise)
    ).then(res => undefined)
  }

  public get itemsBeingProcessed() {
    return [...this.queue].filter(i => i.processing)
  }

  public get pendingItems() {
    return [...this.queue].filter(i => !i.processing)
  }

  private addToQueue(item: object, capacityUnits: number) {
    let startedResolver!: () => void
    const startedPromise = new Promise(resolve => {
      startedResolver = resolve
    })
    let processedResolver!: () => void
    const processedPromise = new Promise(resolve => {
      processedResolver = resolve
    })

    this.queue.add({
      startedPromise,
      startedResolver,
      processedPromise,
      processedResolver,
      item,
      throttleCount: 0,
      processing: false,
      capacityUnits,
    })

    let promise
    if (this.queue.size >= this.bufferSize) {
      promise = [...this.queue.values()][this.queue.size - this.bufferSize]
        .startedPromise
    } else {
      promise = Promise.resolve()
    }

    this.processQueue()

    return promise
  }

  private async processQueue() {
    if (this.processQueueRunning || !this.queue.size) {
      return
    } else {
      this.processQueueRunning = true
    }

    if (this.pendingItems.length < this.maxItemsPerRequest) {
      await new Promise(resolve => setTimeout(resolve, 20))
    }

    try {
      let capacityLeft = Infinity
      const thisSecond = Math.floor(Date.now() / 1000)

      if (this.capacityLimit) {
        if (
          !this.remainingCapacity ||
          this.remainingCapacity.second !== thisSecond
        ) {
          this.remainingCapacity = {
            second: thisSecond,
            units: this.capacityLimit,
          }
        }

        capacityLeft = this.remainingCapacity.units
      }

      let queueItemsToProcess: AwsEventStoreBatchMutatorQueueItem[] = []

      let processingCount = 0
      log(`pending items ${this.pendingItems.length}`)
      for (const queueItem of this.queue) {
        if (queueItem.processing) {
          processingCount++
          continue
        }

        if (queueItemsToProcess.length >= this.maxItemsPerRequest) {
          this.sendRequest(queueItemsToProcess)
          queueItemsToProcess = []
        }

        if (capacityLeft >= queueItem.capacityUnits) {
          queueItem.startedResolver()
          capacityLeft -= queueItem.capacityUnits
          queueItemsToProcess.push(queueItem)
          queueItem.processing = true
        } else {
          if (
            this.capacityLimit &&
            queueItem.capacityUnits > this.capacityLimit
          ) {
            throw new Error(
              `Commit required ${
                queueItem.capacityUnits
              } which is higher than the capacity consumption target`
            )
          }
          if (queueItemsToProcess.length === 0) {
            // we didn't have capacity for anything, wait remainder of window
            setTimeout(
              () => this.processQueue(),
              1000 - (Date.now() - thisSecond * 1000)
            )
            return
          }
        }
      }

      if (queueItemsToProcess.length) {
        this.sendRequest(queueItemsToProcess)
      }

      this.remainingCapacity = {
        second: thisSecond,
        units: capacityLeft,
      }

      // here was send request
    } finally {
      this.processQueueRunning = false
    }
  }

  private sendRequest(queueItemsToSend: AwsEventStoreBatchMutatorQueueItem[]) {
    log(`Sending request with ${queueItemsToSend.length} items`)
    const requestPromise = this.store.dynamodb
      .batchWriteItem({
        RequestItems: {
          [this.store.tableName]: queueItemsToSend.map(i => i.item),
        },
      })
      .promise()

    requestPromise.then(({UnprocessedItems}) => {
      const itemsToRequeue =
        (UnprocessedItems && UnprocessedItems[this.store.tableName]) || []

      for (const queueItem of queueItemsToSend) {
        if (itemsToRequeue.find(item => equal(item, queueItem.item))) {
          this.throttleCount++
          queueItem.processing = false
        } else {
          switch (Object.keys(queueItem.item)[0]) {
            case 'DeleteRequest': {
              this.deleteCount++
              break
            }
            case 'PutRequest': {
              this.writeCount++
              break
            }
          }
          queueItem.processedResolver()
          this.queue.delete(queueItem)
        }
      }
      this.processQueue()
    })
  }

  private capacityUnitsForItem(item: MarshalledCommit) {
    let bytes = 0
    for (const [key, val] of Object.entries(item)) {
      bytes += Buffer.from(key, 'utf8').length
      const valueType = Object.keys(val)[0]
      switch (valueType) {
        case 'S': {
          bytes += Buffer.from(val.S!, 'utf8').length
          break
        }
        case 'B': {
          bytes += (val.B as Buffer).length
          break
        }
        case 'N': {
          bytes += Math.min(parseFloat(val.N!).toString(2).length, 38)
          break
        }
      }
    }

    return Math.ceil(bytes / 1024)
  }
}
