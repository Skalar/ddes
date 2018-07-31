import {BatchMutator, Commit} from '@ddes/core'
import {DatastoreKey} from '@google-cloud/datastore/entity'
import * as debug from 'debug'
import GcpEventStore from './GcpEventStore'
import {GcpEventStoreBatchMutatorQueueItem, MarshalledCommit} from './types'
import {marshallCommit} from './utils'

/**
 * @hidden
 */
const log = debug('@ddes/gcp-store:GcpEventStoreBatchMutator')

export default class GcpEventStoreBatchMutator extends BatchMutator<
  MarshalledCommit
> {
  protected store: GcpEventStore
  protected queue: Set<GcpEventStoreBatchMutatorQueueItem> = new Set()
  protected maxItemsPerRequest: number = 50
  protected processQueueRunning: boolean = false
  protected capacityLimit?: number
  protected bufferSize: number = 100
  protected remainingCapacity?: {second: number; units: number}

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
      const marshalledCommit =
        commit instanceof Commit
          ? ((await marshallCommit(commit, true)) as MarshalledCommit)
          : commit

      const {s, v} = marshalledCommit

      const key = this.store.key(s.split(':')[0], s.split(':')[1], v)

      await this.addToQueue(
        {delete: {key}},
        this.capacityUnitsForItem(marshalledCommit as MarshalledCommit)
      )
    }
  }

  public async put(
    commits: Array<Commit | MarshalledCommit | Commit | MarshalledCommit>
  ): Promise<void> {
    for await (const commit of this.asIterable(commits)) {
      const marshalledCommit =
        commit instanceof Commit
          ? ((await marshallCommit(commit, true)) as MarshalledCommit)
          : commit

      const data =
        commit instanceof Commit
          ? ((await marshallCommit(commit)) as MarshalledCommit)
          : commit

      const {s, v} = marshalledCommit

      const key = this.store.key(s.split(':')[0], s.split(':')[1], v)

      await this.addToQueue(
        {put: {key, data}},
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

  private addToQueue(item: any, capacityUnits: number) {
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

      let queueItemsToProcess: GcpEventStoreBatchMutatorQueueItem[] = []

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

      this.remainingCapacity = {second: thisSecond, units: capacityLeft}
    } finally {
      this.processQueueRunning = false
    }
  }

  private sendRequest(queueItemsToSend: GcpEventStoreBatchMutatorQueueItem[]) {
    log(`Sending request ${queueItemsToSend.length} items`)

    const transaction = this.store.datastore.transaction()
    const deletes: DatastoreKey[] = []
    const puts: Array<{key: DatastoreKey; data: any}> = []

    queueItemsToSend.forEach(item => {
      if (item.item.delete) {
        deletes.push(item.item.delete.key)
        return
      } else if (item.item.put) {
        puts.push(item.item.put)
        return
      }
    })

    transaction
      .run()
      .then(() => {
        transaction.save(puts)
        transaction.delete(deletes)

        return transaction.commit()
      })
      .then(() => {
        this.deleteCount += deletes.length
        this.writeCount += puts.length
        for (const queueItem of queueItemsToSend) {
          queueItem.processedResolver()
          this.queue.delete(queueItem)
        }
      })
      .catch(() => {
        this.throttleCount++
        queueItemsToSend.forEach(item => (item.processing = false))
        return transaction.rollback()
      })
  }

  private capacityUnitsForItem(item: MarshalledCommit) {
    let bytes = 0
    for (const [key, val] of Object.entries(item)) {
      bytes += Buffer.from(key, 'utf8').length

      switch (typeof val) {
        case 'string': {
          bytes += Buffer.from(val, 'utf8').length + 1
          break
        }
        case 'object': {
          // assuming it's a buffer
          bytes += (val as Buffer).length
          break
        }
        case 'number': {
          bytes += Math.min(parseFloat(val).toString(2).length, 38)
          break
        }
      }
    }

    return Math.ceil(bytes / 1024)
  }
}
