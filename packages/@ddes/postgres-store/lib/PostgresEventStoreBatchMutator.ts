/**
 * @module @ddes/postgres-store
 */
import {sql, SQLQuery} from '@databases/pg'
import {BatchMutator, Commit} from '@ddes/core'
import debug from 'debug'

import PostgresEventStore from './PostgresEventStore'
import {PostgresStoreBatchMutatorQueueItem, Row} from './types'

/**
 * @hidden
 */
const log = debug('@ddes/aws-store:AwsEventStoreBatchMutator')

export default class PostgresEventStoreBatchMutator extends BatchMutator<Row> {
  protected store: PostgresEventStore
  protected bufferSize = 100
  protected maxItemsPerRequest = 1000
  protected processQueueRunning = false
  protected queue: Set<PostgresStoreBatchMutatorQueueItem> = new Set()

  constructor(params: {store: PostgresEventStore; capacityLimit?: number}) {
    super()
    const {store, capacityLimit} = params

    this.store = store

    if (capacityLimit) {
      this.bufferSize = capacityLimit
    }
  }

  public get drained() {
    return Promise.all([...this.queue.values()].map(queueItem => queueItem.processedPromise)).then(() => undefined)
  }

  public get itemsBeingProcessed() {
    return [...this.queue].filter(i => i.processing)
  }

  public get pendingItems() {
    return [...this.queue].filter(i => !i.processing)
  }

  public async put(commits: Array<Row | Commit> | Row | Commit) {
    for await (const item of this.asIterable(commits)) {
      const {aggregateType, aggregateKey, aggregateVersion, sortKey, chronologicalGroup, events, timestamp, expiresAt} =
        item instanceof Commit
          ? item
          : new Commit({
              ...item,
              chronologicalGroup: item.partitionKey.substr(8, item.partitionKey.length - 8),
            })

      const query = sql`
        INSERT INTO ${sql.ident(this.store.tableName)} VALUES(
          ${aggregateType},
          ${aggregateKey},
          ${aggregateVersion},
          ${sortKey},
          ${chronologicalGroup},
          ${JSON.stringify(events)},
          ${timestamp}${expiresAt ? sql`, ${new Date(expiresAt).getTime()}` : sql``})
        ON CONFLICT (aggregate_type, aggregate_key, aggregate_version) DO UPDATE
          SET "events" = excluded."events",
          "chronological_group" = excluded."chronological_group",
          "sort_key" = excluded."sort_key",
          "timestamp" = excluded."timestamp",
          "expires_at" = excluded."expires_at"
      `

      await this.addToQueue(query)
    }
  }

  public async delete(commits: Array<Row | Commit> | Row | Commit) {
    for await (const item of this.asIterable(commits)) {
      const {aggregateType, aggregateKey, aggregateVersion} = item

      const query = sql`
        DELETE FROM ${sql.ident(this.store.tableName)}
        WHERE "aggregate_type" = ${aggregateType}
        AND "aggregate_key" = ${aggregateKey}
        AND "aggregate_version" = ${aggregateVersion}
      `

      await this.addToQueue(query)
    }
  }

  private addToQueue(query: SQLQuery) {
    let startedResolver!: (value?: unknown) => void
    const startedPromise = new Promise(resolve => {
      startedResolver = resolve
    })
    let processedResolver!: (value?: unknown) => void
    const processedPromise = new Promise(resolve => {
      processedResolver = resolve
    })

    this.queue.add({
      startedPromise,
      startedResolver,
      processedPromise,
      processedResolver,
      query,
      processing: false,
    })

    let promise
    if (this.queue.size >= this.bufferSize) {
      promise = [...this.queue.values()][this.queue.size - this.bufferSize].startedPromise
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
      let queueItemsToProcess: PostgresStoreBatchMutatorQueueItem[] = []

      log(`pending items ${this.pendingItems.length}`)
      for (const queueItem of this.queue) {
        if (queueItemsToProcess.length >= this.maxItemsPerRequest) {
          this.sendRequest(queueItemsToProcess)
          queueItemsToProcess = []
        }

        queueItem.startedResolver()
        queueItemsToProcess.push(queueItem)
        queueItem.processing = true
      }

      if (queueItemsToProcess.length) {
        this.sendRequest(queueItemsToProcess)
      }
    } finally {
      this.processQueueRunning = false
    }
  }

  private async sendRequest(queueItems: PostgresStoreBatchMutatorQueueItem[]) {
    log(`Starting transaction with ${queueItems.length} items`)
    const {pool} = this.store

    try {
      await pool.tx(async client => {
        for (const queueItem of queueItems) {
          await client.query(queueItem.query)
        }
      })
    } catch (e) {
      log(`Failed transaction with ${queueItems.length} items, rolling back`)
    } finally {
      for (const queueItem of queueItems) {
        queueItem.processing = false
        queueItem.processedResolver()
        this.queue.delete(queueItem)
        if (queueItem.query.toString().startsWith('INSERT')) {
          this.writeCount++
        } else {
          this.deleteCount++
        }
      }
    }
  }
}
