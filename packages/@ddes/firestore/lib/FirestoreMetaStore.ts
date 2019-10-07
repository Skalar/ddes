import {MetaStore, MetaStoreKey} from '@ddes/core'
import {Firestore} from '@google-cloud/firestore'
import * as debug from 'debug'
import {FirestoreConfig} from './types'
import {asyncIterateStream} from './utils'
import deleteQueryBatch from './utils/deleteQueryBatch'

/**
 * @module @ddes/firestore
 */
export default class FirestoreMetaStore extends MetaStore {
  public projectId!: string
  public tableName!: string
  public firestore: Firestore

  protected debug: debug.IDebugger

  constructor(config: FirestoreConfig) {
    super()
    const projectId = config.projectId || process.env.GCLOUD_PROJECT
    const tableName = config.tableName

    if (!projectId) {
      throw new Error(`Missing 'projectId'`)
    }

    if (!config.tableName) {
      throw new Error(`Missing 'tableName'`)
    }

    Object.assign(this, {projectId, tableName})

    this.debug = debug(`DDES.${this.constructor.name}`)
    this.firestore = new Firestore({
      ...config,
      projectId,
    })
  }

  public key(key: MetaStoreKey) {
    return `Meta:${key.join(':')}`
  }

  public async get(key: [string, string]): Promise<any> {
    const metaKey = this.key(key)
    this.debug(`Getting item data: ${metaKey}`)
    const itemRef = this.firestore.collection(this.tableName).doc(metaKey)
    const data = await itemRef.get()
    const item = data.data()

    if (!item) {
      return null
    }

    item.v = JSON.parse(item.v)

    if (item.x && item.x <= Math.floor(Date.now() / 1000)) {
      return null
    }

    return item.v
  }

  public async put(
    key: [string, string],
    value: any,
    options: {expiresAt?: number | Date} = {}
  ): Promise<void> {
    const metaKey = this.key(key)
    this.debug(`Setting item data: ${metaKey}`)

    await this.firestore
      .collection(this.tableName)
      .doc(metaKey)
      .set({
        p: key[0],
        s: key[1],
        v: JSON.stringify(value),
        ...(options.expiresAt && {
          x: Math.floor(options.expiresAt.valueOf() / 1000).toString(),
        }),
      })
  }

  public async delete(key: [string, string]): Promise<void> {
    const metaKey = this.key(key)
    this.debug(`Deleting item data: ${metaKey}`)
    await this.firestore
      .collection(this.tableName)
      .doc(metaKey)
      .delete()
  }

  public async *list(
    partitionKey: string
  ): AsyncIterableIterator<[string, any]> {
    const query = this.firestore
      .collection(this.tableName)
      .where('p', '==', partitionKey)

    this.debug(`Listing items: p == ${partitionKey}`)
    for await (const result of asyncIterateStream(query.stream(), true)) {
      this.debug(`Got item: ${result.id}`)
      const item = result.data()
      if (!item) {
        continue
      }

      item.v = JSON.parse(item.v)
      if (item.x && item.x <= Math.floor(Date.now() / 1000)) {
        continue
      }

      yield [item.s, item.v]
    }
  }

  public setup(): Promise<void> {
    // No setup needed
    return Promise.resolve()
  }

  public async teardown(): Promise<void> {
    const collection = this.firestore.collection(this.tableName)
    const query = collection.orderBy('__name__')

    await deleteQueryBatch(this.firestore, query)
  }
}
