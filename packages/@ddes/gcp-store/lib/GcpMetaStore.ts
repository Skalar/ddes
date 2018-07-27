import {MetaStore, MetaStoreKey} from '@ddes/core'
import Datastore = require('@google-cloud/datastore')
import {asyncIterateStream} from 'async-iterate-stream/asyncIterateStream'
import {DatastoreConfiguration, MetaItem} from './types'
import {gcpRequest, zipper} from './utils'

export default class GcpMetaStore extends MetaStore {
  public projectId!: string
  public tableName!: string
  public datastore: Datastore

  public kind: string = 'Meta'

  constructor(config: DatastoreConfiguration) {
    super()
    if (!config.projectId) {
      throw new Error(`Missing 'projectId'`)
    }

    if (!config.tableName) {
      throw new Error(`Missing 'tableName'`)
    }

    Object.assign(this, config)

    this.datastore = new Datastore({
      projectId: config.projectId,
      apiEndpoint: config.endpoint,
      namespace: config.tableName,
    })
  }

  public key(key: MetaStoreKey) {
    return this.datastore.key({
      namespace: this.tableName,
      path: [this.kind, key.join(':')],
    })
  }

  public async get(key: MetaStoreKey) {
    const metaKey = this.key(key)
    console.log(`Getting: ${key[0]}, ${key[1]}`)
    const item = (await this.datastore.get(metaKey))[0] as MetaItem

    if (!item) {
      return null
    }

    item.v = JSON.parse((await zipper.unzip(item.v)) as string)

    if (item.x && item.x <= Math.floor(Date.now() / 1000)) {
      return null
    }

    return item.v
  }

  public async put(
    key: MetaStoreKey,
    value: any,
    options: {expiresAt?: Date} = {}
  ) {
    const metaKey = this.key(key)
    console.log(`Putting: ${key[0]}, ${key[1]}`)
    await this.datastore.save({
      key: metaKey,
      data: {
        p: key[0],
        s: key[0],
        v: (await zipper.zip(JSON.stringify(value))) as string,
      },
    })
  }

  public async delete(key: MetaStoreKey) {
    const metaKey = this.key(key)

    await this.datastore.delete(metaKey)
  }

  public async *list(primaryKey: string): AsyncIterableIterator<[string, any]> {
    let done = false
    const iterator = asyncIterateStream(
      gcpRequest(this, {
        filters: [
          {
            property: 'p',
            operator: '=',
            value: primaryKey,
          },
        ],
      }),
      true
    )

    do {
      const {value: item, done: isDone} = await iterator.next()

      if (!item) {
        continue
      }

      item.v = JSON.parse((await zipper.unzip(item.v)) as string)

      if (item.x && item.x <= Math.floor(Date.now() / 1000)) {
        continue
      }

      yield [item.s, item.v]

      done = isDone
    } while (!done)
  }

  public async setup() {
    // No setup needed
  }

  public async teardown() {
    // Delete all meta rows
    let keys = []

    for await (const item of asyncIterateStream(gcpRequest(this), true)) {
      keys.push(item[this.datastore.KEY])

      if (keys.length === 100) {
        await this.datastore.delete(keys)
        keys = []
      }
    }

    if (keys.length > 0) {
      await this.datastore.delete(keys)
      keys = []
    }
  }
}
