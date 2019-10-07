import {MetaStore, MetaStoreKey} from '@ddes/core'
import {Datastore} from '@google-cloud/datastore'
import {DatastoreConfiguration, MetaItem} from './types'
import {asyncIterateStream, gcpRequest} from './utils'

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
    const item = (await this.datastore.get(metaKey))[0] as MetaItem

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
    key: MetaStoreKey,
    value: any,
    options: {expiresAt?: Date} = {}
  ) {
    const metaKey = this.key(key)

    await this.datastore.save({
      key: metaKey,
      data: {
        p: key[0],
        s: key[1],
        v: JSON.stringify(value),
        ...(options.expiresAt && {
          x: Math.floor(options.expiresAt.valueOf() / 1000).toString(),
        }),
      },
    })
  }

  public async delete(key: MetaStoreKey) {
    const metaKey = this.key(key)

    await this.datastore.delete(metaKey)
  }

  public async *list(primaryKey: string): AsyncIterableIterator<[string, any]> {
    for await (const item of asyncIterateStream(
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
    )) {
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
