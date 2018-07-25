import {
  AggregateKey,
  AggregateSnapshot,
  AggregateType,
  SnapshotStore,
  Timestamp,
  utils as coreutils,
} from '@ddes/core'
import Datastore = require('@google-cloud/datastore')
import {asyncIterateStream} from 'async-iterate-stream/asyncIterateStream'
import {Readable} from 'stream'
import {DatastoreConfiguration, Snapshot, StoreQueryParams} from './types'
import {gcpRequest, zipper} from './utils'

export default class GcpSnapshotStore extends SnapshotStore {
  public projectId!: string
  public tableName!: string
  public datastore: Datastore
  public kind: string = 'Snapshot'

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

  public async setup() {
    // No setup needed
  }

  public async teardown() {
    // Delete all snapshots
    await this.deleteSnapshots()
  }

  public key(type: AggregateType, key: AggregateKey) {
    return this.datastore.key({
      namespace: this.tableName,
      path: [this.kind, [type, key].join(':')],
    })
  }

  public async readSnapshot(
    type: AggregateType,
    key: AggregateKey
  ): Promise<AggregateSnapshot | null> {
    try {
      const snapshotKey = this.key(type, key)
      const snapshot = (await this.datastore.get(snapshotKey))[0] as Snapshot

      if (!snapshot || !snapshot.data) {
        return null
      }

      const {
        version,
        state,
        timestamp: timestampString,
        compatibilityChecksum,
      } = JSON.parse((await zipper.unzip(snapshot.data)) as string)

      return {
        version,
        state,
        timestamp: coreutils.toTimestamp(timestampString),
        compatibilityChecksum,
      }
    } catch (error) {
      return null
    }
  }

  public async deleteSnapshots() {
    // Delete all snapshots
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

  public async writeSnapshot(
    type: string,
    key: string,
    payload: {
      version: number
      state: object
      timestamp: Timestamp
      compatibilityChecksum: string
    }
  ) {
    const data = (await zipper.zip(JSON.stringify(payload))) as string
    const snapshotKey = this.key(type, key)

    await this.datastore.save({key: snapshotKey, data: {data}})
  }
}
