import {AggregateSnapshot, SnapshotStore, utils} from '@ddes/core'
import {Firestore} from '@google-cloud/firestore'
import {FirestoreConfig} from './types'
import deleteQueryBatch from './utils/deleteQueryBatch'
import zipper from './utils/zipper'

/**
 * @module @ddes/firestore
 */
export default class FirestoreSnapshotStore extends SnapshotStore {
  public projectId!: string
  public tableName!: string
  public firestore: Firestore

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

    this.firestore = new Firestore({
      ...config,
      projectId,
    })
  }

  public setup(): Promise<void> {
    return Promise.resolve()
  }

  public async teardown(): Promise<void> {
    const collection = this.firestore.collection(this.tableName)
    const query = collection.orderBy('__name__')

    await deleteQueryBatch(this.firestore, query)
  }

  public key(type: string, key: string) {
    return `Snapshot:${type}:${key}`
  }

  public async writeSnapshot(
    type: string,
    key: string,
    payload: {
      version: number
      state: object
      timestamp: number
      compatibilityChecksum: string
    }
  ): Promise<void> {
    const data = (await zipper.zip(JSON.stringify(payload))) as string
    const snapshotKey = this.key(type, key)

    await this.firestore
      .collection(this.tableName)
      .doc(snapshotKey)
      .create({data})
  }

  public async readSnapshot(
    type: string,
    key: string
  ): Promise<AggregateSnapshot | null> {
    try {
      const snapshotKey = this.key(type, key)
      const snapshot = await this.firestore
        .collection(this.tableName)
        .doc(snapshotKey)
        .get()

      const data = snapshot.data()

      if (!data) {
        return null
      }

      const {
        version,
        state,
        timestamp: timestampString,
        compatibilityChecksum,
      } = JSON.parse((await zipper.unzip(data.data)) as string)
      return {
        version,
        state,
        timestamp: utils.toTimestamp(timestampString),
        compatibilityChecksum,
      }
    } catch (e) {
      return null
    }
  }
  public deleteSnapshots(): Promise<void> {
    return this.teardown()
  }
}
