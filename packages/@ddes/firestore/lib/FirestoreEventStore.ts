import {
  AggregateKey,
  AggregateType,
  Commit,
  EventStore,
  VersionConflictError,
} from '@ddes/core'
import {Firestore} from '@google-cloud/firestore'
import * as debug from 'debug'
import FirestoreEventStoreBatchMutator from './FirestoreEventStoreBatchMutator'
import FirestoreEventStoreQueryResponse from './FirestoreEventStoreQueryResponse'
import {FirestoreConfig} from './types'
import {
  chronologicalPartitionIterator,
  marshallCommit,
  stringcrementor,
} from './utils'
import asyncIterateStream from './utils/asyncIterateStream'
import deleteQueryBatch from './utils/deleteQueryBatch'

/**
 * @module @ddes/firestore
 */
export default class FirestoreEventStore extends EventStore {
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

  public async setup(): Promise<void> {
    return Promise.resolve()
  }

  public async teardown(): Promise<void> {
    const collection = this.firestore.collection(this.tableName)
    const query = collection.orderBy('__name__')

    await deleteQueryBatch(this.firestore, query)
  }

  public async bestEffortCount(): Promise<number> {
    let count = 0
    const collection = this.firestore.collection(this.tableName)
    const query = collection.orderBy('__name__')

    for await (const item of asyncIterateStream(query.stream(), true)) {
      count++
    }

    return count
  }

  public async commit(commit: Commit): Promise<void> {
    const {aggregateType, aggregateKey, aggregateVersion} = commit

    const marshalledCommit = await marshallCommit(commit, true)
    const key = this.key(aggregateType, aggregateKey, aggregateVersion)
    this.debug(`Creating item: ${key}`)
    try {
      await this.firestore
        .collection(this.tableName)
        .doc(key)
        .create(marshalledCommit)
    } catch (error) {
      if (error.code === 6) {
        throw new VersionConflictError(
          `${commit.aggregateType}[${
            commit.aggregateKey
          }] already has a version ${commit.aggregateVersion} commit`
        )
      }
      throw error
    }
    this.debug(`Created item: ${key}`)
  }

  public key(type: AggregateType, key: AggregateKey, version: number) {
    return `Commit:${type}:${key}:${version}`
  }

  public queryAggregateCommits(
    type: string,
    key: string,
    options: {
      consistentRead?: boolean
      minVersion?: number
      maxVersion?: number
      maxTime?: number | Date
      limit?: number
      descending?: boolean
    } = {}
  ): FirestoreEventStoreQueryResponse {
    const {
      minVersion = 1,
      maxVersion = Number.MAX_SAFE_INTEGER,
      maxTime,
      descending,
      limit,
    } = options

    if (!type || !key) {
      throw new Error('You need to specify "type" and "key"')
    }

    const collection = this.firestore.collection(this.tableName)
    const query = collection
      .where('s', '==', [type, key].join(':'))
      .where('v', '>=', minVersion)
      .where('v', '<=', maxVersion)
      .orderBy('v', descending ? 'desc' : 'asc')
      .orderBy('t', descending ? 'desc' : 'asc')

    if (limit) {
      query.limit(limit)
    }

    this.debug(`Querying aggregate commits: s == ${[type, key].join(':')}`)
    const debugLogger = this.debug
    let timestamp = 0
    return new FirestoreEventStoreQueryResponse(
      (async function*() {
        for await (const result of asyncIterateStream(query.stream(), true)) {
          const data = result.data()
          timestamp = data.t
          if (maxTime && timestamp > maxTime.valueOf()) {
            return
          }
          debugLogger(`Yielding item: ${result.id}`)
          yield data
        }
      })()
    )
  }

  public scanAggregateInstances(
    type: string,
    options: {instanceLimit?: number} = {}
  ): FirestoreEventStoreQueryResponse {
    const store = this
    this.debug(`Scanning aggregate instances`)
    const debugLogger = this.debug
    return new FirestoreEventStoreQueryResponse(
      (async function*() {
        const instances: string[] = []
        const collection = store.firestore.collection(store.tableName)
        const query = collection
          .where('a', '==', type)
          .orderBy('s')
          .orderBy('v')

        debugLogger(`Querying items: a == ${type}`)

        for await (const result of asyncIterateStream(query.stream(), true)) {
          const data = result.data()
          if (!instances.includes(data.s)) {
            instances.push(data.s)
          }

          if (
            options.instanceLimit &&
            instances.length > options.instanceLimit
          ) {
            return
          }
          debugLogger(`Yielding item: ${result.id}`)
          yield data
        }
      })()
    )
  }

  public async getAggregateHeadCommit(
    type: string,
    key: string
  ): Promise<Commit | null> {
    for await (const resultSet of this.queryAggregateCommits(type, key, {
      descending: true,
      limit: 1,
    })) {
      for await (const commit of resultSet.commits) {
        return commit
      }
    }

    return null
  }

  public async getHeadCommit(
    chronologicalGroup?: string,
    startDate?: Date
  ): Promise<Commit | null> {
    const min = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)

    for await (const commit of this.chronologicalQuery({
      group: chronologicalGroup || 'default',
      min,
      descending: true,
    }).commits) {
      return commit
    }

    return null
  }

  public scan(options?: {
    totalSegments?: number
    segment?: number
    filterAggregateTypes?: string[]
    startKey?: any
    limit?: number
    capacityLimit?: number
  }): FirestoreEventStoreQueryResponse {
    const {segment = 0, totalSegments = 1, ...rest} = options || {}

    const query = this.firestore.collection(this.tableName)
    this.debug('Scanning items')
    return new FirestoreEventStoreQueryResponse(
      (async function*() {
        for await (const result of asyncIterateStream(query.stream(), true)) {
          yield result.data()
        }
      })()
    )
  }

  public createBatchMutator(
    params: {capacityLimit?: number} = {}
  ): FirestoreEventStoreBatchMutator {
    const {capacityLimit} = params
    return new FirestoreEventStoreBatchMutator({store: this, capacityLimit})
  }

  public chronologicalQuery(params: {
    group?: string
    min: string | Date
    max?: string | Date
    descending?: boolean
    limit?: number
    exclusiveMin?: boolean
    exclusiveMax?: boolean
    timeDriftCompensation?: number
    filterAggregateTypes?: AggregateType[]
  }): FirestoreEventStoreQueryResponse {
    const store = this
    const {
      group = 'default',
      min,
      descending,
      limit,
      exclusiveMin,
      exclusiveMax,
      timeDriftCompensation = 500,
    } = params
    const {max = new Date(Date.now() + timeDriftCompensation)} = params
    if (!min) {
      throw new Error('You must specify the "min" parameter')
    }

    const maxDate =
      max instanceof Date
        ? max
        : new Date(max.replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))
    const maxSortKey =
      max instanceof Date ? max.toISOString().replace(/[^0-9]/g, '') + ';' : max

    const minDate =
      min instanceof Date
        ? min
        : new Date(min.replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

    const minSortKey =
      min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

    return new FirestoreEventStoreQueryResponse(
      (async function*() {
        let commitCount = 0

        for (const partition of chronologicalPartitionIterator({
          start: minDate,
          end: maxDate,
          group,
          descending,
        })) {
          const collection = store.firestore.collection(store.tableName)
          const query = collection
            .where('p', '==', partition.key)
            .where(
              'g',
              '>=',
              exclusiveMin ? stringcrementor(minSortKey) : minSortKey
            )
            .where(
              'g',
              '<=',
              exclusiveMax ? stringcrementor(maxSortKey, -1) : maxSortKey
            )
            .orderBy('g', descending ? 'desc' : 'asc')
            .orderBy('t', descending ? 'desc' : 'asc')
          const querySnapshot = await query.get()
          for await (const result of querySnapshot.docs) {
            const data = result.data()
            if (
              params.filterAggregateTypes &&
              !params.filterAggregateTypes.includes(data.a)
            ) {
              continue
            }

            commitCount++

            if (limit && commitCount > limit) {
              return
            }

            yield data
          }
        }
      })()
    )
  }

  public toString(): string {
    return `FirestoreEventStore:${this.projectId}`
  }
}
