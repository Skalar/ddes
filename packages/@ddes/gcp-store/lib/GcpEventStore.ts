import {AggregateKey, AggregateType, Commit, EventStore} from '@ddes/core'
import Datastore = require('@google-cloud/datastore')
import {asyncIterateStream} from 'async-iterate-stream/asyncIterateStream'
import {Readable} from 'stream'
import GcpEventStoreBatchMutator from './GcpEventStoreBatchMutator'
import GcpEventStoreQueryResponse from './GcpEventStoreQueryResponse'
import {
  DatastoreConfiguration,
  MarshalledCommit,
  StoreQueryParams,
} from './types'
import {marshallCommit} from './utils'

export default class GcpEventStore extends EventStore {
  public projectId!: string
  public tableName!: string
  public datastore: Datastore

  private kind: string = 'Commit'

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

  public toString() {
    return `GcpEventStore:${this.projectId}`
  }

  public async setup() {
    // No setup needed
  }

  public async teardown() {
    // Delete all commits
    // Might need to do some cleanup?
    let keys = []

    for await (const item of asyncIterateStream(this.request(), true)) {
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

  public async bestEffortCount() {
    let count = 0

    for await (const item of asyncIterateStream(this.request(), true)) {
      count++
    }

    return count || 0
  }

  public async commit(commit: Commit) {
    const {aggregateType, aggregateKey, aggregateVersion} = commit
    const marshalledCommit = await marshallCommit(commit)
    const key = this.datastore.key({
      namespace: this.tableName,
      path: [
        this.kind,
        [aggregateType, aggregateKey, aggregateVersion].join(':'),
      ],
    })

    try {
      await this.datastore.save({
        key,
        data: marshalledCommit,
      })
    } catch (e) {
      throw new Error(`Commit ${commit.aggregateVersion} failed: ${e.message}`)
    }
  }

  public async getAggregateHeadCommit(type: string, key: AggregateKey) {
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

  public async getHeadCommit(chronologicalGroup?: string, startDate?: Date) {
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

  public chronologicalQuery(params: {
    group?: string
    min: string | Date
    max?: string | Date
    exclusiveMin?: boolean
    exclusiveMax?: boolean
    descending?: boolean
    limit?: number
    timeDriftCompensation?: number
    filterAggregateTypes?: AggregateType[]
  }) {
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

    return new GcpEventStoreQueryResponse(
      (async function*() {
        const commitCount = 0

        const queryParams: StoreQueryParams = {
          orders: [
            {
              property: 't',
              options: {descending: descending || false},
            },
          ],
          filters: [
            {property: 't', operator: '>', value: minDate},
            {property: 't', operator: '<', value: maxDate},
          ],
        }

        if (limit) {
          queryParams.limit = limit
        }

        for await (const queryResult of asyncIterateStream(
          store.request(queryParams),
          true
        )) {
          yield queryResult as MarshalledCommit
        }
      })()
    )
  }

  public scanAggregateInstances(
    type: string,
    options: {instanceLimit?: number} = {}
  ): GcpEventStoreQueryResponse {
    const store = this
    return new GcpEventStoreQueryResponse(
      (async function*() {
        const instances: string[] = []
        const queryParams = {
          filters: [
            {
              property: 'a',
              operator: '=',
              value: type,
            },
          ],
          orders: [{property: 's'}, {property: 'v'}],
        } as StoreQueryParams

        for await (const result of asyncIterateStream(
          store.request(queryParams),
          true
        )) {
          if (!instances.includes(result.s)) {
            instances.push(result.s)
          }

          if (
            options.instanceLimit &&
            instances.length > options.instanceLimit
          ) {
            return
          }

          yield result
        }
      })()
    )
  }

  public createBatchMutator(params: {capacityLimit?: number} = {}) {
    const {capacityLimit} = params
    return new GcpEventStoreBatchMutator({store: this, capacityLimit})
  }

  public queryAggregateCommits(
    type: AggregateType,
    key: AggregateKey,
    options: {
      minVersion?: number
      maxVersion?: number
      maxTime?: Date | number
      descending?: boolean
      limit?: number
    }
  ): GcpEventStoreQueryResponse {
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

    const params: StoreQueryParams = {
      filters: [
        {
          property: 's',
          operator: '=',
          value: [type, key].join(':'),
        },
        {
          property: 'v',
          operator: '>=',
          value: minVersion,
        },
        {
          property: 'v',
          operator: '<=',
          value: maxVersion,
        },
        // { This needs to be done in a different way:
        // https: // cloud.google.com/appengine/docs/standard/go/datastore/query-restrictions
        //   property: 't',
        //   operator: '<=',
        //   value: maxTime ? maxTime.valueOf() : Date.now(),
        // },
      ],
      orders: [
        {
          property: 'v',
        },
        {
          property: 't',
          options: {descending: !!descending},
        },
      ],
    }

    if (limit) {
      params.limit = limit
    }

    return new GcpEventStoreQueryResponse(
      asyncIterateStream(this.request(params), true)
    )
  }

  public scan(
    params?: {
      totalSegments?: number
      segment?: number
      capacityLimit?: number
    } & StoreQueryParams
  ): GcpEventStoreQueryResponse {
    const {segment = 0, totalSegments = 1, ...rest} = params || {}

    return new GcpEventStoreQueryResponse(
      asyncIterateStream(this.request(rest), true)
    )
  }

  /**
   * PROTECTED
   */

  protected request(params?: StoreQueryParams): Readable {
    const query = this.datastore.createQuery(this.tableName, this.kind)
    if (params) {
      if (params.filters) {
        for (const {property, operator, value} of params.filters) {
          query.filter(
            property,
            operator ? operator : value,
            operator ? value : null
          )
        }
      }
      if (params.orders) {
        for (const order of params.orders) {
          query.order(order.property, order.options)
        }
      }
      if (params.limit) {
        query.limit(params.limit)
      }
    }

    return this.datastore.runQueryStream(query) as Readable
  }
}
