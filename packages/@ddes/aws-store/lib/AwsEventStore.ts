/**
 * @module @ddes/aws-store
 */

import {
  AggregateKey,
  AggregateType,
  Commit,
  EventStore,
  VersionConflictError,
} from '@ddes/core'

import {DynamoDB} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config'
import AwsEventStoreBatchMutator from './AwsEventStoreBatchMutator'
import AwsEventStoreQueryResponse from './AwsEventStoreQueryResponse'
import {
  AutoscalingConfig,
  AwsEventStoreConfig,
  StoreCapacityConfig,
  StoreQueryParams,
} from './types'
import * as utils from './utils'
import chronologicalPartitionIterator from './utils/chronologicalPartitionIterator'

/**
 * Interface for EventStore powered by AWS DynamoDB
 */
export default class AwsEventStore extends EventStore {
  public tableName!: string

  public initialCapacity: StoreCapacityConfig = {
    tableRead: 2,
    tableWrite: 2,
    chronologicalRead: 2,
    chronologicalWrite: 2,
    instancesRead: 1,
    instancesWrite: 1,
  }

  public autoscaling?: AutoscalingConfig
  public dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  public awsConfig?: ConfigurationOptions
  public dynamodb: DynamoDB

  constructor(config: AwsEventStoreConfig) {
    super()

    if (!config.tableName) {
      throw new Error(`'tableName' must be specified`)
    }

    Object.assign(this, config)
    this.dynamodbClientConfiguration = {
      ...this.awsConfig,
      ...config.dynamodbClientConfiguration,
    }
    this.dynamodb = new DynamoDB(this.dynamodbClientConfiguration)
  }

  public toString() {
    return `AwsEventStore:${this.tableName}`
  }

  /**
   * Create DynamoDB table and auto scaling configuration
   */
  public async setup() {
    await utils.createTable(this.tableSpecification, {
      dynamodbClientConfiguration: this.dynamodbClientConfiguration,
      ttl: true,
    })

    if (this.autoscaling) {
      await utils.setupAutoScaling(this.tableName, this.autoscaling, {
        awsConfig: this.awsConfig,
      })
    }
  }

  /**
   * Remove DynamoDB table and auto scaling configuration
   */
  public async teardown() {
    if (this.autoscaling) {
      await utils.removeAutoScaling(this.tableName, this.awsConfig)
    }

    await utils.deleteTable(this.tableName, {
      dynamodbClientConfiguration: this.dynamodbClientConfiguration,
    })
  }

  /**
   * Get commit count (can be up to 6 hours out of date)
   */
  public async bestEffortCount() {
    const {Table} = await this.dynamodb
      .describeTable({TableName: this.tableName})
      .promise()

    if (!Table) {
      throw new Error('table does not exist')
    }

    return Table.ItemCount || 0
  }

  /**
   * Store commit in DynamoDB table
   */
  public async commit(commit: Commit) {
    try {
      await this.dynamodb
        .putItem({
          TableName: this.tableName,
          Item: await utils.marshallCommit(commit),
          ConditionExpression: 'attribute_not_exists(v)',
          ReturnValues: 'NONE',
        })
        .promise()
    } catch (error) {
      if (error.code === 'ConditionalCheckFailedException') {
        throw new VersionConflictError(
          `${commit.aggregateType}[${
            commit.aggregateKey
          }] already has a version ${commit.aggregateVersion} commit`
        )
      }

      throw error
    }
  }

  /**
   * Get most recent commit for an [[Aggregate]] instance
   */
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

  /**
   * Get the most recent commit in the given chronological group
   */
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

  /**
   * Retrieve commits from the store chronologically
   */
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

    return new AwsEventStoreQueryResponse(
      this,
      (async function*() {
        let commitCount = 0

        for (const partition of chronologicalPartitionIterator({
          start: minDate,
          end: maxDate,
          group,
          descending,
        })) {
          const queryParams = {
            IndexName: 'chronological',
            keyExpressions: ['p = :p', 'g BETWEEN :min AND :max'],
            filterExpressions: params.filterAggregateTypes
              ? [
                  `a IN (${params.filterAggregateTypes.map(
                    (aggregateType, i) => `:a${i}`
                  )})`,
                ]
              : undefined,
            queryVariables: {
              ':p': partition.key,
              ':min': exclusiveMin
                ? utils.stringcrementor(minSortKey)
                : minSortKey,
              ':max': exclusiveMax
                ? utils.stringcrementor(maxSortKey, -1)
                : maxSortKey,
              ...(params.filterAggregateTypes &&
                params.filterAggregateTypes.reduce(
                  (vars: object, type: string, i: number) => ({
                    ...vars,
                    [`:a${i}`]: type,
                  }),
                  {}
                )),
            },
            ScanIndexForward: !descending,
            ...(limit && {Limit: limit - commitCount}),
          }
          for await (const queryResult of store.request('query', queryParams)) {
            commitCount += queryResult.Items ? queryResult.Items.length : 0

            yield {
              ...queryResult,
              cursor:
                queryResult.Items && queryResult.Items.length
                  ? queryResult.Items[0].g.S
                  : new Date(
                      partition.endsAt.valueOf() + timeDriftCompensation <
                      Date.now()
                        ? partition.endsAt
                        : partition.startsAt
                    )
                      .toISOString()
                      .replace(/[^0-9]/g, ''),
            }
            if (limit && commitCount >= limit) {
              return
            }
          }
        }
      })()
    )
  }

  /**
   * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
   */
  public scanAggregateInstances(
    type: string,
    options: {
      instanceLimit?: number
    } = {}
  ): AwsEventStoreQueryResponse {
    const keyExpressions = ['a = :a']
    const queryVariables: {[key: string]: string} = {
      ':a': type,
    }
    const filterExpressions: string[] = []
    const store = this

    return new AwsEventStoreQueryResponse(
      this,
      (async function*() {
        let instanceCount = 0
        for await (const instanceQueryResult of store.request('query', {
          IndexName: 'instances',
          keyExpressions,
          queryVariables,
          filterExpressions,
          limit: options.instanceLimit,
        })) {
          instanceCount++
          if (instanceQueryResult.Items) {
            for (const rawItem of instanceQueryResult.Items) {
              const {a, r} = DynamoDB.Converter.unmarshall(rawItem)

              for await (const rawQueryResult of store.request('query', {
                keyExpressions: ['s = :s'],
                queryVariables: {':s': [a, r].join(':')},
              })) {
                yield rawQueryResult
              }
            }
          }

          if (options.instanceLimit && instanceCount >= options.instanceLimit) {
            return
          }
        }
      })()
    )
  }

  /**
   * Query the commits of an [[Aggregate]] instance
   */
  public queryAggregateCommits(
    type: AggregateType,
    key: AggregateKey,
    options: {
      consistentRead?: boolean
      minVersion?: number
      maxVersion?: number
      maxTime?: Date | number
      descending?: boolean
      limit?: number
    } = {}
  ): AwsEventStoreQueryResponse {
    const {
      consistentRead = true,
      minVersion = 1,
      maxVersion = Number.MAX_SAFE_INTEGER,
      maxTime,
      descending,
      limit,
    } = options

    if (!type || !key) {
      throw new Error('You need to specify "type" and "key"')
    }

    const keyExpressions = [
      's = :streamId',
      'v BETWEEN :minVersion and :maxVersion',
    ]
    const queryVariables: {[key: string]: string | number} = {
      ':streamId': [type, key].join(':'),
      ':minVersion': minVersion,
      ':maxVersion': maxVersion,
    }

    const filterExpressions: string[] = []

    if (maxTime) {
      filterExpressions.push('t <= :maxTime')
      queryVariables[':maxTime'] = maxTime.valueOf()
    }

    const queryParams = {
      limit,
      ConsistentRead: consistentRead,
      ...(descending && {ScanIndexForward: false}),
    }

    return new AwsEventStoreQueryResponse(
      this,
      this.request('query', {
        keyExpressions,
        filterExpressions,
        queryVariables,
        ...queryParams,
      })
    )
  }

  /**
   * Scan store commits
   */
  public scan(
    params?: {
      totalSegments?: number
      segment?: number
      capacityLimit?: number
    } & StoreQueryParams
  ): AwsEventStoreQueryResponse {
    const {segment = 0, totalSegments = 1, ...rest} = params || {}

    return new AwsEventStoreQueryResponse(
      this,
      this.request('scan', {
        TotalSegments: totalSegments,
        Segment: segment,
        ...rest,
      })
    )
  }

  /**
   * Get a [[AwsBatchMutator]] for the store
   */
  public createBatchMutator(params: {capacityLimit?: number} = {}) {
    const {capacityLimit} = params
    return new AwsEventStoreBatchMutator({store: this, capacityLimit})
  }

  //
  // PROTECTED
  //

  protected get tableSpecification() {
    return {
      TableName: this.tableName,
      AttributeDefinitions: [
        {AttributeName: 's', AttributeType: 'S'},
        {AttributeName: 'v', AttributeType: 'N'},
        {AttributeName: 'g', AttributeType: 'S'},
        {AttributeName: 'p', AttributeType: 'S'},
        {AttributeName: 'a', AttributeType: 'S'},
        {AttributeName: 'r', AttributeType: 'S'},
      ],

      KeySchema: [
        {AttributeName: 's', KeyType: 'HASH'},
        {AttributeName: 'v', KeyType: 'RANGE'},
      ],

      ProvisionedThroughput: {
        ReadCapacityUnits: this.initialCapacity.tableRead,
        WriteCapacityUnits: this.initialCapacity.tableWrite,
      },

      GlobalSecondaryIndexes: [
        {
          IndexName: 'chronological',

          KeySchema: [
            {AttributeName: 'p', KeyType: 'HASH'},
            {AttributeName: 'g', KeyType: 'RANGE'},
          ],

          Projection: {
            ProjectionType: 'INCLUDE',
            NonKeyAttributes: ['t', 'e', 'x', 'a'],
          },

          ProvisionedThroughput: {
            ReadCapacityUnits: this.initialCapacity.chronologicalRead,
            WriteCapacityUnits: this.initialCapacity.chronologicalWrite,
          },
        },
        {
          IndexName: 'instances',

          KeySchema: [
            {AttributeName: 'a', KeyType: 'HASH'},
            {AttributeName: 'r', KeyType: 'RANGE'},
          ],

          Projection: {
            ProjectionType: 'KEYS_ONLY',
          },

          ProvisionedThroughput: {
            ReadCapacityUnits: this.initialCapacity.instancesRead,
            WriteCapacityUnits: this.initialCapacity.instancesWrite,
          },
        },
      ],
    }
  }

  protected async *request(
    type: 'scan' | 'query',
    params: StoreQueryParams &
      (Partial<DynamoDB.QueryInput> | Partial<DynamoDB.ScanInput>) = {}
  ): AsyncIterableIterator<DynamoDB.QueryOutput & {throttleCount: number}> {
    const {
      startKey,
      keyExpressions = [],
      filterExpressions = [],
      filterAggregateTypes,
      queryVariables = {},
      limit,
      capacityLimit,
      ...additionalQueryParams
    } = params

    const queryParams = {
      TableName: this.tableName,
      ReturnConsumedCapacity: 'TOTAL',
      ...(keyExpressions.length && {
        KeyConditionExpression: keyExpressions.join(' AND '),
      }),
      ...(filterExpressions.length && {
        FilterExpression: filterExpressions.join(' AND '),
      }),
      ...(Object.keys(queryVariables).length && {
        ExpressionAttributeValues: DynamoDB.Converter.marshall(queryVariables),
      }),
      ...(limit && {Limit: limit}),
      ...additionalQueryParams,
    }

    let lastEvaluatedKey: DynamoDB.Key | undefined = startKey
    let readCapacityLimiter

    if (capacityLimit) {
      readCapacityLimiter = new utils.ReadCapacityLimiter(
        capacityLimit,
        additionalQueryParams.ConsistentRead ? 0.25 : 0.125
      )
    }

    do {
      let request = null
      let throttleCount = 0
      if (readCapacityLimiter) {
        queryParams.Limit = await readCapacityLimiter.getPermittedItemCount()
      }

      if (type === 'scan') {
        request = await this.dynamodb.scan({
          ...(queryParams as DynamoDB.ScanInput),
          ...(lastEvaluatedKey ? {ExclusiveStartKey: lastEvaluatedKey} : {}),
        })
      } else {
        request = await this.dynamodb.query({
          ...(queryParams as DynamoDB.QueryInput),
          ...(lastEvaluatedKey ? {ExclusiveStartKey: lastEvaluatedKey} : {}),
        })
      }

      request.on('retry', response => {
        if (
          response.error &&
          response.error.code === 'ProvisionedThroughputExceededException'
        ) {
          throttleCount++
        }
      })

      const result:
        | DynamoDB.QueryOutput
        | DynamoDB.ScanOutput = await request.promise()

      if (result.Count && result.ConsumedCapacity && readCapacityLimiter) {
        readCapacityLimiter.registerConsumption(
          result.ConsumedCapacity.CapacityUnits!,
          result.Count
        )
      }

      yield {...result, throttleCount}

      lastEvaluatedKey = result.LastEvaluatedKey
    } while (lastEvaluatedKey)
  }
}
