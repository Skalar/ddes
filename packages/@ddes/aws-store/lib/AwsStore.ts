/**
 * @module @ddes/aws-store
 */

import {
  Aggregate,
  AggregateKeyString,
  AggregateSnapshot,
  Commit,
  Event,
  Iso8601Timestamp,
  Store,
  utils as coreutils,
} from '@ddes/core'
import {VersionConflictError} from '@ddes/core'
import {DynamoDB, S3} from 'aws-sdk'
import {promisify} from 'util'
import {gunzip as gunzipCb, gzip as gzipCb} from 'zlib'
import {AwsStoreBatchMutator} from './AwsStoreBatchMutator'
import * as utils from './utils'

import {VersioningConfiguration} from 'aws-sdk/clients/s3'
import {ConfigurationOptions} from 'aws-sdk/lib/config'
import {
  AutoscalingConfig,
  AwsStoreConfig,
  CapacityConfig,
  CommitKey,
  MarshalledCommit,
  SnapshotsConfig,
} from './types'

const gzip = promisify(gzipCb)
const gunzip = promisify(gunzipCb)

export class AwsStore extends Store {
  public tableName!: string

  public initialCapacity: CapacityConfig = {
    read: 2,
    write: 2,
    indexRead: 2,
    indexWrite: 2,
  }

  public autoscaling?: AutoscalingConfig
  public snapshots?: SnapshotsConfig
  public maxVersionDigits: number = 9
  public s3ClientConfiguration?: S3.ClientConfiguration
  public dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  public awsConfig?: ConfigurationOptions
  public dynamodb: DynamoDB

  constructor(config: AwsStoreConfig) {
    super()

    if (!config.tableName) {
      throw new Error(`'tableName' must be specified`)
    }

    Object.assign(this, config)

    this.dynamodb = new DynamoDB({
      ...this.dynamodbClientConfiguration,
      ...this.awsConfig,
    })
  }

  public async setup() {
    await utils.createTable(this.tableSpecification, {
      dynamodbClientConfiguration: this.dynamodbClientConfiguration,
    })

    if (this.snapshots && this.snapshots.manageBucket) {
      await utils.createBucket(this.snapshots.s3BucketName, {
        s3ClientConfiguration: this.s3ClientConfiguration,
      })
    }

    if (this.autoscaling) {
      await utils.setupAutoScaling(this.tableName, this.autoscaling, {
        awsConfig: this.awsConfig,
      })
    }
  }

  public async teardown() {
    await utils.deleteTable(this.tableName, {
      dynamodbClientConfiguration: this.dynamodbClientConfiguration,
    })

    if (this.snapshots && this.snapshots.manageBucket) {
      await utils.deleteBucket(this.snapshots.s3BucketName, {
        s3ClientConfiguration: this.s3ClientConfiguration,
      })
    }

    if (this.autoscaling) {
      await utils.removeAutoScaling(this.tableName, this.awsConfig)
    }
  }

  public async commit(commit: Commit) {
    try {
      await this.dynamodb
        .putItem({
          TableName: this.tableName,
          Item: await this.marshallCommit(commit),
          ConditionExpression: 'attribute_not_exists(k)',
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

  public async getHeadCommit() {
    for await (const commit of this.chronologicalCommits({
      descending: true,
      limit: 1,
    })) {
      return commit
    }
    return null
  }

  public async getAggregateHeadCommit(params: {
    type: string
    key: AggregateKeyString
  }) {
    const {type, key} = params

    for await (const commit of this.queryAggregateCommits({
      descending: true,
      type,
      key,
      limit: 1,
    })) {
      return commit
    }

    return null
  }

  public chronologicalCommits(
    options: {
      from?: string
      after?: string
      before?: string
      descending?: boolean
      filterAggregateTypes?: string[]
      limit?: number
    } = {}
  ): AsyncIterableIterator<Commit> {
    const keyExpressions = ['z = :z']
    const queryVariables: {[key: string]: string} = {
      ':z': 't',
    }
    const filterExpressions: string[] = []

    // query index
    // (ProductCategory IN (:cat1, :cat2))

    if (options.after) {
      keyExpressions.push('c > :after')
      queryVariables[':after'] = options.after
    } else if (options.from) {
      keyExpressions.push('c >= :from')
      queryVariables[':from'] = options.from
    } else if (options.before) {
      keyExpressions.push('c < :before')
      queryVariables[':before'] = options.before
    }

    if (options.filterAggregateTypes) {
      const variableList = options.filterAggregateTypes.map(
        (aggregateType, i) => `:a${i}`
      )
      filterExpressions.push(`a IN (${variableList.join(',')})`)
      options.filterAggregateTypes.forEach((aggregateType, i) => {
        queryVariables[`:a${i}`] = aggregateType
      })
    }

    return this.queryCommits({
      queryParams: {
        IndexName: 'chronologicalCommits',
        ScanIndexForward: !options.descending,
      },
      keyExpressions,
      queryVariables,
      filterExpressions,
    })
  }

  /**
   * Query commits for an aggregate type
   */

  public queryAggregateCommits(params: {
    type: string
    key?: AggregateKeyString
    consistentRead?: boolean
    minVersion?: number
    maxVersion?: number
    maxTime?: Iso8601Timestamp
    descending?: boolean
    limit?: number
  }): AsyncIterableIterator<Commit> {
    const {
      type,
      key,
      consistentRead = true,
      minVersion = 1,
      maxVersion = 10 ** this.maxVersionDigits - 1,
      maxTime,
      descending,
      limit,
    } = params

    if (!type) {
      throw new Error('You need to specify "type"')
    }

    const keyExpressions = ['a = :aggregateType']
    const queryVariables: {[key: string]: string} = {
      ':aggregateType': type,
    }
    const filterExpressions: string[] = []

    if (key) {
      Object.assign(queryVariables, {
        ':fromKeyAndVersion': this.commitKeyString(key, minVersion),
        ':toKeyAndVersion': this.commitKeyString(key, maxVersion),
      })
      keyExpressions.push('k BETWEEN :fromKeyAndVersion AND :toKeyAndVersion')
    }

    if (maxTime) {
      filterExpressions.push('t <= :maxTime')
      queryVariables[':maxTime'] = maxTime
    }

    const queryParams = {
      ConsistentRead: consistentRead,
      ...(limit && {Limit: limit}),
      ...(descending && {ScanIndexForward: false}),
    }

    return this.queryCommits({
      keyExpressions,
      filterExpressions,
      queryVariables,
      queryParams,
    })
  }

  public async marshallCommit(commit: Commit): Promise<MarshalledCommit> {
    const {
      aggregateType,
      aggregateKey,
      sortKey,
      events,
      timestamp,
      aggregateVersion,
      active,
    } = commit

    return DynamoDB.Converter.marshall({
      a: aggregateType,
      t: timestamp,
      k: this.commitKeyString(aggregateKey, aggregateVersion),
      c: sortKey,
      e: await gzip(
        JSON.stringify(
          events.map(({type: t, version: v, properties: p}) => ({
            ...(v && {v}),
            p,
            t,
          }))
        )
      ),
      z: active ? 't' : 'f',
    }) as MarshalledCommit
  }

  public async unmarshallCommit(
    marshalledCommit: MarshalledCommit
  ): Promise<Commit> {
    const unmarshalled = DynamoDB.Converter.unmarshall(marshalledCommit)
    const [, aggregateKeyString, aggregateVersionString] = unmarshalled.k.match(
      /^(.*):([^:]*)$/
    )

    const commit = new Commit({
      aggregateType: unmarshalled.a,
      aggregateKey: aggregateKeyString ? aggregateKeyString : undefined,
      aggregateVersion: parseInt(aggregateVersionString, 10),
      active: unmarshalled.z === 't',
      timestamp: unmarshalled.t,
      events: JSON.parse((await gunzip(unmarshalled.e)) as string).map(
        ({
          t: type,
          v: version = 1,
          p: properties,
        }: {
          t: string
          v: number
          p: object
        }) =>
          ({
            type,
            version,
            properties,
          } as Event)
      ),
    })

    return commit
  }

  public async writeSnapshot(params: {
    type: string
    key: string
    version: number
    state: object
    timestamp: Iso8601Timestamp
    compatibilityChecksum: string
  }) {
    if (!this.snapshots) {
      throw new Error('Snapshots are not configured')
    }

    const {type, key, version, state, timestamp, compatibilityChecksum} = params

    await this.s3
      .putObject({
        Bucket: this.snapshots.s3BucketName,
        Key: `${this.snapshots.keyPrefix}${type}_${key}`,
        Body: JSON.stringify({
          version,
          state,
          compatibilityChecksum,
          timestamp,
        }),
      })
      .promise()
  }

  public async readSnapshot({
    type,
    key,
  }: {
    type: string
    key: string
  }): Promise<AggregateSnapshot | null> {
    if (!this.snapshots) {
      throw new Error('Snapshots are not configured')
    }

    try {
      const {Body: snapshotJSON} = await this.s3
        .getObject({
          Bucket: this.snapshots.s3BucketName,
          Key: `${this.snapshots.keyPrefix}${type}_${key}`,
        })
        .promise()

      if (!snapshotJSON) {
        return null
      }

      const {
        version,
        state,
        timestamp: timestampString,
        compatibilityChecksum,
      } = JSON.parse(snapshotJSON as string)

      return {
        version,
        state,
        timestamp: coreutils.toIso8601Timestamp(timestampString),
        compatibilityChecksum,
      }
    } catch (error) {
      if (error.code !== 'NoSuchKey') {
        throw error
      }

      return null
    }
  }

  public async deleteSnapshots() {
    if (!this.snapshots) {
      throw new Error('Snapshots are not configured')
    }

    let listResult

    do {
      listResult = await this.s3
        .listObjectsV2({
          Bucket: this.snapshots.s3BucketName,
          Prefix: this.snapshots.keyPrefix,
          ContinuationToken: listResult
            ? listResult.NextContinuationToken
            : undefined,
        })
        .promise()

      if (!listResult.Contents) {
        throw new Error('List request returned on content')
      }

      for (const s3Object of listResult.Contents) {
        await this.s3
          .deleteObject({
            Bucket: this.snapshots.s3BucketName,
            Key: s3Object.Key!,
          })
          .promise()
      }
    } while (listResult.NextContinuationToken)
  }

  public createBatchMutator() {
    return new AwsStoreBatchMutator({store: this})
  }

  protected async *queryCommits(params: {
    keyExpressions: string[]
    filterExpressions: string[]
    queryVariables: object
    queryParams: Partial<DynamoDB.QueryInput>
  }): AsyncIterableIterator<Commit> {
    const {
      keyExpressions,
      filterExpressions,
      queryVariables,
      queryParams: additionalQueryParams,
    } = params

    const queryParams = {
      TableName: this.tableName,
      KeyConditionExpression: keyExpressions.join(' AND '),
      ...(filterExpressions.length && {
        FilterExpression: filterExpressions.join(' AND '),
      }),
      ExpressionAttributeValues: DynamoDB.Converter.marshall(queryVariables),
      ...additionalQueryParams,
    }

    let lastEvaluatedKey

    do {
      const queryResult: DynamoDB.QueryOutput = await this.dynamodb
        .query({
          ...queryParams,
          ...(lastEvaluatedKey ? {ExclusiveStartKey: lastEvaluatedKey} : {}),
        })
        .promise()

      if (queryResult.Items) {
        for (const marshalledCommit of queryResult.Items) {
          const commit = await this.unmarshallCommit(
            marshalledCommit as MarshalledCommit
          )
          yield commit
        }
      }

      lastEvaluatedKey = queryResult.LastEvaluatedKey
    } while (lastEvaluatedKey)
  }

  private commitKeyString(
    aggregateKey: string,
    aggregateVersion: number,
    versionDigits = this.maxVersionDigits
  ): CommitKey {
    return [
      aggregateKey,
      `${'0'.repeat(
        versionDigits - aggregateVersion.toString().length
      )}${aggregateVersion}`,
    ].join(':')
  }

  get s3() {
    return new S3(this.s3ClientConfiguration)
  }

  private get tableSpecification() {
    return {
      TableName: this.tableName,
      AttributeDefinitions: [
        {
          AttributeName: 'a', // aggregateType
          AttributeType: 'S',
        },
        {
          AttributeName: 'k', // keyAndVersion
          AttributeType: 'S',
        },
        {
          AttributeName: 'c', // commitId
          AttributeType: 'S',
        },
        {
          AttributeName: 'z', // active
          AttributeType: 'S',
        },
      ],

      KeySchema: [
        {
          AttributeName: 'a',
          KeyType: 'HASH',
        },
        {
          AttributeName: 'k',
          KeyType: 'RANGE',
        },
      ],

      ProvisionedThroughput: {
        ReadCapacityUnits: this.initialCapacity.read,
        WriteCapacityUnits: this.initialCapacity.write,
      },

      GlobalSecondaryIndexes: [
        {
          IndexName: 'chronologicalCommits',

          KeySchema: [
            {
              AttributeName: 'z',
              KeyType: 'HASH',
            },
            {
              AttributeName: 'c',
              KeyType: 'RANGE',
            },
          ],

          Projection: {
            ProjectionType: 'ALL',
          },

          ProvisionedThroughput: {
            ReadCapacityUnits: this.initialCapacity.indexRead,
            WriteCapacityUnits: this.initialCapacity.indexWrite,
          },
        },
      ],
    }
  }
}

export default AwsStore
