/**
 * @module @ddes/aws-store
 */

import {MetaStore, MetaStoreKey} from '@ddes/core'
import {DynamoDB} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config'
import {AutoscalingConfig, StoreCapacityConfig} from './types'
import * as utils from './utils'

/**
 * Interface for MetaStore powered by AWS DynamoDB
 */
export default class AwsMetaStore extends MetaStore {
  public tableName!: string

  public initialCapacity: {tableRead: number; tableWrite: number} = {
    tableRead: 2,
    tableWrite: 2,
  }

  public autoscaling?: AutoscalingConfig
  public awsConfig?: ConfigurationOptions
  public dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  private dynamodb: DynamoDB

  constructor(config: {
    tableName: string
    initialCapacity?: StoreCapacityConfig
    autoscaling?: AutoscalingConfig
    awsConfig?: ConfigurationOptions
    dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  }) {
    super()

    this.tableName = config.tableName

    if (config.initialCapacity) {
      this.initialCapacity = config.initialCapacity
    }

    this.autoscaling = config.autoscaling

    this.dynamodbClientConfiguration = {
      ...config.dynamodbClientConfiguration,
      ...config.awsConfig,
    }

    this.dynamodb = new DynamoDB(this.dynamodbClientConfiguration)
  }

  public async get(key: MetaStoreKey) {
    const {Item: marshalledItem} = await this.dynamodb
      .getItem({
        TableName: this.tableName,
        Key: {
          p: {S: key[0]},
          s: {S: key[1]},
        },
        AttributesToGet: ['v', 'x'],
        ConsistentRead: true,
      })
      .promise()

    if (!marshalledItem) {
      return null
    }

    const item = DynamoDB.Converter.unmarshall(marshalledItem)

    if (item.x && item.x <= Math.floor(Date.now() / 1000)) {
      return null
    }

    return JSON.parse(item.v)
  }

  public async put(
    key: MetaStoreKey,
    value: any,
    options: {expiresAt?: Date} = {}
  ) {
    await this.dynamodb
      .putItem({
        TableName: this.tableName,
        Item: {
          p: {S: key[0]},
          s: {S: key[1]},
          v: {S: JSON.stringify(value)},
          ...(options.expiresAt && {
            x: {N: Math.floor(options.expiresAt.valueOf() / 1000).toString()},
          }),
        },
      })
      .promise()
  }

  public async delete(key: MetaStoreKey) {
    await this.dynamodb
      .deleteItem({
        TableName: this.tableName,
        Key: {
          p: {S: key[0]},
          s: {S: key[1]},
        },
      })
      .promise()
  }

  public async *list(primaryKey: string): AsyncIterableIterator<[string, any]> {
    let lastEvaluatedKey
    const params = {
      TableName: this.tableName,
      ConsistentRead: true,
      KeyConditionExpression: 'p = :p',
      ExpressionAttributeValues: DynamoDB.Converter.marshall({
        ':p': primaryKey,
      }),
    }

    do {
      const queryResult: DynamoDB.QueryOutput = await this.dynamodb
        .query({
          ...params,
          ...(lastEvaluatedKey ? {ExclusiveStartKey: lastEvaluatedKey} : {}),
        })
        .promise()

      if (queryResult.Items) {
        for (const item of queryResult.Items) {
          const {s, v, x} = DynamoDB.Converter.unmarshall(item)

          if (x && x <= Math.floor(Date.now() / 1000)) {
            continue
          }

          yield [s, JSON.parse(v)]
        }
      }

      lastEvaluatedKey = queryResult.LastEvaluatedKey
    } while (lastEvaluatedKey)
  }

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

  public async teardown() {
    if (this.autoscaling) {
      await utils.removeAutoScaling(this.tableName, this.awsConfig)
    }

    await utils.deleteTable(this.tableName, {
      dynamodbClientConfiguration: this.dynamodbClientConfiguration,
    })
  }

  private get tableSpecification() {
    return {
      TableName: this.tableName,
      AttributeDefinitions: [
        {AttributeName: 'p', AttributeType: 'S'}, // "primaryKey"
        {AttributeName: 's', AttributeType: 'S'}, // "secondaryKey"
      ],

      KeySchema: [
        {AttributeName: 'p', KeyType: 'HASH'},
        {AttributeName: 's', KeyType: 'RANGE'},
      ],

      ProvisionedThroughput: {
        ReadCapacityUnits: this.initialCapacity.tableRead,
        WriteCapacityUnits: this.initialCapacity.tableWrite,
      },
    }
  }
}
