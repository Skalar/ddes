/**
 * @module @ddes/aws-store
 */

import {
  AggregateKey,
  AggregateSnapshot,
  AggregateType,
  Timestamp,
  utils as coreutils,
} from '@ddes/core'

import {SnapshotStore} from '@ddes/core'
import {S3} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config'
import {createBucket, deleteBucket} from './utils'

/**
 * Interface for SnapshotStore powered by AWS S3
 */
export default class AwsSnapshotStore extends SnapshotStore {
  public manageBucket = false
  public bucketName: string
  public keyPrefix = ''
  public s3ClientConfiguration?: S3.ClientConfiguration
  public awsConfig?: ConfigurationOptions
  public s3: S3

  constructor(config: {
    bucketName: string
    manageBucket?: boolean
    keyPrefix?: string
    s3ClientConfiguration?: S3.ClientConfiguration
  }) {
    super()
    this.bucketName = config.bucketName
    this.s3ClientConfiguration = config.s3ClientConfiguration
    this.s3 = new S3(this.s3ClientConfiguration)

    if (config.keyPrefix) {
      this.keyPrefix = config.keyPrefix
    }

    if (typeof config.manageBucket !== 'undefined') {
      this.manageBucket = config.manageBucket
    }
  }

  /**
   * Create and configure DynamoDB table, S3 bucket and DynamoDB auto-scaling
   */
  public async setup() {
    if (this.manageBucket) {
      await createBucket(this.bucketName, {
        s3ClientConfiguration: this.s3ClientConfiguration,
      })
    }
  }

  /**
   * Remove DynamoDB auto-scaling, S3 Bucket and DynamoDB table
   */
  public async teardown() {
    if (this.manageBucket) {
      await deleteBucket(this.bucketName, {
        s3ClientConfiguration: this.s3ClientConfiguration,
      })
    }
  }

  /**
   * Read an aggregate instance snapshot from AWS S3
   *
   * @param type e.g. 'Account'
   * @param key  e.g. '1234'
   */
  public async readSnapshot(
    type: AggregateType,
    key: AggregateKey
  ): Promise<AggregateSnapshot | null> {
    try {
      const {Body: snapshotJSON} = await this.s3
        .getObject({
          Bucket: this.bucketName,
          Key: `${this.keyPrefix}${type}_${key}`,
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
        timestamp: coreutils.toTimestamp(timestampString),
        compatibilityChecksum,
      }
    } catch (error) {
      if (error.code !== 'NoSuchKey') {
        throw error
      }

      return null
    }
  }

  /**
   * Delete snapshots from AWS S3 bucket
   */
  public async deleteSnapshots() {
    let listResult

    do {
      listResult = await this.s3
        .listObjectsV2({
          Bucket: this.bucketName,
          Prefix: this.keyPrefix,
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
            Bucket: this.bucketName,
            Key: s3Object.Key!,
          })
          .promise()
      }
    } while (listResult.NextContinuationToken)
  }

  /**
   * Write an aggregate instance snapshot to AWS S3 bucket
   *
   * @param type e.g. 'Account'
   * @param key  e.g. '1234'
   */
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
    const {version, state, timestamp, compatibilityChecksum} = payload

    await this.s3
      .putObject({
        Bucket: this.bucketName,
        Key: `${this.keyPrefix}${type}_${key}`,
        Body: JSON.stringify({
          version,
          state,
          compatibilityChecksum,
          timestamp,
        }),
      })
      .promise()
  }
}
