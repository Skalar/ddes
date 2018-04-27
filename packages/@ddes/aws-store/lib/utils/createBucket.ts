/**
 * @module @ddes/aws-store
 */

import {S3} from 'aws-sdk'

/**
 * @hidden
 */
export async function createBucket(
  name: string,
  options: {
    configuration?: S3.CreateBucketConfiguration
    s3ClientConfiguration?: S3.ClientConfiguration
  } = {}
): Promise<void> {
  const {configuration, s3ClientConfiguration} = options
  const s3 = new S3(s3ClientConfiguration)

  try {
    await s3
      .createBucket({
        Bucket: name,
        ...(configuration && {CreateBucketConfiguration: configuration}),
      })
      .promise()
  } catch (error) {
    if (error.code !== 'BucketAlreadyOwnedByYou') {
      throw error
    }
  }
}
