/**
 * @module @ddes/aws-store
 */

import {S3} from 'aws-sdk'

/**
 * @hidden
 */
export async function deleteBucket(
  name: string,
  options: {
    s3ClientConfiguration?: S3.ClientConfiguration
  } = {}
): Promise<void> {
  const s3 = new S3(options.s3ClientConfiguration)

  try {
    let listResult
    do {
      listResult = await s3.listObjects({Bucket: name}).promise()

      if (listResult.Contents) {
        for (const {Key} of listResult.Contents) {
          await s3.deleteObject({Bucket: name, Key: Key!}).promise()
        }
      }
    } while (listResult.IsTruncated)

    await s3.deleteBucket({Bucket: name}).promise()
  } catch (error) {
    if (error.code !== 'NoSuchBucket') {
      throw error
    }
  }
}
