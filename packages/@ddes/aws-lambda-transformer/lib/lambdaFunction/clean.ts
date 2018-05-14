/**
 * @module @ddes/aws-lambda-transformer
 */

import {IAM, Lambda, S3} from 'aws-sdk'
import {LambdaTransformerConfig} from '../types'

/**
 * @hidden
 */
export default async function clean(
  id: string,
  config: LambdaTransformerConfig
) {
  const lambda = new Lambda(config.awsConfig)
  const s3 = new S3(config.awsConfig)
  const iam = new IAM(config.awsConfig)

  try {
    await s3
      .deleteObject({Bucket: id, Key: 'transformationFunction.zip'})
      .promise()
  } catch (error) {
    // console.dir({errorObject: error}, {showHidden: false, depth: null})
  }

  try {
    await s3.deleteBucket({Bucket: id}).promise()
  } catch (error) {
    // console.dir({errorBucket: error}, {showHidden: false, depth: null})
  }

  try {
    await iam.deleteRole({RoleName: id}).promise()
  } catch (error) {
    // console.dir({errorRole: error}, {showHidden: false, depth: null})
  }

  try {
    await lambda.deleteFunction({FunctionName: id}).promise()
  } catch (error) {
    // console.dir({lambdaError: error}, {showHidden: false, depth: null})
  }
}
