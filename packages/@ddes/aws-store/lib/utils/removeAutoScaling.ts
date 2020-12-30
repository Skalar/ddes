/**
 * @module @ddes/aws-store
 */

import {ApplicationAutoScaling, IAM} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config-base'

/**
 * @hidden
 */
export default async function removeAutoScaling(tableName: string, awsConfig?: ConfigurationOptions) {
  const RoleName = `${tableName}_DDBAutoScalingRole`

  const iam = new IAM(awsConfig)
  const autoscaling = new ApplicationAutoScaling(awsConfig)

  const autoScalingTargets = [
    {
      ResourceId: `table/${tableName}`,
      type: 'table',
    },
    {
      ResourceId: `table/${tableName}/index/chronologicalCommits`,
      type: 'index',
    },
  ]

  for (const {ResourceId, type} of autoScalingTargets) {
    try {
      await autoscaling
        .deregisterScalableTarget({
          ResourceId,
          ServiceNamespace: 'dynamodb',
          ScalableDimension: `dynamodb:${type}:ReadCapacityUnits`,
        })
        .promise()

      await autoscaling
        .deregisterScalableTarget({
          ResourceId,
          ServiceNamespace: 'dynamodb',
          ScalableDimension: `dynamodb:${type}:WriteCapacityUnits`,
        })
        .promise()
    } catch (error) {
      if (error.code !== 'ObjectNotFoundException') {
        throw error
      }
    }
  }

  try {
    await iam
      .deleteRolePolicy({
        PolicyName: 'default',
        RoleName,
      })
      .promise()
    await iam
      .deleteRole({
        RoleName,
      })
      .promise()
  } catch (error) {
    if (!['ObjectNotFoundException', 'NoSuchEntity'].includes(error.code)) {
      throw error
    }
  }
}
