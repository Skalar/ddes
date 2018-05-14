/**
 * @module @ddes/aws-store
 */

import {ApplicationAutoScaling, IAM} from 'aws-sdk'
import {ConfigurationOptions} from 'aws-sdk/lib/config'
import {AutoscalingConfig} from '../types'

/**
 * @hidden
 */
export default async function setupAutoScaling(
  tableName: string,
  autoscalingConfig: AutoscalingConfig,
  options: {awsConfig?: ConfigurationOptions} = {}
) {
  const RoleName = `${tableName}_DDBAutoScalingRole`

  const iam = new IAM(options.awsConfig)
  const autoscaling = new ApplicationAutoScaling(options.awsConfig)

  let role: any

  const AssumeRolePolicyDocument = {
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: {
          Service: 'application-autoscaling.amazonaws.com',
        },
        Action: 'sts:AssumeRole',
      },
    ],
  }

  const inlineRolePolicy = {
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Action: [
          'dynamodb:DescribeTable',
          'dynamodb:UpdateTable',
          'cloudwatch:PutMetricAlarm',
          'cloudwatch:DescribeAlarms',
          'cloudwatch:DeleteAlarms',
        ],
        Resource: '*',
      },
    ],
  }

  try {
    role = await iam
      .createRole({
        AssumeRolePolicyDocument: JSON.stringify(AssumeRolePolicyDocument),
        RoleName,
      })
      .promise()
  } catch (error) {
    if (error.code !== 'EntityAlreadyExists') {
      throw error
    }

    await iam
      .updateAssumeRolePolicy({
        RoleName,
        PolicyDocument: JSON.stringify(AssumeRolePolicyDocument),
      })
      .promise()

    role = await iam.getRole({RoleName}).promise()
  }

  await iam
    .putRolePolicy({
      RoleName,
      PolicyName: 'default',
      PolicyDocument: JSON.stringify(inlineRolePolicy),
    })
    .promise()

  const autoScalingTargets = [
    {
      ResourceId: `table/${tableName}`,
      ScaleInCooldown: autoscalingConfig.tableScaleInCooldown,
      ScaleOutCooldown: autoscalingConfig.tableScaleOutCooldown,
      type: 'table',
      readMin: autoscalingConfig.tableReadMin,
      readMax: autoscalingConfig.tableReadMax,
      writeMin: autoscalingConfig.tableWriteMin,
      writeMax: autoscalingConfig.tableWriteMax,
    },
    {
      ResourceId: `table/${tableName}/index/chronological`,
      ScaleInCooldown: autoscalingConfig.chronologicalScaleInCooldown,
      ScaleOutCooldown: autoscalingConfig.chronologicalScaleOutCooldown,
      type: 'index',
      readMin: autoscalingConfig.chronologicalReadMin,
      readMax: autoscalingConfig.chronologicalReadMax,
      writeMin: autoscalingConfig.chronologicalWriteMin,
      writeMax: autoscalingConfig.chronologicalWriteMax,
    },
    {
      ResourceId: `table/${tableName}/index/instances`,
      ScaleInCooldown: autoscalingConfig.instancesScaleInCooldown,
      ScaleOutCooldown: autoscalingConfig.instancesScaleOutCooldown,
      type: 'index',
      readMin: autoscalingConfig.instancesReadMin,
      readMax: autoscalingConfig.instancesReadMax,
      writeMin: autoscalingConfig.instancesWriteMin,
      writeMax: autoscalingConfig.instancesWriteMax,
    },
  ]

  const registerTargets = async () => {
    for (const {
      ResourceId,
      readMin,
      readMax,
      writeMin,
      writeMax,
      type,
      ScaleInCooldown,
      ScaleOutCooldown,
    } of autoScalingTargets) {
      await autoscaling
        .registerScalableTarget({
          ServiceNamespace: 'dynamodb',
          ResourceId,
          ScalableDimension: `dynamodb:${type}:ReadCapacityUnits`,
          MinCapacity: readMin,
          MaxCapacity: readMax,
          RoleARN: role.Role.Arn,
        })
        .promise()

      await autoscaling
        .registerScalableTarget({
          ServiceNamespace: 'dynamodb',
          ResourceId,
          ScalableDimension: `dynamodb:${type}:WriteCapacityUnits`,
          MinCapacity: writeMin,
          MaxCapacity: writeMax,
          RoleARN: role.Role.Arn,
        })
        .promise()

      await autoscaling
        .putScalingPolicy({
          ResourceId,
          ServiceNamespace: 'dynamodb',
          ScalableDimension: `dynamodb:${type}:ReadCapacityUnits`,
          PolicyName: `DynamoDBReadCapacityUtilization:${ResourceId}`,
          PolicyType: 'TargetTrackingScaling',
          TargetTrackingScalingPolicyConfiguration: {
            PredefinedMetricSpecification: {
              PredefinedMetricType: 'DynamoDBReadCapacityUtilization',
            },
            ScaleOutCooldown,
            ScaleInCooldown,
            TargetValue: autoscalingConfig.utilizationTargetInPercent,
          },
        })
        .promise()

      await autoscaling
        .putScalingPolicy({
          ResourceId,
          ServiceNamespace: 'dynamodb',
          ScalableDimension: `dynamodb:${type}:WriteCapacityUnits`,
          PolicyName: `DynamoDBWriteCapacityUtilization:${ResourceId}`,
          PolicyType: 'TargetTrackingScaling',
          TargetTrackingScalingPolicyConfiguration: {
            PredefinedMetricSpecification: {
              PredefinedMetricType: 'DynamoDBWriteCapacityUtilization',
            },
            ScaleOutCooldown,
            ScaleInCooldown,
            TargetValue: autoscalingConfig.utilizationTargetInPercent,
          },
        })
        .promise()
    }
  }

  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      await registerTargets()

      return
    } catch (error) {
      if (
        error.code !== 'ValidationException' ||
        !(
          error.message.startsWith('Unable to assume IAM role') ||
          error.message.includes(
            'Reason: The security token included in the request is invalid.'
          )
        )
      ) {
        throw error
      }

      await new Promise(resolve => setTimeout(resolve, 500))
    }
  }

  throw new Error('Exhausted attempts to create scaling policies')
}
