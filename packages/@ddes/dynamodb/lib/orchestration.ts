import { ApplicationAutoScaling, DynamoDB, IAM } from 'aws-sdk'
import { ConfigurationOptions } from 'aws-sdk/lib/config-base'
import { AutoscalingConfig } from './DynamodbEventStore'

export async function createTable(
	tableSpecification: DynamoDB.CreateTableInput,
	options: {
		waitTimeout?: number
		statusCheckInterval?: number
		dynamodb: DynamoDB
		ttl?: boolean
	},
) {
	const { waitTimeout = 30000, statusCheckInterval = 1000, dynamodb } = options
	try {
		await dynamodb.createTable(tableSpecification).promise()
	} catch (error: any) {
		if (error?.code === 'ResourceInUseException') {
			return { tableWasCreated: false }
		}

		throw error
	}

	let timedOut = false

	const timer = setTimeout(() => {
		timedOut = true
	}, waitTimeout * 1000)

	while (!timedOut) {
		const { Table } = await dynamodb
			.describeTable({
				TableName: tableSpecification.TableName,
			})
			.promise()
		if (Table) {
			switch (Table.TableStatus) {
				case 'ACTIVE':
					clearTimeout(timer)

					if (options.ttl) {
						try {
							await dynamodb
								.updateTimeToLive({
									TableName: tableSpecification.TableName,
									TimeToLiveSpecification: {
										Enabled: !!options.ttl,
										AttributeName: 'x',
									},
								})
								.promise()
						} catch (error: any) {
							// dynalite does not support ttl
							if (error.code !== 'UnknownOperationException') {
								throw error
							}
						}
					}

					return // done
				case 'CREATING':
					break
				default: {
					throw new Error(
						'Invalid status ${TableStatus} while waiting for table to be created',
					)
				}
			}
		}

		await new Promise((resolve) =>
			setTimeout(resolve, statusCheckInterval, undefined),
		)
	}

	throw new Error(
		`Timed out while waiting for table ${tableSpecification.TableName} to become active.`,
	)
}

export async function deleteTable(
	tableName: string,
	options: {
		waitTimeout?: number
		statusCheckInterval?: number
		dynamodb: DynamoDB
	},
) {
	const { waitTimeout = 30000, statusCheckInterval = 1000, dynamodb } = options

	let timer: NodeJS.Timeout | null = null

	try {
		await dynamodb.deleteTable({ TableName: tableName }).promise()

		let timedOut = false

		timer = setTimeout(() => {
			timedOut = true
		}, waitTimeout * 1000)

		while (!timedOut) {
			const { Table } = await dynamodb
				.describeTable({ TableName: tableName })
				.promise()

			if (Table) {
				switch (Table.TableStatus) {
					case 'DELETING':
						await new Promise((resolve) =>
							setTimeout(resolve, statusCheckInterval),
						)
						continue
					default: {
						throw new Error(
							'Invalid status ${TableStatus} while waiting for table to be deleteTableed',
						)
					}
				}
			}

			return
		}

		throw new Error(
			`Timed out while waiting for table ${tableName} to be deleted.`,
		)
	} catch (error: any) {
		if (error.code === 'ResourceNotFoundException') {
			if (timer) {
				clearTimeout(timer)
			}

			return
		}

		throw error
	}
}

export async function setupAutoScaling(
	tableName: string,
	autoscalingConfig: AutoscalingConfig,
	options: { awsConfig?: ConfigurationOptions } = {},
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
	} catch (error: any) {
		if (error.code !== 'EntityAlreadyExists') {
			throw error
		}

		await iam
			.updateAssumeRolePolicy({
				RoleName,
				PolicyDocument: JSON.stringify(AssumeRolePolicyDocument),
			})
			.promise()

		role = await iam.getRole({ RoleName }).promise()
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
		} catch (error: any) {
			if (
				error.code !== 'ValidationException' ||
				!(
					error.message.startsWith('Unable to assume IAM role') ||
					error.message.includes(
						'Reason: The security token included in the request is invalid.',
					)
				)
			) {
				throw error
			}

			await new Promise((resolve) => setTimeout(resolve, 500))
		}
	}

	throw new Error('Exhausted attempts to create scaling policies')
}

export async function removeAutoScaling(
	tableName: string,
	awsConfig?: ConfigurationOptions,
) {
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

	for (const { ResourceId, type } of autoScalingTargets) {
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
		} catch (error: any) {
			if (error?.code !== 'ObjectNotFoundException') {
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
	} catch (error: any) {
		if (!['ObjectNotFoundException', 'NoSuchEntity'].includes(error.code)) {
			throw error
		}
	}
}
