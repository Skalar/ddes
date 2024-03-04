import {
	AggregateCommit,
	AggregateEvent,
	EventStore,
	VersionConflictError,
} from '@ddes/core'

import { AWSError, DynamoDB, Request } from 'aws-sdk'
import * as orchestration from './orchestration'

/**
 * Interface for EventStore powered by DynamoDB
 */
export class DynamodbEventStore extends EventStore {
	protected tableName: string
	protected initialCapacity: StoreCapacityConfig
	protected autoscaling?: AutoscalingConfig
	protected client: DynamoDB

	constructor(config: {
		tableName: string
		client: DynamoDB
		initialCapacity?: StoreCapacityConfig
		autoscaling?: AutoscalingConfig
	}) {
		super()

		const {
			tableName,
			client,
			initialCapacity = {
				tableRead: 2,
				tableWrite: 2,
				chronologicalRead: 2,
				chronologicalWrite: 2,
				instancesRead: 1,
				instancesWrite: 1,
			},
			autoscaling,
		} = config

		if (!tableName) {
			throw new Error(`'tableName' must be specified`)
		}

		this.client = client
		this.tableName = tableName
		this.initialCapacity = initialCapacity
		this.autoscaling = autoscaling
	}

	/**
	 * Create DynamoDB table and auto scaling configuration
	 */
	public async setup() {
		await orchestration.createTable(this.tableSpecification, {
			dynamodb: this.client,
			ttl: true,
		})

		if (this.autoscaling) {
			await orchestration.setupAutoScaling(this.tableName, this.autoscaling)
		}
	}

	/** @inheritdoc */
	public async teardown() {
		if (this.autoscaling) {
			await orchestration.removeAutoScaling(this.tableName)
		}

		await orchestration.deleteTable(this.tableName, {
			dynamodb: this.client,
		})
	}

	/**
	 * @inheritdoc
	 */
	public async commit<TAggregateCommit extends AggregateCommit>(
		commit: TAggregateCommit,
	) {
		try {
			await this.client
				.putItem({
					TableName: this.tableName,
					Item: this.marshallCommit(commit),
					ConditionExpression: 'attribute_not_exists(v)',
					ReturnValues: 'NONE',
				})
				.promise()
		} catch (error: any) {
			if (error.code === 'ConditionalCheckFailedException') {
				throw new VersionConflictError(commit)
			}

			throw error
		}

		return commit
	}

	/** @inheritdoc */
	public async getAggregateHeadCommit<TAggregateCommit extends AggregateCommit>(
		type: string,
		key: string,
	) {
		for await (const commits of this.queryAggregateCommits<TAggregateCommit>(
			type,
			key,
			{
				descending: true,
				limit: 1,
			},
		)) {
			for await (const commit of commits) {
				return commit
			}
		}
	}

	/**
	 * Retrieve commits from the store chronologically
	 */
	public async *chronologicalQuery<
		TAggregateCommit extends AggregateCommit,
	>(params: {
		group?: string
		min: string | Date
		max?: string | Date
		exclusiveMin?: boolean
		exclusiveMax?: boolean
		descending?: boolean
		limit?: number
		timeDriftCompensation?: number
		aggregateTypes?: string[]
	}) {
		const {
			group = 'default',
			min,
			descending,
			limit,
			exclusiveMin,
			exclusiveMax,
			timeDriftCompensation = 500,
		} = params
		const { max = new Date(Date.now() + timeDriftCompensation) } = params

		if (!min) {
			throw new Error('You must specify the "min" parameter')
		}

		const maxDate =
			max instanceof Date
				? max
				: new Date(max.replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))
		const maxCursor =
			max instanceof Date ? `${max.toISOString().replace(/[^0-9]/g, '')};` : max

		const minDate =
			min instanceof Date
				? min
				: new Date(min.replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

		const minCursor =
			min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

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
				filterExpressions: params.aggregateTypes
					? [
							`a IN (${params.aggregateTypes.map(
								(aggregateType, i) => `:a${i}`,
							)})`,
					  ]
					: undefined,
				queryVariables: {
					':p': partition.key,
					':min': exclusiveMin ? stringcrementor(minCursor) : minCursor,
					':max': exclusiveMax ? stringcrementor(maxCursor, -1) : maxCursor,
					...params.aggregateTypes?.reduce(
						(vars: object, type: string, i: number) =>
							Object.assign(vars, {
								[`:a${i}`]: type,
							}),
						{},
					),
				},
				ScanIndexForward: !descending,
				...(limit && { Limit: limit - commitCount }),
			}
			for await (const queryResult of this.request('query', queryParams)) {
				commitCount += queryResult.Items ? queryResult.Items.length : 0

				if (queryResult.Items?.length) {
					yield queryResult.Items?.map((i) =>
						this.unmarshallCommit<TAggregateCommit>(i),
					)
				}

				if (limit && commitCount >= limit) {
					return
				}
			}
		}
	}

	/**
	 * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
	 */
	public async *scanAggregateCommitsGroupedByKey<
		TAggregateCommit extends AggregateCommit,
	>(type: string) {
		const keyExpressions = ['a = :a']
		const queryVariables: { [key: string]: string } = {
			':a': type,
		}
		const filterExpressions: string[] = []

		for await (const instanceQueryResult of this.request('query', {
			IndexName: 'instances',
			keyExpressions,
			queryVariables,
			filterExpressions,
		})) {
			if (instanceQueryResult.Items) {
				for (const rawItem of instanceQueryResult.Items) {
					const { a, r } = DynamoDB.Converter.unmarshall(rawItem)

					for await (const commits of this.queryAggregateCommits<TAggregateCommit>(
						a,
						r,
					)) {
						yield commits
					}
				}
			}
		}
	}

	/**
	 * Query the commits of an [[Aggregate]] instance
	 */
	public async *queryAggregateCommits<TAggregateCommit extends AggregateCommit>(
		type: string,
		key: string,
		options: {
			consistentRead?: boolean
			minVersion?: number
			maxVersion?: number
			maxTime?: Date | number
			descending?: boolean
			limit?: number
		} = {},
	) {
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
		const queryVariables: { [key: string]: string | number } = {
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
			...(descending && { ScanIndexForward: false }),
		}

		const query = this.request('query', {
			keyExpressions,
			filterExpressions,
			queryVariables,
			...queryParams,
		})

		for await (const result of query) {
			if (result.Items?.length) {
				yield result.Items.map((item) =>
					this.unmarshallCommit<TAggregateCommit>(item),
				)
			}
		}
	}

	/**
	 * Scan store commits
	 */
	public async *scan<TAggregateCommit extends AggregateCommit>(params?: {
		totalSegments?: number
		segment?: number
		keyExpressions?: string[]
		filterExpressions?: string[]
		aggregateTypes?: string[]
		queryVariables?: object
	}) {
		const { segment = 0, totalSegments = 1, ...rest } = params || {}

		const scan = this.request('scan', {
			TotalSegments: totalSegments,
			Segment: segment,
			...rest,
		})

		for await (const result of scan) {
			if (result.Items?.length) {
				yield result.Items.map((item) =>
					this.unmarshallCommit<TAggregateCommit>(item),
				)
			}
		}
	}

	public streamCommits<
		TAggregateCommit extends AggregateCommit<AggregateEvent, string>,
	>(
		params?:
			| {
					aggregateTypes?: string[] | undefined
					chronologicalKey?: string | undefined
			  }
			| undefined,
	): AsyncIterable<TAggregateCommit[]>
	public streamCommits<
		TAggregateCommit extends AggregateCommit<AggregateEvent, string>,
	>(
		params: {
			aggregateTypes?: string[] | undefined
			chronologicalKey?: string | undefined
		},
		yieldEmpty: true,
	): AsyncIterable<TAggregateCommit[] | undefined>
	public streamCommits(
		params: {
			aggregateTypes?: string[] | undefined
			chronologicalKey?: string | undefined
		},
		yieldEmpty?: boolean,
	) {
		// TODO
		return undefined as any
	}

	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit<AggregateEvent, string>,
	>(
		aggregateType: string,
		key: string,
		minVersion?: number | undefined,
	): AsyncIterable<TAggregateCommit[]>
	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit<AggregateEvent, string>,
	>(
		aggregateType: string,
		key: string,
		minVersion: number,
		yieldEmpty: true,
	): AsyncIterable<TAggregateCommit[] | undefined>
	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit<AggregateEvent, string>,
	>(aggregateType: string, key: string, minVersion = 1, yieldEmpty?: boolean) {
		// TODO
		return undefined as any
	}

	//
	// PROTECTED
	//

	public chronologicalKey(data: {
		aggregateType: string
		aggregateKey: string
		aggregateVersion: number
		timestamp: number
	}) {
		return [
			new Date(data.timestamp).toISOString().replace(/[^0-9]/g, ''),
			data.aggregateType,
			data.aggregateKey,
			data.aggregateVersion,
		].join(':')
	}

	protected marshallCommit(commit: AggregateCommit) {
		const {
			aggregateType,
			aggregateKey,
			events,
			timestamp,
			aggregateVersion,
			expiresAt,
			chronologicalPartition = 'default',
			chronologicalKey,
		} = commit
		return DynamoDB.Converter.marshall({
			s: [aggregateType, aggregateKey].join(':'),
			v: aggregateVersion,
			g: chronologicalKey,
			a: aggregateType,
			r: aggregateVersion === 1 ? aggregateKey : undefined,
			t: new Date(timestamp).valueOf(),
			e: JSON.stringify(events),
			x: expiresAt,
			p: `${new Date(timestamp)
				.toISOString()
				.split('T')[0]
				.replace(/\-/g, '')}${chronologicalPartition}`,
		}) as MarshalledCommit
	}

	protected unmarshallCommit<TAggregateCommit extends AggregateCommit>(
		marshalledCommit: Record<string, any>,
	): TAggregateCommit {
		const unmarshalled = DynamoDB.Converter.unmarshall(marshalledCommit)
		const [, aggregateType, aggregateKey] =
			unmarshalled.s.match(/^([^:]*):(.*)$/)

		const commit = {
			aggregateType,
			aggregateKey,
			aggregateVersion: unmarshalled.v,
			expiresAt: unmarshalled.x,
			timestamp: unmarshalled.t,
			events: JSON.parse(unmarshalled.e as unknown as string),
			chronologicalPartition: unmarshalled.p.substr(
				8,
				unmarshalled.p.length - 8,
			),
			chronologicalKey: unmarshalled.g,
		} as TAggregateCommit

		return commit
	}

	protected get tableSpecification() {
		return {
			TableName: this.tableName,
			AttributeDefinitions: [
				{ AttributeName: 's', AttributeType: 'S' },
				{ AttributeName: 'v', AttributeType: 'N' },
				{ AttributeName: 'g', AttributeType: 'S' },
				{ AttributeName: 'p', AttributeType: 'S' },
				{ AttributeName: 'a', AttributeType: 'S' },
				{ AttributeName: 'r', AttributeType: 'S' },
			],

			KeySchema: [
				{ AttributeName: 's', KeyType: 'HASH' },
				{ AttributeName: 'v', KeyType: 'RANGE' },
			],

			ProvisionedThroughput: {
				ReadCapacityUnits: this.initialCapacity.tableRead,
				WriteCapacityUnits: this.initialCapacity.tableWrite,
			},

			GlobalSecondaryIndexes: [
				{
					IndexName: 'chronological',

					KeySchema: [
						{ AttributeName: 'p', KeyType: 'HASH' },
						{ AttributeName: 'g', KeyType: 'RANGE' },
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
						{ AttributeName: 'a', KeyType: 'HASH' },
						{ AttributeName: 'r', KeyType: 'RANGE' },
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

	protected request(
		type: 'scan',
		// params: StoreQueryParams & Partial<DynamoDB.ScanInput>
		params: {
			filterExpressions?: string[]
			limit?: number
		} & Partial<DynamoDB.ScanInput>,
	): AsyncGenerator<DynamoDB.ScanOutput>
	protected request(
		type: 'query',
		params: {
			keyExpressions?: string[]
			filterExpressions?: string[]
			queryVariables?: object
			limit?: number
		} & Partial<DynamoDB.QueryInput>,
	): AsyncGenerator<DynamoDB.QueryOutput>
	protected async *request(
		type: 'scan' | 'query',
		params: {
			keyExpressions?: string[]
			filterExpressions?: string[]
			queryVariables?: object
			limit?: number
		} & Partial<DynamoDB.QueryInput> &
			(Partial<DynamoDB.QueryInput> | Partial<DynamoDB.ScanInput>) = {},
	) {
		const {
			keyExpressions = [],
			filterExpressions = [],
			queryVariables = {},
			limit,
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
			...(limit && { Limit: limit }),
			...additionalQueryParams,
		}

		let lastEvaluatedKey: DynamoDB.Key | undefined

		do {
			let request:
				| Request<DynamoDB.ScanOutput, AWSError>
				| Request<DynamoDB.QueryOutput, AWSError>

			if (type === 'scan') {
				request = this.client.scan({
					...(queryParams as DynamoDB.ScanInput),
					...(lastEvaluatedKey ? { ExclusiveStartKey: lastEvaluatedKey } : {}),
				})
			} else if (type === 'query') {
				request = this.client.query({
					...(queryParams as DynamoDB.QueryInput),
					...(lastEvaluatedKey ? { ExclusiveStartKey: lastEvaluatedKey } : {}),
				})
			} else {
				throw new Error('Invalid request type')
			}

			const result = await request.promise()

			yield result

			lastEvaluatedKey = result.LastEvaluatedKey
		} while (lastEvaluatedKey)
	}
}

function stringcrementor(str: string, step: 1 | -1 = 1): string {
	const buffer = Buffer.from(str)

	if (step === 1) {
		if (buffer[buffer.length - 1] === 255) {
			return Buffer.concat([buffer, Buffer.from(' ')]).toString()
		}
		buffer[buffer.length - 1]++
		return buffer.toString()
	}
	if (step === -1) {
		if (buffer[buffer.length - 1] <= 32) {
			return buffer.subarray(0, buffer.length - 1).toString()
		}
		buffer[buffer.length - 1]--
		return buffer.toString()
	}
	return ''
}

export interface AutoscalingConfig {
	tableReadMin: number
	tableReadMax: number
	tableWriteMin: number
	tableWriteMax: number
	tableScaleInCooldown: number
	tableScaleOutCooldown: number
	chronologicalReadMin: number
	chronologicalReadMax: number
	chronologicalWriteMin: number
	chronologicalWriteMax: number
	chronologicalScaleInCooldown: number
	chronologicalScaleOutCooldown: number
	instancesReadMin: number
	instancesReadMax: number
	instancesWriteMin: number
	instancesWriteMax: number
	instancesScaleInCooldown: number
	instancesScaleOutCooldown: number
	utilizationTargetInPercent: number
}

export interface StoreCapacityConfig {
	tableRead: number
	tableWrite: number
	chronologicalRead: number
	chronologicalWrite: number
	instancesRead: number
	instancesWrite: number
}

interface MarshalledCommit extends DynamoDB.AttributeMap {
	/**
	 * Aggregate stream id type e.g. 'Order:123' (table partition key)
	 */
	s: {
		S: string
	}

	/**
	 * Aggregate version (table sort key)
	 */
	v: {
		N: string
	}

	/**
	 * Chronological sort key (chronological index sort key)
	 */
	g: {
		S: string
	}

	/**
	 * Aggregate type
	 */
	a: {
		S: string
	}

	/**
	 * Aggregate root commit key (only set for version = 1 commits)
	 */
	r: {
		S: string
	}

	/**
	 * Commit timestamp
	 */
	t: {
		N: string
	}

	/**
	 * Events in JSON form
	 */
	e: {
		B: string
	}

	/**
	 * TTL timestamp (commit will be deleted at the set time)
	 */
	x: {
		N: string
	}

	/**
	 * Chronological index partition key
	 *
	 */
	p: {
		S: string
	}
}

function* chronologicalPartitionIterator(params: {
	group?: string
	start: Date
	end?: Date
	descending?: boolean
}): IterableIterator<{ key: string; startsAt: Date; endsAt: Date }> {
	const { start, end = new Date(), group = 'default', descending } = params
	let partitionCursor = new Date(descending ? end : start)
	partitionCursor.setUTCHours(0, 0, 0, 0)

	while (descending ? partitionCursor >= start : partitionCursor <= end) {
		const endsAt = new Date(partitionCursor)
		endsAt.setUTCHours(23, 59, 59, 999)

		yield {
			key: `${partitionCursor
				.toISOString()
				.split('T')[0]
				.replace(/\-/g, '')}${group}`,
			startsAt: partitionCursor,
			endsAt,
		}
		partitionCursor = new Date(
			partitionCursor.valueOf() + 24 * 60 * 60 * 1000 * (descending ? -1 : 1),
		)
	}
}
