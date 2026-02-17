import { createHash } from 'crypto'
import { on } from 'events'
import { AggregateCommit, EventStore, VersionConflictError } from '@ddes/core'
import { Repeater } from '@repeaterjs/repeater'
import { Pool, PoolClient } from 'pg'
import QueryStream from 'pg-query-stream'
import { sql } from 'pg-sql'
import { PostgresListener } from './PostgresListener'

/**
 * Interface for EventStore powered by PostgreSQL
 */
export class PostgresEventStore extends EventStore {
	protected listener?: PostgresListener

	constructor(
		protected tableName: string,
		protected pool: Pool,
	) {
		super()
	}

	/**
	 * Create PostgresQL table
	 */
	public async setup() {
		await this.pool.query(sql`
      CREATE TABLE IF NOT EXISTS ${sql.ident(this.tableName)} (
        aggregate_type      text NOT NULL,
        aggregate_key       text NOT NULL,
        aggregate_version   int NOT NULL,
        chronological_key   text NOT NULL,
        chronological_partition text NOT NULL,
        events              jsonb NOT NULL,
        timestamp           timestamp NOT NULL,
        expires_at          timestamp,
        PRIMARY KEY(aggregate_type, aggregate_key, aggregate_version)
      );

      CREATE INDEX IF NOT EXISTS chronological_key_idx ON ${sql.ident(
				this.tableName,
			)} (chronological_key, aggregate_type);

      CREATE OR REPLACE FUNCTION ${sql.ident(`${this.tableName}_notification`)}() RETURNS TRIGGER AS $$
        BEGIN
        PERFORM pg_notify(left(encode(sha256(('${sql.raw(this.tableName)}')::bytea), 'hex'), 48), '' || NEW.chronological_key);
        PERFORM pg_notify(left(encode(sha256(('${sql.raw(this.tableName)}:' || NEW.aggregate_type)::bytea), 'hex'), 48), NEW.aggregate_key || CHR(9) || NEW.aggregate_version || CHR(9) || NEW.chronological_key);
        PERFORM pg_notify(left(encode(sha256(('${sql.raw(this.tableName)}:' || NEW.aggregate_type || ':' || NEW.aggregate_key)::bytea), 'hex'), 48), '' || NEW.aggregate_version);
        RETURN NULL;
        END;
      $$ LANGUAGE plpgsql;

      CREATE OR REPLACE TRIGGER ${sql.ident(this.tableName)}
        AFTER INSERT
        ON ${sql.ident(this.tableName)}
        FOR EACH ROW
        EXECUTE PROCEDURE ${sql.ident(`${this.tableName}_notification`)}();
    `)
	}

	/**
	 * Remove PostgresQL table
	 */
	public async teardown() {
		await this.pool.query(sql`
      DROP TABLE IF EXISTS ${sql.ident(this.tableName)};
      DROP FUNCTION IF EXISTS ${sql.ident(`${this.tableName}_notification`)};
      DROP TRIGGER IF EXISTS ${sql.ident(this.tableName)} ON ${sql.ident(
				this.tableName,
			)};
    `)
	}

	public async commit<TAggregateCommit extends AggregateCommit>(
		commit: TAggregateCommit,
	) {
		try {
			await this.insertCommit(this.pool, commit)
		} catch (error: any) {
			if (error.code === '23505') {
				throw new VersionConflictError(commit)
			}
			throw error
		}

		return commit
	}

	public async commitInTransaction<TAggregateCommit extends AggregateCommit>(
		commits: TAggregateCommit[],
	) {
		const client = await this.pool.connect()

		try {
			await client.query('BEGIN')
			for (const commit of commits) {
				await this.insertCommit(client, commit)
			}
			await client.query('COMMIT')
		} catch (error: any) {
			await client.query('ROLLBACK')
			if (error.code === '23505') {
				// TODO: Identify what commit caused the conflict
				throw new VersionConflictError(commits[0])
			}
			throw error
		} finally {
			await client.release()
		}

		return commits
	}

	private async insertCommit<TAggregateCommit extends AggregateCommit>(
		pool: Pool | PoolClient,
		commit: TAggregateCommit,
	) {
		const {
			aggregateType,
			aggregateKey,
			aggregateVersion,
			chronologicalPartition = 'default',
			events,
			timestamp,
			expiresAt,
			chronologicalKey,
		} = commit

		const query = sql`INSERT INTO ${sql.ident(this.tableName)} VALUES(
        ${aggregateType},
        ${aggregateKey},
        ${aggregateVersion},
        ${chronologicalKey},
        ${chronologicalPartition},
        ${JSON.stringify(events)},
        ${new Date(timestamp)},
        ${expiresAt ? new Date(expiresAt) : null}
      )`

		await pool.query(query)
	}

	public async *queryAggregateCommits<TAggregateCommit extends AggregateCommit>(
		type: string,
		key: string,
		options: {
			minVersion?: number
			maxVersion?: number
			maxTime?: number | Date
			limit?: number
			descending?: boolean
		} = {},
	) {
		const {
			minVersion = 1,
			maxVersion = 2147483647,
			maxTime,
			descending,
			limit,
		} = options

		if (!type || !key) {
			throw new Error(`You need to specify 'type' and 'key'`)
		}

		const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "aggregate_type" = ${type}
      AND "aggregate_key" = ${key}
      AND "aggregate_version" BETWEEN ${minVersion} AND ${maxVersion}
      AND (expires_at > ${new Date()} OR expires_at IS NULL)
      ${maxTime ? sql`and "timestamp" <= ${new Date(maxTime)}` : sql``}
      ORDER BY "aggregate_version" ${descending ? sql`DESC` : sql`ASC`}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

		const client = await this.pool.connect()
		try {
			const queryStream = client.query(
				new QueryStream(query.text, query.values),
			)
			for await (const row of queryStream) {
				yield [rowToCommit(row) as TAggregateCommit]
			}
		} finally {
			client.release()
		}
	}

	public async *scanAggregateCommitsGroupedByKey<
		TAggregateCommit extends AggregateCommit,
	>(type: string) {
		if (!type) {
			throw new Error(`You need to specify 'type'`)
		}

		const query = sql`
      SELECT * FROM ${sql.ident(
				this.tableName,
			)} WHERE "aggregate_type" = ${type} AND (expires_at > ${new Date()} OR expires_at IS NULL) ORDER BY "aggregate_key", "aggregate_version"
    `

		const client = await this.pool.connect()
		try {
			const queryStream = client.query(
				new QueryStream(query.text, query.values),
			)
			for await (const row of queryStream) {
				yield [rowToCommit(row) as TAggregateCommit]
			}
		} finally {
			client.release()
		}
	}

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
			return commits[0]
		}
		return
	}

	public async *scan<TAggregateCommit extends AggregateCommit>(
		options: {
			totalSegments?: number
			segment?: number
			aggregateTypes?: string[]
			limit?: number
		} = {},
	) {
		const { totalSegments = 1, limit, aggregateTypes } = options || {}

		if (totalSegments > 1) {
			throw new Error(
				'PostgresEventStore does not currently support more than 1 segment',
			)
		}

		const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE (expires_at > ${new Date()} or expires_at IS NULL)
      ${
				aggregateTypes
					? sql`AND "aggregate_type" in (${sql.join(
							aggregateTypes.map((f) => sql`${f}`),
							', ',
					  )})`
					: sql``
			}
      ORDER BY "timestamp", "aggregate_type", "aggregate_key", "aggregate_version"
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

		const client = await this.pool.connect()
		try {
			const queryStream = client.query(
				new QueryStream(query.text, query.values),
			)
			for await (const row of queryStream) {
				yield [rowToCommit(row) as TAggregateCommit]
			}
		} finally {
			client.release()
		}
	}

	public async *chronologicalQuery<
		TAggregateCommit extends AggregateCommit,
	>(params: {
		chronologicalPartition?: string
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
			chronologicalPartition = 'default',
			min,
			descending,
			limit,
			exclusiveMin,
			exclusiveMax,
			timeDriftCompensation = 500,
			aggregateTypes,
		} = params
		const { max = new Date(Date.now() + timeDriftCompensation) } = params

		if (!min) {
			throw new Error('You must specify the "min" parameter')
		}

		const maxCursor =
			max instanceof Date ? max.toISOString().replace(/[^0-9]/g, '') : max

		const minCursor =
			min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

		const orderByDir = descending ? sql`DESC` : sql`ASC`

		const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "chronological_partition" = ${chronologicalPartition}
      ${exclusiveMin ? sql`AND "chronological_key" > ${minCursor}` : sql``}
      ${exclusiveMax ? sql`AND "chronological_key" < ${maxCursor}` : sql``}
      ${
				aggregateTypes
					? sql`AND "aggregate_type" in (${sql.join(
							aggregateTypes.map((f) => sql`${f}`),
							', ',
					  )})`
					: sql``
			}
      AND (expires_at > ${new Date()} or expires_at IS NULL)
      ORDER BY chronological_key ${orderByDir}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

		const client = await this.pool.connect()
		try {
			const queryStream = client.query(
				new QueryStream(query.text, query.values),
			)
			for await (const row of queryStream) {
				yield [rowToCommit(row) as TAggregateCommit]
			}
		} finally {
			client.release()
		}
	}

	public chronologicalKey<TAggregateType = string>(data: {
		aggregateType: TAggregateType
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

	public streamCommits<TAggregateCommit extends AggregateCommit>(params?: {
		aggregateTypes?: string[]
		chronologicalKey?: string
	}): AsyncIterable<TAggregateCommit[]>
	public streamCommits<TAggregateCommit extends AggregateCommit>(
		params: {
			aggregateTypes?: string[]
			chronologicalKey?: string
		},
		yieldEmpty: true,
	): AsyncIterable<TAggregateCommit[] | undefined>
	public streamCommits<TAggregateCommit extends AggregateCommit>(
		params: {
			aggregateTypes?: string[]
			chronologicalKey?: string
		} = {},
		yieldEmpty = false,
	): AsyncIterable<TAggregateCommit[]> {
		const { aggregateTypes } = params
		let { chronologicalKey = ' ' } = params
		const channelNames =
			Array.isArray(aggregateTypes) && aggregateTypes.length
				? aggregateTypes.map((s) =>
						createHash('sha256')
							.update([this.tableName, s].join(':'))
							.digest('hex')
							.substring(0, 48),
				  )
				: [createHash('sha256').update(this.tableName).digest('hex').substring(0, 48)]

		return {
			[Symbol.asyncIterator]: () => {
				const listener = this.getListener()
				const ac = new AbortController()
				const listenerIterator = Repeater.merge(
					channelNames.map((channelName) =>
						on(listener, channelName, { signal: ac.signal }),
					),
				)

				let query: AsyncIterator<TAggregateCommit[]> | undefined
				let waitingForNotification = false

				return {
					next: async () => {
						while (true) {
							if (!query) {
								if (waitingForNotification) {
									// Wait until we are notified of new potential results
									while (true) {
										try {
											const listenerResult = await listenerIterator.next()

											if (listenerResult.value) {
												const payload =
													listenerResult.value[listenerResult.value.length - 1]

												if (
													Array.isArray(aggregateTypes) &&
													aggregateTypes.length
												) {
													const [, , cursor] = payload.split('\t')
													if (cursor > chronologicalKey) {
														break
													}
												} else {
													if (payload > chronologicalKey) {
														break
													}
												}
											}

											if (listenerResult.done) {
												return { value: undefined, done: true }
											}
										} catch (error: any) {
											if (error.name === 'AbortError') {
												return { value: undefined, done: true }
											}
											throw error
										}
									}
								}

								waitingForNotification = false

								query = this.chronologicalQuery<TAggregateCommit>({
									min: chronologicalKey,
									exclusiveMin: true,
									aggregateTypes,
								})
							}

							const { value, done } = await query.next()
							if (value) {
								chronologicalKey = value[value.length - 1].chronologicalKey
							}

							if (done) {
								waitingForNotification = true
								query = undefined
							}

							if (value || yieldEmpty) {
								return { value, done: false }
							}
						}
					},

					async return() {
						ac.abort()
						if (typeof query !== 'undefined') await query.return?.()

						return { value: undefined, done: true }
					},

					async throw(...args: any[]) {
						ac.abort()
						await this.return?.()

						if (query?.throw) {
							return await query.throw(...args)
						}

						return { value: undefined, done: true }
					},
				}
			},
		}
	}

	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit,
	>(
		aggregateType: string,
		key: string,
		minVersion?: number,
	): AsyncIterable<TAggregateCommit[]>
	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit,
	>(
		aggregateType: string,
		key: string,
		minVersion: number,
		yieldEmpty: true,
	): AsyncIterable<TAggregateCommit[] | undefined>
	public streamAggregateInstanceCommits<
		TAggregateCommit extends AggregateCommit,
	>(
		aggregateType: string,
		key: string,
		_minVersion = 1,
		yieldEmpty = false,
	): AsyncIterable<TAggregateCommit[] | undefined> {
		let minVersion = _minVersion
		const channelName = createHash('sha256')
			.update([this.tableName, aggregateType, key].join(':'))
			.digest('hex')
			.substring(0, 48)

		return {
			[Symbol.asyncIterator]: () => {
				const listener = this.getListener()
				const ac = new AbortController()
				const listenerIterator = on(listener, channelName, {
					signal: ac.signal,
				})

				let query: AsyncIterator<TAggregateCommit[]> | undefined
				let waitingForNotification = false

				return {
					next: async () => {
						while (true) {
							if (!query) {
								if (waitingForNotification) {
									// Wait until we are notified of new potential results

									while (true) {
										try {
											const listenerResult = await listenerIterator.next()

											const [versionString] = listenerResult.value

											if (parseInt(versionString, 10) >= minVersion) {
												break
											}

											if (listenerResult.done) {
												return { value: undefined, done: true }
											}
										} catch (error: any) {
											if (error.name === 'AbortError') {
												return { value: undefined, done: true }
											}
											throw error
										}
									}
								}

								waitingForNotification = false
								query = this.queryAggregateCommits<TAggregateCommit>(
									aggregateType,
									key,
									{
										minVersion,
									},
								)
							}

							const { value, done } = await query.next()

							if (value) {
								minVersion = value[value.length - 1].aggregateVersion + 1
							}

							if (done) {
								waitingForNotification = true
								query = undefined
							}

							if (value || yieldEmpty) {
								return { value, done: false }
							}
						}
					},

					async return() {
						ac.abort()
						if (typeof query !== 'undefined') await query.return?.()

						return { value: undefined, done: true }
					},

					async throw(...args: any[]) {
						ac.abort()
						await this.return?.()

						if (query?.throw) {
							return await query.throw(...args)
						}

						return { value: undefined, done: true }
					},
				}
			},
		}
	}

	protected getListener() {
		if (!this.listener) {
			this.listener = new PostgresListener(this.pool)
		}

		return this.listener
	}
}

interface CommitRow {
	composite_id: string
	aggregate_key: string
	aggregate_version: number
	aggregate_type: string
	chronological_key: string
	events: unknown[]
	partition_key: string
	chronological_partition: string
	timestamp: Date
	expires_at?: Date | null
}

function rowToCommit({
	composite_id,
	aggregate_key,
	aggregate_version,
	aggregate_type,
	chronological_key,
	partition_key,
	chronological_partition,
	expires_at,
	timestamp,
	...rest
}: CommitRow): AggregateCommit {
	return {
		aggregateKey: aggregate_key,
		aggregateVersion: aggregate_version,
		aggregateType: aggregate_type,
		chronologicalKey: chronological_key,
		chronologicalPartition: chronological_partition,
		...(expires_at ? { expiresAt: expires_at.valueOf() } : {}),
		...(timestamp ? { timestamp: timestamp.valueOf() } : {}),
		...rest,
	} as AggregateCommit
}
