import {EventStore, AggregateCommit, VersionConflictError, AggregateEvent} from '@ddes/core'
import {ConnectionPool, sql} from '@databases/pg'

/**
 * Interface for EventStore powered by PostgreSQL
 */
export class PostgresEventStore extends EventStore {
  constructor(protected tableName: string, protected pool: ConnectionPool) {
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
        aggregate_version   bigint NOT NULL,
        chronological_key   text NOT NULL,
        chronological_partition text NOT NULL,
        events              jsonb NOT NULL,
        timestamp           bigint NOT NULL,
        expires_at          bigint,
        PRIMARY KEY(aggregate_type, aggregate_key, aggregate_version)
      );
    `)
  }

  /**
   * Remove PostgresQL table
   */
  public async teardown() {
    await this.pool.query(sql`DROP TABLE IF EXISTS ${sql.ident(this.tableName)}`)
  }

  public async commit<TAggregateCommit extends AggregateCommit>(commit: TAggregateCommit) {
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

    try {
      const query = sql`INSERT INTO ${sql.ident(this.tableName)} VALUES(
        ${aggregateType},
        ${aggregateKey},
        ${aggregateVersion},
        ${chronologicalKey},
        ${chronologicalPartition},
        ${JSON.stringify(events)},
        ${new Date(timestamp).getTime()},
        ${expiresAt || null}
      )`

      await this.pool.query(query)
    } catch (error: any) {
      if (error.code === '23505') {
        throw new VersionConflictError(commit)
      }
      throw error
    }

    return commit
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
    } = {}
  ) {
    const {
      minVersion = 1,
      maxVersion = Number.MAX_SAFE_INTEGER,
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
      AND (expires_at > ${Date.now()} OR expires_at IS NULL)
      ${maxTime ? sql`and "timestamp" <= ${new Date(maxTime).getTime()}` : sql``}
      ORDER BY "timestamp" ${descending ? sql`DESC` : sql`ASC`}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

    const rows = this.pool.queryStream(query, {})

    for await (const row of rows) {
      yield [rowToCommit(row) as TAggregateCommit]
    }
  }

  public async *scanAggregateCommitsGroupedByKey<TAggregateCommit extends AggregateCommit>(
    type: string
  ) {
    if (!type) {
      throw new Error(`You need to specify 'type'`)
    }

    const query = sql`
      SELECT * FROM ${sql.ident(
        this.tableName
      )} WHERE "aggregate_type" = ${type} AND (expires_at > ${Date.now()} OR expires_at IS NULL) ORDER BY "aggregate_key", "aggregate_version"
    `

    for await (const row of this.pool.queryStream(query, {})) {
      yield [rowToCommit(row) as TAggregateCommit]
    }
  }

  public async getAggregateHeadCommit<TAggregateCommit extends AggregateCommit>(
    type: string,
    key: string
  ) {
    for await (const commits of this.queryAggregateCommits<TAggregateCommit>(type, key, {
      descending: true,
      limit: 1,
    })) {
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
    } = {}
  ) {
    const {totalSegments = 1, limit, aggregateTypes} = options || {}

    if (totalSegments > 1) {
      throw new Error('PostgresEventStore does not currently support more than 1 segment')
    }

    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE (expires_at > ${Date.now()} or expires_at IS NULL)
      ${
        aggregateTypes
          ? sql`AND "aggregate_type" in (${sql.join(
              aggregateTypes.map(f => sql`${f}`),
              ', '
            )})`
          : sql``
      }
      ORDER BY "timestamp", "aggregate_type", "aggregate_key", "aggregate_version"
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

    for await (const row of this.pool.queryStream(query, {})) {
      yield [rowToCommit(row) as TAggregateCommit]
    }
  }

  public async *chronologicalQuery<TAggregateCommit extends AggregateCommit>(params: {
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
    const {max = new Date(Date.now() + timeDriftCompensation)} = params

    if (!min) {
      throw new Error('You must specify the "min" parameter')
    }

    const maxDate =
      max instanceof Date
        ? max
        : new Date(max.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

    const maxCursor = max instanceof Date ? max.toISOString().replace(/[^0-9]/g, '') : max

    const minDate =
      min instanceof Date
        ? min
        : new Date(min.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

    const minCursor = min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

    const orderByDir = descending ? sql`DESC` : sql`ASC`

    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "chronological_partition" = ${chronologicalPartition}
      AND "timestamp" BETWEEN ${minDate.getTime()} AND ${maxDate.getTime()}
      ${exclusiveMin ? sql`AND "chronological_key" > ${minCursor}` : sql``}
      ${exclusiveMax ? sql`AND "chronological_key" < ${maxCursor}` : sql``}
      ${
        aggregateTypes
          ? sql`AND "aggregate_type" in (${sql.join(
              aggregateTypes.map(f => sql`${f}`),
              ', '
            )})`
          : sql``
      }
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
      ORDER BY chronological_key ${orderByDir}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `
    for await (const row of this.pool.queryStream(query, {})) {
      yield [rowToCommit(row) as TAggregateCommit]
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
}

interface CommitRow {
  composite_id: string
  aggregate_key: string
  aggregate_version: string
  aggregate_type: string
  chronological_key: string
  events: unknown[]
  partition_key: string
  chronological_partition: string
  timestamp: string
  expires_at?: string | null
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
    aggregateVersion: parseInt(aggregate_version, 10),
    aggregateType: aggregate_type,
    chronologicalKey: chronological_key,
    chronologicalPartition: chronological_partition,
    ...(expires_at ? {expiresAt: parseInt(expires_at)} : {}),
    ...(timestamp ? {timestamp: parseInt(timestamp)} : {}),
    ...rest,
  } as AggregateCommit
}
