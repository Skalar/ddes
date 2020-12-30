/**
 * @module @ddes/postgres-store
 */
import createPgPool, {ConnectionPool, sql} from '@databases/pg'
import {AggregateKey, AggregateType, Commit, EventStore, VersionConflictError} from '@ddes/core'

import PostgresEventStoreBatchMutator from './PostgresEventStoreBatchMutator'
import PostgresEventStoreQueryResponse from './PostgresEventStoreQueryResponse'
import {PostgresStoreConfig} from './types'

/**
 * Interface for EventStore powered by PostgresQL
 */
export default class PostgresEventStore extends EventStore {
  public tableName: string
  public pool: ConnectionPool

  constructor(config: PostgresStoreConfig) {
    super()

    if (!config.tableName) throw new Error(`'tableName' must be specified`)
    if (!config.database) throw new Error(`'database' must be specified`)

    this.tableName = config.tableName
    this.pool = createPgPool(config.database)
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
        sort_key            text NOT NULL,
        chronological_group text NOT NULL,
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

  /**
   * Shutdown postgres connection pool
   */
  public async shutdown() {
    await this.pool.dispose()
  }

  /**
   * Get commit count
   */
  public async bestEffortCount() {
    const result = await this.pool.query(sql`SELECT COUNT(*) FROM ${sql.ident(this.tableName)}`)
    return result[0].count
  }

  /**
   * Insert commit into table
   * @param {Object} commit Commit
   */
  public async commit({
    aggregateType,
    aggregateKey,
    aggregateVersion,
    sortKey,
    chronologicalGroup,
    events,
    timestamp,
    expiresAt,
  }: Commit) {
    try {
      const query = sql`INSERT INTO ${sql.ident(this.tableName)} VALUES(
        ${aggregateType},
        ${aggregateKey},
        ${aggregateVersion},
        ${sortKey},
        ${chronologicalGroup},
        ${JSON.stringify(events)},
        ${new Date(timestamp).getTime()},
        ${expiresAt || null}
      )`

      await this.pool.query(query)
    } catch (error) {
      if (error.code === '23505') {
        throw new VersionConflictError(
          `${aggregateType}[${aggregateKey}] already has a version ${aggregateVersion} commit`
        )
      }
      throw error
    }
  }

  /**
   *
   * @param {string} type AggregateType
   * @param {string} key AggregateKey
   * @param {Object} options query options
   */
  public queryAggregateCommits(
    type: string,
    key: string,
    options: {
      consistentRead?: boolean
      minVersion?: number
      maxVersion?: number
      maxTime?: number | Date
      limit?: number
      descending?: boolean
    } = {}
  ): PostgresEventStoreQueryResponse {
    const {minVersion = 1, maxVersion = Number.MAX_SAFE_INTEGER, maxTime, descending, limit} = options

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

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        for await (const row of rows) {
          yield row
        }
      })()
    )
  }

  /**
   * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
   */
  public scanAggregateInstances(type: string, options: {instanceLimit?: number} = {}) {
    const {pool, tableName} = this
    if (!type) {
      throw new Error(`You need to specify 'type'`)
    }

    const query = sql`
      SELECT DISTINCT "aggregate_type", "aggregate_key"
      FROM ${sql.ident(tableName)} WHERE "aggregate_type" = ${type}
    `

    const rows = pool.queryStream(query, {})

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        let instanceCount = 0
        for await (const {aggregate_type, aggregate_key} of rows) {
          instanceCount++
          const instanceQuery = sql`
            SELECT * FROM ${sql.ident(tableName)}
            WHERE "aggregate_type" = ${aggregate_type}
            AND "aggregate_key" = ${aggregate_key}
            AND (expires_at > ${Date.now()} OR expires_at IS NULL)
          `

          const rows = pool.queryStream(instanceQuery, {})

          for await (const row of rows) {
            yield row
          }

          if (options.instanceLimit && instanceCount >= options.instanceLimit) {
            return
          }
        }
      })()
    )
  }

  /**
   * Get most recent commit for an [[Aggregate]] instance
   */
  public async getAggregateHeadCommit(type: string, key: AggregateKey) {
    for await (const commit of this.queryAggregateCommits(type, key, {
      descending: true,
      limit: 1,
    }).commits) {
      return commit
    }
    return null
  }

  /**
   * Get the most recent commit in the given chronological group
   */
  public async getHeadCommit(_?: string, startDate?: Date) {
    const min = startDate || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)

    for await (const commit of this.chronologicalQuery({
      min,
      descending: true,
    }).commits) {
      return commit
    }

    return null
  }

  public scan(
    options: {
      totalSegments?: number
      segment?: number
      filterAggregateTypes?: string[]
      startKey?: any
      limit?: number
      capacityLimit?: number
    } = {}
  ) {
    const {pool, tableName} = this

    // Fix use of startKey
    const {totalSegments = 1, limit, filterAggregateTypes} = options || {}

    if (totalSegments > 1) {
      throw new Error('PostgresEventStore does not currently support more than 1 segment')
    }

    const query = sql`
      SELECT * FROM ${sql.ident(tableName)}
      WHERE (expires_at > ${Date.now()} or expires_at IS NULL)
      ${
        filterAggregateTypes
          ? sql`AND "aggregate_type" in (${sql.join(
              filterAggregateTypes.map(f => sql`${f}`),
              ', '
            )})`
          : sql``
      }
      ORDER BY "timestamp", "aggregate_type", "aggregate_key", "aggregate_version"
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

    const rows = pool.queryStream(query, {})

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        for await (const row of rows) {
          yield row
        }
      })()
    )
  }

  public createBatchMutator(params: {capacityLimit?: number} = {}) {
    const {capacityLimit} = params
    return new PostgresEventStoreBatchMutator({store: this, capacityLimit})
  }

  /**
   * Retrieve commits from the store chronologically
   */
  public chronologicalQuery(params: {
    group?: string
    min: string | Date
    max?: string | Date
    exclusiveMin?: boolean
    exclusiveMax?: boolean
    descending?: boolean
    limit?: number
    timeDriftCompensation?: number
    filterAggregateTypes?: AggregateType[]
  }) {
    const {pool, tableName} = this

    const {
      group = 'default',
      min,
      descending,
      limit,
      exclusiveMin,
      exclusiveMax,
      timeDriftCompensation = 500,
      filterAggregateTypes,
    } = params
    const {max = new Date(Date.now() + timeDriftCompensation)} = params

    if (!min) {
      throw new Error('You must specify the "min" parameter')
    }

    const maxDate = max instanceof Date ? max : new Date(max.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

    const maxSortKey = max instanceof Date ? max.toISOString().replace(/[^0-9]/g, '') : max

    const minDate = min instanceof Date ? min : new Date(min.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3'))

    const minSortKey = min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

    const orderByDir = descending ? sql`DESC` : sql`ASC`

    const query = sql`
      SELECT * FROM ${sql.ident(tableName)}
      WHERE "chronological_group" = ${group}
      AND "timestamp" BETWEEN ${minDate.getTime()} AND ${maxDate.getTime()}
      ${exclusiveMin ? sql`AND "sort_key" > ${minSortKey}` : sql``}
      ${exclusiveMax ? sql`AND "sort_key" < ${maxSortKey}` : sql``}
      ${
        filterAggregateTypes
          ? sql`AND "aggregate_type" in (${sql.join(
              filterAggregateTypes.map(f => sql`${f}`),
              ', '
            )})`
          : sql``
      }
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
      ORDER BY sort_key ${orderByDir}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

    const rows = pool.queryStream(query, {})

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        for await (const row of rows) {
          yield row
        }
      })()
    )
  }

  public toString(): string {
    return `PostgresEventStore:${this.tableName}`
  }
}
