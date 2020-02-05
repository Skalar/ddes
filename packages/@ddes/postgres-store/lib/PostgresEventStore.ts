/**
 * @module @ddes/postgres-store
 */
import {
  EventStore,
  Commit,
  AggregateKey,
  AggregateType,
  VersionConflictError,
} from '@ddes/core'
import {PostgresEventStoreConfig} from './types'
import {Client} from 'pg'
import QueryStream from 'pg-query-stream'
import PostgresEventStoreQueryResponse from './PostgresEventStoreQueryResponse'
import PostgresEventStoreBatchMutator from './PostgresEventStoreBatchMutator'
import {sql} from 'pg-sql'

/**
 * Interface for EventStore powered by PostgresQL
 */
export default class PostgresEventStore extends EventStore {
  public tableName!: string
  public client: Client

  constructor(config: PostgresEventStoreConfig) {
    super()

    if (!config.tableName) {
      throw new Error(`'tableName' must be specified`)
    }

    if (!config.client) {
      throw new Error(`'client' must be specified`)
    }

    Object.assign(this, config)

    this.client = config.client
  }

  /**
   * Create PostgresQL table
   */
  public async setup() {
    try {
      const query = sql`
        CREATE TABLE ${sql.ident(this.tableName)} (
          aggregate_type      text NOT NULL,
          aggregate_key       text NOT NULL,
          aggregate_version   bigint NOT NULL,
          sort_key            text NOT NULL,
          chronological_group text NOT NULL,
          events              jsonb[] NOT NULL,
          timestamp           bigint NOT NULL,
          expires_at          bigint,
          PRIMARY KEY(aggregate_type, aggregate_key, aggregate_version)
        );
      `
      await this.client.query(query.text, query.values)
    } catch (error) {
      if (error.code !== '42P04') {
        throw error
      } // db exists
    }
  }

  /**
   * Remove PostgresQL table
   */
  public async teardown() {
    // const query = sql`DROP TABLE IF EXISTS ${sql.ident(this.tableName)}`
    // await this.client.query(query.text, query.values)
  }

  /**
   * Get commit count
   */
  public async bestEffortCount() {
    const query = sql`SELECT COUNT(*) FROM ${sql.ident(this.tableName)}`

    const result = await this.client.query(query.text, query.values)

    return result.rowCount
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
        ${events},
        ${new Date(timestamp).getTime()},
        ${expiresAt || null}
      )`

      await this.client.query(query.text, query.values)
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
      ${
        maxTime ? sql`and "timestamp" <= ${new Date(maxTime).getTime()}` : sql``
      }
      ORDER BY "timestamp" ${descending ? sql`DESC` : sql`ASC`}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `

    const queryStream = new QueryStream(query.text, query.values)

    const stream = this.client.query(queryStream)

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        for await (const row of stream) {
          yield row
        }
      })()
    )
  }

  /**
   * Retrieve ordered commits for each aggregate instance of [[AggregateType]]
   */
  public scanAggregateInstances(
    type: string,
    options: {instanceLimit?: number} = {}
  ) {
    const {client, tableName} = this
    if (!type) {
      throw new Error(`You need to specify 'type'`)
    }

    const query = sql`
      SELECT DISCTINCT "aggregate_type", "aggregate_key"
      FROM ${sql.ident(this.tableName)} WHERE "aggregate_type" = ${type}
    `

    const queryStream = new QueryStream(query.text, query.values)

    const stream = client.query(queryStream)

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        let instanceCount = 0
        for await (const {aggregateType, aggregateKey} of stream) {
          instanceCount++
          const instanceQuery = sql`
            SELECT * FROM ${tableName}
            WHERE "aggregate_type" = ${aggregateType}
            AND "aggregate_key" = ${aggregateKey}
            AND (expires_at > ${Date.now()} OR expires_at IS NULL)
          `

          const instanceQueryStream = new QueryStream(
            instanceQuery.text,
            instanceQuery.values
          )

          for await (const row of client.query(instanceQueryStream)) {
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
    const client = this.client
    // Fix use of startKey
    const {totalSegments = 1, limit, filterAggregateTypes} = options || {}

    if (totalSegments > 1) {
      throw new Error(
        'PostgresEventStore does not currently support more than 1 segment'
      )
    }

    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
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

    const queryStream = new QueryStream(query.text, query.values)

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        const stream = client.query(queryStream)

        for await (const row of stream) {
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

    const maxDate =
      max instanceof Date
        ? max
        : new Date(
            max.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3')
          )

    const maxSortKey =
      max instanceof Date ? max.toISOString().replace(/[^0-9]/g, '') : max

    const minDate =
      min instanceof Date
        ? min
        : new Date(
            min.toString().replace(/^(\d{4})(\d{2})(\d{2}).*/, '$1-$2-$3')
          )

    const minSortKey =
      min instanceof Date ? min.toISOString().replace(/[^0-9]/g, '') : min

    const orderByDir = descending ? 'DESC' : 'ASC'

    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
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
      ORDER BY "sort_key" ${sql.raw(orderByDir)}
      ${limit ? sql`LIMIT ${limit}` : sql``}
    `
    const queryStream = new QueryStream(query.text, query.values)

    const stream = this.client.query(queryStream)

    return new PostgresEventStoreQueryResponse(
      (async function* () {
        for await (const row of stream) {
          yield row
        }
      })()
    )
  }

  public toString(): string {
    return `PostgresEventStore:${this.tableName}`
  }
}
