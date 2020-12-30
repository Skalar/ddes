import createPgPool, {ConnectionPool, sql} from '@databases/pg'
import {SnapshotStore} from '@ddes/core'

import {PostgresStoreConfig} from './types'

export default class PostgresSnapshotStore extends SnapshotStore {
  public tableName!: string
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
        aggregate_type	        text NOT NULL,
        aggregate_key	          text NOT NULL,
        aggregate_version	      bigint NOT NULL,
        state		                jsonb NOT NULL,
        timestamp               bigint,
        compatibility_checksum  text NOT NULL,
        PRIMARY KEY(aggregate_type, aggregate_key)
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
   * Write an aggregate instance snapshot
   *
   * @param type e.g. 'Account'
   * @param key  e.g. '1234'
   */
  public async writeSnapshot(
    type: string,
    key: string,
    payload: {
      version: number
      state: object
      timestamp: number
      compatibilityChecksum: string
    }
  ) {
    const {version, state, timestamp, compatibilityChecksum} = payload

    const query = sql`
      INSERT INTO ${sql.ident(this.tableName)}
      VALUES(${type}, ${key}, ${version}, ${JSON.stringify(state)}, ${timestamp}, ${compatibilityChecksum})
      ON CONFLICT (aggregate_type, aggregate_key) DO UPDATE
        SET "aggregate_version" = excluded."aggregate_version",
        "state" = excluded."state",
        "timestamp" = excluded."timestamp",
        "compatibility_checksum" = excluded."compatibility_checksum"
    `

    await this.pool.query(query)
  }

  public async readSnapshot(type: string, key: string) {
    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "aggregate_type" = ${type}
      AND "aggregate_key" = ${key}
    `

    const rows = await this.pool.query(query)

    if (rows.length === 0) {
      return null
    }

    const snapshot = rows[0]

    return {
      version: parseInt(snapshot.aggregate_version, 10),
      state: snapshot.state,
      timestamp: parseInt(snapshot.timestamp, 10),
      compatibilityChecksum: snapshot.compatibility_checksum,
    }
  }

  public async deleteSnapshots() {
    await this.pool.query(sql`DELETE FROM ${sql.ident(this.tableName)}`)
  }

  public toString(): string {
    return `PostgresSnapshotStore:${this.tableName}`
  }
}
