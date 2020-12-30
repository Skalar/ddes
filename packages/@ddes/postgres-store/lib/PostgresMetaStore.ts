/**
 * @module @ddes/postgres-store
 */
import createPgPool, {ConnectionPool, sql} from '@databases/pg'
import {MetaStore, MetaStoreKey} from '@ddes/core'

import {PostgresStoreConfig} from './types'

/**
 * Interface for MetaStore powered by PostgreSQL
 */
export default class PostgresMetaStore extends MetaStore {
  public tableName!: string
  public pool: ConnectionPool

  constructor(config: PostgresStoreConfig) {
    super()
    if (!config.tableName) throw new Error(`'tableName' must be specified`)
    if (!config.database) throw new Error(`'database' must be specified`)

    this.tableName = config.tableName
    this.pool = createPgPool(config.database)
  }

  public async get(key: MetaStoreKey) {
    const query = sql`
      SELECT "data" FROM ${sql.ident(this.tableName)}
      WHERE primary_key = ${key[0]}
      AND secondary_key = ${key[1]}
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
    `
    const result = await this.pool.query(query)
    return result.length === 0 ? null : result[0].data
  }

  public async put(key: MetaStoreKey, value: any, options: {expiresAt?: Date} = {}) {
    const expiresAt = options.expiresAt

    const values = [key[0], key[1], JSON.stringify(value)] as any[]

    if (expiresAt) {
      values.push(expiresAt)
    }

    const query = sql`
      INSERT INTO ${sql.ident(this.tableName)}
      VALUES(${key[0]}, ${key[1]}, ${JSON.stringify(value)}${
      expiresAt ? sql`, ${new Date(expiresAt).getTime()}` : sql``
    })
      ON CONFLICT (primary_key, secondary_key) DO UPDATE
      SET "data" = excluded."data",
      "expires_at" = excluded."expires_at"
    `

    await this.pool.query(query)
  }

  public async delete(key: MetaStoreKey) {
    await this.pool.query(sql`
      DELETE FROM ${sql.ident(this.tableName)}
      WHERE primary_key = ${key[0]}
      AND secondary_key = ${key[1]}
    `)
  }

  public async *list(primaryKey: string): AsyncIterableIterator<[string, any]> {
    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "primary_key" = ${primaryKey}
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
    `

    const rows = this.pool.queryStream(query, {})
    for await (const row of rows) {
      yield [row.secondary_key, row.data]
    }
  }

  public async setup() {
    await this.pool.query(sql`
      CREATE TABLE IF NOT EXISTS ${sql.ident(this.tableName)} (
        primary_key		text NOT NULL,
        secondary_key	text NOT NULL,
        data		      jsonb NOT NULL,
        expires_at    bigint,
        PRIMARY KEY(primary_key, secondary_key)
      );
    `)
  }

  public async teardown() {
    await this.pool.query(sql`DROP TABLE IF EXISTS ${sql.ident(this.tableName)}`)
  }

  /**
   * Shutdown postgres connection pool
   */
  public async shutdown() {
    await this.pool.dispose()
  }

  public toString(): string {
    return `PostgresMetaStore:${this.tableName}`
  }
}
