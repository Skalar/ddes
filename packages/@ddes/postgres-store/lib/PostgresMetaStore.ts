/**
 * @module @ddes/postgres-store
 */

import {MetaStore, MetaStoreKey} from '@ddes/core'
import {Client} from 'pg'
import QueryStream from 'pg-query-stream'
import {sql} from 'pg-sql'

/**
 * Interface for MetaStore powered by PostgreSQL
 */
export default class PostgresMetaStore extends MetaStore {
  public tableName!: string
  public client!: Client

  constructor(config: {tableName: string; client: Client}) {
    super()
    this.tableName = config.tableName
    this.client = config.client
  }

  public async get(key: MetaStoreKey) {
    const query = sql`
      SELECT "data" FROM ${sql.ident(this.tableName)}
      WHERE primary_key = ${key[0]}
      AND secondary_key = ${key[1]}
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
    `

    const result = await this.client.query(query.text, query.values)

    if (result.rows.length === 0) {
      return null
    }

    return result.rows[0].data
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

    await this.client.query(query.text, query.values)
  }

  public async delete(key: MetaStoreKey) {
    const query = sql`
      DELETE FROM ${sql.ident(this.tableName)}
      WHERE primary_key = ${key[0]}
      AND secondary_key = ${key[1]}
    `

    await this.client.query(query.text, query.values)
  }

  public async *list(primaryKey: string): AsyncIterableIterator<[string, any]> {
    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "primary_key" = ${primaryKey}
      AND (expires_at > ${Date.now()} or expires_at IS NULL)
    `

    const queryStream = new QueryStream(query.text, query.values)

    for await (const row of this.client.query(queryStream)) {
      yield [row.secondary_key, row.data]
    }
  }

  public async setup() {
    try {
      const query = sql`
        CREATE TABLE IF NOT EXISTS ${sql.ident(this.tableName)} (
          primary_key		text NOT NULL,
          secondary_key	text NOT NULL,
          data		      jsonb NOT NULL,
          expires_at    bigint,
          PRIMARY KEY(primary_key, secondary_key)
        );
      `

      await this.client.query(query.text)
    } catch (error) {
      if (error.code !== '42P04') {
        throw error
      } // db exists
    }
  }

  public async teardown() {
    const query = sql`DROP TABLE IF EXISTS ${sql.ident(this.tableName)}`
    await this.client.query(query.text)
  }

  public toString(): string {
    return `PostgresMetaStore:${this.tableName}`
  }
}
