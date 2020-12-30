import {SnapshotStore} from '@ddes/core'
import {Client} from 'pg'
import {sql} from 'pg-sql'

export default class PostgresSnapshotStore extends SnapshotStore {
  public tableName!: string
  public client!: Client

  constructor(config: {tableName: string; client: Client}) {
    super()
    this.tableName = config.tableName
    this.client = config.client
  }

  /**
   * Create PostgresQL table
   */
  public async setup() {
    try {
      const query = sql`
        CREATE TABLE IF NOT EXISTS ${sql.ident(this.tableName)} (
          aggregate_type	        text NOT NULL,
          aggregate_key	          text NOT NULL,
          aggregate_version	      bigint NOT NULL,
          state		                jsonb NOT NULL,
          timestamp               bigint,
          compatibility_checksum  text NOT NULL,
          PRIMARY KEY(aggregate_type, aggregate_key)
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
    const query = sql`DROP TABLE IF EXISTS ${sql.ident(this.tableName)}`

    await this.client.query(query.text, query.values)
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

    await this.client.query(query.text, query.values)
  }

  public async readSnapshot(type: string, key: string) {
    const query = sql`
      SELECT * FROM ${sql.ident(this.tableName)}
      WHERE "aggregate_type" = ${type}
      AND "aggregate_key" = ${key}
    `

    const results = await this.client.query(query.text, query.values)

    if (results.rows.length === 0) {
      return null
    }

    const snapshot = results.rows[0]

    return {
      version: parseInt(snapshot.aggregate_version, 10),
      state: snapshot.state,
      timestamp: parseInt(snapshot.timestamp, 10),
      compatibilityChecksum: snapshot.compatibility_checksum,
    }
  }
  public async deleteSnapshots() {
    const query = sql`DELETE FROM ${sql.ident(this.tableName)}`
    await this.client.query(query.text, query.values)
  }
}
