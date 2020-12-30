import {ConnectionPoolConfig} from '@databases/pg'
import {PostgresEventStore, PostgresMetaStore, PostgresSnapshotStore} from '@ddes/postgres-store'

import Store from './Store'

const database: string | ConnectionPoolConfig = process.env.DATABASE_URL || {
  bigIntMode: 'number',
  host: 'localhost',
  database: 'ddes',
  user: 'ddes',
  password: 'test',
  port: 5432,
}

type PgStore = PostgresEventStore | PostgresMetaStore | PostgresSnapshotStore

export default class PostgresStores extends Store<PgStore> {
  public async teardown() {
    await super.teardown()
    for (const store of this.stores) {
      await store.shutdown()
    }
  }

  public eventStore(config?: any) {
    return this.addStore(
      new PostgresEventStore({
        tableName: `ddesevent-${config?.testId || this.testId}`,
        database,
      })
    )
  }

  public metaStore(config?: any) {
    return this.addStore(
      new PostgresMetaStore({
        tableName: `ddesmeta-${config?.testId || this.testId}`,
        database,
      })
    )
  }

  public snapshotStore(config?: any) {
    return this.addStore(
      new PostgresSnapshotStore({
        tableName: `ddessnapshot-${config?.testId || this.testId}`,
        database,
      })
    )
  }
}
