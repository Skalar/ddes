import {
  PostgresEventStore,
  PostgresMetaStore,
  PostgresSnapshotStore,
} from '@ddes/postgres-store'
import {Client} from 'pg'
import Store from './Store'

export default class PostgresStores implements Store {
  private client: Client
  
  constructor(private testId: string) {
    // this.client = new Client({host: 'localhost', port: 5432, user: 'ddes', password: 'test'})
    this.client = new Client(process.env.DATABASE_URL)
    this.client.on('error', error => {
      if (error && !(
        error.message === 'Connection terminated unexpectedly'
        || error.message === 'terminating connection due to unexpected postmaster exit')) {
        throw error
      }
    })
  }

  async setup() {
    await this.client.connect()
  }

  async teardown() {
    const client = this.client
    try {
      await client.end()
    } catch (e) {
      console.log(e)
      console.log('fail on client end')
    }
  }


  public eventStore({testId}: {testId?: string} = {}): PostgresEventStore {
    return new PostgresEventStore({
      tableName: `ddesevent${testId || this.testId}`,
      client: this.client
    })
  }

  public metaStore(): PostgresMetaStore {

    return new PostgresMetaStore({
      tableName: `ddesmeta${this.testId}`,
      client: this.client
    })
  }
  
  public snapshotStore(): PostgresSnapshotStore {
    
    return new PostgresSnapshotStore({
      tableName: `ddessnapshot${this.testId}`,
      client: this.client
    })
  }
}