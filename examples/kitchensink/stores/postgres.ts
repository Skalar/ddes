import {PostgresEventStore} from '@ddes/postgres'
import createConnectionPool from '@databases/pg'
import {disposables} from '../support/disposables'

export const postgresConnectionPool = createConnectionPool({
  host: 'localhost',
  user: 'ddes',
  password: 'test',
  bigIntMode: 'bigint',
})

process.once('SIGTERM', () => {
  postgresConnectionPool.dispose().catch(ex => {
    console.error(ex)
  })
})

disposables.push(postgresConnectionPool)

export const postgres = new PostgresEventStore(`ddes-test`, postgresConnectionPool)
