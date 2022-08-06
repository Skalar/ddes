import {PostgresEventStore} from '@ddes/postgres'
import {Pool} from 'pg'
import {disposables} from '../support/disposables'

export const postgresConnectionPool = new Pool({
  host: 'localhost',
  user: 'ddes',
  password: 'test',
  application_name: 'ddes kitchensink',
})

process.once('SIGTERM', () => {
  postgresConnectionPool.end()
})

postgresConnectionPool.on('error', error => {
  console.log('shiit postgres error')
})

disposables.push({dispose: () => postgresConnectionPool.end()})

export const postgres = new PostgresEventStore(`ddes-test`, postgresConnectionPool)
