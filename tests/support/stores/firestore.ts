import {
  FirestoreConfig,
  FirestoreEventStore,
  FirestoreMetaStore,
  FirestoreSnapshotStore,
} from '@ddes/firestore'
import {credentials} from 'grpc'

export function eventStore(
  opts: {testId: string},
  config: Partial<FirestoreConfig>
) {
  const {testId} = opts
  return new FirestoreEventStore({
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-meta-store`,
    sslCreds: credentials.createInsecure(),
    keyFilename: `${__dirname}/fake-cert.json`,
    servicePath: '0.0.0.0',
    port: 8082,
  })
}

export function metaStore(opts: {testId: string}) {
  const {testId} = opts

  return new FirestoreMetaStore({
    'grpc.initial_reconnect_backoff_ms': 500,
    'grpc.max_reconnect_backoff_ms': 1000,
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-meta-store`,
    sslCreds: credentials.createInsecure(),
    keyFilename: `${__dirname}/fake-cert.json`,
    servicePath: '0.0.0.0',
    port: 8082,
  })
}

export function snapshotStore(opts: {testId: string}) {
  const {testId} = opts

  return new FirestoreSnapshotStore({
    'grpc.initial_reconnect_backoff_ms': 500,
    'grpc.max_reconnect_backoff_ms': 1000,
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-snap-store`,
    sslCreds: credentials.createInsecure(),
    keyFilename: `${__dirname}/fake-cert.json`,
    servicePath: '0.0.0.0',
    port: 8082,
  })
}
