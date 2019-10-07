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
    projectId: 'ddes-dev',
    tableName: `ddes-test`,
  })
}

export function metaStore(opts: {testId: string}) {
  const {testId} = opts

  return new FirestoreMetaStore({
    projectId: 'ddes-dev',
    tableName: `ddes-${testId}-meta-store`,
  })
}

export function snapshotStore(opts: {testId: string}) {
  const {testId} = opts

  return new FirestoreSnapshotStore({
    projectId: 'ddes-dev',
    tableName: `ddes-${testId}-snap-store`,
  })
}
