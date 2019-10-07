import {
  DatastoreConfiguration,
  GcpEventStore,
  GcpMetaStore,
  GcpSnapshotStore,
} from '@ddes/gcp-store'

export function eventStore(
  opts: {testId: string},
  config: Partial<DatastoreConfiguration>
) {
  const {testId} = opts

  return new GcpEventStore({
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-main-store`,
    endpoint: 'localhost:8081',
  })
}

export function metaStore(opts: {testId: string}) {
  const {testId} = opts

  return new GcpMetaStore({
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-meta-store`,
    endpoint: 'localhost:8081',
  })
}

export function snapshotStore(opts: {testId: string}) {
  const {testId} = opts

  return new GcpSnapshotStore({
    projectId: 'ddes-test',
    tableName: `ddes-${testId}-snap-store`,
    endpoint: 'localhost:8081',
  })
}
