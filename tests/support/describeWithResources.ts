import {EventStore, MetaStore, SnapshotStore} from '@ddes/core'

import * as stores from './stores'
import Store from './stores/Store'
import {generateTestId} from './testId'

const storeTypes: Array<'postgres' | 'aws'> = ['postgres', 'aws']

export interface TestWithResourcesContext {
  store: Store
  eventStore: EventStore
  snapshotStore: SnapshotStore
  metaStore: MetaStore
  testId: string
}

export function describeWithResources(what: string, fn: (context: TestWithResourcesContext) => void) {
  const describeBlock = (description: string, store: Store) => {
    const eventStore = store.eventStore()
    const metaStore = store.metaStore()
    const snapshotStore = store.snapshotStore()

    const testId = generateTestId()
    const testContext = {
      store,
      testId,
      eventStore,
      metaStore,
      snapshotStore,
    } as TestWithResourcesContext

    describe(description, () => {
      beforeAll(async () => {
        await store.setup()
        Object.assign(testContext, {eventStore, metaStore, snapshotStore})
      })

      afterAll(async () => {
        await store.teardown()
      })

      fn(testContext)
    })
  }

  for (const storeTypeName of storeTypes) {
    const store = new stores[storeTypeName]()
    describeBlock(`${what} (store=${storeTypeName})`, store)
  }
}
