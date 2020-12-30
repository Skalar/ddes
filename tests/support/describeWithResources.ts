import {EventStore, MetaStore, SnapshotStore} from '@ddes/core'
import {randomBytes} from 'crypto'

import * as Resources from './resources'
import * as stores from './stores'
import Store from './stores/Store'

const storeTypes = ['postgres', 'aws']

export interface TestWithResourcesContext {
  store: Store
  eventStore: EventStore
  snapshotStore: SnapshotStore
  metaStore: MetaStore
  testId: string
  [resourceName: string]: any
}

export function describeWithResources(what: string, opts: any, fn: (context: TestWithResourcesContext) => void) {
  // describe with resources
  const {stores: requestedStores, ...resources} = opts || {stores: false}

  const describeBlock = (
    description: string,
    storeInstances: {
      eventStore?: EventStore
      metaStore?: MetaStore
      snapshotStore?: SnapshotStore
    } = {},
    store?: Store
  ) => {
    const testId = randomBytes(8).toString('hex')
    const createdResources: {[resourceName: string]: any} = {}
    const teardownFunctions: Array<() => Promise<void>> = []
    const testContext = {
      ...storeInstances,
      store,
      testId,
      teardownFunctions,
    } as TestWithResourcesContext

    describe(description, () => {
      beforeAll(async () => {
        if (store) {
          await store.setup()
          teardownFunctions.push(store.teardown)
        }
        await Promise.all([
          ...Object.entries(resources).map(async ([resourceName, resourceOptions]) => {
            const {teardown, resource} = await (Resources as any)[resourceName](storeInstances, resourceOptions)
            if (teardown) {
              teardownFunctions.push(teardown)
            }
            createdResources[resourceName] = resource
          }),
          ...Object.values(storeInstances).map(storeInstance =>
            storeInstance ? storeInstance.setup() : Promise.resolve()
          ),
        ])
        Object.assign(testContext, createdResources)
      })

      afterAll(async () => {
        await Promise.all([
          ...teardownFunctions.map(async teardownFn => await teardownFn()),
          ...Object.values(storeInstances).map(storeInstance =>
            storeInstance ? storeInstance.teardown() : Promise.resolve()
          ),
        ])
      })

      fn(testContext)
    })
  }

  if (requestedStores) {
    const allStores = requestedStores === true

    for (const storeTypeName of storeTypes) {
      if (!allStores && !opts.requestedStores.includes(storeTypeName)) {
        continue
      }

      const testId = randomBytes(8).toString('hex')

      const store = new (stores as any)[storeTypeName](testId) as Store
      const eventStore = store.eventStore()
      const metaStore = store.metaStore()
      const snapshotStore = store.snapshotStore()

      describeBlock(
        `${what} (store=${storeTypeName})`,
        {
          eventStore,
          metaStore,
          snapshotStore,
        },
        store
      )
    }
  } else {
    describeBlock(what)
  }
}
