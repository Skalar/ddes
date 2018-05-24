import {EventStore, MetaStore, SnapshotStore} from '@ddes/core'
import {randomBytes} from 'crypto'
import * as Resources from './resources'
import * as stores from './stores'

enum ResourceName {
  eventStreamServer = 'eventStreamServer',
}

const storeTypes = ['aws']

export interface TestWithResourcesContext {
  eventStore: EventStore
  snapshotStore: SnapshotStore
  metaStore: MetaStore
  testId: string
  [resourceName: string]: any
}

export function describeWithResources( // describe with resources
  what: string,
  opts: any,
  fn: (context: TestWithResourcesContext) => void
) {
  const {stores: requestedStores, ...resources} = opts || {stores: false}

  const describeBlock = (
    description: string,
    storeInstances: {
      eventStore?: EventStore
      metaStore?: MetaStore
      snapshotStore?: SnapshotStore
    } = {}
  ) => {
    const testId = randomBytes(8).toString('hex')
    const createdResources: {[resourceName: string]: any} = {}
    const teardownFunctions: Array<() => Promise<void>> = []
    const testContext = {
      ...storeInstances,
      testId,
      teardownFunctions,
    } as TestWithResourcesContext

    describe(description, () => {
      beforeAll(async () => {
        await Promise.all([
          ...Object.entries(resources).map(
            async ([resourceName, resourceOptions]) => {
              const {teardown, resource} = await (Resources as any)[
                resourceName
              ](storeInstances, resourceOptions)
              if (teardown) {
                teardownFunctions.push(teardown)
              }
              createdResources[resourceName] = resource
            }
          ),
          ...Object.values(storeInstances).map(
            storeInstance =>
              storeInstance ? storeInstance.setup() : Promise.resolve()
          ),
        ])
        Object.assign(testContext, createdResources)
      })

      afterAll(async () => {
        await Promise.all([
          ...teardownFunctions.map(teardownFn => teardownFn()),
          ...Object.values(storeInstances).map(
            storeInstance =>
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

      const eventStore = (stores as any)[storeTypeName].eventStore({testId})
      const metaStore = (stores as any)[storeTypeName].metaStore({testId})
      const snapshotStore = (stores as any)[storeTypeName].snapshotStore({
        testId,
      })

      describeBlock(`${what} (store=${storeTypeName})`, {
        eventStore,
        metaStore,
        snapshotStore,
      })
    }
  } else {
    describeBlock(what)
  }
}
