import {MetaStore, Store} from '@ddes/core'
import {randomBytes} from 'crypto'
import * as Resources from './resources'
import * as Stores from './stores'

enum ResourceName {
  eventStreamServer = 'eventStreamServer',
}

const storeTypes = ['aws']

export interface TestWithResourcesContext {
  store: Store
  testId: string
  [resourceName: string]: any
}

export function describeWithResources( // describe with resources
  what: string,
  opts: any,
  fn: (context: TestWithResourcesContext) => void
) {
  const {stores, ...resources} = opts || {stores: false}

  const describeBlock = (
    description: string,
    store?: Store,
    metaStore?: MetaStore
  ) => {
    const testId = randomBytes(8).toString('hex')
    const createdResources: {[resourceName: string]: any} = {}
    const teardownFunctions: Array<() => Promise<void>> = []
    const testContext = {
      store,
      metaStore,
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
              ](store, resourceOptions)
              if (teardown) {
                teardownFunctions.push(teardown)
              }
              createdResources[resourceName] = resource
            }
          ),
          ...[store ? store.setup() : Promise.resolve()],
          ...[metaStore ? metaStore.setup() : Promise.resolve()],
        ])
        Object.assign(testContext, createdResources)
      })

      afterAll(async () => {
        await Promise.all([
          ...teardownFunctions.map(teardownFn => teardownFn()),
          ...[store ? store.teardown() : Promise.resolve()],
          ...[metaStore ? metaStore.teardown() : Promise.resolve()],
        ])
      })

      fn(testContext)
    })
  }

  if (opts.stores) {
    const allStores = opts.stores === true

    for (const storeTypeName of storeTypes) {
      if (!allStores && !opts.stores.includes(storeTypeName)) {
        continue
      }

      const testId = randomBytes(8).toString('hex')

      const store = (Stores as any)[storeTypeName]({testId})
      const metaStore = (Stores as any)[storeTypeName + 'Meta']({testId})
      describeBlock(`${what} (store=${storeTypeName})`, store, metaStore)
    }
  } else {
    describeBlock(what)
  }
}
