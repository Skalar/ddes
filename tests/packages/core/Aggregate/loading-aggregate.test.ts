import {Aggregate, Commit, EventWithMetadata, Store, utils} from '@ddes/core'
import {describeWithResources} from 'support'

class TestAggregate extends Aggregate {
  public static store = {} as Store

  public static stateReducer(state: any, event: EventWithMetadata) {
    switch (event.type) {
      case 'Created': {
        return event.properties
      }

      case 'Updated': {
        return {...state, ...event.properties}
      }
    }
  }
}

const commits = [
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2018-01-01'),
    events: [{type: 'Created', version: 1, properties: {myProperty: 'test'}}],
  }),
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    timestamp: new Date('2018-01-02'),
    events: [
      {type: 'Updated', version: 1, properties: {myProperty: 'changed'}},
    ],
  }),
]

describeWithResources('Aggregate', {stores: true}, context => {
  test('load() with no snapshot', async () => {
    TestAggregate.store = context.store

    for (const commit of commits) {
      await TestAggregate.store.commit(commit)
    }

    const instance = await TestAggregate.load({
      key: 'a',
      useSnapshots: false,
    })

    expect(instance!.toJSON()).toMatchObject({
      key: 'a',
      state: {myProperty: 'changed'},
      timestamp: utils.toTimestamp('2018-01-02T00:00:00.000Z'),
      type: 'TestAggregate',
      version: 2,
    })

    const instanceWithSnapshot = await TestAggregate.load({
      key: 'a',
      useSnapshots: true,
    })

    expect(instanceWithSnapshot!.toJSON()).toMatchObject({
      key: 'a',
      state: {myProperty: 'changed'},
      timestamp: utils.toTimestamp('2018-01-02T00:00:00.000Z'),
      type: 'TestAggregate',
      version: 2,
    })
  })
})

describeWithResources('Aggregate', {stores: true}, context => {
  test('load() with snapshot', async () => {
    TestAggregate.store = context.store

    for (const commit of commits) {
      await TestAggregate.store.commit(commit)
    }

    await TestAggregate.store.writeSnapshot('TestAggregate', 'a', {
      version: 2,
      compatibilityChecksum: TestAggregate.snapshotCompatChecksum,
      state: {
        myProperty: 'snapshotstate',
      },
      timestamp: utils.toTimestamp('2018-01-02'),
    })

    expect(
      (await TestAggregate.load({
        key: 'a',
        useSnapshots: true,
      }))!.toJSON()
    ).toMatchObject({
      key: 'a',
      state: {myProperty: 'snapshotstate'},
      timestamp: utils.toTimestamp('2018-01-02T00:00:00.000Z'),
      type: 'TestAggregate',
      version: 2,
    })

    expect(
      (await TestAggregate.load({
        key: 'a',
        useSnapshots: false,
      }))!.toJSON()
    ).toMatchObject({
      key: 'a',
      state: {myProperty: 'changed'},
      timestamp: utils.toTimestamp('2018-01-02T00:00:00.000Z'),
      type: 'TestAggregate',
      version: 2,
    })
  })
})

describeWithResources('Aggregate', {stores: true}, context => {
  test('loading via manual instantiation and hydrate()', async () => {
    TestAggregate.store = context.store

    for (const commit of commits) {
      await TestAggregate.store.commit(commit)
    }

    const aggregate = new TestAggregate('a')
    await aggregate.hydrate()

    expect(aggregate.toJSON()).toMatchObject({
      key: 'a',
      state: {myProperty: 'changed'},
      timestamp: utils.toTimestamp('2018-01-02T00:00:00.000Z'),
      type: 'TestAggregate',
      version: 2,
    })
  })
})
