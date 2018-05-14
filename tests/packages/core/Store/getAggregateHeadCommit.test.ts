import {Commit} from '@ddes/core'
import {describeWithResources} from 'support'

describeWithResources('Stores', {stores: true}, context => {
  test('getAggregateHeadCommit()', async () => {
    const {store} = context

    const commits = {
      a1: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
        timestamp: '2018-01-01',
      }),
      b1: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'b',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
        timestamp: '2018-01-01',
      }),
      a2: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 2,
        events: [
          {type: 'Updated', version: 1, properties: {testProperty: false}},
        ],
        timestamp: '2018-01-02',
      }),
      a3: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 3,
        events: [
          {type: 'Published', version: 1, properties: {testProperty: false}},
        ],
        timestamp: '2018-01-03',
      }),
    }

    for (const commit of Object.values(commits)) {
      await store.commit(commit)
    }

    await expect(
      store.getAggregateHeadCommit('Test', 'a')
    ).resolves.toMatchObject(commits.a3)
    await expect(
      store.getAggregateHeadCommit('Test', 'b')
    ).resolves.toMatchObject(commits.b1)
  })
})
