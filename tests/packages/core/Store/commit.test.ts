import {Commit} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Stores', {stores: true}, context => {
  test('commit()', async () => {
    const {store} = context

    const commit = new Commit({
      aggregateType: 'Test',
      aggregateKey: 'test',
      aggregateVersion: 1,
      events: [
        {type: 'TestEvent', version: 1, properties: {testProperty: true}},
      ],
      timestamp: '2018-01-01',
      chronologicalGroup: 'default',
    })

    await store.commit(commit)
    await expect(
      iterableToArray(store.queryAggregateCommits('Test', 'test').commits)
    ).resolves.toMatchObject([commit])
  })
})
