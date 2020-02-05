import {Commit} from '@ddes/core'
import {describeWithResources} from 'tests/support'

describeWithResources('Stores', {stores: true}, context => {
  test('getHeadCommit()', async () => {
    const {eventStore} = context

    const commits = {
      a1: {
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
      },
      b1: {
        aggregateType: 'Test',
        aggregateKey: 'b',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
      },
      a2: {
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 2,
        events: [
          {type: 'Updated', version: 1, properties: {testProperty: false}},
        ],
      },
      a3: {
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 3,
        events: [
          {type: 'Published', version: 1, properties: {testProperty: false}},
        ],
      },
    }

    for (const commit of Object.values(commits)) {
      await eventStore.commit(new Commit(commit))
    }

    await expect(eventStore.getHeadCommit()).resolves.toMatchObject(commits.a3)
  })
})
