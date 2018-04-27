import {MarshalledCommit} from '@ddes/aws-store'
import {Commit} from '@ddes/core'

import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Stores', {stores: true}, context => {
  test('*chronologicalCommits()', async () => {
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
      c1: new Commit({
        aggregateType: 'Other',
        aggregateKey: 'a',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
        timestamp: '2018-01-03 10:00',
      }),
      b1: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'b',
        aggregateVersion: 1,
        events: [
          {type: 'Created', version: 1, properties: {testProperty: true}},
        ],
        timestamp: '2018-01-02',
      }),
      a2: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 2,
        events: [
          {type: 'Updated', version: 1, properties: {testProperty: false}},
        ],
        timestamp: '2018-01-05',
      }),
      a3: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 3,
        events: [
          {type: 'Published', version: 1, properties: {testProperty: false}},
        ],
        timestamp: '2018-01-04',
      }),
    }

    for (const commit of Object.values(commits)) {
      await store.commit(commit)
    }

    await expect(
      iterableToArray(store.chronologicalCommits())
    ).resolves.toMatchObject([
      commits.a1,
      commits.b1,
      commits.c1,
      commits.a3,
      commits.a2,
    ])

    await expect(
      iterableToArray(store.chronologicalCommits({reverse: true}))
    ).resolves.toMatchObject([
      commits.a2,
      commits.a3,
      commits.c1,
      commits.b1,
      commits.a1,
    ])

    await expect(
      iterableToArray(store.chronologicalCommits({after: commits.c1.sortKey}))
    ).resolves.toMatchObject([commits.a3, commits.a2])

    await expect(
      iterableToArray(
        store.chronologicalCommits({
          before: commits.c1.sortKey,
        })
      )
    ).resolves.toMatchObject([commits.a1, commits.b1])
  })
})
