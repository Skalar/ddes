import {MarshalledCommit} from '@ddes/aws-store'
import {Commit, utils} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Stores', {stores: true}, context => {
  test('*queryAggregateCommits()', async () => {
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

    // only type specified
    {
      const result = await iterableToArray(
        store.queryAggregateCommits({type: 'Test'})
      )
      expect(result).toMatchObject([
        commits.a1,
        commits.a2,
        commits.a3,
        commits.b1,
      ])
    }

    // key
    {
      const result = await iterableToArray(
        store.queryAggregateCommits({type: 'Test', key: 'b'})
      )
      expect(result).toMatchObject([commits.b1])
    }

    // maxVersion
    {
      const result = await iterableToArray(
        store.queryAggregateCommits({
          type: 'Test',
          key: 'a',
          maxVersion: 2,
        })
      )
      expect(result).toMatchObject([commits.a1, commits.a2])
    }

    // minVersion
    {
      const result = await iterableToArray(
        store.queryAggregateCommits({
          type: 'Test',
          key: 'a',
          minVersion: 2,
        })
      )
      expect(result).toMatchObject([commits.a2, commits.a3])
    }

    // key + maxTime
    {
      const result = await iterableToArray(
        store.queryAggregateCommits({
          type: 'Test',
          key: 'a',
          maxTime: utils.toIso8601Timestamp('2018-01-02'),
        })
      )
      expect(result).toMatchObject([commits.a1, commits.a2])
    }
  })
})
