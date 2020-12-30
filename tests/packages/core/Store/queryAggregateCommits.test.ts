import {Commit} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'tests/support'

describeWithResources('Stores', ({eventStore}) => {
  test('*queryAggregateCommits()', async () => {
    const commits = {
      a1: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 1,
        events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
        timestamp: '2018-01-01',
      }),
      b1: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'b',
        aggregateVersion: 1,
        events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
        timestamp: '2018-01-01',
      }),
      a2: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 2,
        events: [{type: 'Updated', version: 1, properties: {testProperty: false}}],
        timestamp: '2018-01-02',
      }),
      a3: new Commit({
        aggregateType: 'Test',
        aggregateKey: 'a',
        aggregateVersion: 3,
        events: [{type: 'Published', version: 1, properties: {testProperty: false}}],
        timestamp: '2018-01-03',
      }),
    }

    for (const commit of Object.values(commits)) {
      await eventStore.commit(commit)
    }

    {
      const result = await iterableToArray(eventStore.queryAggregateCommits('Test', 'a').commits)
      expect(result).toMatchObject([commits.a1, commits.a2, commits.a3])
    }

    {
      const result = await iterableToArray(eventStore.queryAggregateCommits('Test', 'b').commits)
      expect(result).toMatchObject([commits.b1])
    }

    // maxVersion
    {
      const result = await iterableToArray(
        eventStore.queryAggregateCommits('Test', 'a', {
          maxVersion: 2,
        }).commits
      )
      expect(result).toMatchObject([commits.a1, commits.a2])
    }

    // minVersion
    {
      const result = await iterableToArray(
        eventStore.queryAggregateCommits('Test', 'a', {
          minVersion: 2,
        }).commits
      )
      expect(result).toMatchObject([commits.a2, commits.a3])
    }

    // key + maxTime
    {
      const result = await iterableToArray(
        eventStore.queryAggregateCommits('Test', 'a', {
          maxTime: new Date('2018-01-02'),
        }).commits
      )
      expect(result).toMatchObject([commits.a1, commits.a2])
    }
  })
})
