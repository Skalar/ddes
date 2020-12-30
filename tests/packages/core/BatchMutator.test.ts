import {Commit, utils} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'tests/support'

const commits = [
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: utils.toTimestamp('2018-01-01'),
    events: [{type: 'Created', properties: {myProperty: 'test'}}],
  }),
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    timestamp: utils.toTimestamp('2018-01-02'),
    events: [{type: 'Updated', properties: {myProperty: 'changed'}}],
  }),
]

describeWithResources('Stores', ({eventStore}) => {
  describe('BatchMutator', () => {
    test('without target capacity', async () => {
      for (const commit of commits) {
        await eventStore.commit(commit)
      }

      const batchMutator = eventStore.createBatchMutator()

      await batchMutator.delete(commits[1])
      await batchMutator.put(
        new Commit({
          ...commits[0],
          events: [{type: 'Created', properties: {myProperty: 'test-changed'}}],
        })
      )
      await batchMutator.drained

      await expect(
        iterableToArray(eventStore.queryAggregateCommits('TestAggregate', 'a').commits)
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          events: [
            {
              properties: {myProperty: 'test-changed'},
              type: 'Created',
              version: 1,
            },
          ],
          timestamp: utils.toTimestamp('2018-01-01'),
        },
      ])
    })
  })
})

describeWithResources('Stores', ({eventStore}) => {
  describe('BatchMutator', () => {
    test('with target capacity', async () => {
      for (const commit of commits) {
        await eventStore.commit(commit)
      }

      const batchMutator = eventStore.createBatchMutator({
        capacityLimit: 5,
      })

      await batchMutator.delete(commits[1])
      await batchMutator.put(
        new Commit({
          ...commits[0],
          events: [{type: 'Created', properties: {myProperty: 'test-changed'}}],
        })
      )
      await batchMutator.drained

      await expect(
        iterableToArray(eventStore.queryAggregateCommits('TestAggregate', 'a').commits)
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          events: [
            {
              properties: {myProperty: 'test-changed'},
              type: 'Created',
              version: 1,
            },
          ],
          timestamp: utils.toTimestamp('2018-01-01'),
        },
      ])
    })
  })
})
