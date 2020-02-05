/* tslint:disable:max-classes-per-file */

import {Commit} from '@ddes/core'
import {CommitTransformation, Transformer} from '@ddes/store-transformations'
import {describeWithResources, iterableToArray} from 'tests/support'
import {aws} from 'tests/support/stores'

describeWithResources(
  'scenarios/store-transformations: commit copy-and-transformation',
  {stores: true},
  context => {
    // we need to get target store
    test(`modifying commit`, async () => {
      const {eventStore: source} = context
      const extraStore = context.store.eventStore({testId: context.testId + '-target'})
      const target = extraStore
      context.teardownFunctions.push(() => target.teardown())
      await target.setup()

      const dataForCommits = [
        {
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [
            {type: 'Created', properties: {}},
            {type: 'Woops', properties: {}},
          ],
        },
        {
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        },
      ]

      const testCommits = []

      for (const commitData of dataForCommits) {
        const commit = new Commit(commitData)
        testCommits.push(commit)
        await source.commit(commit)
      }

      const transformation = new CommitTransformation({
        name: 'test',
        source,
        target,

        async transform(commit: Commit) {
          switch (commit.aggregateType) {
            // filter out Woops events
            case 'AggregateA': {
              return [
                new Commit({
                  ...commit,
                  events: commit.events.filter(event => event.type !== 'Woops'),
                }),
              ]
            }
            // no changes
            default: {
              return [commit]
            }
          }
        },
      })

      const transformer = new Transformer(transformation)

      await transformer.execute()

      await expect(
        iterableToArray(source.scan().commits)
      ).resolves.toMatchObject(testCommits)

      await expect(
        iterableToArray(target.scan().commits)
      ).resolves.toMatchObject([
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateA',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateB',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
      ])
    })
  }
)

describeWithResources(
  'scenarios/store-transformations: commit copy-and-transformation',
  {stores: true},
  context => {
    test('deleting commit', async () => {
      const {eventStore: source} = context
      const AwsStores = new aws(context.testId + '-target')
      const target = AwsStores.eventStore({})
      context.teardownFunctions.push(() => target.teardown())
      await target.setup()

      const testCommits = [
        new Commit({
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        }),
        new Commit({
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        }),
      ]

      for (const commit of testCommits) {
        await source.commit(commit)
      }

      const transformation = new CommitTransformation({
        name: 'test',
        source,
        target,

        async transform(commit: Commit) {
          switch (commit.aggregateType) {
            // delete commit
            case 'AggregateA': {
              return []
            }

            // no changes
            default: {
              return [commit]
            }
          }
        },
      })

      const transformer = new Transformer(transformation)

      await transformer.execute()

      await expect(
        iterableToArray(source.scan().commits)
      ).resolves.toMatchObject(testCommits)

      await expect(
        iterableToArray(target.scan().commits)
      ).resolves.toMatchObject([
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateB',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
      ])
    })
  }
)

describeWithResources(
  'scenarios/store-transformations: commit copy-and-transformation',
  {stores: true},
  context => {
    test('creating new commits', async () => {
      const {eventStore: source} = context
      const AwsStores = new aws(context.testId + '-target')
      const target = AwsStores.eventStore({})
      context.teardownFunctions.push(() => target.teardown())
      await target.setup()

      const testCommits = [
        new Commit({
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        }),
        new Commit({
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        }),
      ]
      for (const commit of testCommits) {
        await source.commit(commit)
      }

      const transformation = new CommitTransformation({
        name: 'test',
        source,
        target,

        async transform(commit: Commit) {
          switch (commit.aggregateType) {
            // delete commit
            case 'AggregateA': {
              return [
                new Commit({...commit, aggregateType: 'AggregateC'}),
                new Commit({...commit, aggregateType: 'AggregateD'}),
              ]
            }

            // no changes
            default: {
              return [commit]
            }
          }
        },
      })

      const transformer = new Transformer(transformation)

      await transformer.execute()

      await expect(
        iterableToArray(target.scan().commits)
      ).resolves.toMatchObject([
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateB',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateC',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
        {
          aggregateKey: 'key',
          aggregateType: 'AggregateD',
          aggregateVersion: 1,
          events: [{properties: {}, type: 'Created', version: 1}],
        },
      ])

      await expect(
        iterableToArray(source.scan().commits)
      ).resolves.toMatchObject(testCommits)
    })
  }
)
