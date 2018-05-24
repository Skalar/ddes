/* tslint:disable:max-classes-per-file */

import {Commit} from '@ddes/core'
import {CommitTransformation, Transformer} from '@ddes/store-transformations'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources(
  'scenarios/store-transformations: commit in-place-transformation',
  {stores: true},
  context => {
    test(`modifying commit`, async () => {
      const {eventStore} = context

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        })
      )

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [
            {type: 'Created', properties: {}},
            {type: 'Woops', properties: {}},
          ],
        })
      )

      const transformation = new CommitTransformation({
        name: 'test',
        source: eventStore,
        target: eventStore,

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
        iterableToArray(eventStore.scan().commits)
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
  'scenarios/store-transformations: commit in-place-transformation',
  {stores: true},
  context => {
    test('deleting commit', async () => {
      const {eventStore} = context

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        })
      )

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        })
      )

      const transformation = new CommitTransformation({
        name: 'test',
        source: eventStore,
        target: eventStore,

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
        iterableToArray(eventStore.scan().commits)
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
  'scenarios/store-transformations: commit in-place-transformation',
  {stores: true},
  context => {
    test('creating new commits', async () => {
      const {eventStore} = context

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateA',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        })
      )

      await eventStore.commit(
        new Commit({
          aggregateType: 'AggregateB',
          aggregateKey: 'key',
          aggregateVersion: 1,
          events: [{type: 'Created', properties: {}}],
        })
      )

      const transformation = new CommitTransformation({
        name: 'test',
        source: eventStore,
        target: eventStore,

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
        iterableToArray(eventStore.scan().commits)
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
    })
  }
)
