import {AwsStore} from '@ddes/aws-store'
import {
  Aggregate,
  AggregateEventUpcasters,
  Commit,
  Iso8601Timestamp,
  KeySchema,
} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

const commits = [
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2018-01-01'),
    events: [{type: 'Created', properties: {myProperty: 'test'}}],
  }),
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    timestamp: new Date('2018-01-02'),
    events: [{type: 'Updated', properties: {myProperty: 'changed'}}],
  }),
]

const upcastedCommits = [
  {
    active: true,
    aggregateKey: 'a',
    aggregateType: 'TestAggregate',
    aggregateVersion: 1,
    events: [
      {
        properties: {
          myProperty: 'test',
          wasUpcasted: true,
          wasUpcastedASecondTime: true,
        },
        type: 'Created',
        version: 3,
      },
    ],
  },
  {
    active: true,
    aggregateKey: 'a',
    aggregateType: 'TestAggregate',
    aggregateVersion: 2,
    events: [
      {properties: {myProperty: 'changed'}, type: 'Updated', version: 1},
    ],
  },
]

const upcasters: AggregateEventUpcasters = {
  TestAggregate: {
    Created: {
      1: props => ({...props, wasUpcasted: true}),
      2: props => ({...props, wasUpcastedASecondTime: true}),
    },
  },
}

describeWithResources('Stores', {stores: true}, context => {
  describe('*upcastCommits()', () => {
    test('without lazy transformation', async () => {
      const {store} = context
      store.upcasters = upcasters

      for (const commit of commits) {
        await store.commit(commit)
      }

      await expect(
        iterableToArray(
          store.upcastCommits(
            store.queryAggregateCommits({
              type: 'TestAggregate',
            })
          )
        )
      ).resolves.toMatchObject(upcastedCommits)
    })
  })
})

describeWithResources('Stores', {stores: true}, context => {
  describe('*upcastCommits()', () => {
    test('with lazy transformation', async () => {
      const {store} = context
      store.upcasters = upcasters
      store.lazyTransformation = true

      for (const commit of commits) {
        await store.commit(commit)
      }

      await iterableToArray(
        store.upcastCommits(
          store.queryAggregateCommits({
            type: 'TestAggregate',
          })
        )
      )

      await store.lazyTransformationMutator!.drained

      await expect(
        iterableToArray(
          store.queryAggregateCommits({
            type: 'TestAggregate',
          })
        )
      ).resolves.toMatchObject(upcastedCommits)
    })
  })
})
