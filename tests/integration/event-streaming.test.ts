import {Commit, Store} from '@ddes/core'
import {EventSubscriber} from '@ddes/event-streaming'
import {describeWithResources, iterableToArray} from 'support'

const testCommits = [
  new Commit({
    aggregateType: 'OldAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2000-01-01'),
    events: [{type: 'Created', version: 1, properties: {myProperty: 'test'}}],
  }),

  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2030-01-01'),
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {myProperty: 'test', deep: {property: 4}},
      },
    ],
  }),
  new Commit({
    aggregateType: 'OtherAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2030-01-02'),
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {myProperty: 'somevalue'},
      },
    ],
  }),
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    timestamp: new Date('2030-01-03'),
    events: [
      {type: 'Updated', version: 1, properties: {myProperty: 'changed'}},
    ],
  }),
]

describeWithResources(
  'Event streaming',
  {eventStreamServer: true, stores: true},
  context => {
    test('single client with no filter', async () => {
      const {eventStreamServer} = context
      const store = context.store as Store

      const subscriptionStream = new EventSubscriber({
        wsUrl: `ws://localhost:${eventStreamServer.port}`,
        events: [{}],
      })

      const iterator = subscriptionStream[Symbol.asyncIterator]()

      for (const commit of testCommits) {
        await store.commit(commit)
      }

      await expect(
        iterableToArray(subscriptionStream, {maxWaitTime: 50})
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'test'},
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
        {
          aggregateKey: 'a',
          aggregateType: 'OtherAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'somevalue'},
          timestamp: '2030-01-02T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 2,
          properties: {myProperty: 'changed'},
          timestamp: '2030-01-03T00:00:00.000Z',
          type: 'Updated',
          version: 1,
        },
      ])

      subscriptionStream.close()
    })
  }
)

describeWithResources(
  'Event subscription streams',
  {eventStreamServer: true, stores: true},
  context => {
    test('single client with filter on aggregateType', async () => {
      const {eventStreamServer} = context
      const store = context.store as Store

      const subscriptionStream = new EventSubscriber({
        wsUrl: `ws://localhost:${eventStreamServer.port}`,
        events: [{aggregateType: 'TestAggregate'}],
      })

      const iterator = subscriptionStream[Symbol.asyncIterator]()

      for (const commit of testCommits) {
        await store.commit(commit)
      }

      await expect(
        iterableToArray(subscriptionStream, {maxWaitTime: 50})
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'test'},
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 2,
          properties: {myProperty: 'changed'},
          timestamp: '2030-01-03T00:00:00.000Z',
          type: 'Updated',
          version: 1,
        },
      ])

      subscriptionStream.close()
    })
  }
)

describeWithResources(
  'Event subscription streams',
  {eventStreamServer: true, stores: true},
  context => {
    test('deep property filter', async () => {
      const {eventStreamServer} = context
      const store = context.store as Store

      const subscriptionStream = new EventSubscriber({
        wsUrl: `ws://localhost:${eventStreamServer.port}`,
        events: [{'properties.deep.property': 4}],
      })

      const iterator = subscriptionStream[Symbol.asyncIterator]()

      for (const commit of testCommits) {
        await store.commit(commit)
      }

      await expect(
        iterableToArray(subscriptionStream, {maxWaitTime: 50})
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,

          properties: {deep: {property: 4}, myProperty: 'test'},
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
      ])

      subscriptionStream.close()
    })
  }
)

describeWithResources(
  'Event subscription streams',
  {eventStreamServer: true, stores: true},
  context => {
    test('multiple clients', async () => {
      const {eventStreamServer} = context
      const store = context.store as Store

      const subscriptionStream1 = new EventSubscriber({
        wsUrl: `ws://localhost:${eventStreamServer.port}`,
        events: [{aggregateType: 'TestAggregate'}],
      })

      const subscriptionStream2 = new EventSubscriber({
        wsUrl: `ws://localhost:${eventStreamServer.port}`,
        events: [{type: 'Created'}],
      })

      for (const commit of testCommits) {
        await store.commit(commit)
      }

      await expect(
        iterableToArray(subscriptionStream1, {maxWaitTime: 50})
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'test'},
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 2,
          properties: {myProperty: 'changed'},
          timestamp: '2030-01-03T00:00:00.000Z',
          type: 'Updated',
          version: 1,
        },
      ])

      await expect(
        iterableToArray(subscriptionStream2, {maxWaitTime: 50})
      ).resolves.toMatchObject([
        {
          aggregateKey: 'a',
          aggregateType: 'TestAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'test'},
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
        {
          aggregateKey: 'a',
          aggregateType: 'OtherAggregate',
          aggregateVersion: 1,
          properties: {myProperty: 'somevalue'},
          timestamp: '2030-01-02T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
      ])

      subscriptionStream1.close()
    })
  }
)
