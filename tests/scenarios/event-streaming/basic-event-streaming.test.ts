import {Commit} from '@ddes/core'
import {EventSubscriber} from '@ddes/event-streaming'
import {describeWithResources, iterableToArray} from 'tests/support'
import {eventStreamServer as makeEventStreamServer, EventStreamServer} from 'tests/support/resources/eventStreamServer'

function* getTestCommits() {
  yield new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {myProperty: 'test', deep: {property: 4}},
      },
    ],
  })

  yield new Commit({
    aggregateType: 'OtherAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {myProperty: 'somevalue'},
      },
    ],
  })
  yield new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    events: [{type: 'Updated', version: 1, properties: {myProperty: 'changed'}}],
  })
}

describeWithResources('scenarios/event-streaming: basic event streaming', ({eventStore}) => {
  let eventStreamServer: EventStreamServer

  beforeEach(async () => {
    await eventStore.setup()
    eventStreamServer = await makeEventStreamServer(eventStore)
  })

  afterEach(async () => {
    await eventStore.teardown()
    await eventStreamServer.teardown()
  })

  test('single client with no filter', async () => {
    const subscriptionStream = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{}],
    })

    await subscriptionStream.isReady

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(
      iterableToArray(subscriptionStream, {
        maxWaitTime: 100,
      })
    ).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'OtherAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'somevalue'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 2,
        properties: {myProperty: 'changed'},
        type: 'Updated',
        version: 1,
      },
    ])

    subscriptionStream.close()
  })

  test('single client with filter on aggregateType', async () => {
    const subscriptionStream = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{aggregateType: 'TestAggregate'}],
    })

    await subscriptionStream.isReady

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(iterableToArray(subscriptionStream, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 2,
        properties: {myProperty: 'changed'},
        type: 'Updated',
        version: 1,
      },
    ])

    subscriptionStream.close()
  })

  test('single client with multiple filter sets', async () => {
    const subscriptionStream = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{aggregateType: 'TestAggregate'}, {aggregateType: 'OtherAggregate'}],
    })

    await subscriptionStream.isReady

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(iterableToArray(subscriptionStream, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'OtherAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'somevalue'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 2,
        properties: {myProperty: 'changed'},
        type: 'Updated',
        version: 1,
      },
    ])

    subscriptionStream.close()
  })

  test('deep property filter', async () => {
    const subscriptionStream = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{'properties.deep.property': 4}],
    })

    await subscriptionStream.isReady

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(iterableToArray(subscriptionStream, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {deep: {property: 4}, myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
    ])

    subscriptionStream.close()
  })

  test('multiple clients', async () => {
    const subscriptionStream1 = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{aggregateType: 'TestAggregate'}],
    })

    const subscriptionStream2 = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{type: 'Created'}],
    })

    await subscriptionStream1.isReady
    await subscriptionStream2.isReady

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(iterableToArray(subscriptionStream1, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 2,
        properties: {myProperty: 'changed'},
        type: 'Updated',
        version: 1,
      },
    ])

    await expect(iterableToArray(subscriptionStream2, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'OtherAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'somevalue'},
        type: 'Created',
        version: 1,
      },
    ])

    subscriptionStream1.close()
  })

  test('no old commits', async () => {
    await eventStore.commit(
      new Commit({
        aggregateType: 'OldAggregate',
        aggregateKey: 'a',
        aggregateVersion: 1,
        events: [{type: 'Created', version: 1, properties: {myProperty: 'test'}}],
      })
    )

    const subscriptionStream = new EventSubscriber({
      wsUrl: `ws://localhost:${eventStreamServer.resource.port}`,
      events: [{}],
    })

    await subscriptionStream.isReady
    await new Promise(resolve => setTimeout(resolve, 10))

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
    }

    await expect(iterableToArray(subscriptionStream, {maxWaitTime: 100})).resolves.toMatchObject([
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'test'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'OtherAggregate',
        aggregateVersion: 1,
        properties: {myProperty: 'somevalue'},
        type: 'Created',
        version: 1,
      },
      {
        aggregateKey: 'a',
        aggregateType: 'TestAggregate',
        aggregateVersion: 2,
        properties: {myProperty: 'changed'},
        type: 'Updated',
        version: 1,
      },
    ])

    subscriptionStream.close()
  })
})
