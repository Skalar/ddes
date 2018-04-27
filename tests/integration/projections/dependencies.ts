/* tslint:disable:max-classes-per-file */

import {
  Aggregate,
  Commit,
  EventWithMetadata,
  KeySchema,
  Projection,
  Projector,
  Store,
} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

class Forum extends Aggregate {
  public static keySchema = new KeySchema(['forumId'])
}

class ForumThread extends Aggregate {
  public static keySchema = new KeySchema(['forumId', 'threadId'])
}

const testCommits = [
  new Commit({
    aggregateType: 'OtherAggregate',
    aggregateKey: 'key',
    aggregateVersion: 1,
    timestamp: new Date('2000-01-01'),
    events: [{type: 'Created', version: 1, properties: {myProperty: 'test'}}],
  }),
  new Commit({
    aggregateType: 'Forum',
    aggregateKey: 'forumId1',
    aggregateVersion: 1,
    timestamp: new Date('2030-01-01'),
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {title: 'test forum'},
      },
      {
        type: 'Updated',
        version: 1,
        properties: {title: 'newtest'},
      },
    ],
  }),
  new Commit({
    aggregateType: 'ForumThread',
    aggregateKey: 'forumId1.threadId1',
    aggregateVersion: 1,
    timestamp: new Date('2030-01-02'),
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {title: 'test thread'},
      },
    ],
  }),
  new Commit({
    aggregateType: 'ForumThread',
    aggregateKey: 'forumId2.threadId1',
    aggregateVersion: 1,
    timestamp: new Date('2030-01-03'),
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {title: 'test thread 2'},
      },
    ],
  }),
]

describeWithResources('Projections', {stores: true}, context => {
  test('projection with dependencies', async () => {
    const processedEvents: EventWithMetadata[][] = []

    const projectionA = new Projection({
      name: 'forums',
      metaStore: context.metaStore,

      dependencies: {
        ForumThread: {
          Forum: (forumThread, forum) =>
            forumThread.keyProps.forumId === forum.keyProps.forumId,
        },
      },

      async processEvents(events: Set<EventWithMetadata>) {
        processedEvents.push([...events])
      },

      aggregateClasses: {Forum, ForumThread},
    })

    const projector = new Projector([projectionA], {
      store: context.store,
    })

    for (const commit of testCommits) {
      await context.store.commit(commit)
    }

    projector.start()
    await projectionA.commitIsProcessed(testCommits[testCommits.length - 1])
    projector.stop()

    expect(processedEvents).toMatchObject([
      [
        {
          aggregateKey: 'forumId1',
          aggregateType: 'Forum',
          aggregateVersion: 1,
          properties: {title: 'test forum'},
          sortKey: '20300101000000000:Forum:forumId1',
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
      ],
      [
        {
          aggregateKey: 'forumId1',
          aggregateType: 'Forum',
          aggregateVersion: 1,
          properties: {title: 'newtest'},
          sortKey: '20300101000000000:Forum:forumId1',
          timestamp: '2030-01-01T00:00:00.000Z',
          type: 'Updated',
          version: 1,
        },
        {
          aggregateKey: 'forumId2.threadId1',
          aggregateType: 'ForumThread',
          aggregateVersion: 1,
          properties: {title: 'test thread 2'},
          sortKey: '20300103000000000:ForumThread:forumId2.threadId1',
          timestamp: '2030-01-03T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
      ],
      [
        {
          aggregateKey: 'forumId1.threadId1',
          aggregateType: 'ForumThread',
          aggregateVersion: 1,
          properties: {title: 'test thread'},
          sortKey: '20300102000000000:ForumThread:forumId1.threadId1',
          timestamp: '2030-01-02T00:00:00.000Z',
          type: 'Created',
          version: 1,
        },
      ],
    ])
  })
})
