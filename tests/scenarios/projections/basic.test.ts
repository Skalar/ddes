/* tslint:disable:max-classes-per-file */

import {
  Aggregate,
  Commit,
  EventWithMetadata,
  KeySchema,
  Projection,
  Projector,
} from '@ddes/core'
import {describeWithResources} from 'support'

function* getTestCommits() {
  yield new Commit({
    aggregateType: 'OtherAggregate',
    aggregateKey: 'key',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {myProperty: 'test'}}],
  })
  yield new Commit({
    aggregateType: 'Forum',
    aggregateKey: 'forumId1',
    aggregateVersion: 1,
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
  })
  yield new Commit({
    aggregateType: 'ForumThread',
    aggregateKey: 'forumId1.threadId1',
    aggregateVersion: 1,
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {title: 'test thread'},
      },
    ],
  })
  yield new Commit({
    aggregateType: 'ForumThread',
    aggregateKey: 'forumId2.threadId1',
    aggregateVersion: 1,
    events: [
      {
        type: 'Created',
        version: 1,
        properties: {title: 'test thread 2'},
      },
    ],
  })
}
class Forum extends Aggregate {
  public static keySchema = new KeySchema(['forumId'])
}

class ForumThread extends Aggregate {
  public static keySchema = new KeySchema(['forumId', 'threadId'])
}

describeWithResources('Projections', {stores: true}, context => {
  test('basic projections', async () => {
    const processedEventsA: EventWithMetadata[][] = []
    const processedEventsB: EventWithMetadata[][] = []

    const projectionA = new Projection({
      name: 'forums',
      metaStore: context.metaStore,

      async processEvents(events: Set<EventWithMetadata>) {
        processedEventsA.push([...events])
      },

      aggregateClasses: {Forum},
    })

    await projectionA.setup({startsAt: new Date()})

    const projectionB = new Projection({
      name: 'forumthreads',
      metaStore: context.metaStore,

      async processEvents(events: Set<EventWithMetadata>) {
        processedEventsB.push([...events])
      },

      aggregateClasses: {ForumThread},
    })

    await projectionB.setup({startsAt: new Date()})

    const projector = new Projector([projectionA, projectionB], {
      store: context.store,
    })

    projector.start()

    const testCommits = []

    for (const commit of getTestCommits()) {
      await context.store.commit(commit)
      testCommits.push(commit)
    }

    await Promise.all([
      projectionA.commitIsProcessed(testCommits[1]),
      projectionB.commitIsProcessed(testCommits[3]),
    ])

    projector.stop()

    expect(processedEventsA).toMatchObject([
      [
        {
          aggregateKey: 'forumId1',
          aggregateType: 'Forum',
          aggregateVersion: 1,
          properties: {title: 'test forum'},
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
          type: 'Updated',
          version: 1,
        },
      ],
    ])

    expect(processedEventsB).toMatchObject([
      [
        {
          aggregateKey: 'forumId1.threadId1',
          aggregateType: 'ForumThread',
          aggregateVersion: 1,
          properties: {title: 'test thread'},
          type: 'Created',
          version: 1,
        },
      ],
      [
        {
          aggregateKey: 'forumId2.threadId1',
          aggregateType: 'ForumThread',
          aggregateVersion: 1,
          properties: {title: 'test thread 2'},
          type: 'Created',
          version: 1,
        },
      ],
    ])
  })
})
