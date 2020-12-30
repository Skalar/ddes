import {Aggregate, Commit, EventWithMetadata, KeySchema, Projection, Projector} from '@ddes/core'
import {describeWithResources} from 'tests/support'
import projectorBatchMap from 'tests/support/projectorBatchMap'

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
  }),
    yield new Commit({
      aggregateType: 'Forum',
      aggregateKey: 'forumId2',
      aggregateVersion: 1,
      events: [
        {
          type: 'Created',
          version: 1,
          properties: {title: 'other forum'},
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

describeWithResources('Projections', ({metaStore, eventStore}) => {
  test('projection with dependencies', async () => {
    const processedEvents: EventWithMetadata[][] = []

    const projectionA = new Projection({
      name: 'forums',
      metaStore,

      dependencies: {
        ForumThread: {
          Forum: (forumThread, forum) => forumThread.keyProps.forumId === forum.keyProps.forumId,
        },
      },

      async processEvents(events: Set<EventWithMetadata>) {
        processedEvents.push([...events])
      },

      aggregateClasses: {Forum, ForumThread},
    })

    await projectionA.setup({startsAt: new Date()})

    const projector = new Projector([projectionA], {
      eventStore,
    })

    projector.start()

    const testCommits = []

    for (const commit of getTestCommits()) {
      await eventStore.commit(commit)
      testCommits.push(commit)
    }

    await projectionA.commitIsProcessed(testCommits[testCommits.length - 1])
    projector.stop()

    const batchMap = projectorBatchMap(processedEvents)

    expect(batchMap['ForumThread.forumId1.threadId1.1.Created']).toBeGreaterThan(batchMap['Forum.forumId1.1.Created'])

    expect(batchMap['ForumThread.forumId1.threadId1.1.Created']).toBeGreaterThan(batchMap['Forum.forumId1.1.Updated'])

    expect(batchMap['ForumThread.forumId2.threadId1.1.Created']).toBeGreaterThan(batchMap['Forum.forumId2.1.Created'])
  })
})
