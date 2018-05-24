// tslint:disable:max-classes-per-file

import {Aggregate} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Aggregate', {stores: true}, context => {
  test('commit()', async () => {
    const {eventStore} = context
    //
    class TestAggregate extends Aggregate {
      public static eventStore = eventStore
    }
    const aggregate = new TestAggregate()

    const commitA = await aggregate.commit([
      {type: 'SomeEvent', properties: {myProperty: 'a'}},
    ])
    const commitB = await aggregate.commit([
      {type: 'OtherEvent', properties: {myProperty: 'b'}},
    ])

    expect(commitA).toHaveProperty('chronologicalGroup', 'default')
    expect(commitB).toHaveProperty('chronologicalGroup', 'default')

    const commitsInStore = await iterableToArray(
      TestAggregate.eventStore.queryAggregateCommits('TestAggregate', '@')
        .commits
    )

    expect(commitsInStore).toContainEqual(commitA)
    expect(commitsInStore).toContainEqual(commitB)
  })
})

describeWithResources('Aggregate', {stores: true}, context => {
  test('commit() - custom chronologicalGroup', async () => {
    const {eventStore} = context
    //
    class TestAggregate extends Aggregate {
      public static eventStore = eventStore
      public static chronologicalGroup = 'custom'
    }
    const aggregate = new TestAggregate()

    const commitA = await aggregate.commit([
      {type: 'SomeEvent', properties: {myProperty: 'a'}},
    ])
    const commitB = await aggregate.commit([
      {type: 'OtherEvent', properties: {myProperty: 'b'}},
    ])

    expect(commitA).toHaveProperty('chronologicalGroup', 'custom')
    expect(commitB).toHaveProperty('chronologicalGroup', 'custom')

    const commitsInStore = await iterableToArray(
      TestAggregate.eventStore.queryAggregateCommits('TestAggregate', '@')
        .commits
    )

    expect(commitsInStore).toContainEqual(commitA)
    expect(commitsInStore).toContainEqual(commitB)
  })
})
