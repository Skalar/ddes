// tslint:disable:max-classes-per-file

import {Aggregate, KeySchema} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Aggregate', {stores: true}, context => {
  test('static commit() - no keySchema', async () => {
    const {store} = context
    //
    class TestAggregate extends Aggregate {
      public static store = store
    }

    const a = TestAggregate.commit([
      {type: 'SomeEvent', properties: {myProperty: 'a'}},
    ])
    const b = TestAggregate.commit([
      {type: 'SomeEvent', properties: {myProperty: 'b'}},
    ])

    const [commitA, commitB] = await Promise.all([a, b])

    const commitsInStore = await iterableToArray(
      TestAggregate.store.queryAggregateCommits('TestAggregate', '@').commits
    )

    expect(commitsInStore).toContainEqual(commitA)
    expect(commitsInStore).toContainEqual(commitB)
  })
})

describeWithResources('Aggregate', {stores: true}, context => {
  test('static commit() - with keySchema', async () => {
    const {store} = context
    //
    class TestAggregate extends Aggregate {
      public static keySchema = new KeySchema(['id'])
      public static store = store
    }

    const a = TestAggregate.commit('myid', [
      {type: 'SomeEvent', properties: {myProperty: 'a'}},
    ])
    const b = TestAggregate.commit({id: 'myid'}, [
      {type: 'SomeEvent', properties: {myProperty: 'b'}},
    ])

    const [commitA, commitB] = await Promise.all([a, b])
    const commitsInStore = await iterableToArray(
      TestAggregate.store.queryAggregateCommits('TestAggregate', 'myid').commits
    )
    expect(commitsInStore).toContainEqual(commitA)
    expect(commitsInStore).toContainEqual(commitB)
  })
})
