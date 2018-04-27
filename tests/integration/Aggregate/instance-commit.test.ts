// tslint:disable:max-classes-per-file

import {
  Aggregate,
  AlreadyCommittingError,
  Commit,
  KeySchema,
  VersionConflictError,
} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

describeWithResources('Aggregate', {stores: true}, context => {
  test('commit()', async () => {
    const {store} = context
    //
    class TestAggregate extends Aggregate {
      public static store = store
    }
    const aggregate = new TestAggregate()

    const commitA = await aggregate.commit([
      {type: 'SomeEvent', properties: {myProperty: 'a'}},
    ])
    const commitB = await aggregate.commit([
      {type: 'OtherEvent', properties: {myProperty: 'b'}},
    ])

    const commitsInStore = await iterableToArray(
      TestAggregate.store.queryAggregateCommits({
        type: 'TestAggregate',
      })
    )
    expect(commitsInStore).toContainEqual(commitA)
    expect(commitsInStore).toContainEqual(commitB)
  })
})
