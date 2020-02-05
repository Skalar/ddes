import {
  Aggregate,
  EventStore,
  EventWithMetadata,
  KeySchema,
} from '@ddes/core'
import {describeWithResources} from 'tests/support'

class TestAggregate extends Aggregate {
  public static eventStore = {} as EventStore
  public static keySchema = new KeySchema(['id'])

  public static stateReducer(state: any, event: EventWithMetadata) {
    switch (event.type) {
      case 'Created': {
        return event.properties
      }

      case 'Updated': {
        return {...state, ...event.properties}
      }
    }
  }

  public create(props: {id: string; name: string}) {
    const {id, name} = props

    return this.commit({type: 'Created', properties: {id, name}})
  }
}

describeWithResources('Aggregate.loadOrCreate()', {stores: true}, context => {
  beforeAll(() => {
    TestAggregate.eventStore = context.eventStore
  })

  test('when does not already exist', async () => {
    const aggregate = await TestAggregate.loadOrCreate({
      id: 'test',
      name: 'initial',
    })

    expect(aggregate.state).toMatchObject({id: 'test', name: 'initial'})
  })

  test('when already exists', async () => {
    TestAggregate.eventStore = context.eventStore

    await TestAggregate.commit('test', [
      {type: 'Created', properties: {id: 'test', name: 'initial'}},
    ])

    await TestAggregate.commit('test', [
      {type: 'Updated', properties: {id: 'test', name: 'changed'}},
    ])

    const aggregate = await TestAggregate.loadOrCreate({
      id: 'test',
      name: 'test',
    })

    expect(aggregate.state).toMatchObject({id: 'test', name: 'changed'})
  })
})
