/* tslint:disable:max-classes-per-file */

import {
  Aggregate,
  AggregateKeyString,
  AggregateSnapshot,
  AggregateType,
  Commit,
  Event,
  EventWithMetadata,
  KeySchema,
  Store,
  VersionConflictError,
} from '@ddes/core'

import {commitYielder} from 'support'

function createTestAggregateClass(options?: {store?: object}) {
  class TestAggregate extends Aggregate {
    public static keySchema = new KeySchema(['id'])
    public static store = ((options && options.store) || {}) as any
    public static useSnapshots = true

    public static stateReducer(state = {}, event: EventWithMetadata) {
      switch (event.type) {
        case 'Created':
        case 'NameChanged': {
          const {name} = event.properties
          return {...state, name}
        }
        default:
          return state
      }
    }

    public async create(props: object) {
      return await this.commit({
        type: 'Created',
        version: 1,
        properties: {name: 'My item'},
      })
    }
  }

  return TestAggregate
}

describe('Aggregate [unit]', () => {
  test('static create()', async () => {
    const store = {
      readSnapshot: jest.fn(async => null),
      commit: jest.fn(async => undefined),
      queryAggregateCommits: jest
        .fn()
        .mockReturnValueOnce(
          commitYielder([], {aggregateType: 'TestAggregate'})
        ),
    }

    const TestAggregate = createTestAggregateClass({
      store,
    })

    const aggregate = await TestAggregate.create({
      id: 'myid',
    })

    expect(store.commit.mock.calls[0][0]).toMatchObject({
      active: true,
      aggregateKey: 'myid',
      aggregateType: 'TestAggregate',
      aggregateVersion: 1,
      events: [{properties: {name: 'My item'}, type: 'Created', version: 1}],
    })

    expect(aggregate.toJSON()).toMatchObject({
      key: 'myid',
      state: {name: 'My item'},
      type: 'TestAggregate',
      version: 1,
    })
  })

  test('static load()', async () => {
    const TestAggregate = createTestAggregateClass({
      store: {
        readSnapshot: jest.fn().mockReturnValueOnce(Promise.resolve(null)),
        upcastCommits: jest.fn(commits => commits),
        queryAggregateCommits: jest.fn().mockReturnValueOnce(
          commitYielder(
            [
              {
                aggregateKey: '1',
                aggregateVersion: 1,
                events: [{type: 'Created', properties: {name: 'First user'}}],
              },
              {
                aggregateType: 'User',
                aggregateKey: '1',
                aggregateVersion: 2,
                events: [
                  {type: 'NameChanged', properties: {name: 'Primero user'}},
                ],
              },
            ],
            {aggregateType: 'TestAggregate'}
          )
        ),
      },
    })

    const firstItem = await TestAggregate.load('1')

    expect(TestAggregate.store.readSnapshot).toHaveBeenCalledTimes(1)
    expect(TestAggregate.store.readSnapshot).toHaveBeenCalledWith({
      key: '1',
      type: 'TestAggregate',
    })

    expect(TestAggregate.store.queryAggregateCommits).toHaveBeenCalledTimes(1)
    expect(TestAggregate.store.queryAggregateCommits).toHaveBeenCalledWith({
      type: 'TestAggregate',
      key: '1',
      minVersion: 1,
      consistentRead: true,
    })

    expect(firstItem).toBeTruthy()
    expect(firstItem).toBeInstanceOf(TestAggregate)
    expect(firstItem!.toJSON()).toMatchObject({
      type: 'TestAggregate',
      key: '1',
      version: 2,
      state: {name: 'Primero user'},
    })
  })

  test('static commit()', async () => {
    //
  })

  test('static scanInstances()', async () => {
    const TestAggregate = createTestAggregateClass()
    TestAggregate.store.upcastCommits = jest.fn(commits => commits)
    TestAggregate.store.queryAggregateCommits = jest.fn().mockReturnValueOnce(
      commitYielder(
        [
          {
            aggregateKey: '1',
            aggregateVersion: 1,
            events: [{type: 'Created', properties: {name: 'First user'}}],
          },
          {
            aggregateType: 'User',
            aggregateKey: '1',
            aggregateVersion: 2,
            events: [{type: 'Created', properties: {name: 'Primero user'}}],
          },
          {
            aggregateKey: '2',
            aggregateVersion: 1,
            events: [{type: 'Created', properties: {name: 'Second user'}}],
          },
        ],
        {aggregateType: 'TestAggregate'}
      )
    )

    const usersYielded = []

    for await (const user of TestAggregate.scanInstances()) {
      usersYielded.push(user)
    }

    expect(usersYielded.length).toBe(2)

    const [firstUser, secondUser] = usersYielded.map(u => u.toJSON())

    expect(firstUser).toMatchObject({
      type: 'TestAggregate',
      key: '1',
      version: 2,
      state: {name: 'Primero user'},
    })

    expect(secondUser).toMatchObject({
      type: 'TestAggregate',
      key: '2',
      version: 1,
      state: {name: 'Second user'},
    })

    expect(TestAggregate.store.queryAggregateCommits).toHaveBeenCalledTimes(1)
    expect(TestAggregate.store.queryAggregateCommits).toHaveBeenCalledWith({
      type: 'TestAggregate',
    })
  })

  test('executeCommand()', async () => {
    class TestAggregate extends Aggregate {
      public myCommand = jest
        .fn()
        .mockImplementationOnce(async () => {
          throw new VersionConflictError()
        })
        .mockImplementationOnce(async input => {
          return input
        })

      public async hydrate() {
        // noop
      }
    }

    const aggregate = new TestAggregate()
    const result = await aggregate.executeCommand(
      {name: 'myCommand'},
      {my: 'input'}
    )

    expect(result).toMatchObject({my: 'input'})
  })
})
