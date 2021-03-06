/* tslint:disable:max-classes-per-file */
import {Aggregate, EventWithMetadata, KeySchema, VersionConflictError} from '@ddes/core'
import {randomBytes} from 'crypto'
import {commitYielder} from 'tests/support'

function createTestAggregateClass(options?: {eventStore?: object}) {
  class TestAggregate extends Aggregate {
    public static keySchema = new KeySchema([
      {
        name: 'id',
        value: (props: {id?: string}) => props.id || randomBytes(8).toString('hex'),
      },
    ])

    public static eventStore = ((options && options.eventStore) || {}) as any

    public static stateReducer(state = {}, event: EventWithMetadata) {
      switch (event.type) {
        case 'Created':
        case 'NameChanged': {
          const {id, name} = event.properties
          return {...state, id, name}
        }
        default:
          return state
      }
    }

    public async create(props: {id: string}) {
      const {id} = props

      return await this.commit({
        type: 'Created',
        version: 1,
        properties: {id, name: 'My item'},
      })
    }
  }

  return TestAggregate
}

describe('Aggregate [unit]', () => {
  test('static create()', async () => {
    const eventStore = {
      commit: jest.fn(() => undefined),
      queryAggregateCommits: jest.fn().mockReturnValueOnce({
        commits: commitYielder([], {aggregateType: 'TestAggregate'}),
      }),
    }

    const TestAggregate = createTestAggregateClass({
      eventStore,
    })

    const aggregate = await TestAggregate.create()

    expect(aggregate.key).toBeDefined()

    expect((eventStore.commit.mock.calls as any)[0][0]).toMatchObject({
      aggregateType: 'TestAggregate',
      aggregateVersion: 1,
      events: [{properties: {name: 'My item'}, type: 'Created', version: 1}],
    })

    expect(aggregate.toJSON()).toMatchObject({
      state: {name: 'My item'},
      type: 'TestAggregate',
      version: 1,
    })

    expect(aggregate.key).toBe((eventStore.commit.mock.calls as any)[0][0].events[0].properties.id)
  })

  test('static load()', async () => {
    const TestAggregate = createTestAggregateClass({
      eventStore: {
        upcastCommits: jest.fn(commits => commits),
        queryAggregateCommits: jest.fn().mockReturnValueOnce({
          commits: commitYielder(
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
                events: [{type: 'NameChanged', properties: {name: 'Primero user'}}],
              },
            ],
            {aggregateType: 'TestAggregate'}
          ),
        }),
      },
    })

    const firstItem = await TestAggregate.load('1')

    expect(TestAggregate.eventStore.queryAggregateCommits).toHaveBeenCalledTimes(1)
    expect(TestAggregate.eventStore.queryAggregateCommits).toHaveBeenCalledWith('TestAggregate', '1', {
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
    TestAggregate.eventStore.upcastCommits = jest.fn(commits => commits)
    TestAggregate.eventStore.scanAggregateInstances = jest.fn().mockReturnValueOnce({
      commits: commitYielder(
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
      ),
    })

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

    expect(TestAggregate.eventStore.scanAggregateInstances).toHaveBeenCalledTimes(1)
    expect(TestAggregate.eventStore.scanAggregateInstances).toHaveBeenCalledWith('TestAggregate', {})
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
    const result = await aggregate.executeCommand({name: 'myCommand'}, {my: 'input'})

    expect(result).toMatchObject({my: 'input'})
  })
})
