import {
  AggregateEventUpcasters,
  BatchMutator,
  Commit,
  upcastCommits,
} from '@ddes/core'
import {iterableToArray} from 'tests/support'

const commits = [
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 1,
    timestamp: new Date('2018-01-01'),
    events: [{type: 'Created', properties: {myProperty: 'test'}}],
  }),
  new Commit({
    aggregateType: 'TestAggregate',
    aggregateKey: 'a',
    aggregateVersion: 2,
    timestamp: new Date('2018-01-02'),
    events: [{type: 'Updated', properties: {myProperty: 'changed'}}],
  }),
]

const expectedUpcastedCommits = [
  {
    aggregateKey: 'a',
    aggregateType: 'TestAggregate',
    aggregateVersion: 1,
    events: [
      {
        properties: {
          myProperty: 'test',
          wasUpcasted: true,
          wasUpcastedASecondTime: true,
        },
        type: 'Created',
        version: 3,
      },
    ],
  },
  {
    aggregateKey: 'a',
    aggregateType: 'TestAggregate',
    aggregateVersion: 2,
    events: [
      {properties: {myProperty: 'changed'}, type: 'Updated', version: 1},
    ],
  },
]

const upcasters: AggregateEventUpcasters = {
  TestAggregate: {
    Created: {
      1: props => ({...props, wasUpcasted: true}),
      2: props => ({...props, wasUpcastedASecondTime: true}),
    },
  },
}

describe('*upcastCommits()', () => {
  test('without lazy transformation', async () => {
    await expect(
      iterableToArray(upcastCommits(commits, upcasters))
    ).resolves.toMatchObject(expectedUpcastedCommits)
  })

  test('with lazy transformation', async () => {
    let called = false

    const batchMutator = ({
      put: () => (called = true),
    } as any) as BatchMutator

    const upcasted = await iterableToArray(
      upcastCommits(commits, upcasters, {
        lazyTransformation: true,
        batchMutator,
      })
    )
    expect(called).toBeTruthy()
    expect(upcasted).toMatchObject(expectedUpcastedCommits)
  })
})
