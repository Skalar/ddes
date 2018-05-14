import {Commit} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'support'

const commits = {
  a1: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-01',
  }),
  c1: new Commit({
    aggregateType: 'Other',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-03 10:00',
  }),
  b1: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'b',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-02',
  }),
  o1: new Commit({
    aggregateType: 'Third',
    aggregateKey: 'b',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-02',
    chronologicalGroup: 'other',
  }),
  a3: new Commit({
    aggregateType: 'ZebraAggregate',
    aggregateKey: 'a',
    aggregateVersion: 3,
    events: [
      {type: 'Published', version: 1, properties: {testProperty: false}},
    ],
    timestamp: '2018-01-05',
  }),
  a2: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'a',
    aggregateVersion: 2,
    events: [{type: 'Updated', version: 1, properties: {testProperty: false}}],
    timestamp: '2018-01-05',
  }),
}

describeWithResources('Store.chronologicalQuery()', {stores: true}, context => {
  beforeAll(async () => {
    const {store} = context

    for (const commit of Object.values(commits)) {
      await store.commit(commit)
    }
  })

  test('inclusive range in ascending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
        }).commits
      )
    ).resolves.toMatchObject([commits.b1, commits.c1, commits.a2, commits.a3])
  })

  test('inclusive range in descending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.a3, commits.a2, commits.c1, commits.b1])
  })

  test('exclusiveMin = true in ascending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: commits.a2.sortKey,
          exclusiveMin: true,
          max: new Date('2018-01-06'),
        }).commits
      )
    ).resolves.toMatchObject([commits.a3])
  })

  test('exclusiveMin = true in descending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: commits.a2.sortKey,
          exclusiveMin: true,
          max: new Date('2018-01-06'),
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.a3])
  })

  test('exclusiveMax = true in ascending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-03'),
          max: commits.a3.sortKey,
          exclusiveMax: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.c1, commits.a2])
  })

  test('exclusiveMax = true in descending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-03'),
          max: commits.a3.sortKey,
          exclusiveMax: true,
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.a2, commits.c1])
  })

  test('non-default partition', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          group: 'other',
          min: new Date('2018-01-02'),
          max: new Date('2018-01-06'),
        }).commits
      )
    ).resolves.toMatchObject([commits.o1])
  })

  test('limit and ascending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          limit: 2,
        }).commits
      )
    ).resolves.toMatchObject([commits.b1, commits.c1])
  })

  test('limit and descending order', async () => {
    const {store} = context

    await expect(
      iterableToArray(
        store.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          limit: 2,
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.a3, commits.a2])
  })
})
