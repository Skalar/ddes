import {Commit} from '@ddes/core'
import {describeWithResources, iterableToArray} from 'tests/support'

const commits = {
  testA1: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-01',
  }),
  otherA1: new Commit({
    aggregateType: 'Other',
    aggregateKey: 'a',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-03 10:00',
  }),
  testB1: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'b',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-02',
  }),
  thirdB1: new Commit({
    aggregateType: 'Third',
    aggregateKey: 'b',
    aggregateVersion: 1,
    events: [{type: 'Created', version: 1, properties: {testProperty: true}}],
    timestamp: '2018-01-02',
    chronologicalGroup: 'other',
  }),
  zebraA3: new Commit({
    aggregateType: 'ZebraAggregate',
    aggregateKey: 'a',
    aggregateVersion: 3,
    events: [{type: 'Published', version: 1, properties: {testProperty: false}}],
    timestamp: '2018-01-05',
  }),
  testA2: new Commit({
    aggregateType: 'Test',
    aggregateKey: 'a',
    aggregateVersion: 2,
    events: [{type: 'Updated', version: 1, properties: {testProperty: false}}],
    timestamp: '2018-01-05',
  }),
}

describeWithResources('eventStore.chronologicalQuery()', ({eventStore}) => {
  beforeAll(async () => {
    for (const commit of Object.values(commits)) {
      await eventStore.commit(commit)
    }
  })

  test('inclusive range in ascending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
        }).commits
      )
    ).resolves.toMatchObject([commits.testB1, commits.otherA1, commits.testA2, commits.zebraA3])
  })

  test('inclusive range in descending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.zebraA3, commits.testA2, commits.otherA1, commits.testB1])
  })

  test('exclusiveMin = true in ascending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: commits.testA2.sortKey,
          exclusiveMin: true,
          max: new Date('2018-01-06'),
        }).commits
      )
    ).resolves.toMatchObject([commits.zebraA3])
  })

  test('exclusiveMin = true in descending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: commits.testA2.sortKey,
          exclusiveMin: true,
          max: new Date('2018-01-06'),
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.zebraA3])
  })

  test('exclusiveMax = true in ascending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-03'),
          max: commits.zebraA3.sortKey,
          exclusiveMax: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.otherA1, commits.testA2])
  })

  test('exclusiveMax = true in descending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-03'),
          max: commits.zebraA3.sortKey,
          exclusiveMax: true,
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.testA2, commits.otherA1])
  })

  test('non-default partition', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          group: 'other',
          min: new Date('2018-01-02'),
          max: new Date('2018-01-06'),
        }).commits
      )
    ).resolves.toMatchObject([commits.thirdB1])
  })

  test('limit and ascending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          limit: 2,
        }).commits
      )
    ).resolves.toMatchObject([commits.testB1, commits.otherA1])
  })

  test('limit and descending order', async () => {
    await expect(
      iterableToArray(
        eventStore.chronologicalQuery({
          min: new Date('2018-01-02'),
          max: new Date('2018-01-05'),
          limit: 2,
          descending: true,
        }).commits
      )
    ).resolves.toMatchObject([commits.zebraA3, commits.testA2])
  })
})
