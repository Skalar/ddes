import {describeWithResources} from 'tests/support'
import {aws} from 'tests/support/stores'

describeWithResources('AwsEventStore', {}, context => {
  test.concurrent('setup() [withServices]', async () => {
    const AwsStores = new aws(context.testId)
    const eventStore = AwsStores.eventStore({
      initialCapacity: {
        tableRead: 1,
        tableWrite: 2,
        chronologicalRead: 3,
        chronologicalWrite: 4,
        instancesRead: 5,
        instancesWrite: 6,
      },
    })

    await eventStore.setup()

    const {Table} = await eventStore.dynamodb
      .describeTable({TableName: eventStore.tableName})
      .promise()

    expect(Table).toBeDefined()

    const {ProvisionedThroughput, GlobalSecondaryIndexes, KeySchema} = Table!

    expect(ProvisionedThroughput).toMatchObject({
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 2,
    })

    expect(GlobalSecondaryIndexes).toMatchObject([
      {
        IndexName: 'chronological',
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0,
        KeySchema: [
          {AttributeName: 'p', KeyType: 'HASH'},
          {AttributeName: 'g', KeyType: 'RANGE'},
        ],
        Projection: {
          NonKeyAttributes: ['t', 'e', 'x', 'a'],
          ProjectionType: 'INCLUDE',
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 3,
          WriteCapacityUnits: 4,
        },
      },
      {
        IndexName: 'instances',
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0,
        KeySchema: [
          {AttributeName: 'a', KeyType: 'HASH'},
          {AttributeName: 'r', KeyType: 'RANGE'},
        ],
        Projection: {ProjectionType: 'KEYS_ONLY'},
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 6,
        },
      },
    ])

    expect(KeySchema).toMatchObject([
      {AttributeName: 's', KeyType: 'HASH'},
      {AttributeName: 'v', KeyType: 'RANGE'},
    ])

    await eventStore.teardown()
  })
})
