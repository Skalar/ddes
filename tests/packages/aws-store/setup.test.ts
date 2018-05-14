import {describeWithResources} from 'support'
import {aws as AwsTestStore} from 'support/stores'

describeWithResources('AwsStore', {}, context => {
  test.concurrent('setup() [withServices]', async () => {
    const store = AwsTestStore(context, {
      initialCapacity: {
        tableRead: 1,
        tableWrite: 2,
        chronologicalRead: 3,
        chronologicalWrite: 4,
        instancesRead: 5,
        instancesWrite: 6,
      },
    })

    await store.setup()

    const {Table} = await store.dynamodb
      .describeTable({TableName: store.tableName})
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

    await store.teardown()
  })
})
