import {describeWithResources} from 'support'
import {aws as AwsTestStore} from 'support/stores'

describeWithResources('AwsStore', {}, context => {
  test.concurrent('setup() [withServices]', async () => {
    const store = AwsTestStore(context, {
      initialCapacity: {
        read: 1,
        write: 2,
        indexRead: 3,
        indexWrite: 4,
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
        IndexName: 'chronologicalCommits',
        IndexStatus: 'ACTIVE',
        ItemCount: 0,
        KeySchema: [
          {AttributeName: 'z', KeyType: 'HASH'},
          {AttributeName: 'c', KeyType: 'RANGE'},
        ],
        Projection: {ProjectionType: 'ALL'},
        ProvisionedThroughput: {
          ReadCapacityUnits: 3,
          WriteCapacityUnits: 4,
        },
      },
    ])

    expect(KeySchema).toMatchObject([
      {AttributeName: 'a', KeyType: 'HASH'},
      {AttributeName: 'k', KeyType: 'RANGE'},
    ])

    await store.teardown()
  })
})
