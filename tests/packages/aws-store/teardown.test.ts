import {describeWithResources} from 'support'
import {aws as AwsTestStore} from 'support/stores'

describeWithResources('AwsStore', {}, context => {
  test('teardown() [withServices]', async () => {
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
    await store.teardown()

    expect(
      store.dynamodb.describeTable({TableName: store.tableName}).promise()
    ).rejects.toMatchObject({code: 'ResourceNotFoundException'})
  })
})
