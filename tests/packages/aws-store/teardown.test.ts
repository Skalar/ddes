import {describeWithResources} from 'support'
import {aws as AwsTestStore} from 'support/stores'

describeWithResources('AwsStore', {}, context => {
  test.concurrent('teardown() [withServices]', async () => {
    const store = AwsTestStore(context, {
      initialCapacity: {
        read: 1,
        write: 2,
        indexRead: 3,
        indexWrite: 4,
      },
    })

    await store.setup()
    await store.teardown()

    expect(
      store.dynamodb.describeTable({TableName: store.tableName}).promise()
    ).rejects.toMatchObject({code: 'ResourceNotFoundException'})
  })
})
