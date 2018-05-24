import {describeWithResources} from 'support'
import {aws} from 'support/stores'

describeWithResources('AwsEventStore', {}, context => {
  test('teardown() [withServices]', async () => {
    const eventStore = aws.eventStore(context, {
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
    await eventStore.teardown()

    expect(
      eventStore.dynamodb
        .describeTable({TableName: eventStore.tableName})
        .promise()
    ).rejects.toMatchObject({code: 'ResourceNotFoundException'})
  })
})
