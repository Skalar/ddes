import {describeWithResources} from 'tests/support'
import {postgres} from 'tests/support/stores'

describeWithResources('AwsEventStore', {}, context => {
  test.concurrent('setup() [withServices]', async () => {
    const PostgresStores = new postgres(context.testId)

    await PostgresStores.setup()

    const eventStore = PostgresStores.eventStore()

    await eventStore.setup()

    const {rows} = await eventStore.client.query(`
      SELECT COLUMN_NAME, DATA_TYPE
      FROM information_schema.COLUMNS WHERE TABLE_NAME = '${eventStore.tableName}'
    `)

    expect(rows.length).toBe(8)

    await eventStore.teardown()
    await PostgresStores.teardown()
  })
})
