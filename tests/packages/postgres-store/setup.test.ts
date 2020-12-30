import {sql} from '@databases/pg'
import {postgres} from 'tests/support/stores'

describe('PostgresEventStore', () => {
  test.concurrent('setup() [withServices]', async () => {
    const PostgresStores = new postgres()

    const eventStore = PostgresStores.eventStore()

    await PostgresStores.setup()

    const rows = await eventStore.pool.query(sql`
      SELECT COLUMN_NAME, DATA_TYPE
      FROM information_schema.COLUMNS 
      WHERE TABLE_NAME = ${eventStore.tableName}
    `)

    expect(rows.length).toBe(8)

    await PostgresStores.teardown()
  })
})
