/**
 * @module @ddes/aws-store
 */

import {DynamoDB} from 'aws-sdk'

/**
 * @hidden
 */
export async function deleteTable(
  tableName: string,
  options: {
    waitTimeout?: number
    statusCheckInterval?: number
    dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
  } = {}
) {
  const {
    waitTimeout = 30000,
    statusCheckInterval = 1000,
    dynamodbClientConfiguration,
  } = options

  const dynamodb = new DynamoDB(dynamodbClientConfiguration)

  let timer

  try {
    await dynamodb.deleteTable({TableName: tableName}).promise()

    let timedOut = false

    timer = setTimeout(() => {
      timedOut = true
    }, waitTimeout * 1000)

    while (!timedOut) {
      const {Table} = await dynamodb
        .describeTable({TableName: tableName})
        .promise()

      if (Table) {
        switch (Table.TableStatus) {
          case 'DELETING':
            await new Promise(resolve =>
              setTimeout(resolve, statusCheckInterval)
            )
            continue
          default: {
            throw new Error(
              'Invalid status ${TableStatus} while waiting for table to be deleteTableed'
            )
          }
        }
      }

      return
    }

    throw new Error(
      `Timed out while waiting for table ${tableName} to be deleted.`
    )
  } catch (error) {
    if (error.code === 'ResourceNotFoundException') {
      if (timer) {
        clearTimeout(timer)
      }

      return
    }

    throw error
  }
}
