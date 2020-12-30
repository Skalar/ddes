/**
 * @module @ddes/aws-store
 */

import {DynamoDB} from 'aws-sdk'

/**
 * @hidden
 */
export default async function createTable(
  tableSpecification: DynamoDB.CreateTableInput,
  options: {
    waitTimeout?: number
    statusCheckInterval?: number
    dynamodbClientConfiguration?: DynamoDB.ClientConfiguration
    ttl?: boolean
  } = {}
) {
  const {waitTimeout = 30000, statusCheckInterval = 1000, dynamodbClientConfiguration} = options

  const dynamodb = new DynamoDB(dynamodbClientConfiguration)

  try {
    await dynamodb.createTable(tableSpecification).promise()
  } catch (error) {
    if (error.code === 'ResourceInUseException') {
      return {tableWasCreated: false}
    }

    throw error
  }

  let timedOut = false

  const timer = setTimeout(() => {
    timedOut = true
  }, waitTimeout * 1000)

  while (!timedOut) {
    const {Table} = await dynamodb
      .describeTable({
        TableName: tableSpecification.TableName,
      })
      .promise()
    if (Table) {
      switch (Table.TableStatus) {
        case 'ACTIVE':
          clearTimeout(timer)

          if (options.ttl) {
            try {
              await dynamodb
                .updateTimeToLive({
                  TableName: tableSpecification.TableName,
                  TimeToLiveSpecification: {
                    Enabled: !!options.ttl,
                    AttributeName: 'x',
                  },
                })
                .promise()
            } catch (error) {
              // dynalite does not support ttl
              if (error.code !== 'UnknownOperationException') {
                throw error
              }
            }
          }

          return // done
        case 'CREATING':
          break
        default: {
          throw new Error('Invalid status ${TableStatus} while waiting for table to be created')
        }
      }
    }

    await new Promise(resolve => setTimeout(resolve, statusCheckInterval))
  }

  throw new Error(`Timed out while waiting for table ${tableSpecification.TableName} to become active.`)
}
